import asyncio
import time

from pyrogram.errors import FloodWait, RPCError

from bot_core.config import TELEGRAM_BROADCAST_DELAY, TELEGRAM_MAX_FLOOD_WAIT, TELEGRAM_SEND_DELAY, TELEGRAM_SEND_RETRIES


_last_send_at = 0.0
_send_lock = asyncio.Lock()


async def safe_edit_text(message, text, **kwargs):
    try:
        await message.edit_text(text, **kwargs)
        return True
    except FloodWait as e:
        wait_for = min(int(getattr(e, "value", 1) or 1), TELEGRAM_MAX_FLOOD_WAIT)
        await asyncio.sleep(wait_for)
        try:
            await message.edit_text(text, **kwargs)
            return True
        except Exception:
            return False
    except Exception:
        return False


async def _respect_send_delay():
    global _last_send_at
    if TELEGRAM_SEND_DELAY <= 0:
        return

    async with _send_lock:
        now = time.time()
        wait_for = (_last_send_at + TELEGRAM_SEND_DELAY) - now
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _last_send_at = time.time()


async def safe_send_document(client, chat_id, document, **kwargs):
    """Send a document with FloodWait-aware retry.

    This does not bypass Telegram limits. It waits when Telegram asks the bot to
    wait, so long uploads do not crash the whole process.
    """
    last_error = None
    for attempt in range(TELEGRAM_SEND_RETRIES + 1):
        await _respect_send_delay()
        try:
            return await client.send_document(chat_id=chat_id, document=document, **kwargs)
        except FloodWait as e:
            wait_for = min(int(getattr(e, "value", 1) or 1), TELEGRAM_MAX_FLOOD_WAIT)
            await asyncio.sleep(wait_for + 1)
            last_error = e
        except RPCError as e:
            last_error = e
            if attempt >= TELEGRAM_SEND_RETRIES:
                raise
            await asyncio.sleep(1 + attempt)
        except Exception as e:
            last_error = e
            if attempt >= TELEGRAM_SEND_RETRIES:
                raise
            await asyncio.sleep(1 + attempt)
    raise last_error


async def safe_send_message(client, chat_id, text, **kwargs):
    last_error = None
    for attempt in range(TELEGRAM_SEND_RETRIES + 1):
        await _respect_send_delay()
        try:
            return await client.send_message(chat_id=chat_id, text=text, **kwargs)
        except FloodWait as e:
            wait_for = min(int(getattr(e, "value", 1) or 1), TELEGRAM_MAX_FLOOD_WAIT)
            await asyncio.sleep(wait_for + 1)
            last_error = e
        except RPCError as e:
            last_error = e
            if attempt >= TELEGRAM_SEND_RETRIES:
                raise
            await asyncio.sleep(1 + attempt)
        except Exception as e:
            last_error = e
            if attempt >= TELEGRAM_SEND_RETRIES:
                raise
            await asyncio.sleep(1 + attempt)
    raise last_error


async def safe_copy_message(message, chat_id, **kwargs):
    last_error = None
    for attempt in range(TELEGRAM_SEND_RETRIES + 1):
        if TELEGRAM_BROADCAST_DELAY > 0:
            await asyncio.sleep(TELEGRAM_BROADCAST_DELAY)
        try:
            return await message.copy(chat_id, **kwargs)
        except FloodWait as e:
            wait_for = min(int(getattr(e, "value", 1) or 1), TELEGRAM_MAX_FLOOD_WAIT)
            await asyncio.sleep(wait_for + 1)
            last_error = e
        except RPCError as e:
            last_error = e
            if attempt >= TELEGRAM_SEND_RETRIES:
                raise
            await asyncio.sleep(1 + attempt)
        except Exception as e:
            last_error = e
            if attempt >= TELEGRAM_SEND_RETRIES:
                raise
            await asyncio.sleep(1 + attempt)
    raise last_error
