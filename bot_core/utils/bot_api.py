"""Telegram Bot API helpers.

Pyrogram/MTProto can sometimes raise ``Peer id invalid`` for a channel right
after restart because the local peer cache is empty. The Bot API accepts the
raw channel ID directly, so these helpers are used for channel-style posting.
"""

import asyncio
import os
from typing import Any, Dict, Optional

import aiohttp

from bot_core.config import BOT_TOKEN


class TelegramBotAPIError(RuntimeError):
    pass


def _stringify_data(data: Optional[Dict[str, Any]]) -> Dict[str, str]:
    clean = {}
    for key, value in (data or {}).items():
        if value is None:
            continue
        if isinstance(value, bool):
            clean[key] = "true" if value else "false"
        else:
            clean[key] = str(value)
    return clean


def trim_text(text: str, limit: int) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 2)].rstrip() + "…"


async def bot_api_request(
    method: str,
    data: Optional[Dict[str, Any]] = None,
    files: Optional[Dict[str, str]] = None,
    timeout: int = 90,
    retries: int = 2,
):
    """Call Telegram Bot API with small retries.

    This avoids Pyrogram's peer cache entirely for channel posting. It also
    handles short Telegram/network hiccups after a container restart.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    request_timeout = aiohttp.ClientTimeout(total=timeout)
    last_error = None

    for attempt in range(max(1, retries + 1)):
        try:
            async with aiohttp.ClientSession(timeout=request_timeout) as session:
                if files:
                    form = aiohttp.FormData()
                    for key, value in _stringify_data(data).items():
                        form.add_field(key, value)

                    opened_files = []
                    try:
                        for field_name, file_path in files.items():
                            file_obj = open(file_path, "rb")
                            opened_files.append(file_obj)
                            form.add_field(
                                field_name,
                                file_obj,
                                filename=os.path.basename(file_path),
                                content_type="application/octet-stream",
                            )

                        async with session.post(url, data=form) as response:
                            payload = await response.json(content_type=None)
                    finally:
                        for file_obj in opened_files:
                            try:
                                file_obj.close()
                            except Exception:
                                pass
                else:
                    async with session.post(url, data=_stringify_data(data)) as response:
                        payload = await response.json(content_type=None)

            if payload.get("ok"):
                return payload.get("result")

            description = payload.get("description") or str(payload)
            parameters = payload.get("parameters") or {}
            retry_after = int(parameters.get("retry_after") or 0)
            last_error = TelegramBotAPIError(description)

            # Retry only for temporary conditions. Bad chat IDs/admin issues should fail fast.
            temporary = (
                "Too Many Requests" in description
                or "retry after" in description.lower()
                or "Bad Gateway" in description
                or "Internal Server Error" in description
                or "timeout" in description.lower()
            )
            if attempt < retries and temporary:
                await asyncio.sleep(max(1, retry_after or (attempt + 1) * 2))
                continue

            raise last_error

        except TelegramBotAPIError:
            raise
        except Exception as e:
            last_error = e
            if attempt < retries:
                await asyncio.sleep((attempt + 1) * 2)
                continue
            raise TelegramBotAPIError(str(last_error))

    raise TelegramBotAPIError(str(last_error or "Unknown Bot API error"))


async def get_chat_via_bot_api(chat_id, timeout: int = 30):
    return await bot_api_request("getChat", data={"chat_id": chat_id}, timeout=timeout, retries=1)


async def send_photo_via_bot_api(chat_id, photo, caption: str = "", parse_mode: Optional[str] = None):
    data = {
        "chat_id": chat_id,
        "caption": trim_text(caption or "", 1024),
    }
    if parse_mode:
        data["parse_mode"] = parse_mode

    if isinstance(photo, str) and os.path.exists(photo):
        return await bot_api_request("sendPhoto", data=data, files={"photo": photo})

    data["photo"] = photo
    return await bot_api_request("sendPhoto", data=data)


async def send_message_via_bot_api(chat_id, text: str, parse_mode: Optional[str] = None, disable_web_page_preview: bool = False):
    data = {
        "chat_id": chat_id,
        "text": trim_text(text or "", 4096),
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        data["parse_mode"] = parse_mode

    return await bot_api_request("sendMessage", data=data)
