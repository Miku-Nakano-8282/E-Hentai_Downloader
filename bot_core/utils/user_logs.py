import time

from pyrogram.enums import ChatType

from bot_core.config import LOG_CHANNEL_ID, NEW_USER_CHANNEL_ID
from bot_core.utils.control import get_setting_value, set_setting_value


def _int_or_none(value):
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        return None


def chat_type_name(message_or_chat):
    chat = getattr(message_or_chat, "chat", message_or_chat)
    value = getattr(chat, "type", None)
    if value == ChatType.PRIVATE:
        return "private"
    if value == ChatType.GROUP:
        return "group"
    if value == ChatType.SUPERGROUP:
        return "supergroup"
    if value == ChatType.CHANNEL:
        return "channel"
    return str(value or "unknown")


async def get_new_user_channel_id():
    configured = _int_or_none(await get_setting_value("new_user_channel_id", None))
    if configured:
        return configured
    return int(NEW_USER_CHANNEL_ID or LOG_CHANNEL_ID)


async def set_new_user_channel_id(channel_id, by_user_id):
    channel_id = int(channel_id)
    await set_setting_value("new_user_channel_id", channel_id)
    await set_setting_value("new_user_channel_by", int(by_user_id))
    await set_setting_value("new_user_channel_updated_at", time.time())
    return channel_id


def parse_chat_id_from_message(message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) >= 2:
        raw = parts[1].strip().split()[0]
        parsed = _int_or_none(raw)
        if parsed:
            return parsed

    chat = getattr(message, "chat", None)
    if chat:
        return int(chat.id)

    return None
