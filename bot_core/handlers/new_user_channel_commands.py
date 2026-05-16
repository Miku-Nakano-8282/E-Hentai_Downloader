from pyrogram import filters

from bot_core.config import app
from bot_core.utils.admin import OWNER_ONLY_TEXT, require_owner
from bot_core.utils.control import log_activity
from bot_core.utils.user_logs import (
    chat_type_name,
    get_new_user_channel_id,
    parse_chat_id_from_message,
    set_new_user_channel_id,
)


# ==========================================
#       NEW USER LOG CHANNEL COMMANDS
# ==========================================

@app.on_message(filters.command("chatid"))
async def chatid_command(client, message):
    # This is intentionally open. It only shows the ID of the chat where the
    # command was used, and it helps avoid wrong channel IDs.
    chat = message.chat
    text = (
        "🆔 **Chat ID**\n\n"
        f"**ID:** `{chat.id}`\n"
        f"**Type:** `{chat_type_name(chat)}`\n"
        f"**Title:** `{getattr(chat, 'title', None) or getattr(chat, 'first_name', None) or 'N/A'}`\n\n"
        "Use this with `/aichannel chat_id` or `/newuserchannel chat_id`."
    )
    await message.reply_text(text)


@app.on_message(filters.command("newuserchannel"))
async def newuserchannel_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    chat_id = parse_chat_id_from_message(message)
    if not chat_id:
        current = await get_new_user_channel_id()
        return await message.reply_text(
            f"👤 **Current New User Channel:** `{current}`\n\n"
            "Usage:\n"
            "`/newuserchannel -1001234567890`\n\n"
            "The bot must be admin in that channel. New /start user logs will be sent there."
        )

    await set_new_user_channel_id(chat_id, message.from_user.id)
    await log_activity("newuserchannel", f"New user channel set to {chat_id}", user_id=message.from_user.id)
    await message.reply_text(
        "✅ **New user channel updated.**\n\n"
        f"👤 Channel ID: `{chat_id}`\n\n"
        "New user registration info will now be sent there."
    )
