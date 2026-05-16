import time

from pyrogram import filters

from bot_core.config import app, user_states, users_col
from bot_core.utils.admin import deny_if_banned, is_privileged_user
from bot_core.utils.bot_api import send_message_via_bot_api, send_photo_via_bot_api
from bot_core.utils.control import get_notice_text, is_maintenance_enabled
from bot_core.utils.user_logs import get_new_user_channel_id


# ==========================================
#             GENERAL USER COMMANDS
# ==========================================

@app.on_message(filters.command("start") & (filters.private | filters.group))
async def start_command(client, message):
    if await deny_if_banned(message):
        return

    user = message.from_user

    if await is_maintenance_enabled() and not await is_privileged_user(user.id):
        return await message.reply_text(
            "🛠 **Bot is under maintenance.**\n\n"
            "Please try again later."
        )

    notice_text = await get_notice_text()
    notice_block = f"\n\n📢 **Notice:** {notice_text}" if notice_text else ""

    welcome_text = (
        "👋 **Welcome to the Gallery Downloader!**\n\n"
        "Send me a supported gallery link, and I will extract, convert, and upload every page for you perfectly intact.\n\n"
        "*(Note: You can only process one link at a time to prevent server overload.)*"
        f"{notice_block}"
    )
    await message.reply_text(welcome_text)

    try:
        existing_user = await users_col.find_one({"user_id": user.id})
        if not existing_user:
            await users_col.insert_one({
                "user_id": user.id,
                "username": user.username,
                "first_name": user.first_name,
                "joined_date": time.time()
            })

            try:
                full_user = await client.get_chat(user.id)
                bio = full_user.bio if full_user.bio else "No bio provided"
            except Exception:
                bio = "Hidden / Not available"

            name = f"{user.first_name} {user.last_name or ''}".strip()
            log_caption = (
                f"👤 **New User Registered**\n\n"
                f"**Name:** {name}\n"
                f"**Username:** @{user.username if user.username else 'N/A'}\n"
                f"**ID:** `{user.id}`\n"
                f"**Bio:** {bio}"
            )

            new_user_channel_id = await get_new_user_channel_id()

            if user.photo:
                try:
                    await send_photo_via_bot_api(new_user_channel_id, user.photo.big_file_id, caption=log_caption)
                except Exception:
                    await send_message_via_bot_api(new_user_channel_id, log_caption)
            else:
                await send_message_via_bot_api(new_user_channel_id, log_caption)

    except Exception as e:
        print(f"Database error during /start: {e}")


@app.on_message(filters.command("help") & (filters.private | filters.group))
async def help_command(client, message):
    if await deny_if_banned(message):
        return

    if await is_maintenance_enabled() and not await is_privileged_user(message.from_user.id):
        return await message.reply_text(
            "🛠 **Bot is under maintenance.**\n\n"
            "Please try again later."
        )

    help_text = (
        "📖 **How to use this bot:**\n\n"
        "1. Send any valid E-Hentai gallery link.\n"
        "2. The bot will ask you which pages you want.\n"
        "3. Reply with your desired range, or `0` for all pages.\n\n"
        "🎯 **Range Examples:**\n"
        "• `0` (Downloads every page)\n"
        "• `1-10` (Downloads pages 1 through 10)\n"
        "• `!8` (Downloads everything EXCEPT page 8)\n"
        "• `12, 14-20` (Downloads page 12, and pages 14 to 20)\n"
        "• `30-40/2` (Downloads every 2nd page from 30 to 40)\n\n"
        "⚠️ **Rules:**\n"
        "• You can only process 1 gallery at a time.\n"
        "• Massive files may take a few minutes to process.\n"
        "• Send /cancel anytime to abort an action."
    )
    await message.reply_text(help_text)


@app.on_message(filters.command("cancel") & (filters.private | filters.group))
async def cancel_command(client, message):
    if await deny_if_banned(message):
        return

    user_id = message.from_user.id

    state_key = f"{message.chat.id}:{user_id}"

    if state_key in user_states:
        del user_states[state_key]
        return await message.reply_text("🚫 **Action cancelled.** You can send a new link now.")

    if user_id in user_states:
        del user_states[user_id]
        return await message.reply_text("🚫 **Action cancelled.** You can send a new link now.")

    await message.reply_text("You don't have any pending actions to cancel.")
