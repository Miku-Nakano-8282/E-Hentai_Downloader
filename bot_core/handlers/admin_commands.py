from pyrogram import filters

from bot_core.config import LOG_CHANNEL_ID, OWNER_ID, app, banned_users_col, sudo_users_col
from bot_core.utils.admin import (
    OWNER_ONLY_TEXT,
    add_ban,
    add_sudo,
    build_user_label,
    extract_target_user_id,
    get_user_basic_info,
    is_owner,
    remove_ban,
    remove_sudo,
    require_owner,
)
from bot_core.utils.time_format import format_time


# ==========================================
#       OWNER-ONLY USER CONTROL COMMANDS
# ==========================================

ADMIN_HELP_TEXT = """
🛡 **Owner Control Commands**

**Ban system**
• `/ban user_id reason` — Ban a user
• Reply `/ban reason` — Ban the replied user
• `/unban user_id` — Unban a user
• `/banlist` — Show banned users

**Sudo system**
• `/addsudo user_id` — Add a sudo user
• Reply `/addsudo` — Add the replied user as sudo
• `/delsudo user_id` — Remove sudo
• `/rmsudo user_id` — Same as /delsudo
• `/sudolist` — Show sudo users

**Owner-only extra controls:**
`/setlimit`, `/maintenance`, `/notice`, `/restart`, `/clearusercache`

**New-user log controls:**
`/chatid`, `/newuserchannel`

**Owner + sudo controls:**
`/broadcast`, `/broadcaststats`, `/stats`, `/ping`, `/speedtest`, `/delcache`, `/queue`, `/canceluser`, `/userinfo`, `/cacheinfo`, `/logs`, `/usage user_id`, `/topusers`

**Sudo users also bypass normal download limits.**

Only the main owner can use ban/unban/addsudo/delsudo commands.
""".strip()


def _usage_for(command):
    examples = {
        "ban": "❌ Usage: `/ban user_id reason`\nOr reply to a user's message with `/ban reason`",
        "unban": "❌ Usage: `/unban user_id`\nOr reply to a user's message with `/unban`",
        "addsudo": "❌ Usage: `/addsudo user_id`\nOr reply to a user's message with `/addsudo`",
        "delsudo": "❌ Usage: `/delsudo user_id`\nOr reply to a user's message with `/delsudo`",
    }
    return examples.get(command, "❌ Invalid command usage.")


async def _send_log(client, text):
    try:
        await client.send_message(LOG_CHANNEL_ID, text)
    except Exception:
        pass


@app.on_message(filters.command(["adminhelp", "ownerhelp", "sudohelp"]) & (filters.private | filters.group))
async def admin_help_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)
    await message.reply_text(ADMIN_HELP_TEXT)


@app.on_message(filters.command("ban") & (filters.private | filters.group))
async def ban_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    target_user_id, reason = extract_target_user_id(message)
    if not target_user_id:
        return await message.reply_text(_usage_for("ban"))

    if is_owner(target_user_id):
        return await message.reply_text("❌ You cannot ban the main owner.")

    info = await add_ban(client, target_user_id, message.from_user.id, reason)
    label = build_user_label(
        target_user_id,
        username=info.get("username"),
        first_name=info.get("first_name"),
    )

    text = (
        f"✅ **User banned successfully.**\n\n"
        f"👤 **User:** {label}\n"
        f"📝 **Reason:** {reason or 'No reason provided'}"
    )
    await message.reply_text(text)

    await _send_log(
        client,
        f"🚫 **User Banned**\n\n👤 **User:** {label}\n👑 **By:** `{message.from_user.id}`\n📝 **Reason:** {reason or 'No reason provided'}"
    )


@app.on_message(filters.command("unban") & (filters.private | filters.group))
async def unban_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    target_user_id, _ = extract_target_user_id(message)
    if not target_user_id:
        return await message.reply_text(_usage_for("unban"))

    result = await remove_ban(target_user_id)
    info = await get_user_basic_info(client, target_user_id)
    label = build_user_label(
        target_user_id,
        username=info.get("username"),
        first_name=info.get("first_name"),
    )

    if result.deleted_count > 0:
        await message.reply_text(f"✅ **User unbanned successfully.**\n\n👤 **User:** {label}")
        await _send_log(client, f"✅ **User Unbanned**\n\n👤 **User:** {label}\n👑 **By:** `{message.from_user.id}`")
    else:
        await message.reply_text(f"⚠️ This user was not banned.\n\n👤 **User:** {label}")


@app.on_message(filters.command("banlist") & (filters.private | filters.group))
async def banlist_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    banned = await banned_users_col.find().sort("banned_at", -1).to_list(length=50)
    total = await banned_users_col.count_documents({})

    if not banned:
        return await message.reply_text("✅ **Ban list is empty.**")

    lines = [f"🚫 **Banned Users** (`{total}` total)\n"]
    for i, user in enumerate(banned, start=1):
        user_id = user.get("user_id")
        label = build_user_label(user_id, username=user.get("username"), first_name=user.get("first_name"))
        reason = user.get("reason", "No reason provided")
        banned_at = user.get("banned_at")
        when = format_time(__import__("time").time() - banned_at) + " ago" if banned_at else "Unknown"
        lines.append(f"{i}. {label}\n   📝 {reason}\n   ⏱ {when}")

    if total > len(banned):
        lines.append(f"\nShowing latest {len(banned)} only.")

    await message.reply_text("\n".join(lines))


@app.on_message(filters.command("addsudo") & (filters.private | filters.group))
async def addsudo_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    target_user_id, _ = extract_target_user_id(message)
    if not target_user_id:
        return await message.reply_text(_usage_for("addsudo"))

    if is_owner(target_user_id):
        return await message.reply_text("👑 The main owner already has full access.")

    info = await add_sudo(client, target_user_id, message.from_user.id)
    label = build_user_label(
        target_user_id,
        username=info.get("username"),
        first_name=info.get("first_name"),
    )

    await message.reply_text(f"✅ **Sudo user added successfully.**\n\n👤 **User:** {label}")
    await _send_log(client, f"🛡 **Sudo Added**\n\n👤 **User:** {label}\n👑 **By:** `{message.from_user.id}`")


@app.on_message(filters.command(["delsudo", "rmsudo"]) & (filters.private | filters.group))
async def delsudo_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    target_user_id, _ = extract_target_user_id(message)
    if not target_user_id:
        return await message.reply_text(_usage_for("delsudo"))

    if is_owner(target_user_id):
        return await message.reply_text("❌ You cannot remove the main owner's access.")

    result = await remove_sudo(target_user_id)
    info = await get_user_basic_info(client, target_user_id)
    label = build_user_label(
        target_user_id,
        username=info.get("username"),
        first_name=info.get("first_name"),
    )

    if result.deleted_count > 0:
        await message.reply_text(f"✅ **Sudo access removed successfully.**\n\n👤 **User:** {label}")
        await _send_log(client, f"🛡 **Sudo Removed**\n\n👤 **User:** {label}\n👑 **By:** `{message.from_user.id}`")
    else:
        await message.reply_text(f"⚠️ This user was not in sudo list.\n\n👤 **User:** {label}")


@app.on_message(filters.command("sudolist") & (filters.private | filters.group))
async def sudolist_command(client, message):
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    sudo_users = await sudo_users_col.find().sort("added_at", -1).to_list(length=50)
    total = await sudo_users_col.count_documents({})

    lines = ["🛡 **Sudo Users**\n", f"1. 👑 Main Owner - `{OWNER_ID}`"]

    if sudo_users:
        for i, user in enumerate(sudo_users, start=2):
            user_id = user.get("user_id")
            label = build_user_label(user_id, username=user.get("username"), first_name=user.get("first_name"))
            lines.append(f"{i}. {label}")
    else:
        lines.append("\nNo extra sudo users added yet.")

    if total > len(sudo_users):
        lines.append(f"\nShowing latest {len(sudo_users)} extra sudo users only.")

    await message.reply_text("\n".join(lines))
