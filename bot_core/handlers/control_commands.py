import asyncio
import time

from pyrogram import filters

from bot_core.config import (
    OWNER_ID,
    START_TIME,
    active_downloads,
    active_jobs,
    app,
    banned_users_col,
    galleries_col,
    settings_col,
    sudo_users_col,
    usage_col,
    user_limits_col,
    users_col,
)
from bot_core.utils.admin import (
    OWNER_ONLY_TEXT,
    build_user_label,
    deny_if_banned,
    extract_target_user_id,
    get_user_basic_info,
    is_owner,
    is_privileged_user,
    require_owner,
    require_privileged,
)
from bot_core.utils.control import (
    build_queue_text,
    cancel_jobs_for_user,
    clear_user_cache,
    format_date,
    get_cache_stats,
    get_global_user_limit,
    get_notice_text,
    get_recent_logs,
    get_settings,
    get_top_users,
    get_user_download_limit,
    get_user_usage,
    is_maintenance_enabled,
    log_activity,
    restart_process_soon,
    set_global_user_limit,
    set_maintenance,
    set_notice_text,
    set_user_download_limit,
)
from bot_core.utils.time_format import format_time


# ==========================================
#       EXTRA CONTROL / UTILITY COMMANDS
# ==========================================


def _parse_positive_int(value):
    try:
        parsed = int(value)
        if parsed <= 0:
            return None
        return parsed
    except Exception:
        return None


async def _target_or_reply(message):
    target_user_id, _ = extract_target_user_id(message)
    return target_user_id


@app.on_message(filters.command("queue") & (filters.private | filters.group))
async def queue_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    await message.reply_text(build_queue_text(), disable_web_page_preview=True)


@app.on_message(filters.command("canceluser") & (filters.private | filters.group))
async def canceluser_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    target_user_id = await _target_or_reply(message)
    if not target_user_id:
        return await message.reply_text(
            "❌ Usage: `/canceluser user_id`\n"
            "Or reply to a user's message with `/canceluser`"
        )

    count = await cancel_jobs_for_user(target_user_id)
    if count <= 0:
        return await message.reply_text(f"⚠️ No active download found for `{target_user_id}`.")

    await log_activity("canceluser", f"Cancelled {count} active job(s) for user {target_user_id}", user_id=target_user_id)
    await message.reply_text(f"🚫 **Cancel request sent.**\n\n👤 User: `{target_user_id}`\n📥 Jobs: `{count}`")


@app.on_message(filters.command("setlimit") & (filters.private | filters.group))
async def setlimit_command(client, message):
    if await deny_if_banned(message):
        return
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    parts = (message.text or "").split()

    if len(parts) == 2:
        limit = _parse_positive_int(parts[1])
        if not limit:
            return await message.reply_text("❌ Usage: `/setlimit 2` or `/setlimit user_id 2`")

        limit = await set_global_user_limit(limit, message.from_user.id)
        await log_activity("setlimit", f"Global download limit set to {limit}", user_id=message.from_user.id)
        return await message.reply_text(f"✅ **Global user limit updated.**\n\nNormal users can now run `{limit}` download(s) at a time.")

    if len(parts) >= 3:
        target_user_id = _parse_positive_int(parts[1])
        limit = _parse_positive_int(parts[2])
        if not target_user_id or not limit:
            return await message.reply_text("❌ Usage: `/setlimit 2` or `/setlimit user_id 2`")

        limit = await set_user_download_limit(target_user_id, limit, message.from_user.id)
        await log_activity("setlimit", f"Download limit for user {target_user_id} set to {limit}", user_id=target_user_id)
        return await message.reply_text(
            f"✅ **User limit updated.**\n\n"
            f"👤 User: `{target_user_id}`\n"
            f"📥 Limit: `{limit}` active download(s)"
        )

    global_limit = await get_global_user_limit()
    return await message.reply_text(
        f"📥 **Current global limit:** `{global_limit}`\n\n"
        "Usage:\n"
        "• `/setlimit 2` — set global normal-user limit\n"
        "• `/setlimit user_id 2` — set a specific user's limit"
    )


@app.on_message(filters.command("userinfo") & (filters.private | filters.group))
async def userinfo_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    target_user_id = await _target_or_reply(message)
    if not target_user_id:
        return await message.reply_text(
            "❌ Usage: `/userinfo user_id`\n"
            "Or reply to a user's message with `/userinfo`"
        )

    tg_info = await get_user_basic_info(client, target_user_id)
    user_doc = await users_col.find_one({"user_id": int(target_user_id)}) or {}
    usage = await get_user_usage(target_user_id) or {}
    banned = await banned_users_col.find_one({"user_id": int(target_user_id)})
    sudo = await sudo_users_col.find_one({"user_id": int(target_user_id)})
    specific_limit = await user_limits_col.find_one({"user_id": int(target_user_id)})
    active_count = active_downloads.get(int(target_user_id), 0)
    effective_limit = await get_user_download_limit(target_user_id)

    first_name = tg_info.get("first_name") or user_doc.get("first_name")
    username = tg_info.get("username") or user_doc.get("username")
    label = build_user_label(target_user_id, username=username, first_name=first_name)

    status_parts = []
    if is_owner(target_user_id):
        status_parts.append("Owner")
    if sudo:
        status_parts.append("Sudo")
    if banned:
        status_parts.append("Banned")
    if not status_parts:
        status_parts.append("Normal")

    text = (
        f"👤 **User Info**\n\n"
        f"**User:** {label}\n"
        f"**Status:** `{', '.join(status_parts)}`\n"
        f"**Joined:** `{format_date(user_doc.get('joined_date'))}`\n"
        f"**Active Downloads:** `{active_count}`\n"
        f"**Download Limit:** `{effective_limit}`"
    )

    if specific_limit:
        text += " `(custom)`"

    text += (
        f"\n\n📊 **Usage**\n"
        f"**Total Downloads:** `{usage.get('total_downloads', 0)}`\n"
        f"**Fresh Downloads:** `{usage.get('fresh_downloads', 0)}`\n"
        f"**Cached Deliveries:** `{usage.get('cached_deliveries', 0)}`\n"
        f"**Total Pages:** `{usage.get('total_pages', 0)}`\n"
        f"**Last Download:** `{format_date(usage.get('last_download_at'))}`"
    )

    if banned:
        text += f"\n\n🚫 **Ban Reason:** `{banned.get('reason', 'No reason provided')}`"

    await message.reply_text(text)


@app.on_message(filters.command("cacheinfo") & (filters.private | filters.group))
async def cacheinfo_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    stats = await get_cache_stats()
    text = (
        f"🗂 **Cache Info**\n\n"
        f"**Cached Gallery Entries:** `{stats['total_galleries']}`\n"
        f"**Estimated Cached Files:** `{stats['total_files']}`\n"
        f"**Entries Linked To Users:** `{stats['with_requested_by']}`\n\n"
        "ℹ️ User-linked cache cleanup works for cache entries created after this update."
    )
    await message.reply_text(text)


@app.on_message(filters.command("clearusercache") & (filters.private | filters.group))
async def clearusercache_command(client, message):
    if await deny_if_banned(message):
        return
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    target_user_id = await _target_or_reply(message)
    if not target_user_id:
        return await message.reply_text(
            "❌ Usage: `/clearusercache user_id`\n"
            "Or reply to a user's message with `/clearusercache`"
        )

    result = await clear_user_cache(target_user_id)
    await log_activity("clearusercache", f"Cleared {result.deleted_count} cache entries for user {target_user_id}", user_id=target_user_id)
    await message.reply_text(
        f"✅ **User cache cleanup complete.**\n\n"
        f"👤 User: `{target_user_id}`\n"
        f"🗑 Deleted cache entries: `{result.deleted_count}`"
    )


@app.on_message(filters.command("logs") & (filters.private | filters.group))
async def logs_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    parts = (message.text or "").split()
    limit = 10
    if len(parts) >= 2:
        parsed = _parse_positive_int(parts[1])
        if parsed:
            limit = min(parsed, 25)

    logs = await get_recent_logs(limit)
    if not logs:
        return await message.reply_text("📜 **No activity logs yet.**")

    lines = [f"📜 **Recent Activity Logs** (`{len(logs)}`)\n"]
    for i, item in enumerate(logs, start=1):
        lines.append(
            f"**{i}.** `{item.get('action', 'unknown')}`\n"
            f"   👤 User: `{item.get('user_id') or 'N/A'}`\n"
            f"   🕒 {format_date(item.get('created_at'))}\n"
            f"   🧾 {item.get('text', '')[:180]}"
        )

    await message.reply_text("\n\n".join(lines), disable_web_page_preview=True)


@app.on_message(filters.command("maintenance") & (filters.private | filters.group))
async def maintenance_command(client, message):
    if await deny_if_banned(message):
        return
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    parts = (message.text or "").split(maxsplit=1)
    current = await is_maintenance_enabled()

    if len(parts) < 2:
        return await message.reply_text(
            f"🛠 **Maintenance Mode:** `{'ON' if current else 'OFF'}`\n\n"
            "Usage: `/maintenance on` or `/maintenance off`"
        )

    arg = parts[1].strip().lower()
    if arg not in {"on", "off"}:
        return await message.reply_text("❌ Usage: `/maintenance on` or `/maintenance off`")

    enabled = arg == "on"
    await set_maintenance(enabled, message.from_user.id)
    await log_activity("maintenance", f"Maintenance mode set to {'ON' if enabled else 'OFF'}", user_id=message.from_user.id)
    await message.reply_text(f"✅ **Maintenance Mode:** `{'ON' if enabled else 'OFF'}`")


@app.on_message(filters.command("notice") & (filters.private | filters.group))
async def notice_command(client, message):
    if await deny_if_banned(message):
        return
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    parts = (message.text or "").split(maxsplit=1)

    if len(parts) < 2:
        notice = await get_notice_text()
        if not notice:
            return await message.reply_text("📢 **Notice:** `OFF`\n\nUsage: `/notice your notice text` or `/notice off`")
        return await message.reply_text(f"📢 **Current Notice:**\n\n{notice}")

    notice = parts[1].strip()
    if notice.lower() in {"off", "clear", "none"}:
        await set_notice_text("", message.from_user.id)
        await log_activity("notice", "Notice cleared", user_id=message.from_user.id)
        return await message.reply_text("✅ **Notice cleared.**")

    await set_notice_text(notice, message.from_user.id)
    await log_activity("notice", f"Notice updated: {notice[:150]}", user_id=message.from_user.id)
    await message.reply_text("✅ **Notice updated.** It will appear in `/start`.")


@app.on_message(filters.command("speedtest") & (filters.private | filters.group))
async def speedtest_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    start = time.time()
    msg = await message.reply_text("🏓 Running speed test...")
    telegram_ms = round((time.time() - start) * 1000)

    db_start = time.time()
    db_status = "OK"
    try:
        await users_col.find_one({})
    except Exception as e:
        db_status = f"Error: {str(e)[:80]}"
    db_ms = round((time.time() - db_start) * 1000)

    uptime = time.time() - START_TIME
    await msg.edit_text(
        f"⚡ **Speed Test**\n\n"
        f"🏓 **Telegram Response:** `{telegram_ms}ms`\n"
        f"🗄 **Database Ping:** `{db_ms}ms` ({db_status})\n"
        f"📥 **Active Jobs:** `{len(active_jobs)}`\n"
        f"⏱ **Uptime:** `{format_time(uptime)}`"
    )


@app.on_message(filters.command("restart") & (filters.private | filters.group))
async def restart_command(client, message):
    if await deny_if_banned(message):
        return
    if not require_owner(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    await log_activity("restart", "Restart command used", user_id=message.from_user.id)
    await message.reply_text(
        "🔄 **Restarting bot process...**\n\n"
        "If your host auto-restarts the container/process, the bot will come back online shortly."
    )
    asyncio.create_task(restart_process_soon(2))


@app.on_message(filters.command("usage") & (filters.private | filters.group))
async def usage_command(client, message):
    if await deny_if_banned(message):
        return

    parts = (message.text or "").split()
    target_user_id = message.from_user.id

    if len(parts) >= 2 or (message.reply_to_message and message.reply_to_message.from_user):
        if not await require_privileged(message):
            return await message.reply_text(OWNER_ONLY_TEXT)
        found = await _target_or_reply(message)
        if not found:
            return await message.reply_text("❌ Usage: `/usage` or `/usage user_id`")
        target_user_id = found

    usage = await get_user_usage(target_user_id) or {}
    info = await get_user_basic_info(client, target_user_id)
    label = build_user_label(target_user_id, username=info.get("username"), first_name=info.get("first_name"))

    await message.reply_text(
        f"📊 **Usage Stats**\n\n"
        f"👤 **User:** {label}\n"
        f"📥 **Total Deliveries:** `{usage.get('total_downloads', 0)}`\n"
        f"🌐 **Fresh Downloads:** `{usage.get('fresh_downloads', 0)}`\n"
        f"♻️ **Cached Deliveries:** `{usage.get('cached_deliveries', 0)}`\n"
        f"📄 **Total Pages:** `{usage.get('total_pages', 0)}`\n"
        f"🕒 **Last Download:** `{format_date(usage.get('last_download_at'))}`"
    )


@app.on_message(filters.command("topusers") & (filters.private | filters.group))
async def topusers_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    users = await get_top_users(10)
    if not users:
        return await message.reply_text("🏆 **No usage data yet.**")

    lines = ["🏆 **Top Users By Pages Delivered**\n"]
    for i, item in enumerate(users, start=1):
        label = build_user_label(item.get("user_id"), username=item.get("username"), first_name=item.get("first_name"))
        lines.append(
            f"**{i}.** {label}\n"
            f"   📄 Pages: `{item.get('total_pages', 0)}` | 📥 Deliveries: `{item.get('total_downloads', 0)}`"
        )

    await message.reply_text("\n\n".join(lines))


@app.on_message(filters.command("broadcaststats") & (filters.private | filters.group))
async def broadcaststats_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    settings = await get_settings()
    stats = settings.get("last_broadcast")
    if not stats:
        return await message.reply_text("📣 **No broadcast stats saved yet.**")

    await message.reply_text(
        f"📣 **Last Broadcast Stats**\n\n"
        f"🎯 **Success:** `{stats.get('success', 0)}`\n"
        f"⏭ **Skipped Banned:** `{stats.get('skipped', 0)}`\n"
        f"❌ **Failed:** `{stats.get('failed', 0)}`\n"
        f"👥 **Total Checked:** `{stats.get('total', 0)}`\n"
        f"🕒 **Time:** `{format_date(stats.get('created_at'))}`\n"
        f"👤 **By:** `{stats.get('by_user_id', 'N/A')}`"
    )
