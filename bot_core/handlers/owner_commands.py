import asyncio
import time

from pyrogram import filters


from bot_core.config import START_TIME, active_jobs, app, banned_users_col, galleries_col, settings_col, sudo_users_col, users_col
from bot_core.utils.admin import OWNER_ONLY_TEXT, deny_if_banned, require_privileged
from bot_core.utils.control import is_maintenance_enabled, log_activity
from bot_core.utils.telegram import safe_copy_message, safe_edit_text


# ==========================================
#        OWNER / SUDO OPERATION COMMANDS
# ==========================================

@app.on_message(filters.command("broadcast") & (filters.private | filters.group))
async def broadcast_command(client, message):
    if await deny_if_banned(message):
        return

    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    if not message.reply_to_message:
        return await message.reply_text("❌ Please reply to the message you want to broadcast.")

    msg = await message.reply_text("⏳ **Broadcasting message...**")

    try:
        users = await users_col.find().to_list(length=None)
        banned_docs = await banned_users_col.find({}, {"user_id": 1}).to_list(length=None)
        banned_ids = {int(doc.get("user_id")) for doc in banned_docs if doc.get("user_id")}
        success, failed, skipped = 0, 0, 0

        for u in users:
            try:
                user_id = u.get("user_id")
                if not user_id:
                    failed += 1
                    continue

                if int(user_id) in banned_ids:
                    skipped += 1
                    continue

                await safe_copy_message(message.reply_to_message, user_id)
                success += 1
            except Exception:
                failed += 1
            # safe_copy_message already applies a small broadcast delay

        total_checked = len(users)
        await settings_col.update_one(
            {"_id": "global_settings"},
            {
                "$set": {
                    "last_broadcast": {
                        "success": success,
                        "failed": failed,
                        "skipped": skipped,
                        "total": total_checked,
                        "by_user_id": int(message.from_user.id),
                        "created_at": time.time(),
                    }
                }
            },
            upsert=True,
        )
        await log_activity(
            "broadcast",
            f"Broadcast complete: success={success}, skipped={skipped}, failed={failed}",
            user_id=message.from_user.id,
        )

        await safe_edit_text(msg, 
            f"✅ **Broadcast Complete!**\n\n"
            f"🎯 Success: {success}\n"
            f"⏭ Skipped banned: {skipped}\n"
            f"❌ Failed: {failed}\n"
            f"👥 Total Checked: {total_checked}"
        )

    except Exception:
        await safe_edit_text(msg, "❌ Database Error: Could not retrieve users.")


@app.on_message(filters.command("stats") & (filters.private | filters.group))
async def stats_command(client, message):
    if await deny_if_banned(message):
        return

    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    uptime = time.time() - START_TIME
    uptime_str = time.strftime("%Hh %Mm %Ss", time.gmtime(uptime))

    try:
        total_users = await users_col.count_documents({})
        cached_galleries = await galleries_col.count_documents({})
        banned_users = await banned_users_col.count_documents({})
        sudo_users = await sudo_users_col.count_documents({})
        maintenance = "ON" if await is_maintenance_enabled() else "OFF"
        active_count = len(active_jobs)
    except Exception:
        total_users = "Error"
        cached_galleries = "Error"
        banned_users = "Error"
        sudo_users = "Error"
        maintenance = "Error"
        active_count = "Error"

    stats_text = (
        f"📊 **System Statistics**\n\n"
        f"👥 **Total Users:** {total_users}\n"
        f"🚫 **Banned Users:** {banned_users}\n"
        f"🛡 **Sudo Users:** {sudo_users}\n"
        f"🗂 **Cached Galleries:** {cached_galleries}\n"
        f"📥 **Active Jobs:** {active_count}\n"
        f"🛠 **Maintenance:** {maintenance}\n"
        f"⏱ **Uptime:** {uptime_str}\n"
    )

    await message.reply_text(stats_text)


@app.on_message(filters.command("ping") & (filters.private | filters.group))
async def ping_command(client, message):
    if await deny_if_banned(message):
        return

    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    start_time = time.time()
    msg = await message.reply_text("🏓 Pong!")
    end_time = time.time()

    latency = round((end_time - start_time) * 1000)
    await safe_edit_text(msg, f"🏓 **Pong!**\nLatency: `{latency}ms`")


@app.on_message(filters.command("delcache") & (filters.private | filters.group))
async def delcache_command(client, message):
    if await deny_if_banned(message):
        return

    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text(
            "❌ Please provide the URL you want to delete.\n"
            "Example: `/delcache https://e-hentai.org/g/...`"
        )

    target_url = parts[1].strip().rstrip("/")
    result = await galleries_col.delete_many({"url": target_url})

    if result.deleted_count > 0:
        await message.reply_text(
            f"✅ Successfully deleted **{result.deleted_count}** cache entries for that URL. "
            "It will be downloaded fresh next time."
        )
    else:
        await message.reply_text("⚠️ No cache found for that URL in the database.")
