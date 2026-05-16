import re

from pyrogram import filters

from bot_core.config import app
from bot_core.services.ai_gallery_watcher import (
    check_and_post_ai_galleries,
    get_ai_watch_stats,
    post_gallery_link,
    set_ai_watch_channel,
    set_ai_watch_enabled,
    set_ai_watch_old_enabled,
    verify_channel_can_be_used,
)
from bot_core.utils.admin import OWNER_ONLY_TEXT, deny_if_banned, require_owner, require_privileged
from bot_core.utils.control import format_date, log_activity
from bot_core.utils.ehentai import normalize_gallery_url


# ==========================================
#       AI-GENERATED GALLERY WATCHER
# ==========================================

GALLERY_LINK_RE = re.compile(r"https?://e-hentai\.org/g/[0-9]+/[a-z0-9]+/?", re.IGNORECASE)


def _looks_like_channel_chat(message):
    chat_type = str(getattr(getattr(message, "chat", None), "type", "")).lower()
    return "channel" in chat_type


def _normalize_numeric_channel_id(value):
    number = int(value)

    # Already a Telegram channel/supergroup ID.
    if number < 0:
        return number

    raw = str(number)
    # If user pastes 100xxxxxxxxxx without the minus, just add it.
    if raw.startswith("100") and len(raw) >= 12:
        return -number

    # If user pastes only the internal channel ID, convert it to Bot API style.
    # Example: 3196947430 -> -1003196947430
    if len(raw) >= 8:
        return int(f"-100{raw}")

    return number


def _parse_channel_id(text, message=None):
    parts = (text or "").split(maxsplit=1)

    # Reply to a forwarded channel post with /aichannel. This is the easiest
    # way to avoid typing the wrong -100 ID.
    if message and getattr(message, "reply_to_message", None):
        forwarded_chat = getattr(message.reply_to_message, "forward_from_chat", None)
        if forwarded_chat and getattr(forwarded_chat, "id", None):
            return int(forwarded_chat.id)

    if len(parts) < 2:
        return None

    value = parts[1].strip().split()[0]

    if value.lower() in {"here", "this", "current"} and message:
        return int(message.chat.id)

    if value.lstrip("-").isdigit():
        return _normalize_numeric_channel_id(value)

    if value.startswith("@") and len(value) > 1:
        return value

    return None


def _extract_gallery_link(text):
    match = GALLERY_LINK_RE.search(text or "")
    if not match:
        return None
    return normalize_gallery_url(match.group(0))


def _format_aiwatch_status(stats):
    latest = stats.get("latest") or {}
    latest_text = "None yet"
    if latest:
        latest_text = f"{latest.get('title') or latest.get('url') or 'Unknown'}\n   🕒 {format_date(latest.get('first_seen_at'))}"

    return (
        "🤖 **AI Gallery Watcher**\n\n"
        f"**Status:** `{'ON' if stats.get('enabled') else 'OFF'}`\n"
        f"**Old Fallback:** `{'ON' if stats.get('old_enabled') else 'OFF'}`\n"
        f"**Channel ID:** `{stats.get('channel_id')}`\n"
        f"**Check Interval:** `{stats.get('interval')}s`\n"
        f"**Old Scan Pages:** `{stats.get('old_scan_pages')}`\n"
        f"**Old Posts When Idle:** `{stats.get('old_posts_per_idle_check')}`\n"
        f"**Search URL:** {stats.get('search_url')}\n\n"
        "📊 **Database**\n"
        f"**Seen:** `{stats.get('total_seen', 0)}`\n"
        f"**Posted Total:** `{stats.get('total_posted', 0)}`\n"
        f"**New Posted:** `{stats.get('total_new_posted', 0)}`\n"
        f"**Old Posted:** `{stats.get('total_old_posted', 0)}`\n"
        f"**Manual Posted:** `{stats.get('total_manual_posted', 0)}`\n"
        f"**Seeded:** `{stats.get('total_seeded', 0)}`\n"
        f"**Skipped:** `{stats.get('total_skipped', 0)}`\n"
        f"**Failed:** `{stats.get('total_failed', 0)}`\n\n"
        f"🆕 **Latest Seen:** {latest_text}\n\n"
        "Commands:\n"
        "• `/aiwatch on`\n"
        "• `/aiwatch off`\n"
        "• `/aiwatch old on`\n"
        "• `/aiwatch old off`\n"
        "• `/aiwatch` — show status\n"
        "• `/aiwatchnow` — check now\n"
        "• `/aiwatchnow old` — manually post old fallback if no new is found\n"
        "• `/aiwatchnow postexisting` — first-run post instead of seed\n"
        "• `/postgallery <link>` — post one AI Generated-name gallery now\n"
        "• `/aichannel -100xxxxxxxxxx` or `/aichannel @username` — set target channel\n"
        "• `/aichannel here` — set the current group/channel as target"
    )


@app.on_message(filters.command("aiwatch") & (filters.private | filters.group))
async def aiwatch_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    parts = (message.text or "").split()

    if len(parts) >= 2:
        arg = parts[1].strip().lower()

        if arg in {"on", "enable", "enabled", "1", "true"}:
            if not require_owner(message):
                return await message.reply_text(OWNER_ONLY_TEXT)
            await set_ai_watch_enabled(True, message.from_user.id)
            await log_activity("aiwatch", "AI gallery watcher enabled", user_id=message.from_user.id)
            return await message.reply_text("✅ **AI gallery watcher is now ON.**")

        if arg in {"off", "disable", "disabled", "0", "false"}:
            if not require_owner(message):
                return await message.reply_text(OWNER_ONLY_TEXT)
            await set_ai_watch_enabled(False, message.from_user.id)
            await log_activity("aiwatch", "AI gallery watcher disabled", user_id=message.from_user.id)
            return await message.reply_text("🚫 **AI gallery watcher is now OFF.**")

        if arg in {"old", "oldfallback", "fallback"}:
            if not require_owner(message):
                return await message.reply_text(OWNER_ONLY_TEXT)

            if len(parts) < 3:
                stats = await get_ai_watch_stats()
                return await message.reply_text(
                    f"📦 **Old Gallery Fallback:** `{'ON' if stats.get('old_enabled') else 'OFF'}`\n\n"
                    "Usage:\n"
                    "`/aiwatch old on`\n"
                    "`/aiwatch old off`"
                )

            value = parts[2].strip().lower()
            if value in {"on", "enable", "enabled", "1", "true"}:
                await set_ai_watch_old_enabled(True, message.from_user.id)
                await log_activity("aiwatch_old", "AI old-gallery fallback enabled", user_id=message.from_user.id)
                return await message.reply_text("✅ **Old gallery fallback is now ON.**")

            if value in {"off", "disable", "disabled", "0", "false"}:
                await set_ai_watch_old_enabled(False, message.from_user.id)
                await log_activity("aiwatch_old", "AI old-gallery fallback disabled", user_id=message.from_user.id)
                return await message.reply_text("🚫 **Old gallery fallback is now OFF.**")

        return await message.reply_text(
            "❌ Usage:\n"
            "`/aiwatch`\n"
            "`/aiwatch on`\n"
            "`/aiwatch off`\n"
            "`/aiwatch old on`\n"
            "`/aiwatch old off`"
        )

    stats = await get_ai_watch_stats()
    await message.reply_text(_format_aiwatch_status(stats), disable_web_page_preview=True)


@app.on_message(filters.command("aiwatchnow") & (filters.private | filters.group))
async def aiwatchnow_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    parts = (message.text or "").split()
    lowered = {part.lower() for part in parts[1:]}
    post_existing = bool(lowered & {"post", "postexisting", "post-existing", "force"})
    post_old = bool(lowered & {"old", "older", "fallback"})

    msg = await message.reply_text("🔎 **Checking E-Hentai for galleries named AI Generated...**")

    try:
        summary = await check_and_post_ai_galleries(
            client,
            post_existing=post_existing,
            manual=True,
            post_old=post_old,
        )
        text = (
            "✅ **AI Watch Check Complete**\n\n"
            f"🔎 **Checked Results:** `{summary.get('checked', 0)}`\n"
            f"🆕 **New Found:** `{summary.get('new', 0)}`\n"
            f"📣 **New Posted:** `{summary.get('posted', 0)}`\n"
            f"📦 **Old Checked:** `{summary.get('old_checked', 0)}`\n"
            f"📤 **Old Posted:** `{summary.get('old_posted', 0)}`\n"
            f"🌱 **Seeded:** `{summary.get('seeded', 0)}`\n"
            f"⏭ **Skipped:** `{summary.get('skipped', 0) + summary.get('old_skipped', 0)}`\n"
            f"❌ **Failed:** `{summary.get('failed', 0) + summary.get('old_failed', 0)}`"
        )

        if summary.get("seeded"):
            text += (
                "\n\nℹ️ First run seed completed. Existing results were saved so the channel "
                "does not get spammed. Future new galleries with AI Generated in the title/name will be posted."
            )

        if summary.get("posted", 0) == 0 and summary.get("old_posted", 0) > 0:
            text += "\n\n📦 No fresh upload was found, so the bot posted older unposted gallery result(s) with AI Generated in the title/name."

        if summary.get("errors"):
            text += f"\n\n🧾 **Last Error:** `{summary['errors'][-1][:300]}`"

        await msg.edit_text(text)
    except Exception as e:
        await msg.edit_text(f"❌ **AI watch check failed:** `{str(e)}`")


@app.on_message(filters.command("postgallery") & (filters.private | filters.group))
async def postgallery_command(client, message):
    if await deny_if_banned(message):
        return
    if not await require_privileged(message):
        return await message.reply_text(OWNER_ONLY_TEXT)

    text = message.text or ""
    force = bool(re.search(r"(^|\s)(force|repost)(\s|$)", text, flags=re.IGNORECASE))
    url = _extract_gallery_link(text)

    if not url and message.reply_to_message:
        url = _extract_gallery_link(message.reply_to_message.text or message.reply_to_message.caption or "")

    if not url:
        return await message.reply_text(
            "❌ **Usage:**\n"
            "`/postgallery https://e-hentai.org/g/123/token/`\n\n"
            "Reply style also works:\n"
            "Reply to a message containing a gallery link with `/postgallery`.\n\n"
            "By default, the gallery title/name must contain `AI Generated`."
        )

    msg = await message.reply_text("📤 **Posting gallery thumbnail and information...**")

    result = await post_gallery_link(
        client,
        url,
        force=force,
        source="manual",
        by_user_id=message.from_user.id,
    )

    if result.get("posted"):
        return await msg.edit_text(
            "✅ **Gallery posted to channel.**\n\n"
            f"📚 **Title:** {result.get('title') or 'Unknown'}\n"
            f"📣 **Channel ID:** `{result.get('channel_id')}`"
        )

    if result.get("already_posted"):
        return await msg.edit_text(
            "⚠️ **Already posted.**\n\n"
            f"📚 **Title:** {result.get('title') or 'Unknown'}\n"
            f"Reason: {result.get('reason')}"
        )

    if result.get("skipped"):
        return await msg.edit_text(
            "⏭ **Gallery skipped.**\n\n"
            f"📚 **Title:** {result.get('title') or 'Unknown'}\n"
            f"Reason: {result.get('reason')}\n\n"
            "Use `/postgallery force <link>` only if you intentionally want to post it anyway."
        )

    await msg.edit_text(f"❌ **Failed to post gallery:** `{result.get('reason') or 'Unknown error'}`")


@app.on_message(filters.command("aichannel") & (filters.private | filters.group | filters.channel))
async def aichannel_command(client, message):
    if await deny_if_banned(message):
        return

    text = message.text or message.caption or ""
    using_here_in_channel = (" here" in f" {text.lower()} " or " this" in f" {text.lower()} " or " current" in f" {text.lower()} ") and _looks_like_channel_chat(message)

    # Normal use stays owner-only. The only exception is `/aichannel here`
    # posted inside a channel, because Telegram channel posts do not reliably
    # include the owner's user ID. Only channel admins can post it there.
    if not require_owner(message) and not using_here_in_channel:
        return await message.reply_text(OWNER_ONLY_TEXT)

    channel_id = _parse_channel_id(text, message)
    if not channel_id:
        stats = await get_ai_watch_stats()
        return await message.reply_text(
            f"📣 **Current AI Watch Channel:** `{stats.get('channel_id')}`\n\n"
            "Usage:\n"
            "`/aichannel -1001234567890`\n"
            "`/aichannel @public_channel_username`\n\n"
            "The bot must be admin in that channel."
        )

    by_user_id = getattr(getattr(message, "from_user", None), "id", 0) or 0
    await set_ai_watch_channel(channel_id, by_user_id)
    await log_activity("aichannel", f"AI watch channel set to {channel_id}", user_id=by_user_id)

    ok, chat, error = await verify_channel_can_be_used(channel_id, client)
    if ok:
        chat_name = (chat or {}).get("title") or (chat or {}).get("username") or str(channel_id)
        return await message.reply_text(
            f"✅ **AI watch channel updated and verified.**\n\n"
            f"📣 Channel: `{channel_id}`\n"
            f"🧾 Name: `{chat_name}`\n\n"
            "The bot will try Bot API first, then Pyrogram fallback if needed."
        )

    await message.reply_text(
        f"⚠️ **AI watch channel saved, but verification failed.**\n\n"
        f"📣 Channel: `{channel_id}`\n"
        f"❌ Error: `{error}`\n\n"
        "Make sure the bot is admin in that channel and the ID is correct. "
        "Best fix: post `/chatid` in the channel, then use that exact ID. "
        "You can also post `/aichannel here` inside the channel or use `@public_channel_username`."
    )
