import asyncio
import os
import time
import traceback

from bot_core.config import (
    AI_WATCH_CHANNEL_ID,
    AI_WATCH_ENABLED_BY_DEFAULT,
    AI_WATCH_INTERVAL,
    AI_WATCH_MAX_POSTS_PER_CHECK,
    AI_WATCH_MAX_RESULTS,
    AI_WATCH_OLD_ENABLED_BY_DEFAULT,
    AI_WATCH_OLD_POSTS_PER_IDLE_CHECK,
    AI_WATCH_OLD_SCAN_PAGES,
    AI_WATCH_POST_EXISTING_ON_FIRST_RUN,
    AI_WATCH_SEARCH_URL,
    ai_galleries_col,
    app,
    settings_col,
)
from bot_core.utils.bot_api import get_chat_via_bot_api, send_message_via_bot_api, send_photo_via_bot_api
from bot_core.utils.control import log_activity
from bot_core.utils.ehentai import (
    build_ai_gallery_caption,
    build_search_page_url,
    download_binary,
    extract_gallery_id,
    extract_gallery_urls,
    extract_image_url_from_page,
    fetch_text,
    guess_image_suffix,
    normalize_gallery_url,
    parse_gallery_page,
    prepare_telegram_photo,
)

SETTINGS_DOC_ID = "global_settings"
_ai_watcher_task = None
POSTED_STATUSES = {"posted", "posted_old", "manual_posted"}


def normalize_chat_reference(value, default=None):
    raw = value if value not in (None, "") else default
    raw = str(raw if raw not in (None, "") else "").strip()
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


async def get_ai_watch_settings():
    doc = await settings_col.find_one({"_id": SETTINGS_DOC_ID}) or {}
    return {
        "enabled": bool(doc.get("ai_watch_enabled", AI_WATCH_ENABLED_BY_DEFAULT)),
        "channel_id": normalize_chat_reference(doc.get("ai_watch_channel_id", AI_WATCH_CHANNEL_ID), AI_WATCH_CHANNEL_ID),
        "search_url": doc.get("ai_watch_search_url", AI_WATCH_SEARCH_URL),
        "interval": max(60, int(doc.get("ai_watch_interval", AI_WATCH_INTERVAL))),
        "max_results": max(1, int(doc.get("ai_watch_max_results", AI_WATCH_MAX_RESULTS))),
        "max_posts_per_check": max(1, int(doc.get("ai_watch_max_posts_per_check", AI_WATCH_MAX_POSTS_PER_CHECK))),
        "old_enabled": bool(doc.get("ai_watch_old_enabled", AI_WATCH_OLD_ENABLED_BY_DEFAULT)),
        "old_scan_pages": max(1, int(doc.get("ai_watch_old_scan_pages", AI_WATCH_OLD_SCAN_PAGES))),
        "old_posts_per_idle_check": max(1, int(doc.get("ai_watch_old_posts_per_idle_check", AI_WATCH_OLD_POSTS_PER_IDLE_CHECK))),
    }


async def set_ai_watch_enabled(enabled, by_user_id):
    await settings_col.update_one(
        {"_id": SETTINGS_DOC_ID},
        {
            "$set": {
                "ai_watch_enabled": bool(enabled),
                "ai_watch_enabled_by": int(by_user_id),
                "ai_watch_updated_at": time.time(),
                "updated_at": time.time(),
            }
        },
        upsert=True,
    )


async def set_ai_watch_old_enabled(enabled, by_user_id):
    await settings_col.update_one(
        {"_id": SETTINGS_DOC_ID},
        {
            "$set": {
                "ai_watch_old_enabled": bool(enabled),
                "ai_watch_old_enabled_by": int(by_user_id),
                "ai_watch_old_updated_at": time.time(),
                "updated_at": time.time(),
            }
        },
        upsert=True,
    )


async def set_ai_watch_channel(channel_id, by_user_id):
    channel_id = normalize_chat_reference(channel_id, AI_WATCH_CHANNEL_ID)
    await settings_col.update_one(
        {"_id": SETTINGS_DOC_ID},
        {
            "$set": {
                "ai_watch_channel_id": channel_id,
                "ai_watch_channel_by": int(by_user_id),
                "ai_watch_channel_updated_at": time.time(),
                "updated_at": time.time(),
            }
        },
        upsert=True,
    )


async def get_ai_watch_stats():
    settings = await get_ai_watch_settings()
    total_seen = await ai_galleries_col.count_documents({})
    total_posted = await ai_galleries_col.count_documents({"status": {"$in": list(POSTED_STATUSES)}})
    total_new_posted = await ai_galleries_col.count_documents({"status": "posted"})
    total_old_posted = await ai_galleries_col.count_documents({"status": "posted_old"})
    total_manual_posted = await ai_galleries_col.count_documents({"status": "manual_posted"})
    total_seeded = await ai_galleries_col.count_documents({"status": "seeded"})
    total_skipped = await ai_galleries_col.count_documents({"status": "skipped"})
    total_failed = await ai_galleries_col.count_documents({"status": "failed"})
    latest = await ai_galleries_col.find().sort("first_seen_at", -1).limit(1).to_list(length=1)

    return {
        **settings,
        "total_seen": total_seen,
        "total_posted": total_posted,
        "total_new_posted": total_new_posted,
        "total_old_posted": total_old_posted,
        "total_manual_posted": total_manual_posted,
        "total_seeded": total_seeded,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
        "latest": latest[0] if latest else None,
    }



async def verify_channel_can_be_used(channel_id, client=None):
    """Verify the AI target channel using both Bot API and Pyrogram.

    Bot API is tried first because it does not need Pyrogram's local peer
    cache. If Bot API cannot see the channel yet, Pyrogram is tried as a
    fallback. This makes channel checks less fragile after restarts and also
    supports cases where the peer becomes available after the bot receives a
    channel update.
    """
    bot_api_error = ""

    try:
        chat = await get_chat_via_bot_api(channel_id)
        return True, chat, ""
    except Exception as e:
        bot_api_error = str(e)

    try:
        pyro_client = client or app
        chat = await pyro_client.get_chat(channel_id)
        return True, {
            "id": getattr(chat, "id", channel_id),
            "title": getattr(chat, "title", None),
            "username": getattr(chat, "username", None),
            "source": "pyrogram",
        }, ""
    except Exception as e:
        return False, None, f"Bot API: {bot_api_error} | Pyrogram: {str(e)}"


async def warmup_ai_watch_channel():
    """Check the AI-watch channel once on startup.

    This does not send any message. It only validates that the bot can access
    the configured channel after a restart. Errors are logged but do not stop
    the bot because the owner may set the channel later with /aichannel.
    """
    try:
        settings = await get_ai_watch_settings()
        channel_id = settings.get("channel_id")
        ok, chat, error = await verify_channel_can_be_used(channel_id, app)
        if ok:
            title = (chat or {}).get("title") or (chat or {}).get("username") or str(channel_id)
            await log_activity("ai_channel_ready", f"AI gallery channel is ready: {title}", metadata={"channel_id": channel_id})
            return True

        await log_activity(
            "ai_channel_not_ready",
            f"AI gallery channel is not ready: {error}",
            metadata={"channel_id": channel_id},
        )
        return False
    except Exception as e:
        await log_activity("ai_channel_warmup_error", f"AI channel startup check failed: {str(e)}")
        return False

async def _fetch_latest_gallery_urls(search_url, max_results):
    html = await asyncio.to_thread(fetch_text, search_url)
    urls = extract_gallery_urls(html)
    return urls[:max_results]


async def _fetch_gallery_urls_from_pages(search_url, pages, max_results_per_page):
    """Fetch gallery URLs from multiple E-Hentai search pages.

    E-Hentai search pages usually use page=0 for first page, page=1 for second page, etc.
    This function keeps duplicates out and returns the newest-to-oldest order from the pages it scanned.
    """
    all_urls = []
    seen = set()

    for page in range(max(1, int(pages))):
        page_url = build_search_page_url(search_url, page)
        try:
            html = await asyncio.to_thread(fetch_text, page_url)
            urls = extract_gallery_urls(html)[:max_results_per_page]
        except Exception:
            if page == 0:
                raise
            continue

        for url in urls:
            if url not in seen:
                seen.add(url)
                all_urls.append(url)

        # Be gentle with the site when scanning old pages.
        if page + 1 < pages:
            await asyncio.sleep(1)

    return all_urls


async def _fetch_gallery_info(url):
    html = await asyncio.to_thread(fetch_text, url)
    return parse_gallery_page(url, html)


async def _resolve_best_preview_url(info):
    """Prefer a high-quality first-page preview, then fall back to the gallery thumbnail."""
    first_page_url = info.get("first_page_url")

    if first_page_url:
        try:
            image_page_html = await asyncio.to_thread(fetch_text, first_page_url, 30, info.get("url"))
            image_url = extract_image_url_from_page(image_page_html, first_page_url)
            if image_url:
                return image_url, first_page_url, "high"
        except Exception:
            # The normal thumbnail is still better than failing the whole post.
            pass

    thumbnail_url = info.get("thumbnail_url")
    if thumbnail_url:
        return thumbnail_url, info.get("url"), "thumbnail"

    return "", info.get("url"), "none"


async def _send_ai_gallery_post(client, channel_id, info, *, heading=None):
    caption = build_ai_gallery_caption(info, heading=heading)

    # Do not hard-stop on getChat verification. Some channels are reachable for
    # sendPhoto even when getChat fails right after restart, and Pyrogram may
    # also have the peer after a channel update. The real send attempts below
    # are the source of truth.
    ok, chat, channel_error = await verify_channel_can_be_used(channel_id, client)
    if not ok:
        await log_activity(
            "ai_channel_verify_warning",
            f"AI channel pre-check failed, trying to send anyway: {channel_error}",
            metadata={"channel_id": channel_id},
        )

    preview_url, referer, preview_quality = await _resolve_best_preview_url(info)
    fallback_thumbnail_url = info.get("thumbnail_url")
    raw_path = None
    prepared_path = None
    last_error = ""

    async def _cleanup_paths():
        for path in (raw_path, prepared_path):
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    async def _try_send_photo(image_url, image_referer, download_limit):
        nonlocal raw_path, prepared_path
        raw_path = await asyncio.to_thread(
            download_binary,
            image_url,
            image_referer,
            guess_image_suffix(image_url),
            45,
            download_limit,
        )
        prepared_path = await asyncio.to_thread(
            prepare_telegram_photo,
            raw_path,
            max_side=2560 if preview_quality == "high" else 1800,
            max_bytes=9 * 1024 * 1024,
        )
        bot_api_error = ""
        try:
            # Try Bot API first. It usually works immediately after restart.
            await send_photo_via_bot_api(
                channel_id,
                prepared_path,
                caption=caption,
                parse_mode=None,
            )
            return True
        except Exception as e:
            bot_api_error = str(e)

        try:
            # Fallback to Pyrogram/MTProto. This works when the channel peer is
            # already known to the bot, for example after a channel update.
            await client.send_photo(
                chat_id=channel_id,
                photo=prepared_path,
                caption=caption,
            )
            return True
        except Exception as e:
            raise RuntimeError(f"Bot API send failed: {bot_api_error} | Pyrogram send failed: {str(e)}")

    candidates = []
    if preview_url:
        # Download the first actual gallery image when available. This is much cleaner
        # than Telegram's web preview or E-Hentai's tiny cover thumbnail.
        candidates.append((preview_url, referer, 35 * 1024 * 1024))
    if fallback_thumbnail_url and fallback_thumbnail_url != preview_url:
        candidates.append((fallback_thumbnail_url, info.get("url"), 12 * 1024 * 1024))

    for image_url, image_referer, download_limit in candidates:
        try:
            await _try_send_photo(image_url, image_referer, download_limit)
            return True
        except Exception as e:
            last_error = str(e)
        finally:
            await _cleanup_paths()
            raw_path = None
            prepared_path = None

    # Last fallback: text only. Disable Telegram web previews so it does not show
    # the low-quality link card seen in the channel screenshot.
    fallback_text = caption
    if last_error:
        fallback_text += "\n\n⚠️ Preview image could not be uploaded automatically."

    bot_api_error = ""
    try:
        await send_message_via_bot_api(
            channel_id,
            fallback_text,
            disable_web_page_preview=True,
            parse_mode=None,
        )
        return True
    except Exception as e:
        bot_api_error = str(e)

    try:
        await client.send_message(
            chat_id=channel_id,
            text=fallback_text,
            disable_web_page_preview=True,
        )
        return True
    except Exception as e:
        raise RuntimeError(f"Bot API text fallback failed: {bot_api_error} | Pyrogram text fallback failed: {str(e)}")


async def post_gallery_link(client=None, url=None, *, force=False, source="manual", by_user_id=None):
    """Manually post one gallery thumbnail + information to the AI watch channel.

    By default this only posts galleries whose title/name contains AI Generated.
    Use force=True only when you deliberately want to bypass the AI-name check.
    """
    client = client or app
    settings = await get_ai_watch_settings()
    channel_id = settings["channel_id"]
    normalized_url = normalize_gallery_url(url)

    result = {
        "url": normalized_url or url,
        "posted": False,
        "already_posted": False,
        "skipped": False,
        "failed": False,
        "reason": "",
        "title": "",
        "channel_id": channel_id,
    }

    if not normalized_url:
        result["failed"] = True
        result["reason"] = "Invalid E-Hentai gallery link."
        return result

    existing = await ai_galleries_col.find_one({"url": normalized_url})
    if existing and existing.get("status") in POSTED_STATUSES and not force:
        result["already_posted"] = True
        result["reason"] = "This gallery was already posted. Use /postgallery force <link> to repost it."
        result["title"] = existing.get("title", "")
        return result

    gallery_id = extract_gallery_id(normalized_url)
    now = time.time()
    await ai_galleries_col.update_one(
        {"url": normalized_url},
        {
            "$setOnInsert": {
                "url": normalized_url,
                "gallery_id": gallery_id,
                "first_seen_at": now,
            },
            "$set": {
                "last_seen_at": now,
                "manual_requested_by": int(by_user_id or 0),
                "updated_at": now,
            },
        },
        upsert=True,
    )

    try:
        info = await _fetch_gallery_info(normalized_url)
        result["title"] = info.get("title") or ""

        if not force and not info.get("has_ai_generated"):
            result["skipped"] = True
            result["reason"] = "The gallery title/name does not contain AI Generated."
            await ai_galleries_col.update_one(
                {"url": normalized_url},
                {
                    "$set": {
                        "status": "skipped",
                        "reason": result["reason"],
                        "title": info.get("title"),
                        "category": info.get("category"),
                        "posted_on_site": info.get("posted"),
                        "thumbnail_url": info.get("thumbnail_url"),
                        "first_page_url": info.get("first_page_url"),
                        "updated_at": time.time(),
                    }
                },
            )
            return result

        heading = "🤖 AI Generated Gallery"
        if source == "manual":
            heading = "🤖 Manually Posted AI Generated Gallery"
        elif source == "old":
            heading = "🤖 Older AI Generated Gallery"

        await _send_ai_gallery_post(client, channel_id, info, heading=heading)

        status = "manual_posted" if source == "manual" else "posted_old" if source == "old" else "posted"
        await ai_galleries_col.update_one(
            {"url": normalized_url},
            {
                "$set": {
                    "status": status,
                    "posted_at": time.time(),
                    "posted_to_channel": channel_id,
                    "title": info.get("title"),
                    "category": info.get("category"),
                    "posted_on_site": info.get("posted"),
                    "thumbnail_url": info.get("thumbnail_url"),
                    "updated_at": time.time(),
                    "source": source,
                }
            },
        )

        await log_activity(
            "ai_gallery_manual_post" if source == "manual" else "ai_gallery_old_post",
            f"Posted AI-name gallery: {info.get('title')}",
            user_id=by_user_id,
            metadata={"url": normalized_url, "channel_id": channel_id, "source": source},
        )

        result["posted"] = True
        result["reason"] = "Posted successfully."
        return result

    except Exception as e:
        result["failed"] = True
        result["reason"] = str(e)
        await ai_galleries_col.update_one(
            {"url": normalized_url},
            {
                "$set": {
                    "status": "failed",
                    "error": str(e),
                    "updated_at": time.time(),
                }
            },
        )
        await log_activity(
            "ai_gallery_manual_post_error" if source == "manual" else "ai_gallery_old_post_error",
            f"Failed to post AI-name gallery {normalized_url}: {str(e)}",
            user_id=by_user_id,
            metadata={"traceback": traceback.format_exc()[-1500:]},
        )
        return result


async def _post_old_ai_galleries(client, settings):
    """Post older galleries named AI Generated when no fresh upload was found."""
    summary = {
        "old_checked": 0,
        "old_posted": 0,
        "old_skipped": 0,
        "old_failed": 0,
        "errors": [],
    }

    urls = await _fetch_gallery_urls_from_pages(
        settings["search_url"],
        settings["old_scan_pages"],
        settings["max_results"],
    )
    summary["old_checked"] = len(urls)

    if not urls:
        return summary

    # Post older entries first. This prevents the channel from repeating only the newest page forever.
    for url in reversed(urls):
        if summary["old_posted"] >= settings["old_posts_per_idle_check"]:
            break

        existing = await ai_galleries_col.find_one({"url": url})
        if existing and existing.get("status") in POSTED_STATUSES:
            continue

        result = await post_gallery_link(client, url, force=False, source="old")
        if result.get("posted"):
            summary["old_posted"] += 1
        elif result.get("skipped") or result.get("already_posted"):
            summary["old_skipped"] += 1
        else:
            summary["old_failed"] += 1
            if result.get("reason"):
                summary["errors"].append(result["reason"])

    return summary


async def check_and_post_ai_galleries(client=None, *, post_existing=False, manual=False, post_old=False):
    """Check E-Hentai and post new galleries whose title/name contains AI Generated."""
    client = client or app
    settings = await get_ai_watch_settings()
    channel_id = settings["channel_id"]
    search_url = settings["search_url"]
    max_results = settings["max_results"]
    max_posts_per_check = settings["max_posts_per_check"]

    summary = {
        "checked": 0,
        "new": 0,
        "posted": 0,
        "seeded": 0,
        "skipped": 0,
        "failed": 0,
        "old_checked": 0,
        "old_posted": 0,
        "old_skipped": 0,
        "old_failed": 0,
        "errors": [],
    }

    urls = await _fetch_latest_gallery_urls(search_url, max_results)
    summary["checked"] = len(urls)

    if not urls:
        return summary

    # First automatic run should not spam the channel with old search results.
    already_known_count = await ai_galleries_col.count_documents({})
    first_run_seed_only = (
        already_known_count == 0
        and not post_existing
        and not AI_WATCH_POST_EXISTING_ON_FIRST_RUN
    )

    if first_run_seed_only:
        now = time.time()
        for url in urls:
            gallery_id = extract_gallery_id(url)
            await ai_galleries_col.update_one(
                {"url": url},
                {
                    "$setOnInsert": {
                        "url": url,
                        "gallery_id": gallery_id,
                        "status": "seeded",
                        "first_seen_at": now,
                        "last_seen_at": now,
                    }
                },
                upsert=True,
            )
            summary["seeded"] += 1

        await log_activity(
            "ai_watch_seeded",
            f"Seeded {summary['seeded']} existing AI Generated-name gallery result(s). Future matching uploads will be posted.",
            metadata={"search_url": search_url},
        )
        return summary

    # Search results are usually newest first. Post older new items first so channel order feels natural.
    for url in reversed(urls):
        if summary["posted"] >= max_posts_per_check:
            break

        existing = await ai_galleries_col.find_one({"url": url})
        if existing and existing.get("status") in POSTED_STATUSES:
            continue

        gallery_id = extract_gallery_id(url)
        now = time.time()
        summary["new"] += 1

        await ai_galleries_col.update_one(
            {"url": url},
            {
                "$setOnInsert": {
                    "url": url,
                    "gallery_id": gallery_id,
                    "status": "found",
                    "first_seen_at": now,
                    "last_seen_at": now,
                }
            },
            upsert=True,
        )

        try:
            info = await _fetch_gallery_info(url)
            if not info.get("has_ai_generated"):
                summary["skipped"] += 1
                await ai_galleries_col.update_one(
                    {"url": url},
                    {
                        "$set": {
                            "status": "skipped",
                            "reason": "AI Generated was not found in the gallery title/name",
                            "title": info.get("title"),
                            "updated_at": time.time(),
                        }
                    },
                )
                continue

            await _send_ai_gallery_post(client, channel_id, info, heading="🤖 New AI Generated Gallery")
            summary["posted"] += 1

            await ai_galleries_col.update_one(
                {"url": url},
                {
                    "$set": {
                        "status": "posted",
                        "posted_at": time.time(),
                        "posted_to_channel": channel_id,
                        "title": info.get("title"),
                        "category": info.get("category"),
                        "posted_on_site": info.get("posted"),
                        "thumbnail_url": info.get("thumbnail_url"),
                        "first_page_url": info.get("first_page_url"),
                        "updated_at": time.time(),
                    }
                },
            )
            await log_activity(
                "ai_watch_posted",
                f"Posted new AI Generated-name gallery: {info.get('title')}",
                metadata={"url": url, "channel_id": channel_id},
            )

        except Exception as e:
            summary["failed"] += 1
            summary["errors"].append(str(e))
            await ai_galleries_col.update_one(
                {"url": url},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(e),
                        "updated_at": time.time(),
                    }
                },
            )
            await log_activity(
                "ai_watch_error",
                f"Failed to post AI-name gallery {url}: {str(e)}",
                metadata={"traceback": traceback.format_exc()[-1500:]},
            )

    # New requested feature:
    # If there is no fresh post in this check, fill the channel with older unposted galleries named AI Generated.
    should_post_old = (
        settings.get("old_enabled")
        and (post_old or not manual)
        and summary["posted"] == 0
        and summary["failed"] == 0
    )
    if should_post_old:
        try:
            old_summary = await _post_old_ai_galleries(client, settings)
            summary.update({k: old_summary.get(k, summary.get(k, 0)) for k in old_summary if k != "errors"})
            summary["errors"].extend(old_summary.get("errors", []))
        except Exception as e:
            summary["old_failed"] += 1
            summary["errors"].append(str(e))
            await log_activity(
                "ai_watch_old_error",
                f"Failed while posting older AI-name gallery: {str(e)}",
                metadata={"traceback": traceback.format_exc()[-1500:]},
            )

    return summary


async def ai_gallery_watcher_loop():
    await log_activity("ai_watch_started", "AI Generated-name gallery watcher started.")

    while True:
        try:
            settings = await get_ai_watch_settings()

            if settings["enabled"]:
                await check_and_post_ai_galleries(app)

            await asyncio.sleep(settings["interval"])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await log_activity(
                "ai_watch_loop_error",
                f"AI watcher loop error: {str(e)}",
                metadata={"traceback": traceback.format_exc()[-1500:]},
            )
            await asyncio.sleep(60)


async def start_ai_gallery_watcher():
    global _ai_watcher_task

    if _ai_watcher_task and not _ai_watcher_task.done():
        return _ai_watcher_task

    _ai_watcher_task = asyncio.create_task(ai_gallery_watcher_loop())
    return _ai_watcher_task


async def stop_ai_gallery_watcher():
    global _ai_watcher_task

    if not _ai_watcher_task:
        return

    _ai_watcher_task.cancel()
    try:
        await _ai_watcher_task
    except asyncio.CancelledError:
        pass
    finally:
        _ai_watcher_task = None
