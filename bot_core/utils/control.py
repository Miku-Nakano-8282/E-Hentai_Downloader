import asyncio
import os
import time
from datetime import datetime

from bot_core.config import (
    OWNER_ID,
    active_downloads,
    active_jobs,
    activity_logs_col,
    galleries_col,
    settings_col,
    usage_col,
    user_limits_col,
    users_col,
)
from bot_core.utils.time_format import format_time

SETTINGS_DOC_ID = "global_settings"
DEFAULT_USER_LIMIT = 1

_SETTINGS_CACHE_TTL = 10
_USER_LIMIT_CACHE_TTL = 30
_settings_cache = {"doc": None, "expires_at": 0}
_user_limit_cache = {}


def _now():
    return time.time()


def clear_settings_cache():
    _settings_cache["doc"] = None
    _settings_cache["expires_at"] = 0


def clear_user_limit_cache(user_id=None):
    if user_id is None:
        _user_limit_cache.clear()
    else:
        _user_limit_cache.pop(int(user_id), None)


def format_date(timestamp):
    if not timestamp:
        return "Unknown"
    try:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Unknown"


async def get_settings(force_refresh=False):
    now = _now()
    if not force_refresh and _settings_cache["doc"] is not None and _settings_cache["expires_at"] > now:
        return dict(_settings_cache["doc"])

    doc = await settings_col.find_one({"_id": SETTINGS_DOC_ID})
    if not doc:
        doc = {"_id": SETTINGS_DOC_ID}

    _settings_cache["doc"] = dict(doc)
    _settings_cache["expires_at"] = now + _SETTINGS_CACHE_TTL
    return dict(doc)


async def set_setting_value(key, value):
    await settings_col.update_one(
        {"_id": SETTINGS_DOC_ID},
        {"$set": {key: value, "updated_at": time.time()}},
        upsert=True,
    )
    clear_settings_cache()


async def get_setting_value(key, default=None):
    doc = await get_settings()
    return doc.get(key, default)


async def is_maintenance_enabled():
    return bool(await get_setting_value("maintenance", False))


async def set_maintenance(enabled, by_user_id):
    await settings_col.update_one(
        {"_id": SETTINGS_DOC_ID},
        {
            "$set": {
                "maintenance": bool(enabled),
                "maintenance_by": int(by_user_id),
                "maintenance_updated_at": time.time(),
                "updated_at": time.time(),
            }
        },
        upsert=True,
    )
    clear_settings_cache()


async def get_notice_text():
    text = await get_setting_value("notice_text", "")
    return text or ""


async def set_notice_text(text, by_user_id):
    await settings_col.update_one(
        {"_id": SETTINGS_DOC_ID},
        {
            "$set": {
                "notice_text": text or "",
                "notice_by": int(by_user_id),
                "notice_updated_at": time.time(),
                "updated_at": time.time(),
            }
        },
        upsert=True,
    )
    clear_settings_cache()


async def get_global_user_limit():
    value = await get_setting_value("global_user_limit", DEFAULT_USER_LIMIT)
    try:
        value = int(value)
    except Exception:
        value = DEFAULT_USER_LIMIT
    return max(1, value)


async def set_global_user_limit(limit, by_user_id):
    limit = max(1, int(limit))
    await settings_col.update_one(
        {"_id": SETTINGS_DOC_ID},
        {
            "$set": {
                "global_user_limit": limit,
                "global_user_limit_by": int(by_user_id),
                "global_user_limit_updated_at": time.time(),
                "updated_at": time.time(),
            }
        },
        upsert=True,
    )
    clear_settings_cache()
    clear_user_limit_cache()
    return limit


async def get_user_download_limit(user_id):
    user_id = int(user_id)
    cached = _user_limit_cache.get(user_id)
    now = _now()
    if cached and cached[1] > now:
        return cached[0]

    doc = await user_limits_col.find_one({"user_id": user_id})
    if doc and doc.get("limit"):
        try:
            value = max(1, int(doc.get("limit")))
        except Exception:
            value = await get_global_user_limit()
    else:
        value = await get_global_user_limit()

    _user_limit_cache[user_id] = (value, now + _USER_LIMIT_CACHE_TTL)
    return value


async def set_user_download_limit(user_id, limit, by_user_id):
    user_id = int(user_id)
    limit = max(1, int(limit))
    await user_limits_col.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "user_id": user_id,
                "limit": limit,
                "set_by": int(by_user_id),
                "updated_at": time.time(),
            }
        },
        upsert=True,
    )
    clear_user_limit_cache(user_id)
    return limit


async def log_activity(action, text, user_id=None, metadata=None):
    doc = {
        "action": action,
        "text": text,
        "user_id": int(user_id) if user_id else None,
        "metadata": metadata or {},
        "created_at": time.time(),
    }
    try:
        await activity_logs_col.insert_one(doc)
    except Exception:
        pass


async def record_download_usage(user, total_pages, cached=False):
    if not user:
        return

    user_id = int(user.id)
    pages = int(total_pages or 0)
    now = time.time()

    try:
        await usage_col.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "user_id": user_id,
                    "username": getattr(user, "username", None),
                    "first_name": getattr(user, "first_name", None),
                    "last_download_at": now,
                    "last_pages": pages,
                    "last_cached": bool(cached),
                },
                "$inc": {
                    "total_downloads": 1,
                    "total_pages": pages,
                    "cached_deliveries": 1 if cached else 0,
                    "fresh_downloads": 0 if cached else 1,
                },
                "$setOnInsert": {"first_download_at": now},
            },
            upsert=True,
        )
    except Exception:
        pass


async def get_user_usage(user_id):
    return await usage_col.find_one({"user_id": int(user_id)})


def build_job_label(job):
    name = job.get("first_name") or "User"
    username = job.get("username")
    user_id = job.get("user_id")
    if username:
        return f"{name} (@{username}) - `{user_id}`"
    return f"{name} - `{user_id}`"


def register_active_job(job_id, user, chat_id, url, page_range, status_msg=None):
    active_jobs[job_id] = {
        "job_id": job_id,
        "user_id": int(user.id),
        "username": getattr(user, "username", None),
        "first_name": getattr(user, "first_name", None),
        "chat_id": chat_id,
        "url": url,
        "range": page_range,
        "status": "Starting",
        "started_at": time.time(),
        "process": None,
        "cancel_requested": False,
        "cancel_event": asyncio.Event(),
        "status_msg": status_msg,
    }
    return active_jobs[job_id]


def update_job(job_id, **kwargs):
    job = active_jobs.get(job_id)
    if job:
        job.update(kwargs)
    return job


def remove_active_job(job_id):
    active_jobs.pop(job_id, None)


def build_queue_text():
    if not active_jobs:
        return "✅ **Queue is empty.**\n\nNo active downloads right now."

    lines = [f"📥 **Active Downloads:** `{len(active_jobs)}`\n"]
    now = time.time()

    for i, job in enumerate(sorted(active_jobs.values(), key=lambda item: item.get("started_at", 0)), start=1):
        elapsed = now - job.get("started_at", now)
        lines.append(
            f"**{i}.** {build_job_label(job)}\n"
            f"   🧾 **Status:** `{job.get('status', 'Unknown')}`\n"
            f"   🎯 **Range:** `{job.get('range', 'N/A')}`\n"
            f"   ⏱ **Elapsed:** `{format_time(elapsed)}`\n"
            f"   🔗 **URL:** {job.get('url', 'N/A')}"
        )

    return "\n\n".join(lines)


async def cancel_jobs_for_user(user_id):
    user_id = int(user_id)
    matches = [job for job in active_jobs.values() if int(job.get("user_id", 0)) == user_id]

    for job in matches:
        job["cancel_requested"] = True
        job["status"] = "Cancelling"

        cancel_event = job.get("cancel_event")
        if cancel_event:
            try:
                cancel_event.set()
            except Exception:
                pass

        process = job.get("process")
        if process and getattr(process, "returncode", None) is None:
            try:
                process.terminate()
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass

        status_msg = job.get("status_msg")
        if status_msg:
            try:
                await status_msg.edit_text("🚫 **Download cancelled by admin.**")
            except Exception:
                pass

    return len(matches)


async def get_cache_stats():
    total_galleries = await galleries_col.count_documents({})
    docs = await galleries_col.find({}, {"total_pages": 1, "file_ids": 1, "requested_by": 1}).to_list(length=None)

    total_files = 0
    user_cached_docs = 0
    for doc in docs:
        if doc.get("total_pages") is not None:
            try:
                total_files += int(doc.get("total_pages") or 0)
            except Exception:
                pass
        elif doc.get("file_ids"):
            total_files += len(doc.get("file_ids") or [])

        if doc.get("requested_by"):
            user_cached_docs += 1

    return {
        "total_galleries": total_galleries,
        "total_files": total_files,
        "with_requested_by": user_cached_docs,
    }


async def clear_user_cache(user_id):
    return await galleries_col.delete_many({"requested_by": int(user_id)})


async def get_recent_logs(limit=10):
    return await activity_logs_col.find().sort("created_at", -1).to_list(length=limit)


async def get_top_users(limit=10):
    return await usage_col.find().sort("total_pages", -1).to_list(length=limit)


async def restart_process_soon(delay=2):
    await asyncio.sleep(delay)
    os._exit(0)
