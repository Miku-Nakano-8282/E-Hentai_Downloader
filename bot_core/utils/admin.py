import re
import time

from bot_core.config import OWNER_ID, banned_users_col, sudo_users_col


OWNER_ONLY_TEXT = "🚫 **Access denied.** You are not allowed to use this command."
BANNED_TEXT = "🚫 **You are banned from using this bot.**"

# Hot permission checks happen on almost every message. Cache them briefly so
# normal replies do not wait on the database every time.
_PERMISSION_CACHE_TTL = 30
_banned_cache = {}
_sudo_cache = {}


def _cache_get(cache, user_id):
    item = cache.get(int(user_id))
    if not item:
        return None
    value, expires_at = item
    if expires_at < time.time():
        cache.pop(int(user_id), None)
        return None
    return value


def _cache_set(cache, user_id, value):
    cache[int(user_id)] = (bool(value), time.time() + _PERMISSION_CACHE_TTL)


def clear_permission_cache(user_id=None):
    if user_id is None:
        _banned_cache.clear()
        _sudo_cache.clear()
        return
    _banned_cache.pop(int(user_id), None)
    _sudo_cache.pop(int(user_id), None)


def is_owner(user_id):
    return int(user_id or 0) == OWNER_ID


async def is_sudo_user(user_id):
    if is_owner(user_id):
        return True

    user_id = int(user_id or 0)
    cached = _cache_get(_sudo_cache, user_id)
    if cached is not None:
        return cached

    try:
        found = await sudo_users_col.find_one({"user_id": user_id})
        allowed = found is not None
    except Exception:
        allowed = False

    _cache_set(_sudo_cache, user_id, allowed)
    return allowed


async def is_privileged_user(user_id):
    return await is_sudo_user(user_id)


async def is_banned_user(user_id):
    if is_owner(user_id):
        return False

    user_id = int(user_id or 0)
    cached = _cache_get(_banned_cache, user_id)
    if cached is not None:
        return cached

    try:
        found = await banned_users_col.find_one({"user_id": user_id})
        banned = found is not None
    except Exception:
        # If DB check fails, don't accidentally lock everyone out.
        banned = False

    _cache_set(_banned_cache, user_id, banned)
    return banned


async def deny_if_banned(message):
    user = message.from_user
    if not user:
        return False

    if await is_banned_user(user.id):
        await message.reply_text(BANNED_TEXT)
        return True

    return False


def require_owner(message):
    user = message.from_user
    return bool(user and is_owner(user.id))


async def require_privileged(message):
    user = message.from_user
    return bool(user and await is_privileged_user(user.id))


def build_user_label(user_id, username=None, first_name=None):
    user_id = int(user_id)
    name_part = first_name or "User"
    if username:
        return f"{name_part} (@{username}) - `{user_id}`"
    return f"{name_part} - `{user_id}`"


def extract_target_user_id(message):
    """
    Get a target user ID from:
    1. A replied user's message
    2. First command argument: /ban 123456789 reason
    3. Telegram mention links like tg://user?id=123456789

    Returns: (target_user_id, remaining_text)
    """
    text = message.text or ""
    parts = text.split(maxsplit=2)

    if message.reply_to_message and message.reply_to_message.from_user:
        reason = ""
        if len(parts) >= 2:
            reason = text.split(maxsplit=1)[1]
        return int(message.reply_to_message.from_user.id), reason.strip()

    if len(parts) < 2:
        return None, ""

    target_raw = parts[1].strip()
    remaining = parts[2].strip() if len(parts) >= 3 else ""

    link_match = re.search(r"id=(\d+)", target_raw)
    if link_match:
        return int(link_match.group(1)), remaining

    digits = re.sub(r"\D", "", target_raw)
    if digits:
        return int(digits), remaining

    return None, remaining


async def get_user_basic_info(client, user_id):
    """Best-effort fetch for name/username. Works only when Telegram allows it."""
    info = {
        "user_id": int(user_id),
        "first_name": None,
        "last_name": None,
        "username": None,
    }

    try:
        chat = await client.get_chat(int(user_id))
        info["first_name"] = getattr(chat, "first_name", None) or getattr(chat, "title", None)
        info["last_name"] = getattr(chat, "last_name", None)
        info["username"] = getattr(chat, "username", None)
    except Exception:
        pass

    return info


async def add_ban(client, target_user_id, banned_by, reason=""):
    info = await get_user_basic_info(client, target_user_id)
    now = time.time()

    await banned_users_col.update_one(
        {"user_id": int(target_user_id)},
        {
            "$set": {
                **info,
                "banned_by": int(banned_by),
                "reason": reason or "No reason provided",
                "banned_at": now,
            }
        },
        upsert=True,
    )

    # If a banned user was sudo before, remove sudo access too.
    await sudo_users_col.delete_one({"user_id": int(target_user_id)})
    clear_permission_cache(target_user_id)
    _cache_set(_banned_cache, target_user_id, True)
    _cache_set(_sudo_cache, target_user_id, False)
    return info


async def remove_ban(target_user_id):
    result = await banned_users_col.delete_one({"user_id": int(target_user_id)})
    clear_permission_cache(target_user_id)
    _cache_set(_banned_cache, target_user_id, False)
    return result


async def add_sudo(client, target_user_id, added_by):
    info = await get_user_basic_info(client, target_user_id)
    now = time.time()

    await sudo_users_col.update_one(
        {"user_id": int(target_user_id)},
        {
            "$set": {
                **info,
                "added_by": int(added_by),
                "added_at": now,
            }
        },
        upsert=True,
    )

    # Sudo users should not remain banned.
    await banned_users_col.delete_one({"user_id": int(target_user_id)})
    clear_permission_cache(target_user_id)
    _cache_set(_sudo_cache, target_user_id, True)
    _cache_set(_banned_cache, target_user_id, False)
    return info


async def remove_sudo(target_user_id):
    result = await sudo_users_col.delete_one({"user_id": int(target_user_id)})
    clear_permission_cache(target_user_id)
    _cache_set(_sudo_cache, target_user_id, False)
    return result
