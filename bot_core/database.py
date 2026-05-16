"""MongoDB database helpers for the gallery bot.

The bot uses Motor, the async MongoDB driver. All common lookups are backed by
indexes so permission checks, cache checks, settings reads, and usage stats stay
fast even when the bot is busy in groups.
"""

import asyncio
from typing import Iterable, Tuple

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING


_INDEXES_READY = False
_INDEX_LOCK = asyncio.Lock()


def create_mongo_client(uri: str) -> AsyncIOMotorClient:
    if not uri:
        raise ValueError("MONGO_URI is missing.")

    return AsyncIOMotorClient(
        uri,
        maxPoolSize=80,
        minPoolSize=1,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=45000,
        retryWrites=True,
        uuidRepresentation="standard",
    )


async def _safe_create_index(collection, keys: Iterable[Tuple[str, int]], **kwargs):
    try:
        await collection.create_index(list(keys), background=True, **kwargs)
    except Exception as e:
        # Index creation should never stop the bot from starting. If an index
        # fails because old data has duplicates or the database is briefly busy,
        # the bot still runs and MongoDB can be cleaned/indexed later.
        print(f"Index warning for {collection.name}: {e}")


async def ensure_database_indexes(db):
    """Create performance indexes once per process.

    This is intentionally non-destructive. It does not delete or migrate old
    data, and it avoids unique indexes so existing duplicate records cannot break
    startup.
    """
    global _INDEXES_READY

    if _INDEXES_READY:
        return

    async with _INDEX_LOCK:
        if _INDEXES_READY:
            return

        await asyncio.gather(
            _safe_create_index(db.users, [("user_id", ASCENDING)]),
            _safe_create_index(db.users, [("joined_date", DESCENDING)]),

            _safe_create_index(db.galleries, [("url", ASCENDING), ("range", ASCENDING)]),
            _safe_create_index(db.galleries, [("requested_by", ASCENDING)]),
            _safe_create_index(db.galleries, [("created_at", DESCENDING)]),

            _safe_create_index(db.banned_users, [("user_id", ASCENDING)]),
            _safe_create_index(db.banned_users, [("banned_at", DESCENDING)]),

            _safe_create_index(db.sudo_users, [("user_id", ASCENDING)]),
            _safe_create_index(db.sudo_users, [("added_at", DESCENDING)]),

            _safe_create_index(db.settings, [("updated_at", DESCENDING)]),

            _safe_create_index(db.usage, [("user_id", ASCENDING)]),
            _safe_create_index(db.usage, [("total_pages", DESCENDING)]),
            _safe_create_index(db.usage, [("last_download_at", DESCENDING)]),

            _safe_create_index(db.activity_logs, [("created_at", DESCENDING)]),
            _safe_create_index(db.activity_logs, [("user_id", ASCENDING), ("created_at", DESCENDING)]),

            _safe_create_index(db.user_limits, [("user_id", ASCENDING)]),

            _safe_create_index(db.ai_galleries, [("url", ASCENDING)]),
            _safe_create_index(db.ai_galleries, [("status", ASCENDING)]),
            _safe_create_index(db.ai_galleries, [("posted_at", DESCENDING)]),
            _safe_create_index(db.ai_galleries, [("seen_at", DESCENDING)]),
        )

        _INDEXES_READY = True
        print("MongoDB indexes are ready.")
