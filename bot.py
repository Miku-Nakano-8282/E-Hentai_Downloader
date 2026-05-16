from pyrogram import idle

from bot_core.config import app, db
from bot_core.database import ensure_database_indexes

# Import handlers so Pyrogram registers all commands and message handlers.
from bot_core.handlers import user_commands  # noqa: F401
from bot_core.handlers import owner_commands  # noqa: F401
from bot_core.handlers import admin_commands  # noqa: F401
from bot_core.handlers import control_commands  # noqa: F401
from bot_core.handlers import ai_watch_commands  # noqa: F401
from bot_core.handlers import new_user_channel_commands  # noqa: F401
from bot_core.handlers import download  # noqa: F401
from bot_core.services.ai_gallery_watcher import start_ai_gallery_watcher, stop_ai_gallery_watcher, warmup_ai_watch_channel


async def main():
    print("Bot is starting...")
    await app.start()
    await ensure_database_indexes(db)
    await warmup_ai_watch_channel()
    await start_ai_gallery_watcher()
    print("Bot is running...")

    try:
        await idle()
    finally:
        await stop_ai_gallery_watcher()
        await app.stop()


if __name__ == "__main__":
    app.run(main())
