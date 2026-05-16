import os
import time

from pyrogram import Client

from bot_core.database import create_mongo_client


# --- Configuration & Secrets ---
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "telegram_gallery_bot")
OWNER_ID = int(os.environ.get("OWNER_ID", 0))
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))


def parse_chat_reference(value, default=0):
    raw = str(value if value not in (None, "") else default).strip()
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


AI_WATCH_CHANNEL_ID = parse_chat_reference(os.environ.get("AI_WATCH_CHANNEL_ID"), LOG_CHANNEL_ID or 0)
NEW_USER_CHANNEL_ID = int(os.environ.get("NEW_USER_CHANNEL_ID", LOG_CHANNEL_ID or 0))

if not all([API_ID, API_HASH, BOT_TOKEN, MONGO_URI, OWNER_ID, LOG_CHANNEL_ID]):
    raise ValueError(
        "Missing credentials! Please set API_ID, API_HASH, BOT_TOKEN, "
        "MONGO_URI, OWNER_ID, and LOG_CHANNEL_ID."
    )


# --- Database Initialization ---
db_client = create_mongo_client(MONGO_URI)
db = db_client[MONGO_DB_NAME]
users_col = db["users"]
galleries_col = db["galleries"]
banned_users_col = db["banned_users"]
sudo_users_col = db["sudo_users"]
settings_col = db["settings"]
usage_col = db["usage"]
activity_logs_col = db["activity_logs"]
user_limits_col = db["user_limits"]
ai_galleries_col = db["ai_galleries"]


# --- Pyrogram App ---
app = Client("gallery_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


# --- Constants ---
GALLERY_REGEX = r"https?://e-hentai\.org/g/[0-9]+/[a-z0-9]+/??"
START_TIME = time.time()

# If gallery-dl gives no output and no file/size change for this many seconds, stop it.
GALLERY_DL_IDLE_TIMEOUT = int(os.environ.get("GALLERY_DL_IDLE_TIMEOUT", "300"))
PROGRESS_EDIT_INTERVAL = 3
IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "webp", "gif", "bmp"}


# Upload/send safety. This does not bypass Telegram limits; it handles FloodWait
# properly so the bot stays alive and responsive instead of crashing.
TELEGRAM_SEND_RETRIES = max(1, int(os.environ.get("TELEGRAM_SEND_RETRIES", "3")))
TELEGRAM_SEND_DELAY = max(0.0, float(os.environ.get("TELEGRAM_SEND_DELAY", "0.35")))
TELEGRAM_BROADCAST_DELAY = max(0.0, float(os.environ.get("TELEGRAM_BROADCAST_DELAY", "0.08")))
TELEGRAM_MAX_FLOOD_WAIT = max(30, int(os.environ.get("TELEGRAM_MAX_FLOOD_WAIT", "900")))


# Fast E-Hentai downloader. It tries controlled parallel downloads before falling
# back to gallery-dl. Keep workers modest to avoid server-side throttling.
FAST_EHENTAI_ENABLED = os.environ.get("FAST_EHENTAI_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
FAST_EHENTAI_PAGE_WORKERS = max(1, min(8, int(os.environ.get("FAST_EHENTAI_PAGE_WORKERS", "4"))))
FAST_EHENTAI_DOWNLOAD_WORKERS = max(1, min(8, int(os.environ.get("FAST_EHENTAI_DOWNLOAD_WORKERS", "4"))))
FAST_EHENTAI_RETRIES = max(1, int(os.environ.get("FAST_EHENTAI_RETRIES", "3")))
FAST_EHENTAI_TIMEOUT = max(10, int(os.environ.get("FAST_EHENTAI_TIMEOUT", "45")))
FAST_EHENTAI_INDEX_SCAN_LIMIT = max(1, int(os.environ.get("FAST_EHENTAI_INDEX_SCAN_LIMIT", "500")))
EHENTAI_COOKIE = os.environ.get("EHENTAI_COOKIE", "").strip()
EHENTAI_USER_AGENT = os.environ.get(
    "EHENTAI_USER_AGENT",
    os.environ.get(
        "AI_WATCH_USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
)

# E-Hentai AI-generated gallery watcher.
# The watcher uses LOG_CHANNEL_ID by default. Set AI_WATCH_CHANNEL_ID if you want a separate channel.
AI_WATCH_ENABLED_BY_DEFAULT = os.environ.get("AI_WATCH_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
AI_WATCH_INTERVAL = max(60, int(os.environ.get("AI_WATCH_INTERVAL", "300")))
AI_WATCH_MAX_RESULTS = max(1, int(os.environ.get("AI_WATCH_MAX_RESULTS", "15")))
AI_WATCH_MAX_POSTS_PER_CHECK = max(1, int(os.environ.get("AI_WATCH_MAX_POSTS_PER_CHECK", "5")))
AI_WATCH_POST_EXISTING_ON_FIRST_RUN = os.environ.get("AI_WATCH_POST_EXISTING_ON_FIRST_RUN", "false").lower() in {"1", "true", "yes", "on"}

# If there are no fresh/new AI-generated uploads, the watcher can keep the channel active
# by posting older unposted AI-generated gallery results.
AI_WATCH_OLD_ENABLED_BY_DEFAULT = os.environ.get("AI_WATCH_OLD_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
AI_WATCH_OLD_SCAN_PAGES = max(1, int(os.environ.get("AI_WATCH_OLD_SCAN_PAGES", "3")))
AI_WATCH_OLD_POSTS_PER_IDLE_CHECK = max(1, int(os.environ.get("AI_WATCH_OLD_POSTS_PER_IDLE_CHECK", "1")))
AI_WATCH_SEARCH_URL = os.environ.get("AI_WATCH_SEARCH_URL", "https://e-hentai.org/?f_search=ai+generated")
AI_WATCH_USER_AGENT = os.environ.get(
    "AI_WATCH_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


# --- State Management ---
active_downloads = {}
active_jobs = {}
user_states = {}


# Keep this list updated so command messages are never treated as page-range replies.
BOT_COMMANDS = [
    "start", "help", "cancel",
    "broadcast", "broadcaststats", "stats", "ping", "speedtest", "delcache",
    "ban", "unban", "banlist", "addsudo", "delsudo", "rmsudo", "sudolist",
    "adminhelp", "ownerhelp", "sudohelp",
    "queue", "canceluser", "setlimit", "userinfo", "cacheinfo", "clearusercache",
    "logs", "maintenance", "notice", "restart", "usage", "topusers",
    "aiwatch", "aiwatchnow", "aichannel", "postgallery",
    "chatid", "newuserchannel",
]
