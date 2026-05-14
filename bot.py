import os
import shutil
import asyncio
import threading
import re
import time
import math
from http.server import BaseHTTPRequestHandler, HTTPServer
from pyrogram import Client, filters
from pyrogram.types import InputMediaDocument
from PIL import Image
from motor.motor_asyncio import AsyncIOMotorClient

# --- 1. Dummy Web Server (Keeps HF Space Alive) ---
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Telegram Bot is running smoothly on Hugging Face!")

def run_dummy_server():
    server = HTTPServer(('0.0.0.0', 7860), DummyHandler)
    server.serve_forever()

threading.Thread(target=run_dummy_server, daemon=True).start()

# --- 2. Configuration & Secrets ---
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
OWNER_ID = int(os.environ.get("OWNER_ID", 0))
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))

if not all([API_ID, API_HASH, BOT_TOKEN, MONGO_URI, OWNER_ID, LOG_CHANNEL_ID]):
    raise ValueError("Missing credentials! Please set all required Hugging Face Secrets.")

# --- 3. Database Initialization ---
db_client = AsyncIOMotorClient(MONGO_URI)
db = db_client["telegram_gallery_bot"]
users_col = db["users"]
galleries_col = db["galleries"] 

app = Client("gallery_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

GALLERY_REGEX = r"https?://e-hentai\.org/g/[0-9]+/[a-z0-9]+/??"

# --- State Management ---
active_downloads = {}  
user_states = {} 
START_TIME = time.time() 

# --- Helpers ---
def create_progress_bar(current, total, length=20):
    if total == 0: return "[░░░░░░░░░░░░░░░░░░░░] 0%"
    percent = current / total
    filled = int(length * percent)
    bar = '█' * filled + '░' * (length - filled)
    return f"[{bar}] {percent:.1%}\n({current}/{total})"

def process_single_image(filepath):
    if os.path.getsize(filepath) == 0: return None
    ext = filepath.lower().split('.')[-1]
    
    if ext in ['webp', 'gif', 'bmp']:
        new_path = f"{os.path.splitext(filepath)[0]}.jpg"
        try:
            img = Image.open(filepath).convert("RGB")
            img.save(new_path, "JPEG", quality=95, subsampling=0)
            return new_path
        except:
            return None
    elif ext in ['jpg', 'jpeg', 'png']:
        return filepath
    return None

# --- Commands ---

@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    user = message.from_user
    
    welcome_text = (
        "👋 **Welcome to the Gallery Downloader!**\n\n"
        "Send me a supported gallery link, and I will extract, convert, and upload every page for you perfectly intact.\n\n"
        "*(Note: You can only process one link at a time to prevent server overload.)*"
    )
    await message.reply_text(welcome_text)

    try:
        existing_user = await users_col.find_one({"user_id": user.id})
        if not existing_user:
            await users_col.insert_one({
                "user_id": user.id,
                "username": user.username,
                "first_name": user.first_name,
                "joined_date": time.time()
            })
            
            try:
                full_user = await client.get_chat(user.id)
                bio = full_user.bio if full_user.bio else "No bio provided"
            except:
                bio = "Hidden / Not available"

            name = f"{user.first_name} {user.last_name or ''}".strip()
            log_caption = (
                f"👤 **New User Registered**\n\n"
                f"**Name:** {name}\n"
                f"**Username:** @{user.username if user.username else 'N/A'}\n"
                f"**ID:** `{user.id}`\n"
                f"**Bio:** {bio}"
            )
            
            if user.photo:
                try:
                    await client.send_photo(LOG_CHANNEL_ID, photo=user.photo.big_file_id, caption=log_caption)
                except:
                    await client.send_message(LOG_CHANNEL_ID, log_caption)
            else:
                await client.send_message(LOG_CHANNEL_ID, log_caption)
    except Exception as e:
        print(f"Database error during /start: {e}")

@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID) & filters.private)
async def broadcast_command(client, message):
    if not message.reply_to_message:
        return await message.reply_text("❌ Please reply to the message you want to broadcast.")
    
    msg = await message.reply_text("⏳ **Broadcasting message...**")
    
    try:
        users = await users_col.find().to_list(length=None)
        success, failed = 0, 0
        for u in users:
            try:
                await message.reply_to_message.copy(u["user_id"])
                success += 1
            except:
                failed += 1
            await asyncio.sleep(0.1) 
            
        await msg.edit_text(f"✅ **Broadcast Complete!**\n\n🎯 Success: {success}\n❌ Failed: {failed}")
    except Exception as e:
        await msg.edit_text(f"❌ Database Error: Could not retrieve users.")

@app.on_message(filters.command("stats") & filters.user(OWNER_ID) & filters.private)
async def stats_command(client, message):
    uptime = time.time() - START_TIME
    uptime_str = time.strftime('%Hh %Mm %Ss', time.gmtime(uptime))
    
    try:
        total_users = await users_col.count_documents({})
        cached_galleries = await galleries_col.count_documents({})
    except:
        total_users = "Error"
        cached_galleries = "Error"

    stats_text = (
        f"📊 **System Statistics**\n\n"
        f"👥 **Total Users:** {total_users}\n"
        f"🗂 **Cached Galleries:** {cached_galleries}\n"
        f"⏱ **Uptime:** {uptime_str}\n"
    )
    
    await message.reply_text(stats_text)

# --- 1. Link Handler (Asks for Range) ---

@app.on_message(filters.private & filters.regex(GALLERY_REGEX))
async def handle_gallery_link(client, message):
    user_id = message.from_user.id
    url = message.text.strip().rstrip('/') 
    
    if user_id != OWNER_ID:
        current_downloads = active_downloads.get(user_id, 0)
        if current_downloads >= 1:
            return await message.reply_text("⏳ **Please wait!** You can only download one gallery at a time.")
            
    # Save the URL in the user's state
    user_states[user_id] = {"url": url}
    
    prompt_text = (
        "🔗 **Link accepted!**\n\n"
        "How many pages do you want to download?\n"
        "• Send `0` to download **All Pages**.\n"
        "• Send a range for specific pages (e.g., `1-10`, `!8`, `12, 14-20`, `30-40/2`).\n\n"
        "*(Send `cancel` to abort)*"
    )
    await message.reply_text(prompt_text)

# --- 2. Range Input Handler (Performs the Download) ---

@app.on_message(filters.private & filters.text & ~filters.command(["start", "broadcast", "stats"]))
async def process_range_and_download(client, message):
    user_id = message.from_user.id
    
    # Check if the user is currently being asked for a range
    if user_id not in user_states:
        return
        
    page_range = message.text.strip()
    url = user_states[user_id]["url"]
    
    if page_range.lower() == 'cancel':
        del user_states[user_id]
        return await message.reply_text("🚫 **Download cancelled.**")
        
    # Clear the state so they don't trigger this again by accident
    del user_states[user_id]
    
    active_downloads[user_id] = active_downloads.get(user_id, 0) + 1
    temp_dir = f"downloads/req_{message.chat.id}_{message.id}"
    
    try:
        # --- CACHE CHECK PHASE (Now includes range) ---
        cache_query = {"url": url, "range": page_range}
        cached_gallery = await galleries_col.find_one(cache_query)
        
        if cached_gallery:
            status_msg = await message.reply_text("♻️ **Gallery & Range found in cache! Retrieving...**")
            file_ids = cached_gallery["file_ids"]
            total = len(file_ids)
            
            for i, f_id in enumerate(file_ids):
                if i % 5 == 0 or i == total - 1:
                    bar = create_progress_bar(i + 1, total)
                    try: await status_msg.edit_text(f"♻️ **Sending cached files:**\n{bar}")
                    except: pass
                
                await client.send_document(chat_id=message.chat.id, document=f_id)
                await asyncio.sleep(0.5) 
                
            await status_msg.edit_text(f"✅ **Complete!**\nSuccessfully delivered {total} cached files instantly.")
            
            final_text = (
                f"🎉 **Delivery Finished!**\n\n"
                f"🔗 **Source:** {url}\n"
                f"📄 **Pages Delivered:** {total} (Range: {page_range})\n\n"
                f"*Enjoy!*"
            )
            await client.send_message(
                message.chat.id,
                final_text,
                disable_web_page_preview=True
            )
            return

        # --- NEW DOWNLOAD PHASE ---
        status_msg = await message.reply_text("⏳ **Initializing download...**")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Build the gallery-dl command
        cmd = ["gallery-dl", "-d", temp_dir]
        if page_range != "0":
            cmd.extend(["--range", page_range])
        cmd.append(url)
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        
        last_update_time = time.time()
        
        while True:
            line = await process.stdout.readline()
            if not line: break
                
            text = line.decode('utf-8', errors='ignore').strip()
            match = re.search(r'#\s*(\d+)\s*/\s*(\d+)', text)
            
            if match:
                current, total = int(match.group(1)), int(match.group(2))
                if time.time() - last_update_time > 3:
                    bar = create_progress_bar(current, total)
                    try: await status_msg.edit_text(f"📥 **Downloading Gallery:**\n{bar}")
                    except: pass 
                    last_update_time = time.time()
        
        await process.wait()
        
        if process.returncode != 0:
            return await status_msg.edit_text("❌ Failed to download the gallery.")

        await status_msg.edit_text("⚡ **Processing high-quality files...**")

        conversion_tasks = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                filepath = os.path.join(root, file)
                conversion_tasks.append(asyncio.to_thread(process_single_image, filepath))
        
        processed_paths = await asyncio.gather(*conversion_tasks)
        image_paths = [p for p in processed_paths if p is not None]
        image_paths.sort()

        if not image_paths:
            return await status_msg.edit_text("❌ No valid images found for that range.")

        # --- SANITIZE FILENAMES PHASE ---
        clean_paths = []
        for old_path in image_paths:
            dir_name = os.path.dirname(old_path)
            original_filename = os.path.basename(old_path)
            
            # Strip out emojis/special characters to prevent Telegram crashes,
            # but KEEP the original page numbers intact!
            safe_filename = re.sub(r'[^a-zA-Z0-9_.-]', '', original_filename)
            
            # Fallback just in case the name becomes empty
            if not safe_filename or safe_filename.startswith('.'):
                safe_filename = f"recovered_page_{int(time.time())}.jpg"
                
            new_path = os.path.join(dir_name, safe_filename)
            
            if old_path != new_path:
                shutil.move(old_path, new_path)
            clean_paths.append(new_path)
            
        image_paths = clean_paths

        # --- ONE BY ONE UPLOAD PHASE ---
        total_images = len(image_paths)
        uploaded_file_ids = []

        for i, img_path in enumerate(image_paths):
            current = i + 1
            
            if current % 3 == 0 or current == 1 or current == total_images:
                bar = create_progress_bar(current, total_images)
                try: await status_msg.edit_text(f"📤 **Uploading files:**\n{bar}")
                except: pass

            sent_msg = await client.send_document(
                chat_id=LOG_CHANNEL_ID,
                document=img_path
            )
            
            file_id = sent_msg.document.file_id
            uploaded_file_ids.append(file_id)
            
            await client.send_document(
                chat_id=message.chat.id,
                document=file_id
            )
            
            await asyncio.sleep(0.5) 

        # --- SAVE TO CACHE ---
        try:
            await galleries_col.insert_one({
                "url": url,
                "range": page_range,
                "file_ids": uploaded_file_ids,
                "total_pages": total_images
            })
        except Exception as e:
            print(f"Failed to cache gallery: {e}")
            
        log_text = (
            f"📁 **New Gallery Stored & Cached**\n"
            f"👤 **Requested By:** {message.from_user.first_name} (`{message.from_user.id}`)\n"
            f"🔗 **Link:** {url}\n"
            f"🎯 **Range:** {page_range}\n"
            f"🖼 **Pages Extracted:** {total_images}"
        )
        await client.send_message(LOG_CHANNEL_ID, log_text)

        await status_msg.edit_text(f"✅ **Complete!**\nSuccessfully delivered {total_images} files.")
        
        final_text = (
            f"🎉 **Delivery Finished!**\n\n"
            f"🔗 **Source:** {url}\n"
            f"📄 **Pages Delivered:** {total_images} (Range: {page_range})\n\n"
            f"*Enjoy!*"
        )
        await client.send_message(
            message.chat.id,
            final_text,
            disable_web_page_preview=True
        )

    except Exception as e:
        await status_msg.edit_text(f"❌ An error occurred: {str(e)}")
        
    finally:
        active_downloads[user_id] -= 1
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    print("Bot is running...")
    app.run()
