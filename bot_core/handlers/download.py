import asyncio
import os
import re
import shutil
import time

from pyrogram import filters

from bot_core.config import (
    BOT_COMMANDS,
    FAST_EHENTAI_ENABLED,
    GALLERY_DL_IDLE_TIMEOUT,
    GALLERY_REGEX,
    LOG_CHANNEL_ID,
    PROGRESS_EDIT_INTERVAL,
    active_downloads,
    app,
    galleries_col,
    user_states,
)
from bot_core.utils.admin import deny_if_banned, is_privileged_user
from bot_core.utils.control import (
    get_user_download_limit,
    is_maintenance_enabled,
    log_activity,
    record_download_usage,
    register_active_job,
    remove_active_job,
    update_job,
)
from bot_core.services.fast_ehentai_downloader import (
    FastEHentaiDownloadError,
    download_ehentai_gallery_fast,
    is_ehentai_gallery_url,
)
from bot_core.utils.images import process_single_image, sanitize_image_paths
from bot_core.utils.progress import (
    build_delivery_finished_text,
    build_download_status_text,
    build_progress_text,
    parse_expected_page_count,
    parse_gallery_dl_progress,
    scan_download_folder,
)
from bot_core.utils.telegram import safe_edit_text, safe_send_document, safe_send_message
from bot_core.utils.time_format import format_time


async def live_download_progress_updater(status_msg, progress_state, stop_event):
    """Update the Telegram status message while gallery-dl is running."""
    step = 0
    last_text = None

    while not stop_event.is_set():
        text = build_download_status_text(progress_state, step)

        if text != last_text:
            await safe_edit_text(status_msg, text)
            last_text = text

        step += 1
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PROGRESS_EDIT_INTERVAL)
        except asyncio.TimeoutError:
            continue


# ==========================================
#             CORE DOWNLOAD LOGIC
# ==========================================

# --- 1. Link Handler: asks for range ---
@app.on_message((filters.private | filters.group) & filters.regex(GALLERY_REGEX))
async def handle_gallery_link(client, message):
    text = message.text or message.caption or ""

    # A command can contain a gallery link, for example:
    # /postgallery force https://e-hentai.org/g/...
    # Do not treat command messages as normal download requests.
    if text.lstrip().startswith("/"):
        return

    if await deny_if_banned(message):
        return

    if not message.from_user:
        return

    user_id = message.from_user.id
    match = re.search(GALLERY_REGEX, text, flags=re.IGNORECASE)
    if not match:
        return
    url = match.group(0).rstrip("/")

    if await is_maintenance_enabled() and not await is_privileged_user(user_id):
        return await message.reply_text(
            "🛠 **Bot is under maintenance.**\n\n"
            "Please try again later."
        )

    if not await is_privileged_user(user_id):
        current_downloads = active_downloads.get(user_id, 0)
        user_limit = await get_user_download_limit(user_id)
        if current_downloads >= user_limit:
            return await message.reply_text(
                f"⏳ **Please wait!** You can only run `{user_limit}` download(s) at a time."
            )

    state_key = f"{message.chat.id}:{user_id}"
    user_states[state_key] = {"url": url}

    prompt_text = (
        "🔗 **Link accepted!**\n\n"
        "How many pages do you want to download?\n"
        "• Send `0` to download **All Pages**.\n"
        "• Send a range for specific pages (e.g., `1-10`, `!8`, `12, 14-20`, `30-40/2`).\n\n"
        "*(Send /cancel to abort)*"
    )
    await message.reply_text(prompt_text)


# --- 2. Range Input Handler: performs download ---
@app.on_message((filters.private | filters.group) & filters.text & ~filters.command(BOT_COMMANDS))
async def process_range_and_download(client, message):
    # Important for group speed: this handler sees every normal text message.
    # Return before any database/permission checks unless the user is actually
    # replying with a page range for a pending gallery.
    if not message.from_user:
        return

    user_id = message.from_user.id
    state_key = f"{message.chat.id}:{user_id}"

    if state_key not in user_states:
        return

    if await deny_if_banned(message):
        return

    if await is_maintenance_enabled() and not await is_privileged_user(user_id):
        del user_states[state_key]
        return await message.reply_text(
            "🛠 **Bot is under maintenance now.**\n\n"
            "Your pending download was cancelled. Please try again later."
        )

    page_range = message.text.strip()
    url = user_states[state_key]["url"]
    del user_states[state_key]

    active_downloads[user_id] = active_downloads.get(user_id, 0) + 1
    temp_dir = f"downloads/req_{message.chat.id}_{message.id}"
    status_msg = None
    job_started_at = time.time()
    job_id = f"{message.chat.id}_{message.id}"
    job = register_active_job(job_id, message.from_user, message.chat.id, url, page_range)

    try:
        # --- CACHE CHECK PHASE ---
        cache_query = {"url": url, "range": page_range}
        cached_gallery = await galleries_col.find_one(cache_query)

        if cached_gallery:
            status_msg = await message.reply_text("♻️ **Gallery & Range found in cache! Retrieving...**")
            update_job(job_id, status="Sending cached files", status_msg=status_msg)
            file_ids = cached_gallery["file_ids"]
            total = len(file_ids)
            started_at = time.time()

            for i, f_id in enumerate(file_ids):
                if job.get("cancel_requested"):
                    await safe_edit_text(status_msg, "🚫 **Cached delivery cancelled by admin.**")
                    await log_activity("cancelled", f"Cached delivery cancelled for user {user_id}", user_id=user_id)
                    return

                current = i + 1

                if current == 1 or current % 5 == 0 or current == total:
                    text = build_progress_text(
                        "♻️ **Sending cached files:**",
                        current,
                        total,
                        started_at,
                        unit="files"
                    )
                    await safe_edit_text(status_msg, text)

                await safe_send_document(client, chat_id=message.chat.id, document=f_id)
                await asyncio.sleep(0.5)

            cache_send_duration = time.time() - started_at
            total_duration = time.time() - job_started_at

            await status_msg.edit_text(f"✅ **Complete!**\nSuccessfully delivered {total} cached files instantly.")

            await record_download_usage(message.from_user, total, cached=True)
            await log_activity(
                "cached_delivery",
                f"Cached gallery delivered: {total} files to user {user_id}",
                user_id=user_id,
                metadata={"url": url, "range": page_range, "total_pages": total},
            )

            final_text = build_delivery_finished_text(
                url=url,
                page_range=page_range,
                total_pages=total,
                download_seconds=0,
                upload_seconds=cache_send_duration,
                total_seconds=total_duration,
                cached=True
            )
            await safe_send_message(client, message.chat.id, final_text, disable_web_page_preview=True)
            return

        # --- NEW DOWNLOAD PHASE WITH FAST PARALLEL MODE + GALLERY-DL FALLBACK ---
        os.makedirs(temp_dir, exist_ok=True)

        expected_total = parse_expected_page_count(page_range)
        started_at = time.time()

        progress_state = {
            "current": 0,
            "total": 0,
            "expected_total": expected_total,
            "folder_completed_images": 0,
            "folder_total_files": 0,
            "folder_total_bytes": 0,
            "started_at": started_at,
            "last_activity_at": started_at,
            "last_line": "Starting fast downloader...",
            "method": "Fast parallel mode",
        }

        status_msg = await message.reply_text(build_download_status_text(progress_state, 0))
        update_job(job_id, status="Downloading", status_msg=status_msg)

        stop_progress_event = asyncio.Event()
        progress_task = asyncio.create_task(
            live_download_progress_updater(status_msg, progress_state, stop_progress_event)
        )

        fast_download_succeeded = False
        fast_error_text = ""
        killed_for_idle = False

        # First try a browser/Tampermonkey-style downloader: collect image pages,
        # extract direct image URLs, then download several images at once.
        # If the site blocks this path or the gallery has unusual markup, we fall
        # back to gallery-dl automatically so the bot still works.
        if FAST_EHENTAI_ENABLED and is_ehentai_gallery_url(url):
            try:
                update_job(job_id, status="Fast parallel downloading")
                await download_ehentai_gallery_fast(
                    url=url,
                    page_range=page_range,
                    output_dir=temp_dir,
                    progress_state=progress_state,
                    cancel_event=job.get("cancel_event"),
                )
                fast_download_succeeded = True
            except asyncio.CancelledError:
                if job.get("cancel_requested"):
                    stop_progress_event.set()
                    try:
                        await progress_task
                    except Exception:
                        pass
                    await log_activity("cancelled", f"Fast download cancelled for user {user_id}", user_id=user_id)
                    return await status_msg.edit_text("🚫 **Download cancelled by admin.**")
                raise
            except FastEHentaiDownloadError as e:
                fast_error_text = str(e)
            except Exception as e:
                fast_error_text = str(e)

        if not fast_download_succeeded:
            if fast_error_text:
                progress_state["last_line"] = f"Fast mode failed: {fast_error_text}. Falling back to gallery-dl..."
                progress_state["method"] = "gallery-dl fallback"
                progress_state["last_activity_at"] = time.time()
                await safe_edit_text(status_msg, build_download_status_text(progress_state, 0))
                await asyncio.sleep(1)
                shutil.rmtree(temp_dir, ignore_errors=True)
                os.makedirs(temp_dir, exist_ok=True)

            # Reset visible counters for the fallback run.
            progress_state.update({
                "current": 0,
                "total": 0,
                "expected_total": expected_total,
                "folder_completed_images": 0,
                "folder_total_files": 0,
                "folder_total_bytes": 0,
                "last_activity_at": time.time(),
                "last_line": "Starting gallery-dl fallback..." if fast_error_text else "Starting gallery-dl...",
                "method": "gallery-dl fallback" if fast_error_text else "gallery-dl",
            })

            cmd = ["gallery-dl", "-d", temp_dir]
            if page_range != "0":
                cmd.extend(["--range", page_range])
            cmd.append(url)

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            update_job(job_id, process=process, status="Downloading with gallery-dl")

            last_seen_files = 0
            last_seen_bytes = 0

            while True:
                # Do not wait forever for a new gallery-dl line.
                # We also check the download folder every second.
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), timeout=1)
                except asyncio.TimeoutError:
                    line = None

                folder_stats = scan_download_folder(temp_dir)
                progress_state["folder_completed_images"] = folder_stats["completed_images"]
                progress_state["folder_total_files"] = folder_stats["total_files"]
                progress_state["folder_total_bytes"] = folder_stats["total_bytes"]

                if job.get("cancel_requested"):
                    progress_state["last_line"] = "Cancelled by admin."
                    try:
                        process.terminate()
                    except Exception:
                        pass
                    break

                if (
                    folder_stats["completed_images"] != last_seen_files
                    or folder_stats["total_bytes"] != last_seen_bytes
                ):
                    last_seen_files = folder_stats["completed_images"]
                    last_seen_bytes = folder_stats["total_bytes"]
                    progress_state["last_activity_at"] = time.time()
                    progress_state["last_line"] = "Files are being written to disk..."

                if line == b"":
                    # EOF: process has finished.
                    break

                if line:
                    text = line.decode("utf-8", errors="ignore").strip()
                    if text:
                        progress_state["last_line"] = text
                        progress_state["last_activity_at"] = time.time()

                    current, total = parse_gallery_dl_progress(text)
                    if current is not None and total is not None:
                        progress_state["current"] = current
                        progress_state["total"] = total

                # Timeout only when gallery-dl is still running and absolutely nothing is changing.
                if process.returncode is None:
                    idle_seconds = time.time() - progress_state.get("last_activity_at", started_at)
                    if idle_seconds >= GALLERY_DL_IDLE_TIMEOUT:
                        killed_for_idle = True
                        progress_state["last_line"] = f"No output or file changes for {GALLERY_DL_IDLE_TIMEOUT}s. Stopping gallery-dl."
                        try:
                            process.terminate()
                            await asyncio.wait_for(process.wait(), timeout=10)
                        except Exception:
                            try:
                                process.kill()
                            except Exception:
                                pass
                        break

            if process.returncode is None:
                await process.wait()

        # Final folder scan before stopping the live progress message.
        folder_stats = scan_download_folder(temp_dir)
        progress_state["folder_completed_images"] = folder_stats["completed_images"]
        progress_state["folder_total_files"] = folder_stats["total_files"]
        progress_state["folder_total_bytes"] = folder_stats["total_bytes"]

        stop_progress_event.set()
        try:
            await progress_task
        except Exception:
            pass

        if job.get("cancel_requested"):
            await log_activity("cancelled", f"Download cancelled for user {user_id}", user_id=user_id)
            return await status_msg.edit_text("🚫 **Download cancelled by admin.**")

        if killed_for_idle:
            await log_activity("failed", f"Download idle-timeout for user {user_id}", user_id=user_id, metadata={"url": url, "range": page_range})
            return await status_msg.edit_text(
                "❌ **Download stopped because gallery-dl was stuck.**\n\n"
                f"No output or file changes happened for `{GALLERY_DL_IDLE_TIMEOUT}s`.\n\n"
                "Possible reasons:\n"
                "• E-Hentai blocked/rate-limited the server\n"
                "• The gallery needs login/cookies\n"
                "• The gallery link/range is invalid\n"
                "• Network is too slow from the hosting server\n\n"
                f"🧾 **Last Status:** `{progress_state.get('last_line', 'N/A')}`"
            )

        if not fast_download_succeeded and 'process' in locals() and process.returncode != 0:
            await log_activity("failed", f"gallery-dl failed for user {user_id}", user_id=user_id, metadata={"url": url, "range": page_range})
            return await status_msg.edit_text(
                "❌ **Failed to download the gallery.**\n\n"
                "Please check the link, range, or gallery access permission.\n\n"
                f"🧾 **Last Status:** `{progress_state.get('last_line', 'N/A')}`"
            )

        download_finished_at = time.time()
        download_duration = download_finished_at - started_at

        update_job(job_id, status="Processing files")
        await status_msg.edit_text("⚡ **Processing high-quality files...**")

        # --- PROCESSING / CONVERTING PHASE ---
        conversion_tasks = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                filepath = os.path.join(root, file)
                conversion_tasks.append(asyncio.to_thread(process_single_image, filepath))

        processed_paths = await asyncio.gather(*conversion_tasks)
        image_paths = [p for p in processed_paths if p is not None]
        image_paths.sort()

        if not image_paths:
            await log_activity("failed", f"No valid images found for user {user_id}", user_id=user_id, metadata={"url": url, "range": page_range})
            return await status_msg.edit_text(
                "❌ **No valid images found for that range.**\n\n"
                "The site may have blocked the download, or the selected page range may not exist."
            )

        # --- RENAME / SANITIZE PHASE ---
        image_paths = sanitize_image_paths(image_paths)

        # --- ONE BY ONE UPLOAD PHASE ---
        total_images = len(image_paths)
        uploaded_file_ids = []
        upload_started_at = time.time()
        update_job(job_id, status="Uploading files")

        for i, img_path in enumerate(image_paths):
            if job.get("cancel_requested"):
                await safe_edit_text(status_msg, "🚫 **Upload cancelled by admin.**")
                await log_activity("cancelled", f"Upload cancelled for user {user_id}", user_id=user_id)
                return

            current = i + 1

            if current == 1 or current % 3 == 0 or current == total_images:
                text = build_progress_text(
                    "📤 **Uploading files:**",
                    current,
                    total_images,
                    upload_started_at,
                    unit="files"
                )
                await safe_edit_text(status_msg, text)

            sent_msg = await safe_send_document(
                client,
                chat_id=LOG_CHANNEL_ID,
                document=img_path
            )

            file_id = sent_msg.document.file_id
            uploaded_file_ids.append(file_id)

            await safe_send_document(
                client,
                chat_id=message.chat.id,
                document=file_id
            )

            await asyncio.sleep(0.5)

        upload_duration = time.time() - upload_started_at
        total_duration = time.time() - job_started_at

        # --- SAVE TO CACHE ---
        try:
            await galleries_col.update_one(
                {"url": url, "range": page_range},
                {
                    "$set": {
                        "url": url,
                        "range": page_range,
                        "file_ids": uploaded_file_ids,
                        "total_pages": total_images,
                        "requested_by": int(user_id),
                        "requested_by_username": message.from_user.username,
                        "updated_at": time.time(),
                    },
                    "$setOnInsert": {"created_at": time.time()},
                },
                upsert=True,
            )
        except Exception as e:
            print(f"Failed to cache gallery: {e}")

        log_text = (
            f"📁 **New Gallery Stored & Cached**\n"
            f"👤 **Requested By:** {message.from_user.first_name} (`{message.from_user.id}`)\n"
            f"🔗 **Link:** {url}\n"
            f"🎯 **Range:** {page_range}\n"
            f"🖼 **Pages Extracted:** {total_images}\n"
            f"⏬ **Download Time:** {format_time(download_duration)}\n"
            f"📤 **Upload/Send Time:** {format_time(upload_duration)}\n"
            f"⏱ **Total Time:** {format_time(total_duration)}"
        )
        await safe_send_message(client, LOG_CHANNEL_ID, log_text)
        await record_download_usage(message.from_user, total_images, cached=False)
        await log_activity(
            "download_finished",
            f"Fresh gallery delivered: {total_images} files to user {user_id}",
            user_id=user_id,
            metadata={"url": url, "range": page_range, "total_pages": total_images},
        )

        await status_msg.edit_text(f"✅ **Complete!**\nSuccessfully delivered {total_images} files.")

        final_text = build_delivery_finished_text(
            url=url,
            page_range=page_range,
            total_pages=total_images,
            download_seconds=download_duration,
            upload_seconds=upload_duration,
            total_seconds=total_duration,
            cached=False
        )
        await safe_send_message(client, message.chat.id, final_text, disable_web_page_preview=True)

    except Exception as e:
        await log_activity("error", f"Download error for user {user_id}: {str(e)}", user_id=user_id, metadata={"url": url, "range": page_range})
        if status_msg:
            await safe_edit_text(status_msg, f"❌ An error occurred: {str(e)}")
        else:
            await message.reply_text(f"❌ An error occurred: {str(e)}")

    finally:
        if user_id in active_downloads:
            active_downloads[user_id] -= 1
            if active_downloads[user_id] <= 0:
                del active_downloads[user_id]

        remove_active_job(job_id)

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
