import os
import re
import time

from bot_core.config import IMAGE_EXTENSIONS
from bot_core.utils.time_format import format_time


def format_bytes(num_bytes):
    """Convert bytes into KB/MB/GB."""
    if num_bytes is None:
        return "0 B"

    num_bytes = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(num_bytes)} {unit}"
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024


def create_progress_bar(current, total, length=20):
    """Create a normal percentage progress bar."""
    if total <= 0:
        return "[░░░░░░░░░░░░░░░░░░░░] 0.0%"

    percent = max(0, min(1, current / total))
    filled = int(length * percent)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {percent * 100:.1f}%"


def create_waiting_bar(step, length=20):
    """Create an animated bar for unknown-size phases."""
    block_size = 5
    max_pos = max(1, length - block_size)
    pos = step % (max_pos + 1)
    bar = "░" * pos + "█" * block_size + "░" * (length - pos - block_size)
    return f"[{bar}]"


def estimate_remaining(current, total, started_at):
    """Estimate remaining time based on current progress."""
    if not started_at or current <= 0 or total <= 0 or current >= total:
        return None

    elapsed = time.time() - started_at
    speed = current / elapsed if elapsed > 0 else 0
    if speed <= 0:
        return None

    return (total - current) / speed


def calculate_item_speed(current, started_at, unit="files"):
    """Calculate item speed per minute."""
    if not started_at or current <= 0:
        return "calculating..."

    elapsed = time.time() - started_at
    if elapsed <= 0:
        return "calculating..."

    per_minute = current / elapsed * 60
    return f"{per_minute:.2f} {unit}/min"


def calculate_byte_speed(total_bytes, started_at):
    """Calculate average download speed from bytes."""
    if not started_at or total_bytes <= 0:
        return "calculating..."

    elapsed = time.time() - started_at
    if elapsed <= 0:
        return "calculating..."

    return f"{format_bytes(total_bytes / elapsed)}/s"


def parse_expected_page_count(page_range):
    """
    Estimate total requested pages from the user's range.

    Supports:
    - 195-236 => 42
    - 1-10 => 10
    - 12,14-20 => 8
    - 30-40/2 => 6

    Returns None for:
    - 0/all pages
    - exclusion ranges like !8
    - any format that cannot be safely counted
    """
    page_range = (page_range or "").strip().replace(" ", "")

    if not page_range or page_range == "0" or "!" in page_range:
        return None

    total = 0
    parts = [p for p in page_range.split(",") if p]

    for part in parts:
        single_match = re.fullmatch(r"\d+", part)
        if single_match:
            total += 1
            continue

        range_match = re.fullmatch(r"(\d+)-(\d+)(?:/(\d+))?", part)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            step = int(range_match.group(3) or 1)

            if start <= 0 or end < start or step <= 0:
                return None

            total += ((end - start) // step) + 1
            continue

        return None

    return total if total > 0 else None


def parse_gallery_dl_progress(line):
    """
    Try to read current/total progress from gallery-dl output.
    Supports common formats like: # 5 / 30, 5/30, [5/30].
    """
    patterns = [
        r"#\s*(\d+)\s*/\s*(\d+)",
        r"\[(\d+)\s*/\s*(\d+)\]",
        r"\b(\d+)\s*/\s*(\d+)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            current, total = int(match.group(1)), int(match.group(2))
            if total > 0 and 0 <= current <= total:
                return current, total

    return None, None


def scan_download_folder(folder):
    """
    Read real progress from the download folder.
    This fixes the problem where gallery-dl does not print usable progress lines.
    """
    completed_images = 0
    total_files = 0
    total_bytes = 0

    if not os.path.exists(folder):
        return {
            "completed_images": 0,
            "total_files": 0,
            "total_bytes": 0,
        }

    for root, dirs, files in os.walk(folder):
        for file in files:
            filepath = os.path.join(root, file)
            try:
                total_files += 1
                total_bytes += os.path.getsize(filepath)

                ext = file.lower().rsplit(".", 1)[-1] if "." in file else ""
                if ext in IMAGE_EXTENSIONS and not file.endswith(".part"):
                    completed_images += 1
            except OSError:
                continue

    return {
        "completed_images": completed_images,
        "total_files": total_files,
        "total_bytes": total_bytes,
    }


def build_progress_text(title, current, total, started_at, unit="files", extra_line=None):
    """Build a full progress message with ETA."""
    bar = create_progress_bar(current, total)
    elapsed = time.time() - started_at if started_at else 0
    remaining = estimate_remaining(current, total, started_at)
    speed = calculate_item_speed(current, started_at, unit=unit)

    text = (
        f"{title}\n\n"
        f"`{bar}`\n"
        f"📊 **Progress:** `{current}/{total}` {unit}\n"
        f"⚡ **Speed:** `{speed}`\n"
        f"⏱ **Elapsed:** `{format_time(elapsed)}`\n"
        f"⌛ **Time Left:** `{format_time(remaining)}`"
    )

    if extra_line:
        text += f"\n\n{extra_line}"

    return text


def build_delivery_finished_text(url, page_range, total_pages, download_seconds=None, upload_seconds=None, total_seconds=None, cached=False):
    """Build the final message with clear time stats."""
    if cached:
        download_time_text = "Cache hit — no new download needed"
    else:
        download_time_text = format_time(download_seconds)

    upload_time_text = format_time(upload_seconds)
    total_time_text = format_time(total_seconds)

    return (
        f"🎉 **Delivery Finished!**\n\n"
        f"🔗 **Source:** {url}\n"
        f"📄 **Pages Delivered:** {total_pages} (Range: {page_range})\n\n"
        f"⏬ **Download Time:** `{download_time_text}`\n"
        f"📤 **Upload/Send Time:** `{upload_time_text}`\n"
        f"⏱ **Total Time:** `{total_time_text}`\n\n"
        f"*Enjoy!*"
    )


def build_download_status_text(progress_state, step):
    """Build live status for the gallery-dl phase."""
    started_at = progress_state.get("started_at", time.time())
    elapsed = time.time() - started_at

    folder_count = progress_state.get("folder_completed_images", 0)
    folder_bytes = progress_state.get("folder_total_bytes", 0)
    expected_total = progress_state.get("expected_total")
    output_current = progress_state.get("current", 0)
    output_total = progress_state.get("total", 0)

    # Prefer gallery-dl's own total if it gives one. Otherwise use page range count.
    total = output_total or expected_total
    current = max(output_current, folder_count)

    if total and total > 0:
        title = "📥 **Downloading Gallery:**"
        bar = create_progress_bar(current, total)
        speed = calculate_item_speed(current, started_at, unit="pages")
        remaining = estimate_remaining(current, total, started_at)
        progress = f"`{current}/{total}` pages"
    else:
        # Unknown total: still show real folder progress instead of staying stuck.
        title = "📥 **Preparing / Downloading Gallery:**"
        bar = create_waiting_bar(step)
        speed = calculate_byte_speed(folder_bytes, started_at)
        remaining = None
        progress = f"`{folder_count}` files found"

    last_line = progress_state.get("last_line") or "Watching download folder..."
    safe_line = last_line[-140:]

    idle_seconds = int(time.time() - progress_state.get("last_activity_at", started_at))
    method = progress_state.get("method")
    method_line = f"🚀 **Mode:** `{method}`\n" if method else ""

    text = (
        f"{title}\n\n"
        f"`{bar}`\n"
        f"📊 **Progress:** {progress}\n"
        f"💾 **Downloaded:** `{format_bytes(folder_bytes)}`\n"
        f"⚡ **Speed:** `{speed}`\n"
        f"⏱ **Elapsed:** `{format_time(elapsed)}`\n"
        f"⌛ **Time Left:** `{format_time(remaining)}`\n"
        f"{method_line}\n"
        f"🧾 **Status:** `{safe_line}`\n"
        f"🕒 **No-change timer:** `{format_time(idle_seconds)}`"
    )

    if not total:
        text += "\n\nℹ️ **Note:** `Total pages unknown until gallery-dl reports it or files appear.`"

    return text
