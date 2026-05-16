import os
import re
import shutil
import time

from PIL import Image


def process_single_image(filepath):
    if os.path.getsize(filepath) == 0:
        return None

    ext = filepath.lower().split(".")[-1]

    if ext in ["webp", "gif", "bmp"]:
        new_path = f"{os.path.splitext(filepath)[0]}.jpg"
        try:
            img = Image.open(filepath).convert("RGB")
            img.save(new_path, "JPEG", quality=95, subsampling=0)
            return new_path
        except Exception:
            return None

    if ext in ["jpg", "jpeg", "png"]:
        return filepath

    return None


def sanitize_image_paths(image_paths):
    """Rename files to Telegram-safe names and avoid duplicate sanitized names."""
    clean_paths = []
    used_filenames = set()

    for index, old_path in enumerate(image_paths, start=1):
        dir_name = os.path.dirname(old_path)
        original_filename = os.path.basename(old_path)

        safe_filename = re.sub(r"[^a-zA-Z0-9_.-]", "", original_filename)
        if not safe_filename or safe_filename.startswith("."):
            safe_filename = f"recovered_page_{index}_{int(time.time())}.jpg"

        # Prevent accidental overwrite if two sanitized names become identical.
        base, ext = os.path.splitext(safe_filename)
        candidate = safe_filename
        suffix = 1
        while candidate.lower() in used_filenames:
            candidate = f"{base}_{suffix}{ext}"
            suffix += 1

        used_filenames.add(candidate.lower())
        new_path = os.path.join(dir_name, candidate)

        if old_path != new_path:
            shutil.move(old_path, new_path)

        clean_paths.append(new_path)

    return clean_paths
