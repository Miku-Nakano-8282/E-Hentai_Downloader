import os
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from html import unescape

from PIL import Image, ImageOps

from bot_core.config import AI_WATCH_USER_AGENT


GALLERY_URL_RE = re.compile(r"https?://e-hentai\.org/g/(\d+)/([a-z0-9]+)/?", re.IGNORECASE)


def strip_html(value):
    if not value:
        return ""

    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def fetch_text(url, timeout=30, referer=None):
    headers = {
        "User-Agent": AI_WATCH_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="ignore")


def download_binary(url, referer=None, suffix=".jpg", timeout=30, max_bytes=8 * 1024 * 1024):
    headers = {
        "User-Agent": AI_WATCH_USER_AGENT,
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        data = response.read(max_bytes + 1)

    if len(data) > max_bytes:
        raise ValueError("Image is larger than the safety limit")

    fd, path = tempfile.mkstemp(prefix="ai_gallery_preview_raw_", suffix=suffix)
    with os.fdopen(fd, "wb") as f:
        f.write(data)

    return path


def prepare_telegram_photo(image_path, *, max_side=2560, max_bytes=9 * 1024 * 1024):
    """Convert any downloaded preview into a Telegram-friendly high-quality JPEG.

    Telegram photo uploads can fail when the source image is too large, has an
    unsupported format, huge dimensions, transparency, or odd animation data.
    This function keeps the preview sharp but compresses it safely below the
    photo limit so the channel post is an actual image, not just a link preview.
    """
    if not image_path or not os.path.exists(image_path):
        raise FileNotFoundError("Preview image file was not created")

    with Image.open(image_path) as img:
        img = ImageOps.exif_transpose(img)

        # Animated files: use the first frame as the channel preview.
        try:
            img.seek(0)
        except Exception:
            pass

        if img.mode not in ("RGB", "L"):
            # Preserve transparent PNG/WebP by compositing on white before JPEG.
            if "A" in img.getbands():
                bg = Image.new("RGB", img.size, (255, 255, 255))
                bg.paste(img, mask=img.getchannel("A"))
                img = bg
            else:
                img = img.convert("RGB")
        elif img.mode == "L":
            img = img.convert("RGB")

        width, height = img.size
        largest_side = max(width, height)
        if largest_side > max_side:
            scale = max_side / float(largest_side)
            new_size = (max(1, int(width * scale)), max(1, int(height * scale)))
            img = img.resize(new_size, Image.LANCZOS)

        fd, prepared_path = tempfile.mkstemp(prefix="ai_gallery_preview_ready_", suffix=".jpg")
        os.close(fd)

        # Try high quality first, then step down only if Telegram's size limit is exceeded.
        for quality in (95, 92, 88, 84, 80, 75, 70):
            img.save(prepared_path, "JPEG", quality=quality, optimize=True, progressive=True)
            if os.path.getsize(prepared_path) <= max_bytes:
                return prepared_path

        # Last resort: reduce dimensions once more and save at a safe quality.
        width, height = img.size
        scale = 1800 / float(max(width, height)) if max(width, height) > 1800 else 1
        if scale < 1:
            img = img.resize((max(1, int(width * scale)), max(1, int(height * scale))), Image.LANCZOS)
        img.save(prepared_path, "JPEG", quality=72, optimize=True, progressive=True)
        return prepared_path


def extract_gallery_id(url):
    match = GALLERY_URL_RE.search(url or "")
    if not match:
        return None
    return f"{match.group(1)}/{match.group(2)}"


def normalize_gallery_url(url):
    match = GALLERY_URL_RE.search(url or "")
    if not match:
        return None
    return f"https://e-hentai.org/g/{match.group(1)}/{match.group(2)}/"


def absolutize_url(url, base_url="https://e-hentai.org/"):
    if not url:
        return ""

    url = unescape(str(url).strip().strip('"\''))
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return urllib.parse.urljoin(base_url, url)




def build_search_page_url(search_url, page):
    """Return an E-Hentai search URL for a specific result page.

    E-Hentai search pagination is usually zero-based: page=0 is the first page,
    page=1 is the second page. Keeping page=0 out of the URL avoids changing
    custom search URLs unnecessarily.
    """
    try:
        page = int(page)
    except Exception:
        page = 0

    if page <= 0:
        return search_url

    parsed = urllib.parse.urlsplit(search_url)
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page)
    new_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))


def extract_gallery_urls(search_html):
    urls = []
    seen = set()

    for match in GALLERY_URL_RE.finditer(search_html or ""):
        url = normalize_gallery_url(match.group(0))
        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def _extract_id_text(html, element_id):
    pattern = rf'<[^>]+id=["\']{re.escape(element_id)}["\'][^>]*>(.*?)</[^>]+>'
    match = re.search(pattern, html or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return strip_html(match.group(1))


def _extract_div_block(html, element_id):
    pattern = rf'<div[^>]+id=["\']{re.escape(element_id)}["\'][^>]*>(.*?)(?:<div[^>]+id=["\']|</body>)'
    match = re.search(pattern, html or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return match.group(1)


def _extract_thumbnail_url(html):
    # E-Hentai usually stores the cover thumbnail as a CSS background image inside #gd1.
    # This cover is small, so the watcher now uses it only as a fallback.
    gd1 = _extract_div_block(html, "gd1")

    patterns = [
        r"url\((['\"]?)(.*?)\1\)",
        r'<img[^>]+src=["\']([^"\']+)["\']',
    ]

    for pattern in patterns:
        match = re.search(pattern, gd1 or html or "", flags=re.IGNORECASE | re.DOTALL)
        if match:
            candidate = match.group(2) if "url" in pattern else match.group(1)
            candidate = absolutize_url(candidate)
            if candidate.startswith("http"):
                return candidate

    return ""


def _extract_first_page_url(html, gallery_url=None):
    """Return the first image-page URL from a gallery page.

    The gallery cover thumbnail is usually low resolution. For channel posts,
    we fetch the first image page and use its displayed image as a much cleaner
    preview.
    """
    source = _extract_div_block(html, "gdt") or html or ""

    patterns = [
        r'<a[^>]+href=["\']([^"\']*?/s/[^"\']+)["\']',
        r'href=["\']([^"\']*?/s/[^"\']+)["\']',
    ]

    for pattern in patterns:
        for href in re.findall(pattern, source, flags=re.IGNORECASE | re.DOTALL):
            candidate = absolutize_url(href, gallery_url or "https://e-hentai.org/")
            if "/s/" in candidate and candidate.startswith("http"):
                return candidate

    return ""


def extract_image_url_from_page(image_page_html, page_url=None):
    """Extract the main image URL from an E-Hentai image page."""
    html = image_page_html or ""

    patterns = [
        r'<img[^>]+id=["\']img["\'][^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\'][^>]+id=["\']img["\']',
        r'id=["\']img["\'][^>]*src=["\']([^"\']+)["\']',
    ]

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            candidate = absolutize_url(match.group(1), page_url or "https://e-hentai.org/")
            if candidate.startswith("http"):
                return candidate

    # Rare fallback: use OpenGraph image if the main image selector changes.
    match = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        candidate = absolutize_url(match.group(1), page_url or "https://e-hentai.org/")
        if candidate.startswith("http"):
            return candidate

    return ""


def guess_image_suffix(url, fallback=".jpg"):
    parsed = urllib.parse.urlsplit(url or "")
    ext = os.path.splitext(parsed.path)[1].lower()
    if ext in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
        return ext
    return fallback


def _extract_info_rows(html):
    info = {}
    pattern = re.compile(
        r'<td[^>]*class=["\']gdt1["\'][^>]*>\s*(.*?)\s*</td>\s*'
        r'<td[^>]*class=["\']gdt2["\'][^>]*>\s*(.*?)\s*</td>',
        flags=re.IGNORECASE | re.DOTALL,
    )

    for key_html, value_html in pattern.findall(html or ""):
        key = strip_html(key_html).rstrip(":").strip().lower()
        value = strip_html(value_html)
        if key:
            info[key] = value

    return info


def _extract_category(html):
    gdc = _extract_div_block(html, "gdc")
    category = strip_html(gdc)
    if category:
        return category

    match = re.search(r'<div[^>]+class=["\']cs\s+[^"\']+["\'][^>]*>(.*?)</div>', html or "", flags=re.IGNORECASE | re.DOTALL)
    return strip_html(match.group(1)) if match else "Unknown"


def _extract_rating(html):
    rating = _extract_id_text(html, "rating_label")
    if rating:
        rating = rating.replace("Average:", "").strip()
    return rating


def _extract_tags(html):
    tag_area = _extract_div_block(html, "taglist")
    if not tag_area:
        return []

    tags = []
    seen = set()

    # Prefer link titles when present, but also support plain link text fallback.
    for title in re.findall(r'title=["\']([^"\']+)["\']', tag_area, flags=re.IGNORECASE):
        cleaned = strip_html(title)
        cleaned = re.sub(r"^Search for\s+", "", cleaned, flags=re.IGNORECASE).strip()
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            tags.append(cleaned)

    for text in re.findall(r'<a[^>]*>(.*?)</a>', tag_area, flags=re.IGNORECASE | re.DOTALL):
        cleaned = strip_html(text)
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            tags.append(cleaned)

    return tags


def normalize_ai_text(value):
    """Normalize title/name text so AI Generated matching is reliable."""
    value = strip_html(value or "").lower()
    value = value.replace("_", " ").replace("-", " ")
    value = re.sub(r"[\[\]\(\)\{\}:|/\\]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def has_ai_generated_name(title, japanese_title=None):
    """Return True only when the gallery name/title contains AI Generated.

    This intentionally does NOT check E-Hentai tags. The watcher should post
    galleries named like "[AI Generated]", "AI-Generated", "Ai Generated", etc.
    """
    title_text = normalize_ai_text(title)
    japanese_title_text = normalize_ai_text(japanese_title)
    return "ai generated" in title_text or "ai generated" in japanese_title_text


def parse_gallery_page(url, html):
    info_rows = _extract_info_rows(html)
    tags = _extract_tags(html)
    title = _extract_id_text(html, "gn") or _extract_id_text(html, "gj") or "Untitled Gallery"
    japanese_title = _extract_id_text(html, "gj")

    return {
        "gallery_id": extract_gallery_id(url),
        "url": normalize_gallery_url(url) or url,
        "title": title,
        "japanese_title": japanese_title,
        "category": _extract_category(html),
        "thumbnail_url": _extract_thumbnail_url(html),
        "first_page_url": _extract_first_page_url(html, url),
        "posted": info_rows.get("posted", "Unknown"),
        "language": info_rows.get("language", "Unknown"),
        "file_size": info_rows.get("file size", "Unknown"),
        "length": info_rows.get("length", "Unknown"),
        "favorited": info_rows.get("favorited", "Unknown"),
        "rating": _extract_rating(html) or "Unknown",
        "tags": tags,
        "has_ai_generated": has_ai_generated_name(title, japanese_title),
        "ai_match_source": "name" if has_ai_generated_name(title, japanese_title) else "none",
    }


def build_ai_gallery_caption(info, heading=None):
    title = info.get("title") or "Untitled Gallery"
    tags = info.get("tags") or []
    visible_tags = ", ".join(tags[:12]) if tags else "AI generated"

    caption = (
        f"{heading or '🤖 New AI Generated Gallery'}\n\n"
        f"📚 Title: {title}\n"
        f"🏷 Category: {info.get('category') or 'Unknown'}\n"
        f"📄 Length: {info.get('length') or 'Unknown'}\n"
        f"💾 File Size: {info.get('file_size') or 'Unknown'}\n"
        f"🌐 Language: {info.get('language') or 'Unknown'}\n"
        f"⭐ Rating: {info.get('rating') or 'Unknown'}\n"
        f"🕒 Posted: {info.get('posted') or 'Unknown'}\n"
        f"🔖 Tags: {visible_tags}\n\n"
        f"🔗 Link: {info.get('url')}\n\n"
        "#AIGenerated"
    )

    # Telegram photo captions have a 1024 character limit. Keep room for safety.
    if len(caption) > 950:
        short_title = title[:220] + "..." if len(title) > 220 else title
        caption = (
            f"{heading or '🤖 New AI Generated Gallery'}\n\n"
            f"📚 Title: {short_title}\n"
            f"🏷 Category: {info.get('category') or 'Unknown'}\n"
            f"📄 Length: {info.get('length') or 'Unknown'}\n"
            f"💾 File Size: {info.get('file_size') or 'Unknown'}\n"
            f"🕒 Posted: {info.get('posted') or 'Unknown'}\n\n"
            f"🔗 Link: {info.get('url')}\n\n"
            "#AIGenerated"
        )

    return caption
