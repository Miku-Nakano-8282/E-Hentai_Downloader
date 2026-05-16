import asyncio
import html
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import aiohttp

from bot_core.config import (
    EHENTAI_COOKIE,
    EHENTAI_USER_AGENT,
    FAST_EHENTAI_DOWNLOAD_WORKERS,
    FAST_EHENTAI_INDEX_SCAN_LIMIT,
    FAST_EHENTAI_PAGE_WORKERS,
    FAST_EHENTAI_RETRIES,
    FAST_EHENTAI_TIMEOUT,
)


class FastEHentaiDownloadError(Exception):
    """Raised when the fast E-Hentai downloader cannot safely finish."""


@dataclass
class FastDownloadResult:
    total_pages: int
    downloaded_pages: int
    selected_pages: List[int]
    method: str = "fast_parallel"


GALLERY_RE = re.compile(r"https?://e-hentai\.org/g/(\d+)/([a-z0-9]+)/?", re.IGNORECASE)
IMAGE_PAGE_RE = re.compile(r"https?://e-hentai\.org/s/[a-z0-9]+/(\d+)-(\d+)", re.IGNORECASE)
IMG_TAG_RE = re.compile(r"<img\b[^>]*\bid=[\"']img[\"'][^>]*>", re.IGNORECASE | re.DOTALL)
SRC_RE = re.compile(r"\bsrc=[\"']([^\"']+)[\"']", re.IGNORECASE)


def is_ehentai_gallery_url(url: str) -> bool:
    return bool(GALLERY_RE.search(url or ""))


def _normalise_gallery_url(url: str) -> str:
    match = GALLERY_RE.search(url or "")
    if not match:
        raise FastEHentaiDownloadError("Invalid E-Hentai gallery URL.")
    gallery_id, token = match.group(1), match.group(2)
    return f"https://e-hentai.org/g/{gallery_id}/{token}/"


def _headers() -> Dict[str, str]:
    headers = {
        "User-Agent": EHENTAI_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://e-hentai.org/",
    }
    if EHENTAI_COOKIE:
        headers["Cookie"] = EHENTAI_COOKIE
    return headers


def _image_headers(referer: str) -> Dict[str, str]:
    headers = {
        "User-Agent": EHENTAI_USER_AGENT,
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": referer,
    }
    if EHENTAI_COOKIE:
        headers["Cookie"] = EHENTAI_COOKIE
    return headers


async def _fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    last_error = None
    for attempt in range(1, FAST_EHENTAI_RETRIES + 1):
        try:
            async with session.get(url, headers=_headers(), timeout=FAST_EHENTAI_TIMEOUT) as response:
                if response.status in {429, 509}:
                    raise FastEHentaiDownloadError(f"E-Hentai rate limit/bandwidth response: HTTP {response.status}")
                if response.status >= 400:
                    raise FastEHentaiDownloadError(f"HTTP {response.status} while fetching page")
                return await response.text(errors="ignore")
        except FastEHentaiDownloadError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt < FAST_EHENTAI_RETRIES:
                await asyncio.sleep(1.5 * attempt)

    raise FastEHentaiDownloadError(f"Failed to fetch {url}: {last_error}")


def _parse_total_pages(gallery_html: str) -> Optional[int]:
    patterns = [
        r">\s*Length\s*:</td>\s*<td[^>]*>\s*([\d,]+)\s+pages?",
        r"Length\s*</[^>]+>\s*<[^>]+>\s*([\d,]+)\s+pages?",
        r"([\d,]+)\s+pages?\s*</td>",
    ]

    for pattern in patterns:
        match = re.search(pattern, gallery_html, re.IGNORECASE | re.DOTALL)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except Exception:
                continue

    return None


def _parse_image_page_links(gallery_html: str) -> Dict[int, str]:
    links: Dict[int, str] = {}
    for match in IMAGE_PAGE_RE.finditer(html.unescape(gallery_html)):
        gallery_id = match.group(1)
        page_num = int(match.group(2))
        url = match.group(0)
        # Keep the first link for each page number.
        links.setdefault(page_num, url)
    return links


def _parse_selected_pages(page_range: str, total_pages: Optional[int]) -> Optional[List[int]]:
    """
    Parse gallery-dl style ranges into 1-based page numbers.

    Supports:
    - 0 / empty => all pages, if total_pages is known
    - 1-10
    - 12,14-20
    - 30-40/2
    - !8 or !1-3,8 => all pages except exclusions, if total_pages is known

    Returns None when the range requires knowing all pages but total_pages is unknown.
    """
    cleaned = (page_range or "0").strip().replace(" ", "")
    if not cleaned or cleaned == "0":
        if total_pages is None:
            return None
        return list(range(1, total_pages + 1))

    exclude_mode = cleaned.startswith("!")
    if exclude_mode:
        if total_pages is None:
            return None
        cleaned = cleaned[1:]

    numbers = set()
    for part in [item for item in cleaned.split(",") if item]:
        single_match = re.fullmatch(r"\d+", part)
        if single_match:
            numbers.add(int(part))
            continue

        range_match = re.fullmatch(r"(\d+)-(\d+)(?:/(\d+))?", part)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            step = int(range_match.group(3) or 1)
            if start <= 0 or end < start or step <= 0:
                raise FastEHentaiDownloadError("Invalid page range.")
            numbers.update(range(start, end + 1, step))
            continue

        raise FastEHentaiDownloadError("Unsupported page range for fast mode.")

    if exclude_mode:
        excluded = {num for num in numbers if num >= 1}
        return [num for num in range(1, int(total_pages) + 1) if num not in excluded]

    if total_pages is not None:
        return sorted(num for num in numbers if 1 <= num <= int(total_pages))

    return sorted(num for num in numbers if num >= 1)


def _gallery_index_url(base_url: str, index_page: int) -> str:
    if index_page <= 0:
        return base_url
    return f"{base_url}?p={index_page}"


def _guess_gallery_index_page_count(total_pages: Optional[int], links_on_first_page: int) -> Optional[int]:
    if not total_pages or links_on_first_page <= 0:
        return None
    return max(1, (int(total_pages) + links_on_first_page - 1) // links_on_first_page)


async def _collect_image_page_links(
    session: aiohttp.ClientSession,
    base_url: str,
    selected_pages: Optional[Sequence[int]],
    total_pages: Optional[int],
    first_page_links: Dict[int, str],
    progress_state: dict,
    cancel_event: Optional[asyncio.Event],
) -> Tuple[Dict[int, str], Optional[int], List[int]]:
    links = dict(first_page_links)
    links_on_first_page = len(first_page_links)
    selected = list(selected_pages or [])

    if selected:
        needed = set(selected)
        if needed.issubset(links.keys()):
            return links, total_pages, selected

    index_page_count = _guess_gallery_index_page_count(total_pages, links_on_first_page)

    if selected and links_on_first_page > 0:
        max_needed = max(selected)
        max_index_needed = max(0, (max_needed - 1) // links_on_first_page)
        if index_page_count is not None:
            max_index_needed = min(max_index_needed, index_page_count - 1)
        pages_to_fetch = list(range(1, max_index_needed + 1))
    elif index_page_count is not None:
        pages_to_fetch = list(range(1, index_page_count))
    else:
        # Unknown total: scan until one page produces no new image page links.
        pages_to_fetch = []

    async def fetch_index_page(index_page: int):
        if cancel_event and cancel_event.is_set():
            raise asyncio.CancelledError
        progress_state["last_line"] = f"Collecting gallery page list... index page {index_page + 1}"
        html_text = await _fetch_text(session, _gallery_index_url(base_url, index_page))
        return index_page, _parse_image_page_links(html_text), _parse_total_pages(html_text)

    if pages_to_fetch:
        semaphore = asyncio.Semaphore(max(1, FAST_EHENTAI_PAGE_WORKERS))

        async def limited_fetch(index_page: int):
            async with semaphore:
                return await fetch_index_page(index_page)

        for task in asyncio.as_completed([limited_fetch(page) for page in pages_to_fetch]):
            if cancel_event and cancel_event.is_set():
                raise asyncio.CancelledError
            _, page_links, found_total = await task
            links.update(page_links)
            if found_total and not total_pages:
                total_pages = found_total
            progress_state["folder_total_files"] = len(links)
            progress_state["last_activity_at"] = time.time()

            if selected and set(selected).issubset(links.keys()):
                # Existing already-started tasks will finish, but we do not need more pages.
                pass
    else:
        previous_count = len(links)
        index_page = 1
        while index_page < FAST_EHENTAI_INDEX_SCAN_LIMIT:
            if cancel_event and cancel_event.is_set():
                raise asyncio.CancelledError
            _, page_links, found_total = await fetch_index_page(index_page)
            if found_total and not total_pages:
                total_pages = found_total
            links.update(page_links)
            progress_state["folder_total_files"] = len(links)
            progress_state["last_activity_at"] = time.time()

            if len(links) == previous_count:
                break
            previous_count = len(links)
            index_page += 1

    if not selected:
        if total_pages:
            selected = list(range(1, int(total_pages) + 1))
        else:
            selected = sorted(links)

    missing = [page for page in selected if page not in links]
    if missing:
        sample = ", ".join(str(page) for page in missing[:10])
        raise FastEHentaiDownloadError(f"Could not find image pages for: {sample}")

    return links, total_pages, selected


def _parse_direct_image_url(image_page_html: str) -> str:
    unescaped = html.unescape(image_page_html)
    tag_match = IMG_TAG_RE.search(unescaped)
    if tag_match:
        src_match = SRC_RE.search(tag_match.group(0))
        if src_match:
            return src_match.group(1)

    # Fallback for unusual attribute order / changed markup.
    fallback = re.search(r"id=[\"']img[\"'][\s\S]{0,500}?src=[\"']([^\"']+)[\"']", unescaped, re.IGNORECASE)
    if fallback:
        return fallback.group(1)

    fallback = re.search(r"src=[\"']([^\"']+)[\"'][\s\S]{0,500}?id=[\"']img[\"']", unescaped, re.IGNORECASE)
    if fallback:
        return fallback.group(1)

    raise FastEHentaiDownloadError("Could not extract direct image URL from image page.")


async def _extract_direct_image_url(
    session: aiohttp.ClientSession,
    page_num: int,
    image_page_url: str,
    progress_state: dict,
    cancel_event: Optional[asyncio.Event],
) -> Tuple[int, str, str]:
    if cancel_event and cancel_event.is_set():
        raise asyncio.CancelledError
    progress_state["last_line"] = f"Extracting direct image URL for page {page_num}..."
    page_html = await _fetch_text(session, image_page_url)
    direct_url = _parse_direct_image_url(page_html)
    progress_state["last_activity_at"] = time.time()
    return page_num, image_page_url, direct_url


def _extension_from_url(url: str) -> str:
    path = urlparse(url).path
    _, ext = os.path.splitext(path)
    ext = ext.lower().lstrip(".")
    if ext in {"jpg", "jpeg", "png", "webp", "gif", "bmp"}:
        return "jpg" if ext == "jpeg" else ext
    return "jpg"


async def _download_image(
    session: aiohttp.ClientSession,
    page_num: int,
    image_page_url: str,
    direct_url: str,
    output_dir: str,
    progress_state: dict,
    progress_lock: asyncio.Lock,
    cancel_event: Optional[asyncio.Event],
):
    ext = _extension_from_url(direct_url)
    final_path = os.path.join(output_dir, f"{page_num:04d}.{ext}")
    part_path = f"{final_path}.part"
    last_error = None

    for attempt in range(1, FAST_EHENTAI_RETRIES + 1):
        if cancel_event and cancel_event.is_set():
            raise asyncio.CancelledError

        try:
            async with session.get(
                direct_url,
                headers=_image_headers(image_page_url),
                timeout=FAST_EHENTAI_TIMEOUT,
            ) as response:
                if response.status in {429, 509}:
                    raise FastEHentaiDownloadError(f"E-Hentai rate limit/bandwidth response: HTTP {response.status}")
                if response.status >= 400:
                    raise FastEHentaiDownloadError(f"HTTP {response.status} while downloading page {page_num}")

                os.makedirs(output_dir, exist_ok=True)
                downloaded_this_file = 0
                with open(part_path, "wb") as file:
                    async for chunk in response.content.iter_chunked(128 * 1024):
                        if cancel_event and cancel_event.is_set():
                            raise asyncio.CancelledError
                        if not chunk:
                            continue
                        file.write(chunk)
                        downloaded_this_file += len(chunk)
                        async with progress_lock:
                            progress_state["folder_total_bytes"] = progress_state.get("folder_total_bytes", 0) + len(chunk)
                            progress_state["last_activity_at"] = time.time()
                            progress_state["last_line"] = f"Downloading page {page_num}..."

                if downloaded_this_file <= 0:
                    raise FastEHentaiDownloadError(f"Downloaded empty file for page {page_num}")

                os.replace(part_path, final_path)
                async with progress_lock:
                    progress_state["current"] = progress_state.get("current", 0) + 1
                    progress_state["folder_completed_images"] = progress_state.get("folder_completed_images", 0) + 1
                    progress_state["last_activity_at"] = time.time()
                    progress_state["last_line"] = f"Finished page {page_num}."
                return final_path
        except FastEHentaiDownloadError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            try:
                if os.path.exists(part_path):
                    os.remove(part_path)
            except Exception:
                pass
            if attempt < FAST_EHENTAI_RETRIES:
                async with progress_lock:
                    progress_state["last_line"] = f"Retrying page {page_num} ({attempt}/{FAST_EHENTAI_RETRIES})..."
                    progress_state["last_activity_at"] = time.time()
                await asyncio.sleep(1.5 * attempt)

    raise FastEHentaiDownloadError(f"Failed to download page {page_num}: {last_error}")


async def download_ehentai_gallery_fast(
    url: str,
    page_range: str,
    output_dir: str,
    progress_state: dict,
    cancel_event: Optional[asyncio.Event] = None,
) -> FastDownloadResult:
    """
    Fast E-Hentai downloader used before the gallery-dl fallback.

    It collects image page URLs, extracts direct image URLs, and downloads images
    with a small controlled amount of parallelism. This is closer to how browser
    userscripts/download managers feel fast, but it still keeps workers limited
    to avoid hammering E-Hentai.
    """
    base_url = _normalise_gallery_url(url)
    os.makedirs(output_dir, exist_ok=True)

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=FAST_EHENTAI_TIMEOUT, sock_read=FAST_EHENTAI_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=max(4, FAST_EHENTAI_PAGE_WORKERS + FAST_EHENTAI_DOWNLOAD_WORKERS), ttl_dns_cache=300)

    progress_state["method"] = f"Fast parallel mode ({FAST_EHENTAI_DOWNLOAD_WORKERS} workers)"
    progress_state["last_line"] = "Opening gallery page..."
    progress_state["last_activity_at"] = time.time()

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        first_html = await _fetch_text(session, base_url)
        total_pages = _parse_total_pages(first_html)
        first_page_links = _parse_image_page_links(first_html)

        if not first_page_links:
            raise FastEHentaiDownloadError("Could not find image page links on the gallery page.")

        selected_pages = _parse_selected_pages(page_range, total_pages)
        links, total_pages, selected_pages = await _collect_image_page_links(
            session=session,
            base_url=base_url,
            selected_pages=selected_pages,
            total_pages=total_pages,
            first_page_links=first_page_links,
            progress_state=progress_state,
            cancel_event=cancel_event,
        )

        if not selected_pages:
            raise FastEHentaiDownloadError("No pages selected for download.")

        selected_pages = sorted(selected_pages)
        progress_state["current"] = 0
        progress_state["total"] = len(selected_pages)
        progress_state["expected_total"] = len(selected_pages)
        progress_state["folder_completed_images"] = 0
        progress_state["folder_total_files"] = len(selected_pages)
        progress_state["last_line"] = "Extracting direct image URLs..."
        progress_state["last_activity_at"] = time.time()

        page_semaphore = asyncio.Semaphore(max(1, FAST_EHENTAI_PAGE_WORKERS))

        async def limited_extract(page: int):
            async with page_semaphore:
                return await _extract_direct_image_url(session, page, links[page], progress_state, cancel_event)

        direct_items = []
        for task in asyncio.as_completed([limited_extract(page) for page in selected_pages]):
            if cancel_event and cancel_event.is_set():
                raise asyncio.CancelledError
            direct_items.append(await task)
            progress_state["last_line"] = f"Extracted {len(direct_items)}/{len(selected_pages)} direct image URLs."
            progress_state["last_activity_at"] = time.time()

        direct_items.sort(key=lambda item: item[0])
        progress_state["current"] = 0
        progress_state["folder_completed_images"] = 0
        progress_state["folder_total_bytes"] = 0
        progress_state["last_line"] = "Starting parallel image downloads..."
        progress_state["last_activity_at"] = time.time()

        download_semaphore = asyncio.Semaphore(max(1, FAST_EHENTAI_DOWNLOAD_WORKERS))
        progress_lock = asyncio.Lock()

        async def limited_download(item):
            async with download_semaphore:
                return await _download_image(
                    session=session,
                    page_num=item[0],
                    image_page_url=item[1],
                    direct_url=item[2],
                    output_dir=output_dir,
                    progress_state=progress_state,
                    progress_lock=progress_lock,
                    cancel_event=cancel_event,
                )

        downloaded_paths = []
        for task in asyncio.as_completed([limited_download(item) for item in direct_items]):
            if cancel_event and cancel_event.is_set():
                raise asyncio.CancelledError
            downloaded_paths.append(await task)

        if len(downloaded_paths) != len(selected_pages):
            raise FastEHentaiDownloadError("Fast downloader did not finish every selected page.")

        progress_state["last_line"] = "Fast parallel download complete."
        progress_state["last_activity_at"] = time.time()
        return FastDownloadResult(
            total_pages=int(total_pages or len(selected_pages)),
            downloaded_pages=len(downloaded_paths),
            selected_pages=selected_pages,
        )
