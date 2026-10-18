import os
import re
import shutil
import asyncio
import time
import threading
import socket
import ipaddress
import zipfile
import mimetypes
import subprocess
import select
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import secrets
import json
from pathlib import Path
from urllib.parse import urlparse, urljoin, urldefrag, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# =========================
# CONFIG
# =========================

BOT_TOKEN = "8896123763:AAGz7SmhfCanrbMHcg3PTuK_ObEp73W_M94"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en;q=0.7",
}

MAX_FILES = 250
MAX_DEPTH = 2
MAX_FILE_BYTES = 12 * 1024 * 1024
MAX_TOTAL_BYTES = 60 * 1024 * 1024
MAX_ZIP_BYTES = 49 * 1024 * 1024
MAX_REDIRECTS = 5
MAX_WORKERS = 6
MAX_DOWNLOAD_ATTEMPTS = 4
RETRY_BASE_DELAY = 1.0
JOB_TIMEOUT = None  # لا توجد مهلة إجبارية للسحب؛ الإلغاء اليدوي متاح دائمًا.
RESULT_TTL = 10 * 60
REPORT_JSON_NAME = "SCRAPE_REPORT.json"
REPORT_TEXT_NAME = "SCRAPE_REPORT.txt"

BASE_STORAGE = Path.home() / "website_scraper_bot"
BASE_STORAGE.mkdir(parents=True, exist_ok=True)

# user/chat state
PENDING_URLS = {}
ACTIVE_JOBS = {}
RESULTS = {}
LOCK = threading.RLock()


# =========================
# UI
# =========================


def main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 سحب ملفات موقع", callback_data="start_scrape")],
    ])


def scrape_types_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📄 الصفحة فقط", callback_data="scrape:page"),
            InlineKeyboardButton("🌐 الموقع كامل", callback_data="scrape:full"),
        ],
        [
            InlineKeyboardButton("🖼️ الصور فقط", callback_data="scrape:images"),
            InlineKeyboardButton("📂 الملفات المهمة", callback_data="scrape:important"),
        ],
        [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_setup")],
    ])


def running_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑 إلغاء السحب", callback_data="job:cancel")],
    ])


def result_keyboard(token):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📦 إرسال ZIP", callback_data=f"result:zip:{token}"),
            InlineKeyboardButton("📋 التقرير", callback_data=f"result:report:{token}"),
        ],
        [
            InlineKeyboardButton("🗑️ حذف النتيجة", callback_data=f"result:delete:{token}"),
        ],
    ])


# =========================
# VALIDATION / SECURITY
# =========================


def sanitize_folder_name(url):
    parsed = urlparse(url)
    name = parsed.netloc or parsed.path
    name = re.sub(r'[\\/*?:"<>|]+', "_", name)
    return name[:90] or "downloaded_site"


def normalize_url(value):
    value = (value or "").strip()
    if not value:
        return None
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return urldefrag(value)[0]


def host_is_allowed(host, base_host):
    host = (host or "").lower().rstrip(".")
    base_host = (base_host or "").lower().rstrip(".")
    return host == base_host or host.endswith("." + base_host)


def host_resolves_public(host):
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)}
    except Exception:
        return False

    public_found = False
    for addr in addresses:
        try:
            ip = ipaddress.ip_address(addr)
            if not (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_reserved
                or ip.is_unspecified
            ):
                public_found = True
        except ValueError:
            continue
    return public_found


def validate_target_url(url):
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("الرابط يجب أن يبدأ بـ http أو https.")
    host = parsed.hostname
    if host in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("لا يمكن سحب localhost أو العناوين الداخلية.")
    if not host_resolves_public(host):
        raise ValueError("الرابط يشير إلى عنوان داخلي أو غير متاح بشكل آمن.")


def validate_response_destination(response, base_host):
    final_url = response.url
    parsed = urlparse(final_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("تمت إعادة التوجيه إلى رابط غير صالح.")
    if not host_is_allowed(parsed.hostname, base_host):
        raise ValueError("تم منع إعادة التوجيه خارج نطاق الموقع المحدد.")
    if not host_resolves_public(parsed.hostname):
        raise ValueError("تم منع الوصول إلى عنوان داخلي بعد إعادة التوجيه.")


# =========================
# HTTP
# =========================


def build_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def request_checked(session, url, base_host, stream=False):
    response = session.get(
        url,
        headers=HEADERS,
        timeout=(10, 25),
        allow_redirects=True,
        stream=stream,
    )
    response.raise_for_status()
    validate_response_destination(response, base_host)
    return response


def content_length_or_none(response):
    try:
        value = response.headers.get("content-length")
        return int(value) if value else None
    except (TypeError, ValueError):
        return None


def is_html_content(content_type):
    content_type = (content_type or "").lower()
    return "text/html" in content_type or "application/xhtml+xml" in content_type


# =========================
# LOCAL PATHS / URL MAPPING
# =========================


def clean_path_part(value):
    value = unquote(value or "")
    value = re.sub(r'[<>:"/\\|?*]+', "_", value)
    value = value.strip(" .")
    return value or "index"


def local_path_for_url(url, is_html=False, content_type=""):
    parsed = urlparse(url)
    path = parsed.path or "/"
    pieces = [clean_path_part(p) for p in path.split("/") if p]

    if is_html or is_html_content(content_type):
        if not pieces:
            pieces = ["index.html"]
        elif "." not in pieces[-1]:
            pieces.append("index.html")
        elif not pieces[-1].lower().endswith((".html", ".htm", ".xhtml")):
            pieces[-1] += ".html"
    else:
        if not pieces:
            name = "resource"
            ext = mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip()) or ""
            pieces = [name + ext]

        filename = pieces[-1]
        if "." not in filename:
            ext = mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip()) or ""
            pieces[-1] += ext

    return Path(*pieces)


def unique_local_path(root, desired):
    candidate = root / desired
    candidate.parent.mkdir(parents=True, exist_ok=True)
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    counter = 2
    while True:
        alt = candidate.with_name(f"{stem}_{counter}{suffix}")
        if not alt.exists():
            return alt
        counter += 1


def relative_href(from_file, target_file):
    import os
    try:
        return os.path.relpath(target_file, start=from_file.parent).replace(os.sep, "/")
    except Exception:
        return os.path.basename(target_file)


# =========================
# HTML / CSS DISCOVERY
# =========================


def extract_css_urls(css_text, base_url):
    found = set()
    for match in re.finditer(r"url\(\s*(['\"]?)(.*?)\1\s*\)", css_text, flags=re.I):
        raw = match.group(2).strip()
        if not raw or raw.startswith(("data:", "blob:", "javascript:", "#")):
            continue
        absolute = urljoin(base_url, raw)
        absolute = urldefrag(absolute)[0]
        if urlparse(absolute).scheme in {"http", "https"}:
            found.add(absolute)

    for match in re.finditer(r"@import\s+(?:url\(\s*)?['\"]([^'\"]+)['\"]", css_text, flags=re.I):
        absolute = urldefrag(urljoin(base_url, match.group(1)))[0]
        if urlparse(absolute).scheme in {"http", "https"}:
            found.add(absolute)
    return found


def discover_from_html(html, page_url):
    soup = BeautifulSoup(html, "html.parser")
    pages = set()
    assets = set()

    # HTML pages
    for tag in soup.find_all("a", href=True):
        href = tag.get("href", "").strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        absolute = urldefrag(urljoin(page_url, href))[0]
        if urlparse(absolute).scheme in {"http", "https"}:
            pages.add(absolute)

    # CSS / JS / images / media / sources
    selectors = [
        ("link", "href"),
        ("script", "src"),
        ("img", "src"),
        ("img", "data-src"),
        ("source", "src"),
        ("video", "src"),
        ("audio", "src"),
        ("iframe", "src"),
        ("object", "data"),
    ]
    for tag_name, attr in selectors:
        for tag in soup.find_all(tag_name):
            raw = tag.get(attr)
            if not raw or raw.startswith(("data:", "blob:", "javascript:", "#")):
                continue
            absolute = urldefrag(urljoin(page_url, raw))[0]
            if urlparse(absolute).scheme in {"http", "https"}:
                assets.add(absolute)

    for tag in soup.find_all(srcset=True):
        for item in str(tag.get("srcset", "")).split(","):
            raw = item.strip().split(" ")[0]
            if raw and not raw.startswith("data:"):
                absolute = urldefrag(urljoin(page_url, raw))[0]
                if urlparse(absolute).scheme in {"http", "https"}:
                    assets.add(absolute)

    return pages, assets


# =========================
# PROGRESS
# =========================


class Progress:
    def __init__(self):
        self.lock = threading.Lock()
        self.discovered = 0
        self.completed = 0
        self.failed = 0
        self.bytes = 0
        self.started = time.time()
        self.stage = "بدء السحب"

    def snapshot(self):
        with self.lock:
            elapsed = max(0.1, time.time() - self.started)
            speed = self.bytes / elapsed
            return {
                "discovered": self.discovered,
                "completed": self.completed,
                "failed": self.failed,
                "bytes": self.bytes,
                "stage": self.stage,
                "speed": speed,
            }


def human_bytes(value):
    value = float(value)
    if value < 1024:
        return f"{int(value)} B"
    if value < 1024 ** 2:
        return f"{value / 1024:.1f} KB"
    if value < 1024 ** 3:
        return f"{value / (1024 ** 2):.1f} MB"
    return f"{value / (1024 ** 3):.2f} GB"


def progress_text(progress):
    snap = progress.snapshot()
    done = snap["completed"] + snap["failed"]
    total = max(snap["discovered"], done, 1)
    pct = min(100, int(done * 100 / total))
    return (
        "🔄 **جاري السحب...**\n\n"
        f"📊 التقدم: `{pct}%`\n"
        f"📁 الملفات: `{done}/{total}`\n"
        f"✅ ناجح: `{snap['completed']}`  ❌ فشل: `{snap['failed']}`\n"
        f"📦 الحجم: `{human_bytes(snap['bytes'])}`\n"
        f"⚡ السرعة: `{human_bytes(snap['speed'])}/ث`\n"
        f"🧩 المرحلة: `{snap['stage']}`"
    )


# =========================
# SCRAPER CORE
# =========================


class SiteScraper:
    def __init__(self, target_url, mode, root_dir, cancel_event, progress):
        self.target_url = target_url
        self.mode = mode
        self.root_dir = Path(root_dir)
        self.cancel_event = cancel_event
        self.progress = progress
        self.session = build_session()
        self.base_host = urlparse(target_url).hostname.lower()
        self.base_scheme = urlparse(target_url).scheme
        self.url_map = {}
        self.downloaded_types = {}
        self.html_files = []
        self.pages_seen = set()
        self.pages_queue = []
        self.lock = threading.Lock()
        self.total_bytes = 0
        self.title = "بدون عنوان"
        self.failed_urls = []
        self.failure_details = []
        self.retry_counts = {}
        self.resumed_files = 0
        self.resumed_urls = set()
        self.pages_discovered = set()
        self.pages_crawled = []
        self.page_depths = {}
        self.path_map = {}
        self.downloaded_urls = set()
        self.started_at = time.time()

    def cancelled(self):
        return self.cancel_event.is_set()

    def check_limits(self, size):
        if size > MAX_FILE_BYTES:
            raise ValueError("الملف تجاوز الحد الأقصى المسموح به.")
        with self.lock:
            if self.total_bytes + size > MAX_TOTAL_BYTES:
                raise ValueError("تم الوصول للحد الأقصى لحجم الموقع.")

    def record_bytes(self, amount):
        with self.lock:
            self.total_bytes += amount
            self.progress.bytes = self.total_bytes

    def fetch_small_text(self, url):
        last_error = None
        for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
            try:
                response = request_checked(self.session, url, self.base_host, stream=False)
                content = response.content
                self.check_limits(len(content))
                if attempt > 1:
                    self.retry_counts[url] = attempt - 1
                return response, content
            except Exception as exc:
                last_error = exc
                if attempt >= MAX_DOWNLOAD_ATTEMPTS:
                    break
                self.retry_counts[url] = attempt
                time.sleep(min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), 8.0))
        raise last_error or RuntimeError("فشل جلب الصفحة")

    def write_response(self, response, url, is_html=False):
        content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        declared_length = content_length_or_none(response)
        if declared_length is not None:
            self.check_limits(declared_length)

        desired = local_path_for_url(url, is_html=is_html, content_type=content_type)
        local_path = unique_local_path(self.root_dir, desired)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        written = 0
        with open(local_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if self.cancelled():
                    raise RuntimeError("تم إلغاء السحب")
                if not chunk:
                    continue
                written += len(chunk)
                if written > MAX_FILE_BYTES:
                    raise ValueError("الملف تجاوز الحد الأقصى المسموح به.")
                with self.lock:
                    if self.total_bytes + written > MAX_TOTAL_BYTES:
                        raise ValueError("تم الوصول للحد الأقصى لحجم الموقع.")
                handle.write(chunk)

        self.record_bytes(written)
        self.url_map[url] = local_path
        self.downloaded_types[url] = content_type
        self.progress.completed += 1
        return local_path, content_type

    def _allocate_local_path(self, url, kind="asset", content_type=""):
        with self.lock:
            existing = self.path_map.get(url)
            if existing:
                return existing
            desired = local_path_for_url(
                url,
                is_html=(kind == "page"),
                content_type=content_type,
            )
            local_path = unique_local_path(self.root_dir, desired)
            self.path_map[url] = local_path
            return local_path

    def _record_failure(self, url, kind, error):
        with self.lock:
            attempts = self.retry_counts.get(url, 0) + 1
            self.retry_counts[url] = attempts
            self.failed_urls.append(url)
            self.failure_details.append({
                "url": url,
                "kind": kind,
                "attempts": attempts,
                "error": str(error)[:500],
            })
            self.progress.failed += 1

    def download_one(self, url, kind="asset"):
        if self.cancelled():
            raise RuntimeError("تم إلغاء السحب")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("رابط غير صالح")
        if not host_is_allowed(parsed.hostname, self.base_host):
            raise ValueError("مصدر خارجي تم تجاهله")
        if not host_resolves_public(parsed.hostname):
            raise ValueError("مصدر داخلي تم حظره")

        local_path = self._allocate_local_path(url, kind=kind)
        part_path = Path(str(local_path) + ".part")
        non_retryable_markers = (
            "مصدر خارجي",
            "مصدر داخلي",
            "الرابط أعاد HTML",
            "تجاوز الحد الأقصى",
            "الحد الأقصى لحجم الموقع",
        )

        for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
            if self.cancelled():
                raise RuntimeError("تم إلغاء السحب")

            response = None
            try:
                offset = part_path.stat().st_size if part_path.exists() else 0
                if offset > MAX_FILE_BYTES:
                    part_path.unlink(missing_ok=True)
                    offset = 0

                headers = dict(HEADERS)
                if offset:
                    headers["Range"] = f"bytes={offset}-"

                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=(10, 30),
                    allow_redirects=True,
                    stream=True,
                )
                response.raise_for_status()
                validate_response_destination(response, self.base_host)

                content_type = response.headers.get("content-type", "").lower()
                is_html = is_html_content(content_type) or kind == "page"
                if kind == "asset" and is_html:
                    raise ValueError("الرابط أعاد HTML بدل مورد الوسائط")

                status = response.status_code
                resumed_this_attempt = bool(offset and status == 206)
                if offset and status == 206:
                    mode = "ab"
                    written = offset
                    self.resumed_urls.add(url)
                    self.resumed_files = len(self.resumed_urls)

                    content_range = response.headers.get("content-range", "")
                    start_match = re.search(r"bytes\s+(\d+)-", content_range, flags=re.I)
                    if start_match and int(start_match.group(1)) != offset:
                        response.close()
                        raise RuntimeError("خادم الموقع أعاد جزءًا غير متوافق مع الاستكمال")
                elif offset and status == 200:
                    # The server ignored Range. Restart the file safely.
                    mode = "wb"
                    written = 0
                    offset = 0
                else:
                    mode = "wb"
                    written = 0
                    offset = 0

                declared_remaining = content_length_or_none(response)
                expected_total = None
                if declared_remaining is not None:
                    expected_total = written + declared_remaining
                else:
                    content_range = response.headers.get("content-range", "")
                    total_match = re.search(r"/(\d+)$", content_range)
                    if total_match:
                        expected_total = int(total_match.group(1))

                if expected_total is not None:
                    self.check_limits(expected_total)

                local_path = self._allocate_local_path(url, kind=kind, content_type=content_type)
                local_path.parent.mkdir(parents=True, exist_ok=True)

                with open(part_path, mode) as handle:
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if self.cancelled():
                            raise RuntimeError("تم إلغاء السحب")
                        if not chunk:
                            continue
                        next_size = written + len(chunk)
                        if next_size > MAX_FILE_BYTES:
                            raise ValueError("الملف تجاوز الحد الأقصى المسموح به.")
                        with self.lock:
                            current_total = self.total_bytes
                        previous_written = written
                        if current_total + (next_size - previous_written) > MAX_TOTAL_BYTES:
                            raise ValueError("تم الوصول للحد الأقصى لحجم الموقع.")
                        handle.write(chunk)
                        written = next_size

                final_size = part_path.stat().st_size
                if expected_total is not None and final_size < expected_total:
                    raise RuntimeError("انقطع الاتصال قبل اكتمال الملف")
                if final_size <= 0:
                    raise RuntimeError("تم تنزيل ملف فارغ")

                part_path.replace(local_path)
                normalized_content_type = content_type.split(";", 1)[0].strip()
                self.record_bytes(final_size)
                self.url_map[url] = local_path
                self.downloaded_types[url] = normalized_content_type
                self.downloaded_urls.add(url)
                self.progress.completed += 1
                if attempt > 1 or resumed_this_attempt:
                    self.retry_counts[url] = max(self.retry_counts.get(url, 0), attempt - 1)
                return local_path, normalized_content_type

            except Exception as exc:
                last_error = exc
                message = str(exc)
                if response is not None:
                    try:
                        response.close()
                    except Exception:
                        pass

                if any(marker in message for marker in non_retryable_markers):
                    raise

                if attempt >= MAX_DOWNLOAD_ATTEMPTS:
                    raise last_error

                self.retry_counts[url] = attempt
                time.sleep(min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), 8.0))

        raise RuntimeError("فشل تنزيل المورد")

    def download_parallel(self, urls, kind="asset"):
        unique = []
        seen = set()
        for url in urls:
            clean = urldefrag(url)[0]
            if clean in seen:
                continue
            seen.add(clean)
            unique.append(clean)

        self.progress.discovered += len(unique)
        if not unique:
            return []

        results = []
        pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        futures = {pool.submit(self.download_one, url, kind): url for url in unique}
        try:
            for future in as_completed(futures):
                url = futures[future]
                if self.cancelled():
                    for item in futures:
                        item.cancel()
                    raise RuntimeError("تم إلغاء السحب")
                try:
                    result = future.result()
                    results.append((url, result[0]))
                except Exception as exc:
                    if self.cancelled():
                        raise RuntimeError("تم إلغاء السحب") from exc
                    self._record_failure(url, kind, exc)
            return results
        finally:
            # لا ننتظر مستقبلات لم تُنفذ عند الإلغاء؛ كل download_one يفحص
            # cancel_event أثناء التنزيل، وبالتالي يتوقف بشكل متعاون وآمن.
            pool.shutdown(wait=True, cancel_futures=True)

    def rewrite_html(self, file_path, page_url):
        if not file_path.exists():
            return
        html = file_path.read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup.find_all(True):
            for attr in ("href", "src", "poster", "data-src"):
                raw = tag.get(attr)
                if not raw or raw.startswith(("data:", "blob:", "javascript:", "mailto:", "tel:", "#")):
                    continue
                absolute = urldefrag(urljoin(page_url, raw))[0]
                local = self.url_map.get(absolute)
                if local:
                    tag[attr] = relative_href(file_path, local)

            if tag.has_attr("srcset"):
                replacements = []
                for item in str(tag.get("srcset", "")).split(","):
                    parts = item.strip().split()
                    if not parts:
                        continue
                    raw = parts[0]
                    absolute = urldefrag(urljoin(page_url, raw))[0]
                    local = self.url_map.get(absolute)
                    if local:
                        parts[0] = relative_href(file_path, local)
                    replacements.append(" ".join(parts))
                tag["srcset"] = ", ".join(replacements)

        file_path.write_text(str(soup), encoding="utf-8")

    def rewrite_css(self, file_path, source_url):
        if not file_path.exists():
            return
        css = file_path.read_text(encoding="utf-8", errors="ignore")

        def replace_url(match):
            raw = match.group(2).strip()
            if not raw or raw.startswith(("data:", "blob:", "#")):
                return match.group(0)
            absolute = urldefrag(urljoin(source_url, raw))[0]
            local = self.url_map.get(absolute)
            if not local:
                return match.group(0)
            relative = relative_href(file_path, local)
            return f"url({match.group(1)}{relative}{match.group(1)})"

        css = re.sub(r"url\(\s*(['\"]?)(.*?)\1\s*\)", replace_url, css, flags=re.I)
        file_path.write_text(css, encoding="utf-8")

    def discover_and_fetch_page(self, page_url, depth):
        if self.cancelled():
            raise RuntimeError("تم إلغاء السحب")
        if page_url in self.pages_seen:
            return [], []

        self.pages_seen.add(page_url)
        self.pages_discovered.add(page_url)
        self.page_depths[page_url] = depth
        self.progress.discovered += 1

        try:
            response, content = self.fetch_small_text(page_url)
            if not is_html_content(response.headers.get("content-type", "")):
                response.close()
                self.progress.failed += 1
                return [], []

            html = content.decode(response.encoding or "utf-8", errors="ignore")
            if self.title == "بدون عنوان":
                soup = BeautifulSoup(html, "html.parser")
                title = soup.title.get_text(" ", strip=True) if soup.title else "بدون عنوان"
                self.title = title[:200] or "بدون عنوان"

            final_page_url = response.url
            local_path, _ = self.write_response(response, final_page_url, is_html=True)
            response.close()
            self.html_files.append((local_path, final_page_url))
            self.pages_crawled.append(final_page_url)
            pages, assets = discover_from_html(html, final_page_url)
            pages = [p for p in pages if urlparse(p).hostname and host_is_allowed(urlparse(p).hostname, self.base_host)]
            assets = [a for a in assets if urlparse(a).hostname and host_is_allowed(urlparse(a).hostname, self.base_host)]
            if depth >= MAX_DEPTH:
                pages = []
            return pages, assets
        except Exception as exc:
            # The root page is mandatory; internal page failures are reported and skipped.
            if depth == 0:
                raise
            self._record_failure(page_url, "page", exc)
            return [], []

    def run_page_only(self):
        # الصفحة فقط = الصفحة + كل الموارد المباشرة التي تحتاجها فعليًا للمعاينة.
        self.progress.stage = "جلب الصفحة"
        pages, assets = self.discover_and_fetch_page(self.target_url, 0)
        asset_urls = [u for u in assets if u not in self.url_map]
        if asset_urls:
            self.progress.stage = "تحميل موارد الصفحة"
            self.download_parallel(asset_urls[:max(0, MAX_FILES - len(self.url_map))], kind="asset")

        # CSS imports / fonts / background images مهمة للمعاينة الحقيقية.
        css_assets = set()
        for css_url in list(self.url_map):
            if not css_url.lower().split("?", 1)[0].endswith(".css"):
                continue
            local = self.url_map.get(css_url)
            if not local or not local.exists():
                continue
            css = local.read_text(encoding="utf-8", errors="ignore")
            css_assets.update(
                u for u in extract_css_urls(css, css_url)
                if urlparse(u).hostname and host_is_allowed(urlparse(u).hostname, self.base_host)
            )
        extra = [u for u in css_assets if u not in self.url_map]
        if extra and len(self.url_map) < MAX_FILES:
            self.progress.stage = "تحميل موارد CSS"
            self.download_parallel(extra[:max(0, MAX_FILES - len(self.url_map))], kind="asset")

        self.progress.stage = "إصلاح روابط المعاينة"
        for path, page_url in self.html_files:
            self.rewrite_html(path, page_url)
        for url, local in list(self.url_map.items()):
            if local.suffix.lower() == ".css":
                self.rewrite_css(local, url)

    def run_images_only(self):
        self.progress.stage = "تحليل الصور"
        pages, assets = self.discover_and_fetch_page(self.target_url, 0)
        image_urls = []
        for url in assets:
            path_lower = urlparse(url).path.lower()
            if re.search(r"\.(jpg|jpeg|png|webp|gif|bmp|svg)(?:$|\?)", path_lower):
                image_urls.append(url)
        self.progress.stage = "تحميل الصور"
        self.download_parallel(image_urls, kind="asset")
        for path, page_url in self.html_files:
            self.rewrite_html(path, page_url)

    def run_important(self):
        self.progress.stage = "تحليل الملفات المهمة"
        pages, assets = self.discover_and_fetch_page(self.target_url, 0)
        important = []
        for url in assets:
            path_lower = urlparse(url).path.lower()
            if re.search(r"\.(css|js|json|woff2?|ttf|otf|eot|svg|png|jpe?g|webp|gif|ico)(?:$|\?)", path_lower):
                important.append(url)
        self.progress.stage = "تحميل CSS / JS / الصور"
        self.download_parallel(important, kind="asset")
        for path, page_url in self.html_files:
            self.rewrite_html(path, page_url)
        for url, local in list(self.url_map.items()):
            if local.suffix.lower() == ".css":
                self.rewrite_css(local, url)

    def run_full(self):
        self.progress.stage = "استكشاف الصفحات"
        pending = [(self.target_url, 0)]
        scheduled = {self.target_url}
        all_assets = set()

        while pending and len(self.pages_seen) < MAX_FILES and not self.cancelled():
            current_url, depth = pending.pop(0)
            pages, assets = self.discover_and_fetch_page(current_url, depth)
            all_assets.update(assets)

            if depth < MAX_DEPTH:
                for page in pages:
                    if page in scheduled or page in self.pages_seen:
                        continue
                    if len(scheduled) >= MAX_FILES:
                        break
                    scheduled.add(page)
                    pending.append((page, depth + 1))

            self.progress.stage = f"استكشاف الصفحات ({len(self.pages_crawled)})"

        # Download discovered assets in parallel with retry + resume support.
        self.progress.stage = "تحميل الملفات المرتبطة"
        asset_urls = []
        for asset in all_assets:
            if asset in self.url_map:
                continue
            asset_urls.append(asset)
            if len(asset_urls) >= max(0, MAX_FILES - len(self.url_map)):
                break
        self.download_parallel(asset_urls, kind="asset")

        # CSS can discover additional fonts/images/imports.
        css_urls = [url for url in asset_urls if urlparse(url).path.lower().endswith(".css")]
        extra_css_assets = set()
        for css_url in css_urls:
            local = self.url_map.get(css_url)
            if not local or not local.exists():
                continue
            css = local.read_text(encoding="utf-8", errors="ignore")
            for extra in extract_css_urls(css, css_url):
                if urlparse(extra).hostname and host_is_allowed(urlparse(extra).hostname, self.base_host):
                    extra_css_assets.add(extra)
        if extra_css_assets and len(self.url_map) < MAX_FILES:
            self.progress.stage = "تحميل موارد CSS"
            remaining = list(extra_css_assets)[: max(0, MAX_FILES - len(self.url_map))]
            self.download_parallel(remaining, kind="asset")

        # Rewrite all known HTML/CSS links.
        self.progress.stage = "إصلاح الروابط"
        for path, page_url in self.html_files:
            self.rewrite_html(path, page_url)
        for url, local in list(self.url_map.items()):
            if local.suffix.lower() == ".css":
                self.rewrite_css(local, url)

    def build_report_data(self):
        elapsed = max(0.0, time.time() - self.started_at)
        retries_total = sum(self.retry_counts.values())
        return {
            "url": self.target_url,
            "title": self.title,
            "mode": self.mode,
            "elapsed_seconds": round(elapsed, 2),
            "max_depth": MAX_DEPTH,
            "pages_discovered": len(self.pages_discovered),
            "pages_crawled": len(self.pages_crawled),
            "pages": [
                {"url": url, "depth": self.page_depths.get(url, 0)}
                for url in self.pages_crawled
            ],
            "files_downloaded": len(self.downloaded_urls),
            "files_failed": len(self.failure_details),
            "total_downloaded_bytes": self.total_bytes,
            "retries": retries_total,
            "resumed_files": self.resumed_files,
            "failed_urls": self.failure_details,
        }

    def create_report_files(self):
        data = self.build_report_data()
        json_path = self.root_dir / REPORT_JSON_NAME
        text_path = self.root_dir / REPORT_TEXT_NAME

        json_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        lines = [
            "WEBSITE SCRAPER REPORT",
            "=" * 60,
            f"URL: {data['url']}",
            f"Title: {data['title']}",
            f"Mode: {data['mode']}",
            f"Elapsed: {data['elapsed_seconds']} seconds",
            f"Max depth: {data['max_depth']}",
            f"Pages discovered: {data['pages_discovered']}",
            f"Pages crawled: {data['pages_crawled']}",
            f"Files downloaded: {data['files_downloaded']}",
            f"Files failed: {data['files_failed']}",
            f"Downloaded bytes: {data['total_downloaded_bytes']}",
            f"Retries: {data['retries']}",
            f"Resumed files: {data['resumed_files']}",
            "",
            "CRAWLED PAGES",
            "-" * 60,
        ]
        for page in data["pages"]:
            lines.append(f"[depth {page['depth']}] {page['url']}")

        lines += ["", "FAILED ITEMS", "-" * 60]
        if data["failed_urls"]:
            for item in data["failed_urls"]:
                lines.append(
                    f"[{item['kind']}] {item['url']} | attempts={item['attempts']} | {item['error']}"
                )
        else:
            lines.append("None")

        text_path.write_text("\n".join(lines), encoding="utf-8")
        return json_path, text_path

    def run(self):
        self.root_dir.mkdir(parents=True, exist_ok=True)
        validate_target_url(self.target_url)
        if self.mode == "page":
            self.run_page_only()
        elif self.mode == "images":
            self.run_images_only()
        elif self.mode == "important":
            self.run_important()
        else:
            self.run_full()
        if self.cancelled():
            raise RuntimeError("تم إلغاء السحب")
        if not self.url_map:
            raise RuntimeError("لم يتم العثور على ملفات قابلة للسحب")

        self.progress.stage = "إعداد التقرير"
        self.create_report_files()
        self.progress.stage = "إنشاء ZIP"
        return self.create_zip()

    def create_zip(self):
        zip_path = self.root_dir.with_suffix(".zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in self.root_dir.rglob("*"):
                if self.cancelled():
                    raise RuntimeError("تم إلغاء السحب")
                if path.is_file():
                    zf.write(path, path.relative_to(self.root_dir))
        if zip_path.stat().st_size > MAX_ZIP_BYTES:
            raise RuntimeError("حجم ملف ZIP أكبر من الحد المسموح لإرساله عبر Telegram.")
        return zip_path


# =========================
# LOCAL REAL PREVIEW SERVER
# =========================

class PreviewServer:
    """سيرفر معاينة حقيقي مع روابط LAN، ومحاولة نفق Cloudflare اختياري."""

    def __init__(self, root_dir):
        self.root_dir = Path(root_dir).resolve()
        self.server = None
        self.thread = None
        self.port = None
        self.token = secrets.token_urlsafe(18)
        self.tunnel_process = None
        self.preview_url = None

    @staticmethod
    def local_addresses():
        """يرجع كل عناوين IPv4 المحلية القابلة للاستخدام، بدون اتصال خارجي."""
        addresses = {"127.0.0.1"}

        # hostname -I أدق داخل Kali/VirtualBox عند وجود أكثر من واجهة.
        try:
            output = subprocess.check_output(
                ["hostname", "-I"],
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=2.0,
            )
            for raw in output.split():
                try:
                    ip = ipaddress.ip_address(raw)
                    if ip.version == 4 and not ip.is_loopback:
                        addresses.add(str(ip))
                except ValueError:
                    continue
        except Exception:
            pass

        # fallback بدون DNS خارجي.
        try:
            host_ips = socket.gethostbyname_ex(socket.gethostname())[2]
            for raw in host_ips:
                try:
                    ip = ipaddress.ip_address(raw)
                    if ip.version == 4 and not ip.is_loopback:
                        addresses.add(str(ip))
                except ValueError:
                    continue
        except Exception:
            pass

        # محاولة أخيرة سريعة لمعرفة الواجهة الافتراضية، مع timeout صغير جدًا.
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.settimeout(0.5)
            sock.connect(("8.8.8.8", 80))
            ip = sock.getsockname()[0]
            if ip:
                addresses.add(ip)
        except Exception:
            pass
        finally:
            sock.close()

        return sorted(addresses, key=lambda value: (value == "127.0.0.1", value))

    @staticmethod
    def find_cloudflared():
        """يكتشف cloudflared إن كان مثبتًا بالفعل."""
        for command in ("cloudflared", "/usr/local/bin/cloudflared", "/usr/bin/cloudflared"):
            try:
                path = shutil.which(command) if "/" not in command else command
                if path and Path(path).exists():
                    return path
            except Exception:
                continue
        return None

    def _start_cloudflare_tunnel(self):
        """يحاول إنشاء رابط HTTPS عام مؤقت. لا يعتمد عليه إذا لم يكن cloudflared موجودًا."""
        cloudflared = self.find_cloudflared()
        if not cloudflared:
            return None

        try:
            self.tunnel_process = subprocess.Popen(
                [
                    cloudflared,
                    "tunnel",
                    "--url",
                    f"http://127.0.0.1:{self.port}",
                    "--no-autoupdate",
                    "--loglevel",
                    "info",
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception:
            self.tunnel_process = None
            return None

        deadline = time.time() + 15.0
        public_url = None
        pattern = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com(?:/[^\s]*)?", re.I)

        while time.time() < deadline:
            if self.tunnel_process.poll() is not None:
                break
            if self.tunnel_process.stdout is not None:
                try:
                    ready, _, _ = select.select([self.tunnel_process.stdout], [], [], 0.25)
                    if ready:
                        line = self.tunnel_process.stdout.readline()
                        match = pattern.search(line or "")
                        if match:
                            public_url = match.group(0).rstrip(".,")
                            break
                except Exception:
                    pass

        if not public_url:
            self._stop_tunnel()
            return None

        return public_url.rstrip("/")

    def _stop_tunnel(self):
        process = self.tunnel_process
        self.tunnel_process = None
        if not process:
            return
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
        except Exception:
            pass

    def _preview_target_url(self, base_url):
        prefix = f"/__preview__/{self.token}/index.html"
        return base_url.rstrip("/") + prefix

    def start(self):
        root = self.root_dir
        if not root.exists():
            raise FileNotFoundError("ملفات المعاينة غير موجودة")

        # لو السيرفر شغال بالفعل لا ننشئ سيرفرًا ثانيًا.
        if self.server and self.port:
            return self.preview_url or self._preview_target_url(
                self._base_display_url()
            )

        token = self.token

        class PreviewHandler(SimpleHTTPRequestHandler):
            server_version = "LEVI-Preview"
            sys_version = ""

            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=str(root), **kwargs)

            def _authorized_path(self):
                path = self.path.split("?", 1)[0]
                prefix = "/__preview__/" + token
                if not (path == prefix or path.startswith(prefix + "/")):
                    self.send_error(403, "Preview token required")
                    return None
                clean = path[len(prefix):] or "/index.html"
                if "\x00" in clean or ".." in Path(unquote(clean)).parts:
                    self.send_error(400, "Invalid path")
                    return None
                self.path = clean
                return clean

            def end_headers(self):
                # تقليل تسريب معلومات الزائر ومنع التخزين المؤقت لمعاينة مؤقتة.
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("X-Content-Type-Options", "nosniff")
                super().end_headers()

            def do_GET(self):
                if self._authorized_path() is None:
                    return
                super().do_GET()

            def do_HEAD(self):
                if self._authorized_path() is None:
                    return
                super().do_HEAD()

            def log_message(self, format, *args):
                # لا نسجل عناوين IP للزوار.
                return

        self.server = ThreadingHTTPServer(("0.0.0.0", 0), PreviewHandler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        # أفضل تجربة: رابط HTTPS عام مؤقت إذا كان cloudflared متاحًا.
        public_base = self._start_cloudflare_tunnel()
        if public_base:
            self.preview_url = self._preview_target_url(public_base)
            return self.preview_url

        addresses = self.local_addresses()
        # الأولوية لعنوان الشبكة، ثم localhost.
        network = [ip for ip in addresses if ip != "127.0.0.1"]
        primary = network[0] if network else "127.0.0.1"
        self.preview_url = self._preview_target_url(f"http://{primary}:{self.port}")
        return self.preview_url

    def _base_display_url(self):
        if self.preview_url:
            return self.preview_url.rsplit("/index.html", 1)[0]
        addresses = self.local_addresses()
        ip = next((x for x in addresses if x != "127.0.0.1"), "127.0.0.1")
        return f"http://{ip}:{self.port}"

    def all_urls(self):
        """يعرض الرابط العام إن وجد + كل روابط LAN المناسبة."""
        urls = []
        if self.preview_url:
            urls.append(self.preview_url)
        for ip in self.local_addresses():
            url = self._preview_target_url(f"http://{ip}:{self.port}")
            if url not in urls:
                urls.append(url)
        return urls

    def stop(self):
        self._stop_tunnel()
        if self.server:
            try:
                self.server.shutdown()
                self.server.server_close()
            except Exception:
                pass
            self.server = None
        self.thread = None
        self.port = None
        self.preview_url = None


# =========================
# JOB MANAGEMENT
# =========================


def cleanup_paths(*paths):
    for path in paths:
        try:
            if not path:
                continue
            p = Path(path)
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            elif p.exists():
                p.unlink(missing_ok=True)
        except Exception:
            pass


async def delayed_cleanup(token):
    await asyncio.sleep(RESULT_TTL)
    with LOCK:
        result = RESULTS.pop(token, None)
    if result:
        preview = result.get("preview_server")
        if preview:
            preview.stop()
        cleanup_paths(result.get("root_dir"), result.get("zip_path"))


async def progress_updater(message, progress, cancel_event):
    while not cancel_event.is_set():
        await asyncio.sleep(1.0)
        try:
            await message.edit_text(progress_text(progress), reply_markup=running_keyboard(), parse_mode="Markdown")
        except Exception:
            pass
        # The worker's stage reaches a final state before this loop stops.
        snap = progress.snapshot()
        if snap["stage"] in {"إنشاء ZIP", "اكتمل", "ملغي", "فشل"}:
            break


# =========================
# BOT HANDLERS
# =========================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = "أهـلًا وسـهـلًا بـكـم فـي بـوت سـحـب مـلـفـات الـمـواقـع الشامل 🌐"
    if update.message:
        await update.message.reply_text(welcome_text, reply_markup=main_keyboard())
    elif update.callback_query:
        await update.callback_query.message.edit_text(welcome_text, reply_markup=main_keyboard())


async def begin_scrape_setup(query, chat_id):
    if chat_id in ACTIVE_JOBS:
        await query.edit_message_text("⚠️ يوجد سحب جارٍ بالفعل لهذا المستخدم.", reply_markup=running_keyboard())
        return
    PENDING_URLS.pop(chat_id, None)
    await query.edit_message_text(
        "🔗 **أرسل الآن رابط السيرفر/الموقع**\n\n"
        "بعدها اختر نوع السحب المطلوب.\n"
        "الحدود الأمنية: روابط HTTP/HTTPS عامة فقط، وحجم الموقع محدود لمنع العمليات الخطرة.",
        parse_mode="Markdown",
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    data = query.data or ""

    if data == "start_scrape":
        await begin_scrape_setup(query, chat_id)
        return

    if data == "cancel_setup":
        PENDING_URLS.pop(chat_id, None)
        await query.edit_message_text("✅ تم إلغاء الإعداد.", reply_markup=main_keyboard())
        return

    if data.startswith("scrape:"):
        url = PENDING_URLS.get(chat_id)
        if not url:
            await query.edit_message_text("❌ أرسل الرابط أولًا.", reply_markup=main_keyboard())
            return
        mode = data.split(":", 1)[1]
        await start_job(update, context, url, mode)
        return

    if data == "job:cancel":
        job = ACTIVE_JOBS.get(chat_id)
        if not job:
            await query.answer("لا توجد عملية نشطة.", show_alert=False)
            return
        job["cancel_requested"] = True
        job["cancel_event"].set()
        job["progress"].stage = "ملغي"
        await query.edit_message_text("🛑 جاري إيقاف السحب وتنظيف الملفات المؤقتة...", parse_mode="Markdown")
        return

    if data.startswith("result:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        action, token = parts[1], parts[2]
        with LOCK:
            result = RESULTS.get(token)
        if not result or result["chat_id"] != chat_id:
            await query.answer("❌ انتهت صلاحية النتيجة.", show_alert=True)
            return

        if action == "preview":
            index_path = Path(result["index_path"])
            if not index_path.exists():
                await query.answer("❌ ملفات المعاينة انتهت صلاحيتها.", show_alert=True)
                return
            try:
                await query.answer("⏳ جاري تجهيز المعاينة الحقيقية...")
                preview_server = result.get("preview_server")
                if preview_server is None:
                    preview_server = PreviewServer(result["root_dir"])
                    result["preview_server"] = preview_server

                preview_url = preview_server.start()
                result["preview_url"] = preview_url

                # إرسال رابط المعاينة فقط بدون أي عناوين شبكة بديلة أو معلومات عن IP.
                await query.message.reply_text(
                    preview_url,
                    disable_web_page_preview=False,
                )
            except Exception as exc:
                try:
                    await query.answer(f"❌ تعذر تشغيل المعاينة: {str(exc)[:150]}", show_alert=True)
                except Exception:
                    pass
            return

        if action == "report":
            report_path = Path(result.get("report_text", ""))
            if not report_path.exists():
                await query.answer("❌ التقرير غير متاح.", show_alert=True)
                return
            with open(report_path, "rb") as report_file:
                await query.message.reply_document(
                    document=report_file,
                    filename=report_path.name,
                    caption=(
                        "📋 **تقرير سحب الموقع**\n\n"
                        f"📄 الصفحات: `{result.get('pages', 0)}`\n"
                        f"📁 الملفات: `{result.get('files', 0)}`\n"
                        f"🔁 إعادة المحاولة: `{result.get('retries', 0)}`\n"
                        f"♻️ الاستكمال: `{result.get('resumed_files', 0)}`\n"
                        f"⚠️ الفشل: `{result.get('failed', 0)}`"
                    ),
                    parse_mode="Markdown",
                )
            await query.answer("✅ تم إرسال التقرير.")
            return

        if action == "zip":
            zip_path = Path(result["zip_path"])
            if not zip_path.exists():
                await query.answer("❌ ملف ZIP انتهت صلاحيته.", show_alert=True)
                return
            with open(zip_path, "rb") as archive:
                await query.message.reply_document(
                    document=archive,
                    filename=zip_path.name,
                    caption=(
                        f"📦 **تم تجهيز ZIP**\n\n"
                        f"🌐 {result['url']}\n"
                        f"📄 الصفحات: {result.get('pages', 0)}\n"
                        f"📁 الملفات: {result['files']}\n"
                        f"📦 الحجم: {human_bytes(result['total_bytes'])}\n"
                        f"📋 التقرير داخل ZIP: نعم"
                    ),
                    parse_mode="Markdown",
                )
            await query.answer("✅ تم إرسال ZIP.")
            return

        if action == "delete":
            with LOCK:
                RESULTS.pop(token, None)
            preview = result.get("preview_server")
            if preview:
                preview.stop()
            cleanup_paths(result.get("root_dir"), result.get("zip_path"))
            await query.edit_message_text("🗑️ تم حذف نتيجة السحب والملفات المؤقتة.", reply_markup=main_keyboard())
            return


async def start_job(update, context, url, mode):
    query = update.callback_query
    chat_id = query.message.chat.id
    PENDING_URLS.pop(chat_id, None)

    cancel_event = threading.Event()
    progress = Progress()
    domain = sanitize_folder_name(url)
    root_dir = BASE_STORAGE / f"site_{domain}_{int(time.time())}"
    job = {
        "cancel_event": cancel_event,
        "progress": progress,
        "root_dir": root_dir,
        "cancel_requested": False,
        "timed_out": False,
    }
    ACTIVE_JOBS[chat_id] = job

    await query.edit_message_text(progress_text(progress), reply_markup=running_keyboard(), parse_mode="Markdown")

    scraper = SiteScraper(url, mode, root_dir, cancel_event, progress)

    async def worker():
        try:
            return await asyncio.to_thread(scraper.run)
        except RuntimeError:
            # الإلغاء اليدوي/المهلة تتم معالجتهما في start_job بعد
            # استرجاع الـ Task بالكامل.
            if cancel_event.is_set():
                return None
            raise
        except asyncio.CancelledError:
            # لا نحول إلغاء مهمة الإطار إلى "زر إلغاء".
            raise

    task = asyncio.create_task(worker())

    try:
        while not task.done():
            await asyncio.sleep(1.0)
            try:
                await query.message.edit_text(
                    progress_text(progress),
                    reply_markup=running_keyboard(),
                    parse_mode="Markdown",
                )
            except Exception:
                pass

            if JOB_TIMEOUT is not None and time.time() - progress.started > JOB_TIMEOUT:
                job["timed_out"] = True
                cancel_event.set()
                progress.stage = "إيقاف بسبب المهلة"
                break

        # ننتظر الـ worker دائمًا حتى لا يظل Task في الخلفية.
        try:
            zip_path = await asyncio.shield(task)
        except asyncio.CancelledError:
            # إلغاء خارجي لمهمة Telegram: أوقف السحب وانتظر worker.
            cancel_event.set()
            try:
                await asyncio.shield(task)
            except BaseException:
                pass
            raise

        if job["timed_out"]:
            raise TimeoutError("انتهت المهلة المحددة للعملية.")

        if job["cancel_requested"]:
            raise RuntimeError("تم إلغاء السحب بواسطة المستخدم")

        if zip_path is None:
            if cancel_event.is_set():
                raise RuntimeError("توقف السحب قبل اكتماله")
            raise RuntimeError("لم يُنشئ السحب ملف ZIP")

        progress.stage = "اكتمل"
        files_count = sum(1 for p in root_dir.rglob("*") if p.is_file())
        index_candidates = [p for p in root_dir.rglob("index.html")]
        index_path = index_candidates[0] if index_candidates else next(iter(root_dir.rglob("*.html")), None)
        if index_path is None:
            raise RuntimeError("لم يتم العثور على صفحة HTML للمعاينة")

        token = f"{chat_id}_{int(time.time())}_{files_count}"
        report_json = root_dir / REPORT_JSON_NAME
        report_text = root_dir / REPORT_TEXT_NAME
        result = {
            "chat_id": chat_id,
            "user_id": update.effective_user.id,
            "url": url,
            "mode": mode,
            "root_dir": str(root_dir),
            "zip_path": str(zip_path),
            "index_path": str(index_path),
            "report_json": str(report_json),
            "report_text": str(report_text),
            "files": files_count,
            "total_bytes": progress.bytes,
            "zip_bytes": zip_path.stat().st_size if zip_path.exists() else 0,
            "title": scraper.title,
            "pages": len(scraper.pages_crawled),
            "retries": sum(scraper.retry_counts.values()),
            "resumed_files": scraper.resumed_files,
            "failed": len(scraper.failure_details),
            "preview_server": None,
            "preview_url": None,
        }
        with LOCK:
            RESULTS[token] = result
        asyncio.create_task(delayed_cleanup(token))

        await query.message.edit_text(
            "✅ **اكتمل سحب الموقع بنجاح**\n\n"
            f"🌐 الرابط: `{url}`\n"
            f"📄 العنوان: `{scraper.title}`\n"
            f"📄 الصفحات: `{len(scraper.pages_crawled)}`\n"
            f"📁 الملفات: `{files_count}`\n"
            f"📦 الحجم: `{human_bytes(progress.bytes)}`\n"
            f"🔁 المحاولات الإضافية: `{sum(scraper.retry_counts.values())}`\n"
            f"♻️ ملفات تم استكمالها: `{scraper.resumed_files}`\n"
            f"⚠️ ملفات فشلت: `{len(scraper.failure_details)}`\n\n"
            "اختر المعاينة أو التقرير أو ZIP:",
            reply_markup=result_keyboard(token),
            parse_mode="Markdown",
        )

    except asyncio.CancelledError:
        progress.stage = "ملغي"
        cleanup_paths(root_dir, root_dir.with_suffix(".zip"))
        raise
    except Exception as exc:
        if job.get("timed_out"):
            progress.stage = "انتهت المهلة"
            text = "⏱️ انتهت المهلة المحددة للسحب وتم تنظيف العملية."
        elif job.get("cancel_requested"):
            progress.stage = "ملغي"
            text = "🛑 تم إلغاء العملية."
        else:
            progress.stage = "فشل"
            detail = str(exc).strip() or exc.__class__.__name__
            text = f"❌ فشل السحب:\n`{detail[:700]}`"

        cleanup_paths(root_dir, root_dir.with_suffix(".zip"))
        try:
            await query.message.edit_text(text, reply_markup=main_keyboard(), parse_mode="Markdown")
        except Exception:
            pass
    finally:
        ACTIVE_JOBS.pop(chat_id, None)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = normalize_url(update.message.text)
    if not url:
        await update.message.reply_text(
            "❌ **الرجاء إرسال رابط سيرفر صحيح.**",
            reply_markup=main_keyboard(),
            parse_mode="Markdown",
        )
        return

    try:
        validate_target_url(url)
    except Exception as exc:
        await update.message.reply_text(
            f"❌ {str(exc)}",
            reply_markup=main_keyboard(),
        )
        return

    chat_id = update.message.chat.id
    if chat_id in ACTIVE_JOBS:
        await update.message.reply_text("⚠️ يوجد سحب جارٍ بالفعل. استخدم زر الإلغاء أولًا.", reply_markup=running_keyboard())
        return

    PENDING_URLS[chat_id] = url
    await update.message.reply_text(
        "✅ تم قبول الرابط. اختر نوع السحب:",
        reply_markup=scrape_types_keyboard(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🆘 **طريقة الاستخدام**\n\n"
        "1️⃣ اضغط سحب ملفات موقع.\n"
        "2️⃣ أرسل رابط الموقع.\n"
        "3️⃣ اختر: الصفحة فقط / الموقع كامل / الصور فقط / الملفات المهمة.\n"
        "4️⃣ البوت يزحف للصفحات الداخلية داخل نفس الموقع حسب عمق السحب المسموح.\n"
        "5️⃣ التحميل يستخدم إعادة محاولة تلقائية واستكمال الملفات الجزئية عند انقطاع الاتصال.\n"
        "6️⃣ بعد الانتهاء يمكنك معاينة index.html أو إرسال ZIP أو تقرير كامل.\n\n"
        "🔒 يوجد حد أقصى للحجم والملفات، ويتم منع الروابط الداخلية وإعادة التوجيه الخطرة.",
        parse_mode="Markdown",
        reply_markup=main_keyboard(),
    )


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN غير مضبوط. ضع توكن البوت في متغير البيئة BOT_TOKEN ثم شغّل البرنامج.")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .get_updates_connect_timeout(30.0)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Website Scraper Bot PRO is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
