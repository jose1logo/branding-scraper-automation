import asyncio
import csv
import datetime
import io
import json
import logging
import os
import re
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from aiohttp import web
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from dotenv import load_dotenv

def int_from_env(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


# Load environment variables
load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PORT = int_from_env("PORT", 10000)
ALLOWED_CHAT_ID = os.getenv("CHAT_ID")
HTTP_TIMEOUT_SECONDS = int_from_env("HTTP_TIMEOUT_SECONDS", 20)
ZERO_NEW_ALERT_DAYS_DAILY = int_from_env("ZERO_NEW_ALERT_DAYS_DAILY", 2)
ZERO_NEW_ALERT_DAYS_WEEKLY = int_from_env("ZERO_NEW_ALERT_DAYS_WEEKLY", 14)
DIGEST_LOOKBACK_DAYS = int_from_env("DIGEST_LOOKBACK_DAYS", 7)
DIGEST_TOP_PER_SOURCE = int_from_env("DIGEST_TOP_PER_SOURCE", 3)
FORBES_SCRAPEDO_TOKEN = (
    os.getenv("FORBES_SCRAPEDO_TOKEN")
    or os.getenv("SCRAPEDO_TOKEN")
    or ""
).strip()
FORBES_SCRAPEDO_ENDPOINT = (
    os.getenv("FORBES_SCRAPEDO_ENDPOINT")
    or "https://api.scrape.do/"
).strip()
FORBES_SCRAPEDO_TIMEOUT_SECONDS = int_from_env("FORBES_SCRAPEDO_TIMEOUT_SECONDS", 90)
FORBES_SCRAPEDO_RETRIES = int_from_env("FORBES_SCRAPEDO_RETRIES", 2)
FORBES_SCRAPEDO_RETRY_DELAY_SECONDS = int_from_env("FORBES_SCRAPEDO_RETRY_DELAY_SECONDS", 2)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(message)s",
)
LOGGER = logging.getLogger("branding_scraper")

# Notion "Blog Name" mapping (source -> option name in your Notion column)
BLOG_NAME_MAP = {
    "Branding Mag": "Brand Mag",
    "Brand New": "underconsideration",
    "Branding Journal": "Branding Journal",
    "BP&O": "Bpando",
    "The Drum": "The Drum",
    "Forbes CMO Network": "Forbes CMO Network",
}

SOURCE_NAMES = {
    1: "Brand New",
    2: "Branding Journal",
    3: "Branding Mag",
    4: "BP&O",
    5: "The Drum",
    6: "Forbes CMO Network",
}
SOURCE_ZERO_ALERT_DAYS = {
    1: ZERO_NEW_ALERT_DAYS_DAILY,
    2: ZERO_NEW_ALERT_DAYS_WEEKLY,
    3: ZERO_NEW_ALERT_DAYS_WEEKLY,
    4: ZERO_NEW_ALERT_DAYS_WEEKLY,
    5: ZERO_NEW_ALERT_DAYS_WEEKLY,
    6: ZERO_NEW_ALERT_DAYS_WEEKLY,
}
REVERSE_BLOG_NAME_MAP = {value: key for key, value in BLOG_NAME_MAP.items()}


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def iso_utc(value):
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def format_human_utc(value):
    if value is None:
        return "Never"
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def log_event(event, level="info", **fields):
    payload = {"timestamp": iso_utc(utc_now()), "event": event}
    payload.update(fields)
    line = json.dumps(payload, ensure_ascii=True, default=str)
    level_method = getattr(LOGGER, level, LOGGER.info)
    level_method(line)


def new_source_state():
    return {
        "last_run": None,
        "last_success": None,
        "last_error": "",
        "last_error_at": None,
        "last_added_count": 0,
        "total_runs": 0,
        "total_added": 0,
        "total_failures": 0,
        "consecutive_failures": 0,
        "consecutive_zero_runs": 0,
        "first_run_at": None,
        "last_non_zero_at": None,
        "last_zero_alert_at": None,
    }


APP_STARTED_AT = utc_now()
STATE_LOCK = threading.Lock()
METRICS_STATE = {
    "sync_runs_total": 0,
    "sync_runs_success_total": 0,
    "sync_runs_failure_total": 0,
    "articles_uploaded_total": 0,
    "telegram_notifications_total": 0,
    "notion_query_failures_total": 0,
    "duplicate_skipped_notion_total": 0,
    "duplicate_skipped_runtime_total": 0,
    "duplicate_skipped_batch_total": 0,
    "dedupe_check_unavailable_total": 0,
}
SOURCE_STATE = {source_id: new_source_state() for source_id in SOURCE_NAMES}

# --- Scraping Functions ---

def get_brand_new_articles(start_date, end_date):
    url = "https://www.underconsideration.com/brandnew/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error fetching Brand New: {e}") from e

    soup = BeautifulSoup(response.text, 'html.parser')
    articles = []
    daily_sections = soup.find_all('section', class_='daily')

    for section in daily_sections:
        date_header = section.find('h1', class_='daily_date')
        if not date_header: continue
        
        date_str = date_header.get_text(strip=True)
        try:
            clean_date_str = " ".join(date_str.split()[1:])
            current_date = datetime.datetime.strptime(clean_date_str, "%B %d, %Y").date()
        except: continue

        if start_date <= current_date <= end_date:
            modules = section.find_all('div', class_='module')
            for module in modules:
                link_tag = module.find('a', href=re.compile(r'brandnew/archives/.*\.php'))
                if not link_tag:
                    title_link = module.find(['h1', 'h2'])
                    link_tag = title_link.find('a', href=True) if title_link else None

                if link_tag:
                    link = link_tag['href']
                    if '/category/' in link or link.endswith('#respond'): continue
                    
                    # Better title extraction
                    h1 = module.find('h1')
                    h2 = module.find('h2')
                    
                    h1_text = ""
                    if h1:
                        h1_copy = BeautifulSoup(str(h1), 'html.parser')
                        for span in h1_copy.find_all('span', class_='homepage_editorial_category'):
                            span.decompose()
                        h1_text = h1_copy.get_text(strip=True)
                        
                    h2_text = ""
                    if h2:
                        h2_copy = BeautifulSoup(str(h2), 'html.parser')
                        for span in h2_copy.find_all('span', class_='homepage_editorial_category'):
                            span.decompose()
                        h2_text = h2_copy.get_text(strip=True)
                    
                    generic_cats = ["Quirky", "News", "Nice", "Job Board", "Linked"]
                    if any(cat == h2_text for cat in generic_cats) and h1_text:
                        title = h1_text
                    elif h2_text:
                        title = h2_text
                    else:
                        title = h1_text or link_tag.get_text(strip=True)

                    articles.append({"title": title, "link": link, "date": current_date.isoformat(), "source": "Brand New"})
    return articles

def get_bj_articles(start_date, end_date):
    url = "https://www.thebrandingjournal.com/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error fetching Branding Journal: {e}") from e

    soup = BeautifulSoup(response.text, 'html.parser')
    articles = []
    for item in soup.find_all('article'):
        title_tag = item.find(['h2', 'h3'], class_='cs-entry__title')
        date_tag = item.find('div', class_='cs-meta-date')
        if title_tag and date_tag:
            link_tag = title_tag.find('a', href=True)
            if link_tag:
                title = link_tag.get_text(strip=True)
                link = link_tag['href']
                try:
                    current_date = datetime.datetime.strptime(date_tag.get_text(strip=True), "%B %d, %Y").date()
                except: continue
                if start_date <= current_date <= end_date:
                    articles.append({"title": title, "link": link, "date": current_date.isoformat(), "source": "Branding Journal"})
    return articles

def get_bm_articles(start_date, end_date):
    url = "https://www.brandingmag.com/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error fetching Branding Mag: {e}") from e

    soup = BeautifulSoup(response.text, 'html.parser')
    potential_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('/'): href = 'https://www.brandingmag.com' + href
        title = a.get_text(strip=True)
        if 'brandingmag.com/' in href and len(title) > 25:
            if any(x in href for x in ['/product/', '/category/', '/author/', '/tag/', '/about/', '/contact/', '/subscribe/']): continue
            potential_links.append({"title": title, "link": href})

    unique_links = []
    seen = set()
    for l in potential_links:
        if l['link'] not in seen:
            unique_links.append(l); seen.add(l['link'])

    articles = []
    for item in unique_links:
        try:
            res = requests.get(item['link'], headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
            match = re.search(r'"datePublished":"(\d{4}-\d{2}-\d{2})', res.text)
            if not match:
                match = re.search(r'itemprop="dateModified" content="(\d{4}-\d{2}-\d{2})', res.text)
            if match:
                art_date = datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
                if start_date <= art_date <= end_date:
                    articles.append({"title": item['title'], "link": item['link'], "date": art_date.isoformat(), "source": "Branding Mag"})
        except: continue
    return articles

def get_bp_articles(start_date, end_date):
    url = "https://bpando.org/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error fetching BP&O: {e}") from e

    soup = BeautifulSoup(response.text, 'html.parser')
    articles = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        title_tag = a.find(['h2', 'h1', 'h3'])
        if title_tag:
            title = title_tag.get_text(strip=True)
        else:
            title = a.get_text(strip=True)
        
        date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', href)
        if date_match and len(title) > 5:
            title = title.replace("BP&O VoicesPackaging:", "").replace("BP&O VoicesJobs:", "").strip()
            try:
                current_date = datetime.date(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)))
                if start_date <= current_date <= end_date:
                    articles.append({"title": title, "link": href, "date": current_date.isoformat(), "source": "BP&O"})
            except: continue
    unique_articles = []
    seen_links = set()
    for a in articles:
        if a['link'] not in seen_links:
            unique_articles.append(a); seen_links.add(a['link'])
    return unique_articles


def parse_article_date_from_html(html_text, url_hint=""):
    patterns = [
        r'property="article:published_time"\s+content="(\d{4}-\d{2}-\d{2})',
        r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})',
        r'"dateModified"\s*:\s*"(\d{4}-\d{2}-\d{2})',
        r'itemprop="datePublished"\s+content="(\d{4}-\d{2}-\d{2})',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), "%Y-%m-%d").date()
            except Exception:
                continue

    url_match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url_hint or "")
    if url_match:
        try:
            return datetime.date(
                int(url_match.group(1)),
                int(url_match.group(2)),
                int(url_match.group(3)),
            )
        except Exception:
            return None
    return None


def forbes_request_headers():
    return {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.forbes.com/cmo-network/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def is_forbes_block_page(text):
    lower = (text or "").lower()
    return (
        "please enable js and disable any ad blocker" in lower
        or "you have been blocked | forbes" in lower
        or "captcha-delivery.com" in lower
        or "geo.captcha-delivery.com/captcha" in lower
    )


FORBES_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]*)\]\((https?://[^)]+)\)")
FORBES_NAV_LABELS = {
    "newsletters",
    "games",
    "share a news tip",
    "breaking news",
    "white house watch",
    "see all",
    "billionaires",
    "innovation",
}


def clean_forbes_extracted_text(text):
    cleaned_lines = []
    nav_like_streak = 0

    for raw_line in (text or "").splitlines():
        line = re.sub(r"\s+", " ", (raw_line or "")).strip()
        if not line:
            continue

        non_link_text = FORBES_MARKDOWN_LINK_RE.sub("", line)
        non_link_text = re.sub(r"[\s\-\*\|]+", "", non_link_text)
        has_markdown_link = bool(FORBES_MARKDOWN_LINK_RE.search(line))
        if has_markdown_link and not non_link_text:
            nav_like_streak += 1
            if nav_like_streak >= 10 and len(cleaned_lines) >= 3:
                break
            continue

        if has_markdown_link:
            line = FORBES_MARKDOWN_LINK_RE.sub(lambda m: f" {m.group(1)} ", line)
            line = re.sub(r"\s+", " ", line).strip()

        line = re.sub(r"^[\*\-]\s+", "", line).strip()
        if not line:
            nav_like_streak += 1
            continue

        lower = line.lower()
        if lower in FORBES_NAV_LABELS:
            nav_like_streak += 1
            if nav_like_streak >= 10 and len(cleaned_lines) >= 3:
                break
            continue
        if re.search(r"see\s*all$", lower) and len(line) <= 40:
            nav_like_streak += 1
            continue
        if re.fullmatch(r"https?://\S+", line):
            nav_like_streak += 1
            continue

        cleaned_lines.append(line)
        nav_like_streak = 0

    return "\n".join(cleaned_lines).strip()


def normalize_forbes_text_for_compare(text):
    value = (text or "").lower()
    value = value.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_forbes_article_url(raw_url):
    if not raw_url:
        return ""

    href = (raw_url or "").strip()
    if not href:
        return ""

    if href.startswith("//"):
        href = "https:" + href
    elif href.startswith("/"):
        href = "https://www.forbes.com" + href
    elif href.startswith("http://"):
        href = "https://" + href[len("http://"):]

    parsed = urlsplit(href)
    netloc = (parsed.netloc or "").lower()
    if netloc == "forbes.com":
        netloc = "www.forbes.com"
    if not netloc.endswith("forbes.com"):
        return ""

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urlunsplit(("https", netloc, path, "", ""))


def extract_forbes_cmo_candidates(next_data):
    data = next_data.get("props", {}).get("pageProps", {}).get("data", {}) or {}
    candidates = []

    def add_candidate(raw_title, raw_url, raw_summary=""):
        link = normalize_forbes_article_url(raw_url)
        if not link or "/sites/" not in link:
            return
        if not re.search(r"/\d{4}/\d{2}/\d{2}/", link):
            return

        title = re.sub(r"\s+", " ", (raw_title or "").strip())
        if not title:
            slug = link.rstrip("/").split("/")[-1]
            title = re.sub(r"[-_]+", " ", slug).strip()
        if title.endswith(" - Forbes"):
            title = title[: -len(" - Forbes")].strip()

        summary = re.sub(r"\s+", " ", (raw_summary or "").strip())
        candidates.append({"title": title, "link": link, "summary": summary})

    for item in data.get("editorsPicks", []) or []:
        if isinstance(item, dict):
            add_candidate(item.get("title", ""), item.get("uri", ""), "")

    for row in data.get("rows", []) or []:
        if not isinstance(row, dict):
            continue

        latest_content = row.get("latestContent") or []
        for item in latest_content:
            if isinstance(item, dict):
                add_candidate(
                    item.get("title", ""),
                    item.get("uri", "") or item.get("url", ""),
                    item.get("description", ""),
                )

        blocks = row.get("blocks")
        if isinstance(blocks, dict):
            items = blocks.get("items") or []
            for item in items:
                if isinstance(item, dict):
                    add_candidate(
                        item.get("title", ""),
                        item.get("url", "") or item.get("uri", ""),
                        item.get("description", ""),
                    )
        elif isinstance(blocks, list):
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                for item in block.get("items", []) or []:
                    if isinstance(item, dict):
                        add_candidate(
                            item.get("title", ""),
                            item.get("url", "") or item.get("uri", ""),
                            item.get("description", ""),
                        )

    return candidates


def fetch_forbes_html_via_scrapedo(article_url):
    if not FORBES_SCRAPEDO_TOKEN:
        return ""

    target_url = normalize_forbes_article_url(article_url) or article_url
    configured_endpoint = (FORBES_SCRAPEDO_ENDPOINT or "").strip()
    if configured_endpoint and not configured_endpoint.startswith(("http://", "https://")):
        configured_endpoint = "https://" + configured_endpoint.lstrip("/")
    if not configured_endpoint:
        configured_endpoint = "https://api.scrape.do/"

    endpoint_candidates = [configured_endpoint]
    if configured_endpoint.startswith("http://"):
        secure_endpoint = "https://" + configured_endpoint[len("http://") :]
        if secure_endpoint not in endpoint_candidates:
            endpoint_candidates.insert(0, secure_endpoint)
    elif configured_endpoint.startswith("https://"):
        insecure_endpoint = "http://" + configured_endpoint[len("https://") :]
        if insecure_endpoint not in endpoint_candidates:
            endpoint_candidates.append(insecure_endpoint)

    max_attempts = max(int(FORBES_SCRAPEDO_RETRIES), 1)
    retry_delay = max(int(FORBES_SCRAPEDO_RETRY_DELAY_SECONDS), 0)
    read_timeout = max(int(FORBES_SCRAPEDO_TIMEOUT_SECONDS), 20)
    connect_timeout = min(max(HTTP_TIMEOUT_SECONDS, 5), 20)

    for endpoint in endpoint_candidates:
        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.get(
                    endpoint,
                    params={
                        "url": target_url,
                        "token": FORBES_SCRAPEDO_TOKEN,
                        "output": "raw",
                    },
                    timeout=(connect_timeout, read_timeout),
                )
            except requests.exceptions.Timeout as e:
                log_event(
                    "forbes_scrapedo_timeout",
                    level="warning",
                    url=target_url,
                    endpoint=endpoint,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    error=str(e),
                )
                if attempt < max_attempts and retry_delay > 0:
                    time.sleep(retry_delay)
                continue
            except Exception as e:
                log_event(
                    "forbes_scrapedo_fetch_error",
                    level="error",
                    url=target_url,
                    endpoint=endpoint,
                    attempt=attempt,
                    error=str(e),
                )
                if attempt < max_attempts and retry_delay > 0:
                    time.sleep(retry_delay)
                continue

            if response.status_code != 200:
                log_event(
                    "forbes_scrapedo_fetch_failed",
                    level="error",
                    url=target_url,
                    endpoint=endpoint,
                    attempt=attempt,
                    status_code=response.status_code,
                )
                if attempt < max_attempts and retry_delay > 0:
                    time.sleep(retry_delay)
                continue

            html_text = (response.text or "").strip()
            if not html_text:
                log_event(
                    "forbes_scrapedo_empty_response",
                    level="warning",
                    url=target_url,
                    endpoint=endpoint,
                    attempt=attempt,
                )
                if attempt < max_attempts and retry_delay > 0:
                    time.sleep(retry_delay)
                continue

            if is_forbes_block_page(html_text):
                log_event(
                    "forbes_scrapedo_block_page",
                    level="warning",
                    url=target_url,
                    endpoint=endpoint,
                    attempt=attempt,
                )
                if attempt < max_attempts and retry_delay > 0:
                    time.sleep(retry_delay)
                continue

            log_event(
                "forbes_scrapedo_content_used",
                source="Forbes CMO Network",
                url=target_url,
                endpoint=endpoint,
                attempt=attempt,
                chars=len(html_text),
            )
            return html_text

    return ""


def fetch_forbes_article_html(article_url):
    return fetch_forbes_html_via_scrapedo(article_url)


def get_thedrum_articles(start_date, end_date):
    headers = {"User-Agent": "Mozilla/5.0"}
    seed_urls = ["https://www.thedrum.com/", "https://www.thedrum.com/news"]
    candidate_links = []
    seen_candidates = set()

    for url in seed_urls:
        try:
            response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
            response.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Error fetching The Drum seed page {url}: {e}") from e

        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.thedrum.com" + href
            if not href.startswith("https://www.thedrum.com/"):
                continue
            if "#" in href:
                href = href.split("#", 1)[0]
            if "?" in href:
                href = href.split("?", 1)[0]

            if not any(segment in href for segment in ["/news/", "/opinion/", "/work/"]):
                continue
            if any(
                segment in href
                for segment in ["/author/", "/topic/", "/topics/", "/events/", "/jobs/", "/directory/"]
            ):
                continue

            title = a.get_text(strip=True)
            if len(title) < 15:
                continue

            if href not in seen_candidates:
                seen_candidates.add(href)
                candidate_links.append({"title": title, "link": href})

    articles = []
    for item in candidate_links[:90]:
        try:
            article_response = requests.get(item["link"], headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
            article_response.raise_for_status()
            article_date = parse_article_date_from_html(article_response.text, item["link"])
            if not article_date:
                continue
            if start_date <= article_date <= end_date:
                articles.append(
                    {
                        "title": item["title"],
                        "link": item["link"],
                        "date": article_date.isoformat(),
                        "source": "The Drum",
                    }
                )
        except Exception:
            continue

    unique_articles = []
    seen_links = set()
    for article in articles:
        if article["link"] not in seen_links:
            unique_articles.append(article)
            seen_links.add(article["link"])
    return unique_articles


def get_forbes_cmo_articles(start_date, end_date):
    headers = forbes_request_headers()
    seed_url = "https://www.forbes.com/cmo-network/"
    try:
        response = requests.get(seed_url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error fetching Forbes CMO Network: {e}") from e

    soup = BeautifulSoup(response.text, "html.parser")
    candidate_links = []
    next_data_script = soup.find("script", id="__NEXT_DATA__")
    if next_data_script:
        try:
            next_data = json.loads(next_data_script.get_text() or "{}")
            candidate_links.extend(extract_forbes_cmo_candidates(next_data))
        except Exception as e:
            log_event("forbes_next_data_parse_error", level="error", error=str(e))

    if not candidate_links:
        for a in soup.find_all("a", href=True):
            href = normalize_forbes_article_url(a.get("href", ""))
            if not href:
                continue
            lower_href = href.lower()
            if "/sites/" not in lower_href:
                continue
            if "ss=cmo-network" not in (a.get("href", "").lower()) and "/cmo-network/" not in lower_href:
                continue
            if not re.search(r"/\d{4}/\d{2}/\d{2}/", href):
                continue

            title = (a.get_text() or "").strip()
            summary = ""
            candidate_links.append({"title": title, "link": href, "summary": summary})

    articles_by_link = {}
    for item in candidate_links[:180]:
        url_date = parse_article_date_from_html("", item["link"])
        if not url_date:
            continue
        if not (start_date <= url_date <= end_date):
            continue

        title = (item.get("title") or "").strip()
        if " | " in title:
            title = title.split(" | ", 1)[0].strip()
        if len(title) < 10:
            slug = item["link"].rstrip("/").split("/")[-1]
            title = re.sub(r"[-_]+", " ", slug).strip()
        if title.endswith(" - Forbes"):
            title = title[: -len(" - Forbes")].strip()
        summary = re.sub(r"\s+", " ", (item.get("summary") or "").strip())

        existing = articles_by_link.get(item["link"])
        if existing:
            if len(summary) > len(existing.get("summary", "")):
                existing["summary"] = summary
            continue

        articles_by_link[item["link"]] = {
            "title": title,
            "link": item["link"],
            "date": url_date.isoformat(),
            "source": "Forbes CMO Network",
            "summary": summary,
        }

    return list(articles_by_link.values())

def text_to_notion_paragraph_blocks(text, max_blocks=50):
    blocks = []
    for raw_line in (text or "").splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if len(line) < 20:
            continue
        blocks.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": line[:2000]}}]},
            }
        )
        if len(blocks) >= max_blocks:
            break
    return blocks


FORBES_SECTION_HEADINGS = {
    "Topline",
    "Key Facts",
    "Crucial Quote",
    "Big Number",
    "Key Background",
    "Tangent",
    "What To Watch",
}


def notion_text_block(block_type, text):
    content = re.sub(r"\s+", " ", (text or "")).strip()
    if not content:
        return None

    payload = {"rich_text": [{"text": {"content": content[:2000]}}]}
    if block_type == "heading_2":
        return {"object": "block", "type": "heading_2", "heading_2": payload}
    if block_type == "bulleted_list_item":
        return {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": payload,
        }
    return {"object": "block", "type": "paragraph", "paragraph": payload}


def forbes_text_to_notion_blocks(text, max_blocks=50):
    blocks = []
    last_line = ""

    for raw_line in (text or "").splitlines():
        raw = (raw_line or "").strip()
        line = re.sub(r"\s+", " ", raw).strip()
        if not line:
            continue
        if line == last_line:
            continue
        last_line = line

        block = None
        if line in FORBES_SECTION_HEADINGS:
            block = notion_text_block("heading_2", line)
        elif raw.startswith(("* ", "- ", "• ")):
            bullet_text = re.sub(r"^[\*\-•]\s*", "", line).strip()
            if len(bullet_text) >= 12:
                block = notion_text_block("bulleted_list_item", bullet_text)
        elif len(line) >= 20:
            block = notion_text_block("paragraph", line)

        if block:
            blocks.append(block)
        if len(blocks) >= max_blocks:
            break

    return blocks


def build_forbes_snapshot_blocks(article, article_url, max_blocks=12):
    blocks = []
    summary = re.sub(r"\s+", " ", (article.get("summary") or "")).strip()
    title = re.sub(r"\s+", " ", (article.get("title") or "")).strip()

    intro_heading = notion_text_block("heading_2", "Article Snapshot")
    if intro_heading:
        blocks.append(intro_heading)

    added_summary = False
    if summary:
        sentences = [
            sentence.strip()
            for sentence in re.split(r"(?<=[.!?])\s+", summary)
            if len(sentence.strip()) >= 20
        ]
        if not sentences and len(summary) >= 20:
            sentences = [summary]

        for sentence in sentences[:4]:
            block = notion_text_block("bulleted_list_item", sentence)
            if block:
                blocks.append(block)
                added_summary = True
            if len(blocks) >= max_blocks:
                return blocks

    if not added_summary and title:
        block = notion_text_block(
            "paragraph",
            f"Summary unavailable. Added article metadata only for: {title}",
        )
        if block:
            blocks.append(block)

    source_heading = notion_text_block("heading_2", "Source")
    if source_heading and len(blocks) < max_blocks:
        blocks.append(source_heading)

    source_url_block = notion_text_block("paragraph", article_url)
    if source_url_block and len(blocks) < max_blocks:
        blocks.append(source_url_block)

    note_block = notion_text_block(
        "paragraph",
        "Full article body could not be extracted automatically due to source restrictions.",
    )
    if note_block and len(blocks) < max_blocks:
        blocks.append(note_block)

    return blocks[:max_blocks]


def extract_notion_block_text(block):
    if not isinstance(block, dict):
        return ""
    block_type = (block.get("type") or "").strip()
    if not block_type:
        return ""

    payload = block.get(block_type)
    if not isinstance(payload, dict):
        return ""

    rich_text = payload.get("rich_text")
    if not isinstance(rich_text, list):
        return ""

    chunks = []
    for item in rich_text:
        if not isinstance(item, dict):
            continue
        text_obj = item.get("text")
        if not isinstance(text_obj, dict):
            continue
        content = (text_obj.get("content") or "").strip()
        if content:
            chunks.append(content)

    return " ".join(chunks).strip()


def extract_notion_blocks_from_soup(soup, source, url):
    blocks = []
    if source == "Brand New":
        main = soup.find('div', class_='module') or soup.find('article') or soup.find('main') or soup
    elif source == "Branding Journal":
        main = soup.find('div', class_='entry-content') or soup.find('div', class_='cs-entry__content') or soup
    elif source == "BP&O":
        main = soup.find('div', class_='article-content') or soup.find('article') or soup
    elif source == "The Drum":
        main = soup.find("article") or soup.find("main") or soup
    elif source == "Forbes CMO Network":
        main = soup.find("article") or soup.find("main") or soup
    else:
        main = (
            soup.find('div', class_='entry-content')
            or soup.find('section', class_='post-content')
            or soup.find('div', class_='article-content')
            or soup
        )

    elements = main.find_all(['p', 'h2', 'h3', 'img', 'blockquote', 'div'])
    for el in elements:
        if el.name == 'img':
            src = el.get('src') or el.get('data-src')
            if src:
                if src.startswith('//'):
                    src = 'https:' + src
                elif src.startswith('/'):
                    if source == "Brand New":
                        src = "https://www.underconsideration.com" + src
                    elif source == "Branding Mag":
                        src = "https://www.brandingmag.com" + src
                    elif source == "BP&O":
                        src = "https://bpando.org" + src
                    elif source == "The Drum":
                        src = "https://www.thedrum.com" + src
                    elif source == "Forbes CMO Network":
                        src = "https://www.forbes.com" + src
                elif not src.startswith('http'):
                    base = url.rsplit('/', 1)[0]
                    src = base + '/' + src
                blocks.append(
                    {
                        "object": "block",
                        "type": "image",
                        "image": {"type": "external", "external": {"url": src}},
                    }
                )
        elif el.name == 'div' and el.get('style') and 'background-image' in el.get('style'):
            style = el.get('style')
            start = style.find("url('") + 5
            if start == 4:
                start = style.find("url(") + 4
            end = style.find("')", start)
            if end == -1:
                end = style.find(")", start)
            if start > 3 and end > start:
                src = style[start:end].strip("'\"")
                blocks.append(
                    {
                        "object": "block",
                        "type": "image",
                        "image": {"type": "external", "external": {"url": src}},
                    }
                )
        elif el.name in ['h2', 'h3']:
            text = el.get_text(strip=True)
            if text:
                blocks.append(
                    {
                        "object": "block",
                        "type": "heading_2",
                        "heading_2": {"rich_text": [{"text": {"content": text[:2000]}}]},
                    }
                )
        elif el.name == 'p':
            text = el.get_text(strip=True)
            if text and len(text) > 10:
                if source == "Brand New" and any(
                    x in text for x in ["Subscribe to Brand New", "DID YOU WORK ON THIS PROJECT", "Comments (", "Industry /"]
                ):
                    continue
                blocks.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {"rich_text": [{"text": {"content": text[:2000]}}]},
                    }
                )

    return blocks[:50]


def get_article_content(url, source, fallback_text=""):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        html_text = ""
        if source == "Forbes CMO Network":
            html_text = fetch_forbes_article_html(url)
        else:
            response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
            response.raise_for_status()
            html_text = response.text

        if not html_text:
            if source == "Forbes CMO Network":
                blocks = forbes_text_to_notion_blocks(fallback_text, max_blocks=8)
                if blocks:
                    return blocks
            return text_to_notion_paragraph_blocks(fallback_text, max_blocks=8)

        if source == "Forbes CMO Network":
            looks_like_html = bool(re.search(r"<[a-zA-Z!/][^>]*>", html_text))
            if not looks_like_html:
                blocks = forbes_text_to_notion_blocks(html_text, max_blocks=50)
                if blocks:
                    return blocks[:50]

        soup = BeautifulSoup(html_text, 'html.parser')
        blocks = extract_notion_blocks_from_soup(soup, source, url)
        if blocks:
            return blocks[:50]

        text_fallback = soup.get_text("\n", strip=True)
        if source == "Forbes CMO Network":
            text_fallback = clean_forbes_extracted_text(text_fallback)
            blocks = forbes_text_to_notion_blocks(text_fallback, max_blocks=50)
        else:
            blocks = text_to_notion_paragraph_blocks(text_fallback, max_blocks=50)
        if blocks:
            return blocks[:50]

        if source == "Forbes CMO Network":
            blocks = forbes_text_to_notion_blocks(fallback_text, max_blocks=8)
            if blocks:
                return blocks
        return text_to_notion_paragraph_blocks(fallback_text, max_blocks=8)
    except Exception as e:
        log_event("article_content_fetch_error", level="error", source=source, url=url, error=str(e))
        if source == "Forbes CMO Network":
            blocks = forbes_text_to_notion_blocks(fallback_text, max_blocks=8)
            if blocks:
                return blocks
        return text_to_notion_paragraph_blocks(fallback_text, max_blocks=8)


SOURCE_FETCHERS = {
    1: get_brand_new_articles,
    2: get_bj_articles,
    3: get_bm_articles,
    4: get_bp_articles,
    5: get_thedrum_articles,
    6: get_forbes_cmo_articles,
}


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }


def notion_error_text(response, limit=500):
    try:
        payload = response.json()
        if isinstance(payload, dict):
            text = payload.get("message") or payload.get("error") or json.dumps(payload, ensure_ascii=True)
        else:
            text = str(payload)
    except Exception:
        text = response.text or ""

    text = text.strip()
    if len(text) > limit:
        return text[:limit] + "...(truncated)"
    return text


def increment_metric(metric_name, value=1):
    with STATE_LOCK:
        METRICS_STATE[metric_name] = METRICS_STATE.get(metric_name, 0) + value


TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}


def normalize_article_url(raw_url):
    if not raw_url:
        return ""

    value = raw_url.strip()
    if not value:
        return ""

    parsed = urlsplit(value)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
    filtered_pairs = []
    for key, query_value in query_pairs:
        lower_key = key.lower()
        if lower_key in TRACKING_QUERY_KEYS:
            continue
        filtered_pairs.append((key, query_value))
    filtered_pairs.sort()
    query = urlencode(filtered_pairs, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def dedupe_url_candidates(raw_url):
    candidates = []
    seen = set()

    def add(url_value):
        if not url_value:
            return
        normalized_value = url_value.strip()
        if not normalized_value or normalized_value in seen:
            return
        seen.add(normalized_value)
        candidates.append(normalized_value)

    add(raw_url)
    add((raw_url or "").split("#", 1)[0])

    canonical = normalize_article_url(raw_url)
    add(canonical)

    if canonical:
        parsed = urlsplit(canonical)
        add(urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", "")))
        if parsed.path and parsed.path != "/" and not parsed.path.endswith("/"):
            add(urlunsplit((parsed.scheme, parsed.netloc, parsed.path + "/", parsed.query, "")))
        elif parsed.path.endswith("/") and parsed.path != "/":
            add(urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), parsed.query, "")))

    return candidates


def dedupe_articles_for_upload(articles, source_name):
    unique_articles = []
    seen_urls = set()
    seen_content_keys = set()

    for article in articles:
        normalized_url = normalize_article_url(article.get("link", ""))
        normalized_title = re.sub(r"\s+", " ", (article.get("title", "") or "").strip().lower())
        content_key = (article.get("source", ""), article.get("date", ""), normalized_title)

        duplicate_reason = ""
        if normalized_url and normalized_url in seen_urls:
            duplicate_reason = "url_in_batch"
        elif content_key in seen_content_keys:
            duplicate_reason = "title_date_in_batch"

        if duplicate_reason:
            increment_metric("duplicate_skipped_batch_total", 1)
            log_event(
                "duplicate_skipped_batch",
                source=source_name,
                reason=duplicate_reason,
                url=normalized_url or article.get("link", ""),
                title=article.get("title", ""),
            )
            continue

        if normalized_url:
            seen_urls.add(normalized_url)
        seen_content_keys.add(content_key)

        normalized_article = dict(article)
        if normalized_url:
            normalized_article["link"] = normalized_url
        unique_articles.append(normalized_article)

    return unique_articles

def url_exists_in_notion(url):
    if not NOTION_TOKEN or not DATABASE_ID:
        return False
    query_url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"

    for candidate_url in dedupe_url_candidates(url):
        payload = {"filter": {"property": "URL", "url": {"equals": candidate_url}}}
        try:
            response = requests.post(
                query_url,
                headers=notion_headers(),
                json=payload,
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            if response.status_code == 200:
                if len(response.json().get("results", [])) > 0:
                    return True
                continue

            increment_metric("notion_query_failures_total", 1)
            log_event(
                "notion_url_query_failed",
                level="error",
                status_code=response.status_code,
                url=candidate_url,
            )
            return None
        except Exception as e:
            increment_metric("notion_query_failures_total", 1)
            log_event("notion_url_query_error", level="error", error=str(e), url=candidate_url)
            return None

    return False


def upload_to_notion(article, run_seen_urls=None):
    if not NOTION_TOKEN or not DATABASE_ID:
        return False

    normalized_url = normalize_article_url(article.get("link", ""))
    if run_seen_urls is not None and normalized_url:
        if normalized_url in run_seen_urls:
            increment_metric("duplicate_skipped_runtime_total", 1)
            log_event(
                "duplicate_skipped_runtime",
                source=article.get("source", ""),
                url=normalized_url,
                title=article.get("title", ""),
            )
            return False
        run_seen_urls.add(normalized_url)

    duplicate_check = url_exists_in_notion(normalized_url or article.get("link", ""))
    if duplicate_check is True:
        increment_metric("duplicate_skipped_notion_total", 1)
        log_event(
            "duplicate_skipped_notion",
            source=article.get("source", ""),
            url=normalized_url or article.get("link", ""),
            title=article.get("title", ""),
        )
        return False
    if duplicate_check is None:
        increment_metric("dedupe_check_unavailable_total", 1)
        log_event(
            "duplicate_check_unavailable_skip",
            source=article.get("source", ""),
            url=normalized_url or article.get("link", ""),
            title=article.get("title", ""),
        )
        return False

    article_url = normalized_url or article.get("link", "")
    content_blocks = get_article_content(
        article_url,
        article["source"],
        fallback_text=article.get("summary") or article.get("title", ""),
    )
    if article.get("source") == "Forbes CMO Network":
        first_block_text = extract_notion_block_text(content_blocks[0]) if content_blocks else ""
        normalized_first_text = normalize_forbes_text_for_compare(first_block_text)
        normalized_title = normalize_forbes_text_for_compare(article.get("title") or "")
        normalized_summary = normalize_forbes_text_for_compare(article.get("summary") or "")
        single_block_matches_fallback = (
            len(content_blocks) == 1
            and (
                normalized_first_text == normalized_title
                or (
                    normalized_summary
                    and (
                        normalized_first_text == normalized_summary
                        or normalized_first_text in normalized_summary
                        or normalized_summary in normalized_first_text
                    )
                )
            )
        )
        if not content_blocks or single_block_matches_fallback:
            snapshot_blocks = build_forbes_snapshot_blocks(article, article_url, max_blocks=12)
            if snapshot_blocks:
                content_blocks = snapshot_blocks
                log_event(
                    "forbes_snapshot_fallback_used",
                    level="warning",
                    source=article.get("source", ""),
                    link=article_url,
                    title=article.get("title", ""),
                    reason="single_block_fallback_or_empty",
                )
            else:
                log_event(
                    "forbes_content_unavailable_skip",
                    level="warning",
                    source=article.get("source", ""),
                    link=article_url,
                    title=article.get("title", ""),
                    reason="single_block_fallback_or_empty",
                )
                return False

    url = "https://api.notion.com/v1/pages"
    blog_name = BLOG_NAME_MAP.get(article.get("source"))
    properties = {
        "Name": {"title": [{"text": {"content": article["title"]}}]},
        "URL": {"url": article_url},
        "Status": {"select": {"name": "Not Published"}},
        "Platform": {"select": {"name": "Web"}},
        "Date": {"date": {"start": article["date"]}},
    }
    if blog_name:
        properties["Blog Name"] = {"select": {"name": blog_name}}

    data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": properties,
        "children": content_blocks,
    }
    try:
        response = requests.post(
            url,
            headers=notion_headers(),
            json=data,
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        if response.status_code == 200:
            increment_metric("articles_uploaded_total", 1)
            return True

        initial_error = notion_error_text(response)
        del data["children"]
        fallback_response = requests.post(
            url,
            headers=notion_headers(),
            json=data,
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        if fallback_response.status_code == 200:
            increment_metric("articles_uploaded_total", 1)
            return True

        log_event(
            "notion_page_create_failed",
            level="error",
            source=article.get("source", ""),
            link=article_url,
            status_code=fallback_response.status_code,
            initial_status_code=response.status_code,
            notion_error=notion_error_text(fallback_response),
            initial_notion_error=initial_error,
        )
    except Exception as e:
        log_event(
            "notion_page_create_error",
            level="error",
            source=article.get("source", ""),
            link=article_url,
            error=str(e),
        )
    return False

# --- Notification and Sync Logic ---

def query_notion_database(payload):
    if not NOTION_TOKEN or not DATABASE_ID:
        return []
    query_url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    try:
        response = requests.post(
            query_url,
            headers=notion_headers(),
            json=payload,
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            increment_metric("notion_query_failures_total", 1)
            log_event("notion_query_failed", level="error", status_code=response.status_code)
            return []
        return response.json().get("results", [])
    except Exception as e:
        increment_metric("notion_query_failures_total", 1)
        log_event("notion_query_error", level="error", error=str(e))
        return []
def parse_notion_entry(page):
    properties = page.get("properties", {})
    title = "Untitled"
    url = ""
    source = "Unknown"
    date_text = ""
    created_time = page.get("created_time", "") or ""
    name_prop = properties.get("Name", {})
    if isinstance(name_prop, dict):
        title_parts = name_prop.get("title", [])
        if title_parts:
            title = "".join(part.get("plain_text", "") for part in title_parts).strip() or "Untitled"
    url_prop = properties.get("URL", {})
    if isinstance(url_prop, dict):
        url = url_prop.get("url", "") or ""
    if not url:
        url = page.get("url", "")
    blog_prop = properties.get("Blog Name", {})
    if isinstance(blog_prop, dict):
        select_value = blog_prop.get("select", {}) or {}
        raw_source = ""
        if isinstance(select_value, dict):
            raw_source = (select_value.get("name") or "").strip()
        if not raw_source:
            options = blog_prop.get("multi_select", [])
            if options:
                raw_source = (options[0].get("name") or "").strip()
        if raw_source:
            source = REVERSE_BLOG_NAME_MAP.get(raw_source, raw_source or "Unknown")
    date_prop = properties.get("Date", {})
    if isinstance(date_prop, dict):
        date_obj = date_prop.get("date", {}) or {}
        date_text = date_obj.get("start", "") or ""
    return {
        "title": title,
        "url": url,
        "source": source,
        "date": date_text,
        "created_time": created_time,
    }
def get_recent_notion_entries(limit=10):
    payload = {
        "page_size": int(limit),
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
    }
    pages = query_notion_database(payload)
    return [parse_notion_entry(page) for page in pages]
def get_digest_notion_entries(days=7, page_size=100):
    lookback_days = max(int(days), 1)
    start_date = (datetime.date.today() - datetime.timedelta(days=lookback_days)).isoformat()
    payload = {
        "page_size": min(max(int(page_size), 1), 100),
        "filter": {"property": "Date", "date": {"on_or_after": start_date}},
        "sorts": [{"property": "Date", "direction": "descending"}],
    }
    pages = query_notion_database(payload)
    return [parse_notion_entry(page) for page in pages]


def query_notion_database_paginated(base_payload, max_pages=5):
    if not NOTION_TOKEN or not DATABASE_ID:
        return []

    query_url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    all_results = []
    cursor = None

    for _ in range(max(int(max_pages), 1)):
        payload = dict(base_payload)
        if cursor:
            payload["start_cursor"] = cursor
        try:
            response = requests.post(
                query_url,
                headers=notion_headers(),
                json=payload,
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            if response.status_code != 200:
                increment_metric("notion_query_failures_total", 1)
                log_event("notion_query_failed", level="error", status_code=response.status_code)
                break

            body = response.json()
            all_results.extend(body.get("results", []))
            if not body.get("has_more"):
                break
            cursor = body.get("next_cursor")
            if not cursor:
                break
        except Exception as e:
            increment_metric("notion_query_failures_total", 1)
            log_event("notion_query_error", level="error", error=str(e))
            break

    return all_results


def search_notion_entries(term, limit=20, max_pages=6):
    search_term = (term or "").strip().lower()
    if not search_term:
        return []

    payload = {
        "page_size": 100,
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
    }
    pages = query_notion_database_paginated(payload, max_pages=max_pages)
    parsed_entries = [parse_notion_entry(page) for page in pages]

    matched = []
    for entry in parsed_entries:
        haystack = " ".join(
            [
                entry.get("title", ""),
                entry.get("url", ""),
                entry.get("source", ""),
                entry.get("date", ""),
            ]
        ).lower()
        if search_term in haystack:
            matched.append(entry)
        if len(matched) >= int(limit):
            break
    return matched


def format_search_results_message(term, entries, limit=20):
    if not entries:
        return f'No matches found for "{term}".'

    lines = [f'Search results for "{term}" ({len(entries)} shown):']
    for idx, entry in enumerate(entries[: int(limit)], start=1):
        title = (entry.get("title") or "Untitled").replace("\n", " ").strip()
        if len(title) > 100:
            title = title[:97] + "..."
        source = (entry.get("source") or "Unknown").strip()
        date_text = (entry.get("date") or "No date").strip()
        url = (entry.get("url") or "No URL").strip()
        lines.append(f"{idx}. {title}")
        lines.append(f"{source} | {date_text}")
        lines.append(url)
        lines.append("")
    return trim_telegram_message(lines)


def entries_to_markdown(entries, title="Branding Recent Entries"):
    lines = [f"# {title}", ""]
    for idx, entry in enumerate(entries, start=1):
        entry_title = (entry.get("title") or "Untitled").replace("\n", " ").strip()
        source = (entry.get("source") or "Unknown").strip()
        date_text = (entry.get("date") or "").strip()
        url = (entry.get("url") or "").strip()
        metadata = " | ".join([part for part in [source, date_text] if part])
        if url:
            lines.append(f"{idx}. [{entry_title}]({url})")
        else:
            lines.append(f"{idx}. {entry_title}")
        if metadata:
            lines.append(f"   - {metadata}")
    return "\n".join(lines).strip() + "\n"


def entries_to_csv(entries):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["title", "source", "date", "url", "created_time"])
    for entry in entries:
        writer.writerow(
            [
                entry.get("title", ""),
                entry.get("source", ""),
                entry.get("date", ""),
                entry.get("url", ""),
                entry.get("created_time", ""),
            ]
        )
    return output.getvalue()
def trim_telegram_message(lines):
    message = "\n".join(lines).strip()
    if len(message) <= 3900:
        return message
    trimmed = []
    current_len = 0
    for line in lines:
        next_len = current_len + len(line) + 1
        if next_len > 3800:
            break
        trimmed.append(line)
        current_len = next_len
    trimmed.append("")
    trimmed.append("... output truncated.")
    return "\n".join(trimmed).strip()
def format_recent_entries_message(entries, requested_limit):
    if not entries:
        return f"No entries found in Notion for last {requested_limit} items."
    lines = [f"Latest {len(entries)} entries from Notion:"]
    for idx, entry in enumerate(entries, start=1):
        title = (entry.get("title") or "Untitled").replace("\n", " ").strip()
        if len(title) > 110:
            title = title[:107] + "..."
        url = (entry.get("url") or "No URL").strip()
        lines.append(f"{idx}. {title}")
        lines.append(url)
        lines.append("")
    return trim_telegram_message(lines)
def format_weekly_digest_message(entries, days=7, per_source=3):
    if not entries:
        return f"No entries found in Notion for the last {days} days."
    groups = {name: [] for name in SOURCE_NAMES.values()}
    for entry in entries:
        source = entry.get("source") or "Unknown"
        groups.setdefault(source, []).append(entry)
    lines = [
        f"Weekly digest (last {days} days)",
        f"Total new entries: {len(entries)}",
        "",
    ]
    for source_name in SOURCE_NAMES.values():
        source_entries = groups.get(source_name, [])
        if not source_entries:
            continue
        lines.append(f"{source_name} ({len(source_entries)}):")
        for idx, entry in enumerate(source_entries[:per_source], start=1):
            title = (entry.get("title") or "Untitled").replace("\n", " ").strip()
            if len(title) > 105:
                title = title[:102] + "..."
            lines.append(f"{idx}. {title}")
            lines.append((entry.get("url") or "No URL").strip())
        lines.append("")
    for source_name, source_entries in groups.items():
        if source_name in SOURCE_NAMES.values() or not source_entries:
            continue
        lines.append(f"{source_name} ({len(source_entries)}):")
        for idx, entry in enumerate(source_entries[:per_source], start=1):
            title = (entry.get("title") or "Untitled").replace("\n", " ").strip()
            if len(title) > 105:
                title = title[:102] + "..."
            lines.append(f"{idx}. {title}")
            lines.append((entry.get("url") or "No URL").strip())
        lines.append("")
    return trim_telegram_message(lines)
def build_weekly_digest(days=7, per_source=3):
    entries = get_digest_notion_entries(days=days, page_size=100)
    return format_weekly_digest_message(entries, days=days, per_source=per_source)
def get_state_snapshot():
    with STATE_LOCK:
        metrics = dict(METRICS_STATE)
        sources = {source_id: dict(values) for source_id, values in SOURCE_STATE.items()}
    return metrics, sources
def update_source_state_on_start(source_id):
    now = utc_now()
    with STATE_LOCK:
        source_state = SOURCE_STATE[source_id]
        source_state["last_run"] = now
        source_state["total_runs"] += 1
        if source_state["first_run_at"] is None:
            source_state["first_run_at"] = now
        METRICS_STATE["sync_runs_total"] += 1
def update_source_state_on_success(source_id, added_count):
    now = utc_now()
    with STATE_LOCK:
        source_state = SOURCE_STATE[source_id]
        source_state["last_success"] = now
        source_state["last_added_count"] = added_count
        source_state["total_added"] += added_count
        source_state["consecutive_failures"] = 0
        if added_count > 0:
            source_state["consecutive_zero_runs"] = 0
            source_state["last_non_zero_at"] = now
        else:
            source_state["consecutive_zero_runs"] += 1
        METRICS_STATE["sync_runs_success_total"] += 1
def update_source_state_on_failure(source_id, error):
    now = utc_now()
    with STATE_LOCK:
        source_state = SOURCE_STATE[source_id]
        source_state["last_error"] = str(error)
        source_state["last_error_at"] = now
        source_state["consecutive_failures"] += 1
        source_state["total_failures"] += 1
        METRICS_STATE["sync_runs_failure_total"] += 1
def maybe_send_zero_new_alert(source_id, trigger):
    source_name = SOURCE_NAMES[source_id]
    threshold_days = SOURCE_ZERO_ALERT_DAYS.get(source_id, ZERO_NEW_ALERT_DAYS_DAILY)
    now = utc_now()
    should_alert = False
    days_without_new = 0
    with STATE_LOCK:
        source_state = SOURCE_STATE[source_id]
        baseline = source_state["last_non_zero_at"] or source_state["first_run_at"]
        if baseline is None:
            return
        days_without_new = (now.date() - baseline.date()).days
        already_alerted_for_period = (
            source_state["last_zero_alert_at"] is not None
            and source_state["last_zero_alert_at"] >= baseline
        )
        if days_without_new >= threshold_days and not already_alerted_for_period:
            source_state["last_zero_alert_at"] = now
            should_alert = True
    if not should_alert:
        return
    message = (
        f"ALERT: {source_name} has produced zero new articles for {days_without_new} days. "
        f"(threshold={threshold_days}, trigger={trigger})"
    )
    send_telegram_notif(message)
    log_event(
        "zero_new_alert_sent",
        source=source_name,
        days_without_new=days_without_new,
        threshold_days=threshold_days,
        trigger=trigger,
    )
def format_status_message():
    metrics, sources = get_state_snapshot()
    uptime = utc_now() - APP_STARTED_AT
    uptime_hours = round(uptime.total_seconds() / 3600, 2)
    lines = [
        "Branding Scraper status",
        f"Uptime: {uptime_hours}h",
        (
            f"Sync runs total={metrics.get('sync_runs_total', 0)}, "
            f"success={metrics.get('sync_runs_success_total', 0)}, "
            f"failed={metrics.get('sync_runs_failure_total', 0)}"
        ),
        f"Articles uploaded total={metrics.get('articles_uploaded_total', 0)}",
        "",
    ]
    for source_id, source_name in SOURCE_NAMES.items():
        source_state = sources[source_id]
        last_error = source_state.get("last_error") or "None"
        if len(last_error) > 150:
            last_error = last_error[:147] + "..."
        lines.append(f"{source_name}:")
        lines.append(f"Last success: {format_human_utc(source_state.get('last_success'))}")
        lines.append(f"Last added count: {source_state.get('last_added_count', 0)}")
        lines.append(f"Last error: {last_error}")
        if source_state.get("last_error"):
            lines.append(f"Last error time: {format_human_utc(source_state.get('last_error_at'))}")
        lines.append("")
    return trim_telegram_message(lines)
def send_telegram_notif(message):
    if not TELEGRAM_TOKEN or not ALLOWED_CHAT_ID:
        log_event("telegram_notification_skipped", reason="missing_token_or_chat_id")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": ALLOWED_CHAT_ID, "text": message}
    try:
        response = requests.post(url, json=payload, timeout=HTTP_TIMEOUT_SECONDS)
        if response.status_code != 200:
            log_event("telegram_notification_failed", level="error", status_code=response.status_code)
            return False
        increment_metric("telegram_notifications_total", 1)
        return True
    except Exception as e:
        log_event("telegram_notification_error", level="error", error=str(e))
        return False
def run_sync(source_id, days=2, silent=False, trigger="manual", run_seen_urls=None):
    if run_seen_urls is None:
        run_seen_urls = set()

    if source_id == 0:
        total_added = 0
        errors = []
        for child_source_id in SOURCE_NAMES:
            result = run_sync(
                child_source_id,
                days=days,
                silent=True,
                trigger=trigger,
                run_seen_urls=run_seen_urls,
            )
            total_added += result["added"]
            errors.extend(result["errors"])
        if total_added > 0 and not silent:
            send_telegram_notif(f"Articles were added to the database ({total_added} across all sources).")
        if errors and not silent:
            summary = "; ".join(errors[:3])
            send_telegram_notif(f"One or more syncs failed: {summary}")
        return {"added": total_added, "errors": errors}
    source_name = SOURCE_NAMES.get(source_id)
    if not source_name:
        return {"added": 0, "errors": [f"Unknown source_id {source_id}"]}
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=days)
    update_source_state_on_start(source_id)
    log_event(
        "sync_started",
        source=source_name,
        source_id=source_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        days=days,
        trigger=trigger,
    )
    fetcher = SOURCE_FETCHERS[source_id]
    try:
        articles = fetcher(start_date, end_date)
    except Exception as e:
        update_source_state_on_failure(source_id, str(e))
        log_event(
            "sync_failed",
            level="error",
            source=source_name,
            source_id=source_id,
            days=days,
            trigger=trigger,
            error=str(e),
        )
        send_telegram_notif(
            f"ALERT: Sync failed for {source_name} (days={days}, trigger={trigger}). Error: {str(e)[:220]}"
        )
        return {"added": 0, "errors": [f"{source_name}: {e}"]}

    raw_fetched_count = len(articles)
    articles = dedupe_articles_for_upload(articles, source_name)
    deduped_fetched_count = len(articles)
    added_count = 0
    for article in articles:
        if upload_to_notion(article, run_seen_urls=run_seen_urls):
            added_count += 1
    update_source_state_on_success(source_id, added_count)
    log_event(
        "sync_finished",
        source=source_name,
        source_id=source_id,
        trigger=trigger,
        days=days,
        fetched=raw_fetched_count,
        fetched_after_dedupe=deduped_fetched_count,
        added=added_count,
    )
    if added_count == 0:
        maybe_send_zero_new_alert(source_id, trigger)
    if added_count > 0 and not silent:
        send_telegram_notif(f"Articles were added to the database ({added_count} from {source_name}).")
    return {"added": added_count, "errors": []}
def to_unix_timestamp(value):
    if value is None:
        return 0
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return int(value.timestamp())
def build_metrics_payload():
    metrics, sources = get_state_snapshot()
    lines = [
        "# HELP branding_sync_runs_total Total number of sync runs.",
        "# TYPE branding_sync_runs_total counter",
        f"branding_sync_runs_total {metrics.get('sync_runs_total', 0)}",
        "# HELP branding_sync_runs_success_total Total successful sync runs.",
        "# TYPE branding_sync_runs_success_total counter",
        f"branding_sync_runs_success_total {metrics.get('sync_runs_success_total', 0)}",
        "# HELP branding_sync_runs_failure_total Total failed sync runs.",
        "# TYPE branding_sync_runs_failure_total counter",
        f"branding_sync_runs_failure_total {metrics.get('sync_runs_failure_total', 0)}",
        "# HELP branding_articles_uploaded_total Total articles uploaded to Notion.",
        "# TYPE branding_articles_uploaded_total counter",
        f"branding_articles_uploaded_total {metrics.get('articles_uploaded_total', 0)}",
        "# HELP branding_telegram_notifications_total Total Telegram notifications sent.",
        "# TYPE branding_telegram_notifications_total counter",
        f"branding_telegram_notifications_total {metrics.get('telegram_notifications_total', 0)}",
        "# HELP branding_duplicate_skipped_notion_total Duplicates skipped due to existing Notion URLs.",
        "# TYPE branding_duplicate_skipped_notion_total counter",
        f"branding_duplicate_skipped_notion_total {metrics.get('duplicate_skipped_notion_total', 0)}",
        "# HELP branding_duplicate_skipped_runtime_total Duplicates skipped within one sync runtime.",
        "# TYPE branding_duplicate_skipped_runtime_total counter",
        f"branding_duplicate_skipped_runtime_total {metrics.get('duplicate_skipped_runtime_total', 0)}",
        "# HELP branding_duplicate_skipped_batch_total Duplicates skipped during batch preprocessing.",
        "# TYPE branding_duplicate_skipped_batch_total counter",
        f"branding_duplicate_skipped_batch_total {metrics.get('duplicate_skipped_batch_total', 0)}",
        "# HELP branding_dedupe_check_unavailable_total Items skipped because dedupe check failed.",
        "# TYPE branding_dedupe_check_unavailable_total counter",
        f"branding_dedupe_check_unavailable_total {metrics.get('dedupe_check_unavailable_total', 0)}",
    ]
    for source_id, source_name in SOURCE_NAMES.items():
        source_state = sources[source_id]
        source_label = source_name.replace('"', "")
        lines.append(f'branding_source_total_runs{{source="{source_label}"}} {source_state.get("total_runs", 0)}')
        lines.append(f'branding_source_total_added{{source="{source_label}"}} {source_state.get("total_added", 0)}')
        lines.append(f'branding_source_total_failures{{source="{source_label}"}} {source_state.get("total_failures", 0)}')
        lines.append(
            f'branding_source_consecutive_zero_runs{{source="{source_label}"}} '
            f'{source_state.get("consecutive_zero_runs", 0)}'
        )
        lines.append(
            f'branding_source_last_success_timestamp{{source="{source_label}"}} '
            f'{to_unix_timestamp(source_state.get("last_success"))}'
        )
        lines.append(
            f'branding_source_last_run_timestamp{{source="{source_label}"}} '
            f'{to_unix_timestamp(source_state.get("last_run"))}'
        )
    return "\n".join(lines) + "\n"
# --- Web Server Health and Metrics Endpoints ---
async def handle_health(request):
    return web.Response(text="Branding Scraper is Live and Healthy")
async def handle_metrics(request):
    return web.Response(
        text=build_metrics_payload(),
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )
# --- Main Bot & Scheduler Logic ---

async def start_all():
    from zoneinfo import ZoneInfo
    damascus_tz = ZoneInfo("Asia/Damascus")
    
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()
    scheduler = AsyncIOScheduler(timezone=damascus_tz)

    # Automated Schedules (Daily/Sunday defaults)
    scheduler.add_job(
        lambda: run_sync(1, days=2, trigger="scheduled_daily"),
        "cron",
        hour=6,
        minute=0,
    )
    scheduler.add_job(
        lambda: [run_sync(i, days=8, trigger="scheduled_weekly") for i in [2, 3, 4, 5, 6]],
        "cron",
        day_of_week="sun",
        hour=6,
        minute=0,
    )
    scheduler.start()
    log_event("scheduler_started", timezone="Asia/Damascus")

    @dp.message(Command("get"))
    async def handle_get(message: types.Message):
        if str(message.chat.id) != ALLOWED_CHAT_ID:
            return
        
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="Brand New (Daily)", callback_data="src_1"))
        builder.row(types.InlineKeyboardButton(text="Branding Journal", callback_data="src_2"))
        builder.row(types.InlineKeyboardButton(text="Branding Mag", callback_data="src_3"))
        builder.row(types.InlineKeyboardButton(text="BP&O", callback_data="src_4"))
        builder.row(types.InlineKeyboardButton(text="The Drum", callback_data="src_5"))
        builder.row(types.InlineKeyboardButton(text="Forbes CMO Network", callback_data="src_6"))
        builder.row(types.InlineKeyboardButton(text="All Sources", callback_data="src_0"))
        
        await message.answer("Step 1: Choose your source:", reply_markup=builder.as_markup())

    @dp.message(Command("last"))
    async def handle_last(message: types.Message):
        if str(message.chat.id) != ALLOWED_CHAT_ID:
            return

        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="Last 3", callback_data="last_3"))
        builder.row(types.InlineKeyboardButton(text="Last 10", callback_data="last_10"))
        builder.row(types.InlineKeyboardButton(text="Last 20", callback_data="last_20"))
        await message.answer("Choose how many latest Notion entries to show:", reply_markup=builder.as_markup())

    @dp.message(Command("status"))
    async def handle_status(message: types.Message):
        if str(message.chat.id) != ALLOWED_CHAT_ID:
            return

        loop = asyncio.get_event_loop()
        report = await loop.run_in_executor(None, format_status_message)
        await message.answer(report)

    @dp.message(Command("digest"))
    async def handle_digest(message: types.Message):
        if str(message.chat.id) != ALLOWED_CHAT_ID:
            return

        await message.answer(
            f"Building digest for the last {DIGEST_LOOKBACK_DAYS} days. Please wait..."
        )
        loop = asyncio.get_event_loop()
        report = await loop.run_in_executor(
            None,
            build_weekly_digest,
            DIGEST_LOOKBACK_DAYS,
            DIGEST_TOP_PER_SOURCE,
        )
        await message.answer(report)

    @dp.message(Command("search"))
    async def handle_search(message: types.Message):
        if str(message.chat.id) != ALLOWED_CHAT_ID:
            return

        raw_text = (message.text or "").strip()
        parts = raw_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.answer('Usage: /search <keyword>\nExample: /search packaging')
            return

        term = parts[1].strip()
        await message.answer(f'Searching for "{term}"...')
        loop = asyncio.get_event_loop()
        entries = await loop.run_in_executor(None, search_notion_entries, term, 20, 6)
        report = format_search_results_message(term, entries, limit=20)
        await message.answer(report)

    @dp.message(Command("export"))
    async def handle_export(message: types.Message):
        if str(message.chat.id) != ALLOWED_CHAT_ID:
            return

        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="Markdown (20)", callback_data="exp_md_20"))
        builder.row(types.InlineKeyboardButton(text="Markdown (50)", callback_data="exp_md_50"))
        builder.row(types.InlineKeyboardButton(text="CSV (20)", callback_data="exp_csv_20"))
        builder.row(types.InlineKeyboardButton(text="CSV (50)", callback_data="exp_csv_50"))
        await message.answer(
            "Choose export format for recent Notion entries:",
            reply_markup=builder.as_markup(),
        )

    @dp.callback_query(F.data.startswith("src_"))
    async def process_source_selection(callback: types.CallbackQuery):
        callback_chat_id = str(callback.message.chat.id) if callback.message and callback.message.chat else ""
        if callback_chat_id != ALLOWED_CHAT_ID:
            await callback.answer("Unauthorized", show_alert=True)
            return

        source_id = callback.data.split("_")[1]
        
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="1 Day", callback_data=f"sync_{source_id}_1"))
        builder.row(types.InlineKeyboardButton(text="3 Days", callback_data=f"sync_{source_id}_3"))
        builder.row(types.InlineKeyboardButton(text="7 Days", callback_data=f"sync_{source_id}_7"))
        builder.row(types.InlineKeyboardButton(text="1 Month", callback_data=f"sync_{source_id}_30"))
        
        await callback.message.edit_text("Step 2: Choose timeframe:", reply_markup=builder.as_markup())
        await callback.answer()

    @dp.callback_query(F.data.startswith("sync_"))
    async def process_sync_callback(callback: types.CallbackQuery):
        callback_chat_id = str(callback.message.chat.id) if callback.message and callback.message.chat else ""
        if callback_chat_id != ALLOWED_CHAT_ID:
            await callback.answer("Unauthorized", show_alert=True)
            return

        parts = callback.data.split("_")
        source_id = int(parts[1])
        days = int(parts[2])
        
        await callback.message.edit_text(f"Syncing last {days} days... please wait.")
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_sync, source_id, days)
        count = result.get("added", 0)
        errors = result.get("errors", [])
        
        if errors:
            summary = "; ".join(errors[:2])
            await callback.message.edit_text(
                f"Finished with errors. Added {count} new articles.\nErrors: {summary}"
            )
        elif count == 0:
            await callback.message.edit_text("No new articles found for this timeframe.")
        else:
            await callback.message.edit_text(f"Finished. Added {count} new articles.")
        await callback.answer()

    @dp.callback_query(F.data.startswith("last_"))
    async def process_last_callback(callback: types.CallbackQuery):
        callback_chat_id = str(callback.message.chat.id) if callback.message and callback.message.chat else ""
        if callback_chat_id != ALLOWED_CHAT_ID:
            await callback.answer("Unauthorized", show_alert=True)
            return

        try:
            limit = int(callback.data.split("_")[1])
        except (IndexError, ValueError):
            await callback.answer("Invalid request", show_alert=True)
            return

        if limit not in (3, 10, 20):
            await callback.answer("Invalid request", show_alert=True)
            return

        await callback.message.edit_text(f"Fetching latest {limit} entries from Notion...")
        loop = asyncio.get_event_loop()
        entries = await loop.run_in_executor(None, get_recent_notion_entries, limit)
        report = format_recent_entries_message(entries, limit)
        await callback.message.edit_text(report)
        await callback.answer()

    @dp.callback_query(F.data.startswith("exp_"))
    async def process_export_callback(callback: types.CallbackQuery):
        callback_chat_id = str(callback.message.chat.id) if callback.message and callback.message.chat else ""
        if callback_chat_id != ALLOWED_CHAT_ID:
            await callback.answer("Unauthorized", show_alert=True)
            return

        try:
            _, export_format, limit_text = callback.data.split("_")
            limit = int(limit_text)
        except (ValueError, AttributeError):
            await callback.answer("Invalid export request", show_alert=True)
            return

        if export_format not in {"md", "csv"} or limit not in {20, 50}:
            await callback.answer("Invalid export request", show_alert=True)
            return

        await callback.message.edit_text(f"Preparing {export_format.upper()} export for latest {limit} entries...")
        loop = asyncio.get_event_loop()
        entries = await loop.run_in_executor(None, get_recent_notion_entries, limit)

        if not entries:
            await callback.message.edit_text("No entries available to export.")
            await callback.answer()
            return

        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if export_format == "md":
            content = entries_to_markdown(entries, title=f"Branding Recent Entries ({len(entries)})")
            filename = f"branding_recent_{len(entries)}_{timestamp}.md"
            file_bytes = content.encode("utf-8")
        else:
            content = entries_to_csv(entries)
            filename = f"branding_recent_{len(entries)}_{timestamp}.csv"
            file_bytes = content.encode("utf-8")

        document = types.BufferedInputFile(file=file_bytes, filename=filename)
        await callback.message.answer_document(
            document=document,
            caption=f"Export ready: {filename}",
        )
        await callback.message.edit_text("Export completed.")
        await callback.answer()

    # Setup Web App
    app = web.Application()
    app.router.add_get('/', handle_health)
    app.router.add_get('/metrics', handle_metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    log_event("service_started", port=PORT)
    # Ensure no webhook is active before polling
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(1) # Give Telegram a moment to process the deletion
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(start_all())
    except KeyboardInterrupt:
        log_event("service_stopped", reason="keyboard_interrupt")

