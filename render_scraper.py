import requests
from bs4 import BeautifulSoup
import datetime
import os
import json
from dotenv import load_dotenv
import argparse
import re
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

# Load environment variables
load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
PORT = os.getenv("PORT", "10000")

# --- Scraping Functions ---

def get_brand_new_articles(start_date, end_date):
    url = "https://www.underconsideration.com/brandnew/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching Brand New: {e}")
        return []

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
                title_link = module.find(['h1', 'h2'])
                link_tag = title_link.find('a', href=True) if title_link else None
                if not link_tag:
                    link_tag = module.find('a', href=re.compile(r'brandnew/archives/.*\.php'))

                if link_tag:
                    title = link_tag.get_text(strip=True)
                    link = link_tag['href']
                    if '/category/' in link or link.endswith('#respond'): continue
                    articles.append({"title": title, "link": link, "date": current_date.isoformat(), "source": "Brand New"})
    return articles

def get_bj_articles(start_date, end_date):
    url = "https://www.thebrandingjournal.com/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching Branding Journal: {e}")
        return []

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
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching Branding Mag: {e}")
        return []

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
            res = requests.get(item['link'], headers=headers, timeout=5)
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
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching BP&O: {e}")
        return []

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

def get_article_content(url, source):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        blocks = []
        if source == "Brand New":
            main = soup.find('div', class_='module') or soup.find('article') or soup.find('main') or soup
        elif source == "Branding Journal":
            main = soup.find('div', class_='entry-content') or soup.find('div', class_='cs-entry__content') or soup
        elif source == "BP&O":
            main = soup.find('div', class_='article-content') or soup.find('article') or soup
        else:
            main = soup.find('div', class_='entry-content') or soup.find('section', class_='post-content') or soup.find('div', class_='article-content') or soup

        elements = main.find_all(['p', 'h2', 'h3', 'img', 'blockquote', 'div'])
        for el in elements:
            if el.name == 'img':
                src = el.get('src') or el.get('data-src')
                if src:
                    if src.startswith('//'): src = 'https:' + src
                    elif src.startswith('/'): src = 'https://www.underconsideration.com' if source == "Brand New" else 'https://www.brandingmag.com' if source == "Branding Mag" else 'https://bpando.org' if source == "BP&O" else src
                    elif not src.startswith('http'): 
                        base = url.rsplit('/', 1)[0]
                        src = base + '/' + src
                    blocks.append({"object": "block", "type": "image", "image": {"type": "external", "external": {"url": src}}})
            elif el.name == 'div' and el.get('style') and 'background-image' in el.get('style'):
                style = el.get('style')
                start = style.find("url('") + 5
                if start == 4: start = style.find("url(") + 4
                end = style.find("')", start)
                if end == -1: end = style.find(")", start)
                if start > 3 and end > start:
                    src = style[start:end].strip("'\"")
                    blocks.append({"object": "block", "type": "image", "image": {"type": "external", "external": {"url": src}}})
            elif el.name in ['h2', 'h3']:
                text = el.get_text(strip=True)
                if text:
                    blocks.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": text[:2000]}}]} })
            elif el.name == 'p':
                text = el.get_text(strip=True)
                if text and len(text) > 10:
                    if source == "Brand New" and any(x in text for x in ["Subscribe to Brand New", "DID YOU WORK ON THIS PROJECT", "Comments (", "Industry /"]): continue
                    blocks.append({"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": text[:2000]}}]} })
        return blocks[:50]
    except Exception as e:
        print(f"      Error fetching content: {e}")
        return []

def url_exists_in_notion(url):
    if not NOTION_TOKEN or not DATABASE_ID:
        return False
    query_url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    payload = {"filter": {"property": "URL", "url": {"equals": url}}}
    try:
        response = requests.post(query_url, headers=headers, json=payload)
        if response.status_code == 200:
            return len(response.json().get("results", [])) > 0
    except: pass
    return False

def upload_to_notion(article):
    if not NOTION_TOKEN or not DATABASE_ID:
        return False
    if url_exists_in_notion(article['link']):
        return False
    content_blocks = get_article_content(article['link'], article['source'])
    url = "https://api.notion.com/v1/pages"
    headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            "Name": {"title": [{"text": {"content": article['title']}}]},
            "URL": {"url": article['link']},
            "Status": {"select": {"name": "Not Published"}},
            "Platform": {"select": {"name": "Web"}},
            "Date": {"date": {"start": article['date']}}
        },
        "children": content_blocks
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            return True
        else:
            del data["children"]
            requests.post(url, headers=headers, json=data)
            return True
    except:
        return False

# --- Notification and Sync Logic ---

def send_telegram_notif(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Telegram error: {e}")

def run_sync(source_id, days=2, silent=False):
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days)
    
    if source_id == 1:
        arts = get_brand_new_articles(start, end)
    elif source_id == 2:
        arts = get_bj_articles(start, end)
    elif source_id == 3:
        arts = get_bm_articles(start, end)
    elif source_id == 4:
        arts = get_bp_articles(start, end)
    elif source_id == 0: # All sources
        total = 0
        for i in range(1, 5):
            total += run_sync(i, days=days, silent=True)
        return total
    else:
        return 0

    count = 0
    if arts:
        for a in arts:
            if upload_to_notion(a): count += 1
    
    if count > 0 and not silent:
        source_name = arts[0]['source'] if arts else "Sources"
        send_telegram_notif(f"Articles Were Added to the Database ✅ ({count} from {source_name})")
    return count

# --- Web Server Health Check ---

async def handle_health(request):
    return web.Response(text="Branding Scraper is Live and Healthy 🚀")

# --- Main Bot & Scheduler Logic ---

async def start_all():
    from zoneinfo import ZoneInfo
    damascus_tz = ZoneInfo("Asia/Damascus")
    
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()
    scheduler = AsyncIOScheduler(timezone=damascus_tz)

    # Automated Schedules (Daily/Sunday defaults)
    scheduler.add_job(lambda: run_sync(1, days=2), 'cron', hour=6, minute=0)
    scheduler.add_job(lambda: [run_sync(i, days=8) for i in [2,3,4]], 'cron', day_of_week='sun', hour=6, minute=0)
    scheduler.start()

    @dp.message(Command("get"))
    async def handle_get(message: types.Message):
        if str(message.from_user.id) != CHAT_ID:
            return
        
        builder = InlineKeyboardBuilder()
        builder.row(types.InlineKeyboardButton(text="Brand New (Daily)", callback_data="src_1"))
        builder.row(types.InlineKeyboardButton(text="Branding Journal", callback_data="src_2"))
        builder.row(types.InlineKeyboardButton(text="Branding Mag", callback_data="src_3"))
        builder.row(types.InlineKeyboardButton(text="BP&O", callback_data="src_4"))
        builder.row(types.InlineKeyboardButton(text="All Sources", callback_data="src_0"))
        
        await message.answer("Step 1: Choose your source:", reply_markup=builder.as_markup())

    @dp.callback_query(F.data.startswith("src_"))
    async def process_source_selection(callback: types.CallbackQuery):
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
        parts = callback.data.split("_")
        source_id = int(parts[1])
        days = int(parts[2])
        
        await callback.message.edit_text(f"🔄 Syncing last {days} days... please wait.")
        
        loop = asyncio.get_event_loop()
        count = await loop.run_in_executor(None, run_sync, source_id, days)
        
        if count == 0:
            await callback.message.edit_text("No new articles found for this timeframe.")
        else:
            await callback.message.edit_text(f"✅ Finished! Added {count} new articles.")
        await callback.answer()

    # Setup Web App
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    print("Bot and Scheduler are running...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(start_all())
