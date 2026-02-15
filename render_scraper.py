import requests
from bs4 import BeautifulSoup
import datetime
import os
import json
from dotenv import load_dotenv
import argparse
import re
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

# Load environment variables
load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

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
        title = a.get_text(strip=True)
        date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', href)
        if date_match and len(title) > 10:
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

def upload_to_notion(article):
    if not NOTION_TOKEN or not DATABASE_ID:
        return False
    content_blocks = get_article_content(article['link'], article['source'])
    url = "https://api.notion.com/v1/pages"
    headers = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}
    data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            "Name": {"title": [{"text": {"content": article['title']}}]},
            "URL": {"url": article['link']},
            "Status": {"select": {"name": "Published"}},
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

# --- Notification and Bot Logic ---

def send_telegram_notif(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Telegram error: {e}")

def run_sync(source_id):
    end = datetime.date.today()
    if source_id == 1:
        start = end - datetime.timedelta(days=2)
        arts = get_brand_new_articles(start, end)
    else:
        start = end - datetime.timedelta(days=8)
        if source_id == 2: arts = get_bj_articles(start, end)
        elif source_id == 3: arts = get_bm_articles(start, end)
        elif source_id == 4: arts = get_bp_articles(start, end)
        else: return 0

    count = 0
    if arts:
        for a in arts:
            if upload_to_notion(a): count += 1
    
    if count > 0:
        send_telegram_notif(f"Articles Were Added to the Database ✅ ({count} from {arts[0]['source']})")
    return count

# --- Telegram Bot Handler ---

async def start_bot():
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()

    @dp.message(Command("get"))
    async def handle_get(message: types.Message):
        if str(message.from_user.id) != CHAT_ID:
            return
        await message.answer("🔄 Starting sync for all sources...")
        total = 0
        for i in range(1, 5):
            total += run_sync(i)
        if total == 0:
            await message.answer("No new articles found today.")
        else:
            await message.answer(f"✅ Finished! Added {total} articles across all platforms.")

    print("Bot is listening...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=int, help="1: Brand New, 2: Journal, 3: Mag, 4: BP&O")
    parser.add_argument("--bot", action="store_true", help="Start the Telegram bot listener")
    args = parser.parse_args()

    if args.bot:
        asyncio.run(start_bot())
    elif args.source:
        run_sync(args.source)
    else:
        print("Please specify --source X or --bot")
