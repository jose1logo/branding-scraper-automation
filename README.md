# Branding Scraper Automation (All-in-One Instance)

This repository contains an automated Branding News Scraper that runs as a single instance on Render. It handles both automated schedules and an interactive Telegram bot.

## Features
- **All-in-One Instance:** Runs the Telegram Bot and Automated Scheduler in a single process.
- **Automated Schedule:** 
  - **Daily (9 AM UTC):** Brand New.
  - **Weekly (Monday 10 AM UTC):** Branding Journal, Branding Mag, and BP&O.
- **Telegram Command:** Send `/get` to the bot to trigger an instant sync of all 4 sources.
- **Database:** All articles are synced to Notion with full text and images.

## Render Deployment (Single Service)

### 1. Create a Background Worker
On [Render](https://render.com), click **New +** and select **Background Worker**.

### 2. Configure the Worker
- **Connect Repository:** Select your `branding-scraper-automation` repo.
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python render_scraper.py`

### 3. Environment Variables
Add these 4 variables in the **Environment** tab of your worker:
- `NOTION_TOKEN`: (Your Notion Secret)
- `NOTION_DATABASE_ID`: (Your Database ID)
- `TELEGRAM_TOKEN`: `7537449069:AAH_CmfGnRNIg0h6x1k1dVNwj7U3HI6XkzE`
- `CHAT_ID`: `817335970`

## Telegram Command
Once the worker is "Live", you can control the scraper from Telegram:
- **/get**: Force the bot to check all 4 sites immediately and sync new articles to Notion.
