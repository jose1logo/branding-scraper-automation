# Branding Scraper Automation (Render + Telegram)

This repository contains the automated version of the Branding & Rebranding News Scraper, designed to run on Render with Telegram notifications.

## Features
- **Daily Scrapes:** UnderConsideration (Brand New).
- **Weekly Scrapes:** Branding Journal, Branding Mag, and BP&O.
- **Telegram Bot:** Send `/get` to the bot to trigger an instant sync of all sources.
- **Notifications:** Automatic messages sent when new articles are added to Notion.

## Render Setup

### 1. Cron Jobs (Scheduled Scrapes)
Create a new **Cron Job** on Render for each schedule you want:
- **Daily (Brand New):**
  - Command: `python render_scraper.py --source 1`
  - Schedule: `0 9 * * *`
- **Weekly (Others):**
  - Command: `python render_scraper.py --source 2` (or 3, or 4)
  - Schedule: `0 10 * * 1` (Every Monday)

### 2. Background Worker (Telegram Bot)
Create a new **Background Worker** on Render to keep the `/get` command alive:
- Command: `python render_scraper.py --bot`

### 3. Environment Variables
Add these to **all** services created on Render:
- `NOTION_TOKEN`: Your Notion Internal Integration Token.
- `NOTION_DATABASE_ID`: Your Notion Database ID.

## Local Testing
To test the bot locally:
```bash
pip install -r requirements.txt
python render_scraper.py --bot
```
