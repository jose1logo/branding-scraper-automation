# Branding Scraper Automation (All-in-One Instance)

This repository contains an automated Branding News Scraper that runs as a single instance on Render. It handles both automated schedules and an interactive Telegram bot.

## Features
- **All-in-One Instance:** Runs the Telegram Bot and Automated Scheduler in a single process.
- **Automated Schedule (Damascus Time):** 
  - **Daily (6 AM):** Brand New.
  - **Weekly (Sunday 6 AM):** Branding Journal, Branding Mag, BP&O, The Drum, and Forbes CMO Network.
- **Telegram Controls:** `/get`, `/last`, `/status`, `/digest`, `/search`, and `/export`.
- **Database:** All articles are synced to Notion with full text and images.
- **Anti-Duplicate Protection:** Canonical URL dedupe (tracking params removed), in-batch dedupe, and Notion pre-check before insert.
- **Failure Alerts:** Sends Telegram alerts when a source scrape fails.
- **Zero-New Alerts:** Sends Telegram alerts if a source has no new articles for the configured threshold.
- **Metrics Endpoint:** Exposes `/metrics` in Prometheus text format.
- **Structured Logs:** JSON-style event logs for sync starts, completions, and failures.

## Render Deployment (Single Service)

### 1. Create a Web Service
On [Render](https://render.com), click **New +** and select **Web Service**.

### 2. Configure the Service
- **Connect Repository:** Select your `branding-scraper-automation` repo.
- **Environment:** Select **Python**.
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python render_scraper.py`

### 3. Environment Variables
Add these variables in the **Environment** tab of your service:
- `NOTION_TOKEN`: (Your Notion Secret)
- `NOTION_DATABASE_ID`: (Your Database ID)
- `TELEGRAM_TOKEN`: (Your Bot Token)
- `CHAT_ID`: (Allowed Telegram chat ID)
- `PORT`: 10000 (Render usually sets this automatically)
- `HTTP_TIMEOUT_SECONDS`: Optional, default `20`
- `ZERO_NEW_ALERT_DAYS_DAILY`: Optional, default `2`
- `ZERO_NEW_ALERT_DAYS_WEEKLY`: Optional, default `14`
- `DIGEST_LOOKBACK_DAYS`: Optional, default `7`
- `DIGEST_TOP_PER_SOURCE`: Optional, default `3`
- `FORBES_SCRAPEDO_TOKEN`: Required for Forbes content extraction. Forbes raw HTML is fetched through Scrape.do (`output=raw`).
- `FORBES_SCRAPEDO_ENDPOINT`: Optional, default `https://api.scrape.do/`.
- `FORBES_SCRAPEDO_TIMEOUT_SECONDS`: Optional, default `90` (read timeout per request).
- `FORBES_SCRAPEDO_RETRIES`: Optional, default `2` attempts per endpoint.
- `FORBES_SCRAPEDO_RETRY_DELAY_SECONDS`: Optional, default `2` seconds between retries.

> Note: Bot command/callback access is restricted to `CHAT_ID`.

## Telegram Command
Once the worker is "Live", you can control the scraper from Telegram:
- **/get**: The bot will show buttons allowing you to choose which specific site to sync, or to sync all 6 at once.
- **/last**: The bot will show buttons for last 3 / 10 / 20 entries and return the latest Notion pages with title + URL.
- **/status**: Returns runtime status per source (last success, last added count, last error).
- **/digest**: Returns a weekly digest grouped by source from recent Notion entries.
- **/search `<keyword>`**: Searches stored Notion entries and returns matched posts.
- **/export**: One-click export buttons for recent entries as Markdown or CSV.
- **Notifications**: You receive messages for added articles, source failures, and zero-new thresholds.

## Duplicate Policy
- The scraper normalizes article URLs before saving (`www` cleanup, trailing-slash normalization, tracking query removal).
- It de-duplicates within each sync batch before upload.
- It checks Notion for existing URL variants before creating a page.
- If duplicate check is unavailable (Notion query error), the item is skipped to avoid accidental duplicates.
