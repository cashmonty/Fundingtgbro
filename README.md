# Funding Telegram Signal Bot

A Render-ready Telegram bot that turns the funding-rate research into **Telegram alerts**.

## What it does
- Polls Hyperliquid funding data for your symbols
- Uses a regime model based on funding **z-score** and **percentile**
- Sends Telegram alerts when it finds:
  - `long_watch` = extreme negative funding
  - `short_watch` = extreme positive funding
- Supports Telegram commands:
  - `/start`
  - `/help`
  - `/latest`
  - `/signals`
  - `/status`
- Exposes `/health` and `/latest` for Render health checks and debugging

## Research logic built in
This bot does **not** treat funding as a direct next-candle predictor. It uses it as a **crowding / contrarian regime signal**:
- Extreme negative funding → contrarian long watch
- Extreme positive funding → crowded-long caution / short watch
- Mild crowding → optional crowding alerts
- Neutral funding → no trade

## Files
- `main.py` - bot + polling loop + API
- `requirements.txt` - Python dependencies
- `render.yaml` - Render blueprint

## Render deploy
1. Push this folder to a GitHub repo.
2. In Render, click **New +** → **Blueprint**.
3. Select the repo.
4. Add these secret env vars:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
5. Deploy.

## Required env vars
- `TELEGRAM_BOT_TOKEN` - from BotFather
- `TELEGRAM_CHAT_ID` - your Telegram chat ID

## Optional env vars
- `SYMBOLS=BTC,ETH,SOL`
- `POLL_SECONDS=300`
- `LOOKBACK_POINTS=90`
- `ALERT_ON_CROWDING=false`
- `DROP_PENDING_UPDATES=true`
- `DB_PATH=/tmp/funding_signals.db`

## Telegram usage
After deploy, open your bot in Telegram and send:
- `/start`
- `/latest`
- `/status`

The bot will also push alerts automatically to the `TELEGRAM_CHAT_ID` chat.

## Important Render note
The free web-service filesystem is ephemeral. SQLite data in `/tmp` can be lost on restart/redeploy, which is okay for a short test.

## Quick local run
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=your_token
export TELEGRAM_CHAT_ID=your_chat_id
uvicorn main:app --host 0.0.0.0 --port 8000
```
