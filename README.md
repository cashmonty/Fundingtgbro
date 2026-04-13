# Funding Regime Bot - Render Test Deploy

This package is set up for the fastest Render test deploy.

## What it does
- Runs a FastAPI web service
- Starts a background worker on boot
- Polls Hyperliquid funding data every `POLL_SECONDS`
- Computes a simple funding regime signal
- Stores recent rows in SQLite at `/tmp/funding_signals.db`
- Exposes:
  - `/health`
  - `/latest`
  - `/`

## Important test limitation
This is for a short Render test only. On Render free web services, the service spins down after 15 minutes with no incoming traffic.

Also, `/tmp` is ephemeral, so the SQLite database will not survive restarts or redeploys.

## Fastest deploy path
1. Unzip this folder.
2. Create a GitHub repo.
3. Push these files to the repo root.
4. In Render, choose **New +** -> **Blueprint**.
5. Connect the GitHub repo.
6. Render will detect `render.yaml`.
7. Add secret env vars if you want Telegram alerts:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
8. Deploy.

## After deploy
Open:
- `/health` to confirm the worker started
- `/latest` to inspect the most recent rows

## Manual deploy instead of Blueprint
If you do not want to use `render.yaml`, create a **Web Service** and use:
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Health check path: `/health`

## Suggested first test settings
- `SYMBOLS=BTC,ETH,SOL`
- `POLL_SECONDS=300`

## Notes
- Because this is a free web service, it is not the right choice for continuous production monitoring.
- For one-day testing, it is the easiest option.
