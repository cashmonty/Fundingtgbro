import asyncio
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTC,ETH,SOL").split(",") if s.strip()]
LOOKBACK_POINTS = int(os.getenv("LOOKBACK_POINTS", "90"))
DB_PATH = os.getenv("DB_PATH", "/tmp/funding_signals.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ALERT_ON_CROWDING = os.getenv("ALERT_ON_CROWDING", "false").lower() == "true"
DROP_PENDING_UPDATES = os.getenv("DROP_PENDING_UPDATES", "true").lower() == "true"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("funding-telegram-bot")

app = FastAPI(title="Funding Telegram Signal Bot", version="2.0.0")


@dataclass
class MarketSnapshot:
    symbol: str
    mark_px: float
    current_funding: float
    open_interest: float
    predicted_funding: Optional[float] = None


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    ts TEXT,
                    symbol TEXT,
                    mark_px REAL,
                    current_funding REAL,
                    predicted_funding REAL,
                    regime TEXT,
                    signal TEXT,
                    zscore REAL,
                    percentile REAL,
                    open_interest REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_state (
                    symbol TEXT PRIMARY KEY,
                    last_signal TEXT,
                    last_regime TEXT,
                    last_alert_ts TEXT
                )
                """
            )
            conn.commit()

    def save_signal(self, row: dict):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO signals (
                    ts, symbol, mark_px, current_funding, predicted_funding,
                    regime, signal, zscore, percentile, open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["ts"], row["symbol"], row["mark_px"], row["current_funding"], row["predicted_funding"],
                    row["regime"], row["signal"], row["zscore"], row["percentile"], row["open_interest"],
                ),
            )
            conn.commit()

    def latest(self, limit: int = 20) -> List[dict]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT ts, symbol, mark_px, current_funding, predicted_funding, regime, signal, zscore, percentile, open_interest
                FROM signals
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def latest_by_symbol(self) -> List[dict]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT s.ts, s.symbol, s.mark_px, s.current_funding, s.predicted_funding, s.regime, s.signal, s.zscore, s.percentile, s.open_interest
                FROM signals s
                INNER JOIN (
                    SELECT symbol, MAX(ts) AS max_ts
                    FROM signals
                    GROUP BY symbol
                ) latest ON latest.symbol = s.symbol AND latest.max_ts = s.ts
                ORDER BY s.symbol ASC
                """
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_symbol_state(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT last_signal, last_regime FROM bot_state WHERE symbol = ?",
                (symbol,),
            )
            row = cur.fetchone()
            if row:
                return row[0], row[1]
            return None, None

    def set_symbol_state(self, symbol: str, signal: str, regime: str):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO bot_state (symbol, last_signal, last_regime, last_alert_ts)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    last_signal = excluded.last_signal,
                    last_regime = excluded.last_regime,
                    last_alert_ts = excluded.last_alert_ts
                """,
                (symbol, signal, regime, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()


class HyperliquidClient:
    def __init__(self, base_url: str = HL_INFO_URL, timeout: int = 20):
        self.base_url = base_url
        self.timeout = timeout

    def _post(self, payload: dict):
        response = requests.post(self.base_url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_meta_and_asset_ctxs(self) -> List[MarketSnapshot]:
        data = self._post({"type": "metaAndAssetCtxs"})
        universe = data[0]["universe"]
        ctxs = data[1]
        out: List[MarketSnapshot] = []
        for meta, ctx in zip(universe, ctxs):
            try:
                out.append(
                    MarketSnapshot(
                        symbol=meta["name"],
                        mark_px=float(ctx.get("markPx", 0.0)),
                        current_funding=float(ctx.get("funding", 0.0)),
                        open_interest=float(ctx.get("openInterest", 0.0)),
                    )
                )
            except Exception:
                continue
        return out

    def get_predicted_fundings(self) -> Dict[str, float]:
        data = self._post({"type": "predictedFundings"})
        out: Dict[str, float] = {}

        if isinstance(data, dict):
            for symbol, val in data.items():
                try:
                    if isinstance(val, (int, float, str)):
                        out[symbol.upper()] = float(val)
                    elif isinstance(val, list) and val:
                        # docs describe different venues; use first numeric value found
                        for item in val:
                            if isinstance(item, (int, float, str)):
                                out[symbol.upper()] = float(item)
                                break
                            if isinstance(item, dict):
                                for inner in item.values():
                                    if isinstance(inner, (int, float, str)):
                                        out[symbol.upper()] = float(inner)
                                        break
                    elif isinstance(val, dict):
                        for inner in val.values():
                            if isinstance(inner, (int, float, str)):
                                out[symbol.upper()] = float(inner)
                                break
                except Exception:
                    pass
        elif isinstance(data, list):
            for row in data:
                if not isinstance(row, dict):
                    continue
                symbol = (row.get("coin") or row.get("name") or row.get("symbol") or "").upper()
                if not symbol:
                    continue
                for key in ("predictedFunding", "funding", "rate", "value"):
                    if key in row:
                        try:
                            out[symbol] = float(row[key])
                            break
                        except Exception:
                            pass
        return out

    def get_funding_history(self, coin: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        data = self._post(
            {
                "type": "fundingHistory",
                "coin": coin,
                "startTime": start_ms,
                "endTime": end_ms,
            }
        )
        rows = []
        for row in data:
            try:
                rows.append(
                    {
                        "time": pd.to_datetime(row["time"], unit="ms", utc=True),
                        "funding_rate": float(row["fundingRate"]),
                    }
                )
            except Exception:
                continue
        return pd.DataFrame(rows).sort_values("time")


class FeatureEngine:
    @staticmethod
    def add_funding_features(df: pd.DataFrame, col: str = "funding_rate", lookback: int = LOOKBACK_POINTS):
        if df.empty or len(df) < max(lookback, 20):
            return df
        df = df.copy()
        df["mean"] = df[col].rolling(lookback).mean()
        df["std"] = df[col].rolling(lookback).std()
        df["zscore"] = (df[col] - df["mean"]) / df["std"]

        def trailing_percentile(x):
            s = pd.Series(x)
            return s.rank(pct=True).iloc[-1]

        df["percentile"] = df[col].rolling(lookback).apply(trailing_percentile, raw=False)
        df["velocity_3"] = df[col].diff(3)
        return df


class RegimeModel:
    def classify(self, latest_z: float, latest_pct: float) -> str:
        if pd.isna(latest_z) or pd.isna(latest_pct):
            return "insufficient_data"
        if latest_z <= -2.0 and latest_pct <= 0.05:
            return "extreme_negative_funding"
        if latest_z >= 2.0 and latest_pct >= 0.95:
            return "extreme_positive_funding"
        if latest_z < -1.0:
            return "bearish_crowding"
        if latest_z > 1.0:
            return "bullish_crowding"
        return "neutral"

    def signal(self, regime: str) -> str:
        if regime == "extreme_negative_funding":
            return "long_watch"
        if regime == "extreme_positive_funding":
            return "short_watch"
        if regime in {"bearish_crowding", "bullish_crowding"}:
            return "crowding_watch"
        return "no_trade"


class SignalEngine:
    def __init__(self, storage: Storage, hl: HyperliquidClient):
        self.storage = storage
        self.hl = hl
        self.features = FeatureEngine()
        self.regimes = RegimeModel()

    def run_once(self, symbols: List[str]) -> Tuple[List[dict], List[str]]:
        snapshots = {s.symbol.upper(): s for s in self.hl.get_meta_and_asset_ctxs()}
        predicted = self.hl.get_predicted_fundings()
        now = pd.Timestamp.utcnow()
        start_ms = int((now - pd.Timedelta(days=120)).timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)

        rows: List[dict] = []
        alerts: List[str] = []

        for symbol in symbols:
            snap = snapshots.get(symbol)
            if not snap:
                logger.warning("No snapshot found for %s", symbol)
                continue

            snap.predicted_funding = predicted.get(symbol)
            hist = self.hl.get_funding_history(symbol, start_ms, end_ms)
            if hist.empty:
                logger.warning("No funding history for %s", symbol)
                continue

            if snap.predicted_funding is not None:
                hist = pd.concat(
                    [
                        hist,
                        pd.DataFrame(
                            [{"time": now, "funding_rate": snap.predicted_funding}]
                        ),
                    ],
                    ignore_index=True,
                )

            hist = self.features.add_funding_features(hist, lookback=LOOKBACK_POINTS)
            latest = hist.iloc[-1]
            regime = self.regimes.classify(latest.get("zscore", float("nan")), latest.get("percentile", float("nan")))
            signal = self.regimes.signal(regime)

            row = {
                "ts": now.isoformat(),
                "symbol": symbol,
                "mark_px": snap.mark_px,
                "current_funding": snap.current_funding,
                "predicted_funding": snap.predicted_funding,
                "regime": regime,
                "signal": signal,
                "zscore": None if pd.isna(latest.get("zscore")) else float(latest["zscore"]),
                "percentile": None if pd.isna(latest.get("percentile")) else float(latest["percentile"]),
                "open_interest": snap.open_interest,
            }
            self.storage.save_signal(row)
            rows.append(row)

            last_signal, last_regime = self.storage.get_symbol_state(symbol)
            should_alert = False
            if signal in {"long_watch", "short_watch"} and signal != last_signal:
                should_alert = True
            elif ALERT_ON_CROWDING and signal == "crowding_watch" and (signal != last_signal or regime != last_regime):
                should_alert = True

            if should_alert:
                alerts.append(format_alert(row))
                self.storage.set_symbol_state(symbol, signal, regime)
            elif last_signal is None and last_regime is None:
                self.storage.set_symbol_state(symbol, signal, regime)

        rows.sort(key=lambda x: (signal_priority(x["signal"]), x["symbol"]))
        return rows, alerts


def signal_priority(signal: str) -> int:
    order = {
        "long_watch": 0,
        "short_watch": 1,
        "crowding_watch": 2,
        "no_trade": 3,
    }
    return order.get(signal, 99)


def pct(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{x * 100:.4f}%"


def format_alert(row: dict) -> str:
    emoji = "🟢" if row["signal"] == "long_watch" else "🔴" if row["signal"] == "short_watch" else "🟡"
    reason = {
        "long_watch": "Extreme negative funding. Contrarian long watch.",
        "short_watch": "Extreme positive funding. Crowded-long caution.",
        "crowding_watch": "Funding crowding building.",
    }.get(row["signal"], "Signal update.")
    return (
        f"{emoji} <b>{row['symbol']}</b> {row['signal']}\n"
        f"{reason}\n"
        f"Mark: <b>{row['mark_px']:.4f}</b>\n"
        f"Predicted funding: <b>{pct(row['predicted_funding'])}</b>\n"
        f"Current funding: <b>{pct(row['current_funding'])}</b>\n"
        f"Z-score: <b>{'n/a' if row['zscore'] is None else format(row['zscore'], '.2f')}</b>\n"
        f"Percentile: <b>{'n/a' if row['percentile'] is None else format(row['percentile'] * 100, '.1f') + '%'} </b>\n"
        f"OI: <b>{row['open_interest']:.2f}</b>\n"
        f"Regime: <b>{row['regime']}</b>"
    )


def format_latest(rows: List[dict]) -> str:
    if not rows:
        return "No data yet. Wait for the first polling cycle."

    lines = ["<b>Latest funding signals</b>"]
    for row in rows:
        lines.append(
            f"\n<b>{row['symbol']}</b> | {row['signal']}"
            f"\nPred: {pct(row['predicted_funding'])} | Cur: {pct(row['current_funding'])}"
            f"\nZ: {'n/a' if row['zscore'] is None else format(row['zscore'], '.2f')} | Pctile: {'n/a' if row['percentile'] is None else format(row['percentile'] * 100, '.1f') + '%'}"
            f"\nMark: {row['mark_px']:.4f} | Regime: {row['regime']}"
        )
    return "\n".join(lines)


class FundingTelegramBot:
    def __init__(self):
        self.storage = Storage(DB_PATH)
        self.hl = HyperliquidClient(HL_INFO_URL)
        self.engine = SignalEngine(self.storage, self.hl)
        self.application: Optional[Application] = None
        self.signal_task: Optional[asyncio.Task] = None
        self.started_at: Optional[str] = None
        self.last_run: Optional[str] = None
        self.last_error: Optional[str] = None
        self.running = False

    async def start(self):
        if not TELEGRAM_BOT_TOKEN:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

        self.application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_help))
        self.application.add_handler(CommandHandler("latest", self.cmd_latest))
        self.application.add_handler(CommandHandler("signals", self.cmd_latest))
        self.application.add_handler(CommandHandler("status", self.cmd_status))
        self.application.add_error_handler(self.on_error)

        await self.application.initialize()
        await self.application.start()
        if self.application.updater is None:
            raise RuntimeError("Telegram updater unavailable")
        await self.application.updater.start_polling(drop_pending_updates=DROP_PENDING_UPDATES)

        self.running = True
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.signal_task = asyncio.create_task(self.signal_loop())
        logger.info("Telegram bot started")

    async def stop(self):
        self.running = False
        if self.signal_task:
            self.signal_task.cancel()
            try:
                await self.signal_task
            except asyncio.CancelledError:
                pass
        if self.application:
            if self.application.updater:
                await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
        logger.info("Telegram bot stopped")

    async def signal_loop(self):
        while True:
            try:
                rows, alerts = await asyncio.to_thread(self.engine.run_once, SYMBOLS)
                self.last_run = datetime.now(timezone.utc).isoformat()
                logger.info("Signal scan complete for %s symbols, alerts=%s", len(rows), len(alerts))
                if alerts and self.application and TELEGRAM_CHAT_ID:
                    for msg in alerts:
                        await self.application.bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID,
                            text=msg,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                        await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("Signal loop failed")
            await asyncio.sleep(POLL_SECONDS)

    async def on_error(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        logger.exception("Telegram bot error", exc_info=context.error)

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "<b>Funding Signal Bot</b>\n"
            "Research-driven Telegram alerts using Hyperliquid funding extremes.\n\n"
            "Commands:\n"
            "/latest - latest signal snapshot\n"
            "/status - bot status\n"
            "/help - command list"
        )
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.cmd_start(update, context)

    async def cmd_latest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        rows = self.storage.latest_by_symbol()
        await update.effective_message.reply_text(format_latest(rows), parse_mode=ParseMode.HTML)

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            f"<b>Status</b>\n"
            f"Running: <b>{self.running}</b>\n"
            f"Started: <b>{self.started_at or 'n/a'}</b>\n"
            f"Last run: <b>{self.last_run or 'n/a'}</b>\n"
            f"Poll seconds: <b>{POLL_SECONDS}</b>\n"
            f"Symbols: <b>{', '.join(SYMBOLS)}</b>\n"
            f"Last error: <b>{self.last_error or 'none'}</b>"
        )
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)


bot_controller = FundingTelegramBot()


@app.get("/")
async def root():
    return JSONResponse(
        {
            "ok": True,
            "service": "funding-telegram-bot",
            "running": bot_controller.running,
            "symbols": SYMBOLS,
            "poll_seconds": POLL_SECONDS,
        }
    )


@app.get("/health")
async def health():
    return JSONResponse(
        {
            "ok": True,
            "running": bot_controller.running,
            "started_at": bot_controller.started_at,
            "last_run": bot_controller.last_run,
            "last_error": bot_controller.last_error,
        }
    )


@app.get("/latest")
async def latest():
    return JSONResponse({"rows": bot_controller.storage.latest_by_symbol()})


@app.on_event("startup")
async def on_startup():
    await bot_controller.start()


@app.on_event("shutdown")
async def on_shutdown():
    await bot_controller.stop()
