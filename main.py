import logging
import os
import signal
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

HL_INFO_URL = os.getenv("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTC,ETH,SOL").split(",") if s.strip()]
DB_PATH = os.getenv("DB_PATH", "./data/funding_signals.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("funding-bot")

app = FastAPI(title="Funding Regime Bot", version="1.0.0")
shutdown_event = threading.Event()
worker_thread: Optional[threading.Thread] = None
LAST_STATUS: Dict[str, object] = {
    "running": False,
    "last_run": None,
    "last_error": None,
    "symbols": SYMBOLS,
    "poll_seconds": POLL_SECONDS,
}


@dataclass
class MarketSnapshot:
    symbol: str
    mark_px: float
    current_funding: float
    open_interest: float
    predicted_funding: Optional[float] = None


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
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
                    percentile REAL
                )
                """
            )
            conn.commit()

    def save(self, row: dict):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO signals (
                    ts, symbol, mark_px, current_funding, predicted_funding,
                    regime, signal, zscore, percentile
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["ts"], row["symbol"], row["mark_px"], row["current_funding"],
                    row["predicted_funding"], row["regime"], row["signal"],
                    row["zscore"], row["percentile"],
                ),
            )
            conn.commit()

    def latest(self, limit: int = 20):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT ts, symbol, mark_px, current_funding, predicted_funding, regime, signal, zscore, percentile FROM signals ORDER BY ts DESC LIMIT ?",
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


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
        out = []
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

        if isinstance(data, list):
            for row in data:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("coin") or row.get("name") or row.get("symbol")
                if not symbol:
                    continue
                for key in ("predictedFunding", "funding", "rate", "value"):
                    if key in row:
                        try:
                            out[symbol] = float(row[key])
                            break
                        except Exception:
                            pass
        elif isinstance(data, dict):
            for symbol, val in data.items():
                try:
                    if isinstance(val, (int, float, str)):
                        out[symbol] = float(val)
                    elif isinstance(val, dict):
                        for inner in val.values():
                            if isinstance(inner, (int, float, str)):
                                out[symbol] = float(inner)
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
    def add_funding_features(df: pd.DataFrame, col: str = "funding_rate", lookback: int = 90):
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
        return "no_trade"


class TelegramAlerter:
    @staticmethod
    def send(message: str):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
                timeout=15,
            ).raise_for_status()
        except Exception as exc:
            logger.warning("Telegram alert failed: %s", exc)


def compute_signals(symbols: List[str]) -> List[dict]:
    hl = HyperliquidClient()
    storage = Storage()
    feature_engine = FeatureEngine()
    regime_model = RegimeModel()

    snapshots = {s.symbol: s for s in hl.get_meta_and_asset_ctxs()}
    predicted = hl.get_predicted_fundings()

    now = pd.Timestamp.utcnow()
    start_ms = int((now - pd.Timedelta(days=120)).timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    results = []
    for symbol in symbols:
        snap = snapshots.get(symbol)
        if not snap:
            logger.warning("Skipping %s: not found in asset contexts", symbol)
            continue

        snap.predicted_funding = predicted.get(symbol)
        hist = hl.get_funding_history(symbol, start_ms, end_ms)
        if hist.empty:
            logger.warning("Skipping %s: no funding history", symbol)
            continue

        if snap.predicted_funding is not None:
            hist = pd.concat(
                [
                    hist,
                    pd.DataFrame([{"time": now, "funding_rate": snap.predicted_funding}]),
                ],
                ignore_index=True,
            )

        hist = feature_engine.add_funding_features(hist)
        latest = hist.iloc[-1]
        regime = regime_model.classify(latest.get("zscore"), latest.get("percentile"))
        signal_name = regime_model.signal(regime)

        row = {
            "ts": now.isoformat(),
            "symbol": symbol,
            "mark_px": snap.mark_px,
            "current_funding": snap.current_funding,
            "predicted_funding": snap.predicted_funding,
            "regime": regime,
            "signal": signal_name,
            "zscore": None if pd.isna(latest.get("zscore")) else float(latest.get("zscore")),
            "percentile": None if pd.isna(latest.get("percentile")) else float(latest.get("percentile")),
        }
        storage.save(row)
        results.append(row)

        if signal_name in {"long_watch", "short_watch"}:
            TelegramAlerter.send(
                f"{symbol} | {signal_name} | regime={regime} | pred_funding={snap.predicted_funding} | z={row['zscore']} | pct={row['percentile']}"
            )
    return results


def worker_loop():
    LAST_STATUS["running"] = True
    logger.info("Worker started for symbols=%s poll=%ss", SYMBOLS, POLL_SECONDS)
    while not shutdown_event.is_set():
        try:
            results = compute_signals(SYMBOLS)
            LAST_STATUS["last_run"] = pd.Timestamp.utcnow().isoformat()
            LAST_STATUS["last_error"] = None
            LAST_STATUS["last_count"] = len(results)
            logger.info("Run completed with %s results", len(results))
        except Exception as exc:
            LAST_STATUS["last_error"] = str(exc)
            logger.exception("Worker cycle failed")
        shutdown_event.wait(POLL_SECONDS)
    LAST_STATUS["running"] = False
    logger.info("Worker stopped")


@app.on_event("startup")
def startup_event():
    global worker_thread
    if worker_thread and worker_thread.is_alive():
        return
    worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker_thread.start()


@app.on_event("shutdown")
def shutdown_event_handler():
    shutdown_event.set()
    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=5)


@app.get("/health")
def health():
    return JSONResponse(LAST_STATUS)


@app.get("/latest")
def latest(limit: int = 20):
    storage = Storage()
    return storage.latest(limit=limit)


@app.get("/")
def root():
    return {"ok": True, "service": "funding-regime-bot", "symbols": SYMBOLS}


def _handle_signal(signum, frame):
    shutdown_event.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)
