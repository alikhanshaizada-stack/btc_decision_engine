import os
import ccxt
import pandas as pd
from datetime import datetime, timezone

# ==========================
# CONFIG
# ==========================

SYMBOL = "BTC/USDT"
TIMEFRAME = "1d"
HISTORY_LIMIT = 1000
DATA_FILE = "btc_daily.csv"

exchange = ccxt.binance({
    "enableRateLimit": True,
})


# ==========================
# FETCH FUNCTIONS
# ==========================

def fetch_full_history():
    """
    Initial historical backfill (called once).
    """
    ohlcv = exchange.fetch_ohlcv(
        SYMBOL,
        timeframe=TIMEFRAME,
        limit=HISTORY_LIMIT
    )

    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def fetch_latest_daily_candle():
    """
    Fetch the most recent daily candle (for incremental updates).
    """
    ohlcv = exchange.fetch_ohlcv(
        SYMBOL,
        timeframe=TIMEFRAME,
        limit=2
    )

    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.iloc[-1]


# ==========================
# MAIN UPDATE LOGIC
# ==========================

def update_data_if_needed():
    """
    Ensures local daily BTC data is up-to-date.
    Performs:
    - full historical backfill on first run
    - daily incremental updates afterwards
    """

    # ---------- First run ----------
    if not os.path.exists(DATA_FILE):
        print("Initial historical backfill...")
        df = fetch_full_history()
        df.to_csv(DATA_FILE, index=False)
        return df

    # ---------- Load existing data ----------
    df = pd.read_csv(DATA_FILE, parse_dates=["timestamp"])

    latest_local_ts = df["timestamp"].iloc[-1].replace(tzinfo=timezone.utc)
    latest_market_candle = fetch_latest_daily_candle()

    # ---------- Append new daily candle ----------
    if latest_market_candle["timestamp"] > latest_local_ts:
        print("Appending new daily candle")
        df = pd.concat(
            [df, latest_market_candle.to_frame().T],
            ignore_index=True
        )
        df.to_csv(DATA_FILE, index=False)

    return df
