import ccxt
import pandas as pd
import os
from datetime import datetime

DATA_PATH = "data/btc_daily.csv"

exchange = ccxt.binance({
    "enableRateLimit": True
})


def fetch_daily_ohlcv(limit=365):
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=limit)
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


def update_data_if_needed():
    os.makedirs("data", exist_ok=True)

    if not os.path.exists(DATA_PATH):
        df = fetch_daily_ohlcv()
        df.to_csv(DATA_PATH, index=False)
        return df

    df_existing = pd.read_csv(DATA_PATH, parse_dates=["timestamp"])
    last_saved_date = df_existing["timestamp"].iloc[-1].date()

    df_latest = fetch_daily_ohlcv(limit=2)
    latest_date = df_latest["timestamp"].iloc[-1].date()

    if latest_date > last_saved_date:
        df_existing = pd.concat(
            [df_existing, df_latest.iloc[-1:]],
            ignore_index=True
        )
        df_existing.to_csv(DATA_PATH, index=False)

    return df_existing
