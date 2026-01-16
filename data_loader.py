import ccxt
import pandas as pd
from datetime import datetime, timedelta
import os

DATA_PATH = "btc_daily.csv"

exchange = ccxt.binance({"enableRateLimit": True})


def load_or_fetch_data():
    if os.path.exists(DATA_PATH):
        df = pd.read_csv(DATA_PATH, parse_dates=["timestamp"])
        last_ts = df["timestamp"].iloc[-1]
        if last_ts.date() >= datetime.utcnow().date() - timedelta(days=1):
            return df

    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=1500)
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.to_csv(DATA_PATH, index=False)
    return df
