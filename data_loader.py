import ccxt
import pandas as pd
from datetime import datetime
import os

exchange = ccxt.binance({"enableRateLimit": True})

CSV_PATH = "btc_1d.csv"


def fetch_latest_daily_candle():
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=2)
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df.iloc[[-1]]


def load_existing_data():
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    return pd.read_csv(CSV_PATH, parse_dates=["timestamp"])


def update_data_if_needed():
    df = load_existing_data()

    if df.empty:
        candles = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=400)
        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.to_csv(CSV_PATH, index=False)
        return df

    last_date = df["timestamp"].iloc[-1].date()
    today = datetime.utcnow().date()

    if last_date < today:
        new_candle = fetch_latest_daily_candle()
        df = pd.concat([df, new_candle]).drop_duplicates("timestamp")
        df.to_csv(CSV_PATH, index=False)

    return df
