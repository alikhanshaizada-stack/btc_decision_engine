import ccxt
import pandas as pd
import os

DATA_PATH = "data/btc_daily.csv"

exchange = ccxt.binance({"enableRateLimit": True})


def load_or_fetch_data():
    os.makedirs("data", exist_ok=True)

    if not os.path.exists(DATA_PATH):
        df = fetch_history()
        df.to_csv(DATA_PATH, index=False)
        return df

    df = pd.read_csv(DATA_PATH, parse_dates=["timestamp"])
    last_date = df["timestamp"].iloc[-1].date()

    latest = fetch_latest()
    latest_date = latest["timestamp"].iloc[-1].date()

    if latest_date > last_date:
        df = pd.concat([df, latest.iloc[-1:]], ignore_index=True)
        df.to_csv(DATA_PATH, index=False)

    return df


def fetch_history(limit=400):
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=limit)
    return to_df(ohlcv)


def fetch_latest():
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=2)
    return to_df(ohlcv)


def to_df(ohlcv):
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df
