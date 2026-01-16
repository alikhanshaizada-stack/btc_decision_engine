import ccxt
import pandas as pd
from pathlib import Path

DATA_FILE = Path("btc_data.csv")

exchange = ccxt.binance()

def load_data():
    if DATA_FILE.exists():
        return pd.read_csv(DATA_FILE, parse_dates=["timestamp"])
    return pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])

def save_data(df):
    df.to_csv(DATA_FILE, index=False)

def fetch_latest_daily_candle():
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=2)
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp","open","high","low","close","volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df.iloc[-1]

def update_data_if_needed():
    df = load_data()

    latest = fetch_latest_daily_candle()

    if len(df) == 0 or latest["timestamp"] > df["timestamp"].max():
        df = pd.concat([df, latest.to_frame().T], ignore_index=True)
        save_data(df)

    return df
