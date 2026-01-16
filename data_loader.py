import ccxt
import pandas as pd
import os

DATA_PATH = "data/btc_daily.csv"

exchange = ccxt.binance({
    "enableRateLimit": True
})


def load_or_fetch_data():
    os.makedirs("data", exist_ok=True)

    # --- First run: no data ---
    if not os.path.exists(DATA_PATH):
        df = _fetch_history()
        df.to_csv(DATA_PATH, index=False)
        return df

    df_existing = pd.read_csv(DATA_PATH, parse_dates=["timestamp"])
    last_saved_date = df_existing["timestamp"].iloc[-1].date()

    df_latest = _fetch_latest()
    latest_date = df_latest["timestamp"].iloc[-1].date()

    if latest_date > last_saved_date:
        df_existing = pd.concat(
            [df_existing, df_latest.iloc[-1:]],
            ignore_index=True
        )
        df_existing.to_csv(DATA_PATH, index=False)

    return df_existing


def _fetch_history(limit=400):
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=limit)
    return _to_df(ohlcv)


def _fetch_latest():
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=2)
    return _to_df(ohlcv)


def _to_df(ohlcv):
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df
