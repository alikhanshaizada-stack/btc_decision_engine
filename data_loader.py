import ccxt
import pandas as pd

def load_btc_history(days: int = 180) -> pd.DataFrame:
    exchange = ccxt.binance()
    ohlcv = exchange.fetch_ohlcv(
        "BTC/USDT",
        timeframe="1d",
        limit=days
    )

    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df

