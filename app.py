from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import numpy as np
from datetime import datetime

from data_loader import update_data_if_needed

app = FastAPI()
templates = Jinja2Templates(directory="templates")


def calculate_market_context():
    df = update_data_if_needed()

    if len(df) < 120:
        return {
            "regime": "Not enough data yet",
            "trend": "—",
            "volatility": "—",
            "extension": "—",
            "drivers": [],
            "interpretation": "System is collecting historical data.",
            "date": "—",
            "confidence": "Low"
        }

    # === Volatility ===
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["volatility"] = df["log_return"].rolling(20).std()
    df["volatility_pct"] = df["volatility"].rank(pct=True) * 100

    # === Extension ===
    df["mean"] = df["close"].rolling(20).mean()
    df["std"] = df["close"].rolling(20).std()
    df["extension"] = abs(df["close"] - df["mean"]) / df["std"]
    df["extension_pct"] = df["extension"].rank(pct=True) * 100

    # === Trend ===
    df["ema_100"] = df["close"].ewm(span=100, adjust=False).mean()
    df["above_ratio"] = (df["close"] > df["ema_100"]).rolling(20).mean()

    def slope(series):
        x = np.arange(len(series))
        return np.polyfit(x, series.values, 1)[0]

    df["ema_slope"] = df["ema_100"].rolling(20).apply(slope, raw=False)

    def structure(row):
        if abs(row["ema_slope"]) > 0 and row["above_ratio"] > 0.65:
            return "trend"
        elif 0.4 <= row["above_ratio"] <= 0.6:
            return "range"
        else:
            return "transition"

    df["structure"] = df.apply(structure, axis=1)

    latest = df.iloc[-1]
    drivers = []

    if latest["volatility_pct"] > 80:
        drivers.append("Elevated volatility")
    if latest["extension_pct"] > 85:
        drivers.append("Price far from equilibrium")
    if latest["structure"] == "transition":
        drivers.append("Unstable market structure")

    if latest["volatility_pct"] > 80 and latest["structure"] == "transition":
        regime = "Dangerous"
        interpretation = "Market conditions are statistically unstable. Risk of decision errors is elevated."
    elif latest["structure"] == "trend" and latest["volatility_pct"] < 70:
        regime = "Favorable"
        interpretation = "Market conditions are orderly with controlled risk."
    else:
        regime = "Neutral"
        interpretation = "Mixed conditions. Selective and cautious decision-making advised."

    trend_icon = "↑" if latest["ema_slope"] > 0 else "↓" if latest["ema_slope"] < 0 else "→"

    return {
        "regime": regime,
        "trend": f"{trend_icon} {latest['structure']}",
        "volatility": round(latest["volatility_pct"], 1),
        "extension": round(latest["extension_pct"], 1),
        "drivers": drivers,
        "interpretation": interpretation,
        "date": latest["timestamp"].date(),
        "confidence": "High"
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    context = calculate_market_context()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            **context,
            "update_policy": "Data updates automatically using latest daily close (Binance)."
        }
    )
