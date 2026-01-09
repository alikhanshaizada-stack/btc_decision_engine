from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import numpy as np
from datetime import datetime, date
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# =======================
# GLOBAL STATE (CACHE)
# =======================
cached_result = None
last_updated = None


# =======================
# CORE CALCULATION
# =======================
def calculate_market_context():
    global cached_result, last_updated

    df = pd.read_csv("btc_1d.csv", parse_dates=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Volatility
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["volatility"] = df["log_return"].rolling(20).std()
    df["volatility_pct"] = df["volatility"].rank(pct=True) * 100

    # Extension
    df["price_mean"] = df["close"].rolling(20).mean()
    df["price_std"] = df["close"].rolling(20).std()
    df["extension"] = (df["close"] - df["price_mean"]).abs() / df["price_std"]
    df["extension_pct"] = df["extension"].rank(pct=True) * 100

    # Trend
    df["ema_100"] = df["close"].ewm(span=100, adjust=False).mean()

    def slope(series):
        y = series.values
        x = np.arange(len(y))
        return np.polyfit(x, y, 1)[0]

    df["ema_slope"] = df["ema_100"].rolling(20).apply(slope, raw=False)

    df["trend_direction"] = np.where(
        df["ema_slope"] > 0, "up",
        np.where(df["ema_slope"] < 0, "down", "flat")
    )

    df["above_ratio"] = (df["close"] > df["ema_100"]).rolling(20).mean()

    def structure(row):
        if abs(row["ema_slope"]) > 0 and row["above_ratio"] > 0.65:
            return "trend"
        elif 0.4 <= row["above_ratio"] <= 0.6:
            return "range"
        else:
            return "transition"

    df["market_structure"] = df.apply(structure, axis=1)

    latest = df.iloc[-1]
    drivers = []

    # Trend icon
    trend_icon = "↑" if latest["trend_direction"] == "up" else "↓" if latest["trend_direction"] == "down" else "→"

    if latest["volatility_pct"] > 80:
        drivers.append("Elevated volatility")
    if latest["extension_pct"] > 85:
        drivers.append("Price far from equilibrium")
    if latest["market_structure"] == "transition":
        drivers.append("Unstable market structure")

    if latest["volatility_pct"] > 80 and latest["market_structure"] == "transition":
        regime = "dangerous"
        interpretation = (
            "Market conditions are statistically rare and unstable. "
            "Historically, such environments are associated with higher decision error rates."
        )
    elif latest["market_structure"] == "trend" and latest["volatility_pct"] < 70:
        regime = "favorable"
        interpretation = (
            "Market conditions are orderly with controlled risk. "
            "Decision-making environments are historically more stable."
        )
    else:
        regime = "neutral"
        interpretation = (
            "Market conditions are mixed. Risk is present but not extreme. "
            "Caution and selectivity are advised."
        )

    confidence = "High" if len(df) > 300 else "Medium" if len(df) > 100 else "Low"

    cached_result = {
        "regime": regime,
        "trend": f"{trend_icon} {latest['trend_direction']} ({latest['market_structure']})",
        "volatility": round(latest["volatility_pct"], 1),
        "extension": round(latest["extension_pct"], 1),
        "drivers": drivers,
        "interpretation": interpretation,
        "date": latest["timestamp"].date(),
        "confidence": confidence,
        "structure": latest["market_structure"],
    }

    last_updated = datetime.utcnow()


# =======================
# SCHEDULER (DAILY)
# =======================
scheduler = BackgroundScheduler()
scheduler.add_job(calculate_market_context, "interval", days=1)
scheduler.start()

# First run on startup
calculate_market_context()


# =======================
# WEB ROUTE
# =======================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            **cached_result,
            "last_updated": last_updated.strftime("%Y-%m-%d %H:%M UTC"),
            "update_policy": "Updated automatically once per day (daily close).",
        },
    )