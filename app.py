from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime, date
import pandas as pd

from data_loader import update_data_if_needed


app = FastAPI()
templates = Jinja2Templates(directory="templates")


# =========================
# REGIME CLASSIFICATION v1.0
# =========================

def classify_regime(volatility_pct, extension_pct, structure):
    if (
        volatility_pct >= 80
        or extension_pct >= 85
        or structure == "Transition"
    ):
        return "Dangerous"

    if volatility_pct < 70 and extension_pct < 80 and structure == "Trend":
        return "Favorable"

    return "Neutral"


# =========================
# METRIC CALCULATIONS
# =========================

def calculate_extension(df, window=20):
    ma = df["close"].rolling(window).mean()
    std = df["close"].rolling(window).std()

    extension = abs(df["close"].iloc[-1] - ma.iloc[-1]) / std.iloc[-1]
    return extension * 10  # normalized scale


def detect_structure(df):
    ma_short = df["close"].rolling(20).mean()
    ma_long = df["close"].rolling(50).mean()

    if ma_short.iloc[-1] > ma_long.iloc[-1]:
        return "Trend"
    elif ma_short.iloc[-1] < ma_long.iloc[-1]:
        return "Transition"
    return "Range"


def detect_trend(df):
    ma_short = df["close"].rolling(20).mean()
    ma_long = df["close"].rolling(50).mean()

    if ma_short.iloc[-1] > ma_long.iloc[-1]:
        return "up"
    elif ma_short.iloc[-1] < ma_long.iloc[-1]:
        return "down"
    return "flat"


# =========================
# MAIN UI ENDPOINT
# =========================

@app.get("/", response_class=HTMLResponse)
def market_context(request: Request):
    # 1. Update data once per day (if needed)
    df = update_data_if_needed()

    if len(df) < 60:
        return HTMLResponse("Not enough data yet")

    # 2. Calculate metrics
    volatility_pct = calculate_volatility(df)
    extension_pct = calculate_extension(df)
    structure = detect_structure(df)
    trend = detect_trend(df)

    regime = classify_regime(
        volatility_pct=volatility_pct,
        extension_pct=extension_pct,
        structure=structure
    )

    # 3. Human-readable labels
    trend_label = f"{trend} (transition)" if structure == "Transition" else trend

    interpretation = {
        "Dangerous": "Risk is elevated. Historical conditions are associated with higher failure rates.",
        "Neutral": "Market conditions are mixed. Risk is present but not extreme.",
        "Favorable": "Market conditions are relatively stable with controlled risk."
    }[regime]

    # 4. Render UI
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "symbol": "BTCUSDT",
            "regime": regime,
            "trend": trend_label,
            "volatility": round(volatility_pct, 1),
            "extension": round(extension_pct, 1),
            "drivers": ["Unstable market structure"] if structure == "Transition" else [],
            "interpretation": interpretation,
            "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        }
    )

