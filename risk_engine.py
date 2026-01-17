import numpy as np
import pandas as pd


def compute_risk_engine(df: pd.DataFrame) -> dict:
    df = df.copy()

    # === RETURNS ===
    df["ret"] = np.log(df["close"] / df["close"].shift(1))

    # === VOLATILITY PRESSURE ===
    vol_hist = df["ret"].rolling(14).std()
    vol_pressure = vol_hist.rank(pct=True).iloc[-1] * 100

    # === VOLATILITY EXPANSION ===
    vol_short = df["ret"].rolling(5).std()
    vol_long = df["ret"].rolling(30).std()
    vol_expansion = (vol_short / vol_long).iloc[-1] * 100

    # === DRAWDOWN STRESS ===
    rolling_max = df["close"].rolling(30).max()
    drawdown = df["close"] / rolling_max - 1
    drawdown_stress = drawdown.rank(pct=True).iloc[-1] * 100

    # === TAIL RISK ===
    tail_events = (df["ret"] < df["ret"].quantile(0.05)).rolling(60).mean()
    tail_risk = tail_events.iloc[-1] * 100

    # === RISK SCORE ===
    risk_score = (
        0.30 * vol_pressure +
        0.25 * vol_expansion +
        0.25 * drawdown_stress +
        0.20 * tail_risk
    )

    # === REGIME ===
    if risk_score >= 70:
        regime = "DANGEROUS"
    elif risk_score <= 40:
        regime = "FAVORABLE"
    else:
        regime = "NEUTRAL"

    return {
        "risk_score": round(risk_score, 1),
        "regime": regime,
        "drivers": {
            "Volatility Pressure": round(vol_pressure, 1),
            "Volatility Expansion": round(vol_expansion, 1),
            "Drawdown Stress": round(drawdown_stress, 1),
            "Tail Risk": round(tail_risk, 1),
        }
    }
