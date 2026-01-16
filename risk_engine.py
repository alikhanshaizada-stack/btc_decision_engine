import numpy as np
import pandas as pd


def percentile(series):
    return series.rank(pct=True) * 100


def compute_risk(df: pd.DataFrame) -> dict:
    df = df.copy()

    # === RETURNS ===
    df["ret"] = df["close"].pct_change()

    # === VOLATILITY RISK ===
    df["vol"] = df["ret"].rolling(20).std()
    vol_risk = percentile(df["vol"]).iloc[-1]

    # === DOWNSIDE RISK ===
    downside = df["ret"].clip(upper=0)
    downside_std = downside.rolling(20).std()
    downside_risk = percentile(downside_std).iloc[-1]

    # === TAIL RISK (CVaR 5%) ===
    q = df["ret"].rolling(250).quantile(0.05)
    cvar = df["ret"][df["ret"] < q].rolling(20).mean()
    tail_risk = percentile(cvar.abs()).iloc[-1]

    # === STRUCTURE INSTABILITY ===
    ema = df["close"].ewm(span=100).mean()
    flips = np.sign(df["close"] - ema).diff().abs()
    flip_rate = flips.rolling(20).mean()
    structure_risk = percentile(flip_rate).iloc[-1]

    # === LIQUIDITY / STRESS PROXY ===
    range_pct = (df["high"] - df["low"]) / df["close"]
    stress = range_pct.rolling(20).mean()
    stress_risk = percentile(stress).iloc[-1]

    # === TOTAL RISK SCORE ===
    risk_score = round(
        0.25 * vol_risk
        + 0.25 * downside_risk
        + 0.20 * tail_risk
        + 0.15 * structure_risk
        + 0.15 * stress_risk,
        1,
    )

    drivers = sorted(
        {
            "Volatility": vol_risk,
            "Downside Risk": downside_risk,
            "Tail Risk": tail_risk,
            "Structure Instability": structure_risk,
            "Market Stress": stress_risk,
        }.items(),
        key=lambda x: x[1],
        reverse=True,
    )[:3]

    return {
        "date": df["timestamp"].iloc[-1].date(),
        "risk_score": risk_score,
        "components": {
            "volatility": round(vol_risk, 1),
            "downside": round(downside_risk, 1),
            "tail": round(tail_risk, 1),
            "structure": round(structure_risk, 1),
            "stress": round(stress_risk, 1),
        },
        "drivers": [d[0] for d in drivers],
    }
