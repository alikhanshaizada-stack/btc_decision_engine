import pandas as pd
import numpy as np

# =======================
# LOAD DATA
# =======================
df = pd.read_csv('btc_1d.csv', parse_dates=['timestamp'])
df = df.sort_values('timestamp').reset_index(drop=True)

# =======================
# RETURNS & VOLATILITY
# =======================
df['log_return'] = np.log(df['close'] / df['close'].shift(1))

VOL_WINDOW = 20
df['volatility'] = df['log_return'].rolling(VOL_WINDOW).std()

def percentile_rank(series):
    return series.rank(pct=True) * 100

df['volatility_pct'] = percentile_rank(df['volatility'])

# =======================
# EXTENSION
# =======================
EXT_WINDOW = 20
df['price_mean'] = df['close'].rolling(EXT_WINDOW).mean()
df['price_std'] = df['close'].rolling(EXT_WINDOW).std()
df['extension'] = (df['close'] - df['price_mean']).abs() / df['price_std']
df['extension_pct'] = percentile_rank(df['extension'])

# =======================
# TREND STRUCTURE
# =======================
EMA_WINDOW = 100
df['ema_100'] = df['close'].ewm(span=EMA_WINDOW, adjust=False).mean()

SLOPE_WINDOW = 20

def calculate_slope(series):
    y = series.values
    x = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]
    return slope

df['ema_slope'] = df['ema_100'].rolling(SLOPE_WINDOW).apply(calculate_slope, raw=False)

df['trend_direction'] = np.where(
    df['ema_slope'] > 0, 'up',
    np.where(df['ema_slope'] < 0, 'down', 'flat')
)

DIST_WINDOW = 20
df['above_ema'] = df['close'] > df['ema_100']
df['above_ratio'] = df['above_ema'].rolling(DIST_WINDOW).mean()

def classify_structure(row):
    if abs(row['ema_slope']) > 0 and row['above_ratio'] > 0.65:
        return 'trend'
    elif 0.4 <= row['above_ratio'] <= 0.6:
        return 'range'
    else:
        return 'transition'

df['market_structure'] = df.apply(classify_structure, axis=1)

# =======================
# DECISION LAYER
# =======================
latest = df.iloc[-1]
drivers = []

# Risk drivers
if latest['volatility_pct'] > 80:
    drivers.append("Elevated volatility")

if latest['extension_pct'] > 85:
    drivers.append("Price far from equilibrium")

if latest['market_structure'] == 'transition':
    drivers.append("Unstable market structure")

# Regime classification
if latest['volatility_pct'] > 80 and latest['market_structure'] == 'transition':
    regime = "dangerous"
elif latest['market_structure'] == 'trend' and latest['volatility_pct'] < 70:
    regime = "favorable"
else:
    regime = "neutral"

# =======================
# OUTPUT
# =======================
print("\nBTCUSDT — DAILY DECISION CONTEXT\n")
print(f"Date: {latest['timestamp'].date()}")
print(f"Market regime: {regime.upper()}")
print(f"Trend: {latest['trend_direction']} ({latest['market_structure']})")
print(f"Volatility: {latest['volatility_pct']:.1f} percentile")
print(f"Extension: {latest['extension_pct']:.1f} percentile")

print("\nKey drivers:")
if drivers:
    for d in drivers:
        print(f"- {d}")
else:
    print("- No dominant risk factors")

# Human-readable interpretation
print("\nInterpretation:")
if regime == "dangerous":
    print("Market conditions are statistically rare and unstable. "
          "Historically, such environments are associated with higher decision error rates.")
elif regime == "favorable":
    print("Market conditions are orderly with controlled risk. "
          "Decision-making environments are historically more stable.")
else:
    print("Market conditions are mixed. Risk is present but not extreme. "
          "Caution and selectivity are advised.")