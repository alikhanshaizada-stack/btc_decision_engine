import ccxt
import pandas as pd

# 1. Подключаемся к Binance (spot)
exchange = ccxt.binance()

# 2. Параметры
symbol = 'BTC/USDT'
timeframe = '1d'
limit = 500  # ~1.5 года данных

# 3. Загружаем OHLCV
ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

# 4. Преобразуем в DataFrame
df = pd.DataFrame(
    ohlcv,
    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
)

# 5. Приводим timestamp к дате
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# 6. Сохраняем в CSV
df.to_csv('btc_1d.csv', index=False)

# 7. Показываем первые 5 строк
print(df.head())