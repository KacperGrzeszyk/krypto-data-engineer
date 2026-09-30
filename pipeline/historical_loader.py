"""
Zapis danych historycznych z Binance REST API do PostgreSQL.
Uruchom raz na starcie projektu, zeby wypelnic market_candles_1m
danymi z ostatnich 7 dni dla 10 kryptowalut.
"""
from datetime import datetime, timedelta
import time
import requests

from db import get_connection

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT",
    "XRPUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

INTERVAL = "1m"
BINANCE_URL = "https://api.binance.com/api/v3/klines"

INSERT_QUERY = """
INSERT INTO market_candles_1m (
    candle_time, symbol, open_price, high_price, low_price, close_price,
    total_volume, trade_count, vwap, price_range, price_change_pct, is_high_volume
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (candle_time, symbol) DO UPDATE
SET
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    total_volume = EXCLUDED.total_volume,
    trade_count = EXCLUDED.trade_count,
    vwap = EXCLUDED.vwap,
    price_range = EXCLUDED.price_range,
    price_change_pct = EXCLUDED.price_change_pct,
    is_high_volume = EXCLUDED.is_high_volume;
"""

UPDATE_ANOMALY_QUERY = """
WITH avg_vol AS (
    SELECT symbol, AVG(total_volume) AS mean_vol
    FROM market_candles_1m
    GROUP BY symbol
)
UPDATE market_candles_1m m
SET is_high_volume = CASE
    WHEN m.total_volume > (av.mean_vol * 2) THEN TRUE
    ELSE FALSE
END
FROM avg_vol av
WHERE m.symbol = av.symbol;
"""


def fetch_symbol_candles(symbol: str, start_time: int, end_time: int) -> list:
    candles = []
    current_start = start_time
    while current_start < end_time:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": current_start,
            "limit": 1000,
        }
        response = requests.get(BINANCE_URL, params=params, timeout=10)
        data = response.json()

        if not data or isinstance(data, dict):
            break

        candles.extend(data)
        current_start = data[-1][0] + 1
        time.sleep(0.1)
    return candles


def main():
    conn = get_connection()
    cursor = conn.cursor()

    end_time = int(time.time() * 1000)
    start_time = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)

    for symbol in SYMBOLS:
        print(f"Pobieranie danych historycznych dla {symbol}...")
        symbol_candles = fetch_symbol_candles(symbol, start_time, end_time)
        print(f"Zapisywanie {len(symbol_candles)} swiec dla {symbol} do bazy...")

        prev_close = None
        for kline in symbol_candles:
            candle_time = datetime.fromtimestamp(kline[0] / 1000.0)
            open_p = float(kline[1])
            high_p = float(kline[2])
            low_p = float(kline[3])
            close_p = float(kline[4])
            volume = float(kline[5])
            trade_count = int(kline[8])

            vwap = (open_p + close_p + high_p + low_p) / 4
            price_range = high_p - low_p

            if prev_close is not None and prev_close > 0:
                price_change_pct = round(((close_p - prev_close) / prev_close) * 100, 4)
            else:
                price_change_pct = 0.0
            prev_close = close_p

            cursor.execute(INSERT_QUERY, (
                candle_time, symbol, open_p, high_p, low_p, close_p,
                volume, trade_count, vwap, price_range, price_change_pct, False,
            ))
        conn.commit()

    cursor.execute(UPDATE_ANOMALY_QUERY)
    conn.commit()

    cursor.close()
    conn.close()
    print("Sukces! Dane historyczne dla 10 kryptowalut gotowe.")


if __name__ == "__main__":
    main()
