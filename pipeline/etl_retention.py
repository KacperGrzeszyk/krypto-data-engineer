"""
ETL STREAMING + RETENCJA
Krok 1: czyszczenie danych starszych niz 7 dni
Krok 2: agregacja surowych transakcji do swiec 1-minutowych
Krok 3: przebudowa widoku godzinowego market_candles_1h

Uruchamiaj cyklicznie (cron/scheduler) obok producer_ws.py i
consumer_kafka.py dzialajacych w tle.
"""
from db import get_connection

DELETE_TRADES_QUERY = """
DELETE FROM crypto_trades
WHERE trade_time < NOW() - INTERVAL '7 days';
"""

DELETE_CANDLES_QUERY = """
DELETE FROM market_candles_1m
WHERE candle_time < NOW() - INTERVAL '7 days';
"""

TRANSFORM_STREAMING_QUERY = """
WITH calculated_candles AS (
    SELECT
        date_trunc('minute', trade_time) AS candle_time,
        symbol,
        (ARRAY_AGG(price ORDER BY trade_time ASC))[1] AS open_price,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        (ARRAY_AGG(price ORDER BY trade_time DESC))[1] AS close_price,
        SUM(quantity) AS total_volume,
        COUNT(trade_id) AS trade_count,
        ROUND(CAST(SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS NUMERIC), 2) AS vwap,
        MAX(price) - MIN(price) AS price_range,
        AVG(SUM(quantity)) OVER (PARTITION BY symbol) AS avg_historical_volume
    FROM crypto_trades
    GROUP BY date_trunc('minute', trade_time), symbol
),
with_lag AS (
    SELECT
        *,
        LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY candle_time) AS prev_close_price
    FROM calculated_candles
)
INSERT INTO market_candles_1m (
    candle_time, symbol, open_price, high_price, low_price, close_price,
    total_volume, trade_count, vwap, price_range, price_change_pct, is_high_volume
)
SELECT
    candle_time, symbol, open_price, high_price, low_price, close_price,
    total_volume, trade_count, vwap, price_range,
    CASE
        WHEN prev_close_price IS NOT NULL AND prev_close_price > 0
        THEN ROUND(CAST(((close_price - prev_close_price) / prev_close_price) * 100 AS NUMERIC), 4)
        ELSE 0
    END AS price_change_pct,
    CASE
        WHEN total_volume > (avg_historical_volume * 2) THEN TRUE
        ELSE FALSE
    END AS is_high_volume
FROM with_lag
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

DROP_VIEW_QUERY = "DROP VIEW IF EXISTS market_candles_1h;"

CREATE_VIEW_QUERY = """
CREATE VIEW market_candles_1h AS
SELECT
    date_trunc('hour',  candle_time) AS candle_time,
    date_trunc('day',   candle_time) AS dzien,
    date_trunc('month', candle_time) AS miesiac,
    date_trunc('year',  candle_time) AS rok,
    symbol,
    AVG(open_price) AS open_price,
    MAX(high_price) AS high_price,
    MIN(low_price) AS low_price,
    AVG(close_price) AS close_price,
    SUM(total_volume) AS total_volume,
    SUM(trade_count) AS trade_count,
    AVG(vwap) AS vwap,
    MAX(price_range) AS price_range,
    AVG(price_change_pct) AS price_change_pct,
    MAX(CAST(is_high_volume AS INT))::BOOLEAN AS is_high_volume
FROM market_candles_1m
GROUP BY
    date_trunc('hour', candle_time),
    date_trunc('day', candle_time),
    date_trunc('month', candle_time),
    date_trunc('year', candle_time),
    symbol
ORDER BY candle_time ASC;
"""


def main():
    conn = get_connection()
    cursor = conn.cursor()

    print("Krok 1/3: Czyszczenie danych historycznych starszych niz 7 dni...")
    cursor.execute(DELETE_TRADES_QUERY)
    print(f"Usunieto stare surowe transakcje. Liczba usunietych wierszy: {cursor.rowcount}")

    cursor.execute(DELETE_CANDLES_QUERY)
    print(f"Usunieto stare swiece minutowe. Liczba usunietych wierszy: {cursor.rowcount}")
    conn.commit()

    print("Krok 2/3: Przetwarzanie biezacych transakcji na swiece analityczne (1m)...")
    cursor.execute(TRANSFORM_STREAMING_QUERY)
    conn.commit()
    print(f"Zaktualizowano tabele market_candles_1m. Wierszy: {cursor.rowcount}")

    print("Krok 3/3: Odswiezanie widoku godzinowego (1h) pod Power BI...")
    cursor.execute(DROP_VIEW_QUERY)
    cursor.execute(CREATE_VIEW_QUERY)
    conn.commit()

    cursor.close()
    conn.close()
    print("Pelny proces ETL zakonczony sukcesem! Power BI gotowy do odswiezenia.")


if __name__ == "__main__":
    main()
