-- ============================================================
-- SCHEMA: Krypto Data Engineer Project
-- ============================================================

CREATE TABLE IF NOT EXISTS crypto_trades (
    trade_id        BIGINT PRIMARY KEY,
    symbol          VARCHAR(20),
    price           DOUBLE PRECISION,
    quantity        DOUBLE PRECISION,
    trade_time      TIMESTAMP,
    is_buyer_maker  BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_crypto_trades_time ON crypto_trades (trade_time);
CREATE INDEX IF NOT EXISTS idx_crypto_trades_symbol ON crypto_trades (symbol);

CREATE TABLE IF NOT EXISTS market_candles_1m (
    candle_time         TIMESTAMP,
    symbol              VARCHAR(20),
    open_price          DOUBLE PRECISION,
    high_price          DOUBLE PRECISION,
    low_price           DOUBLE PRECISION,
    close_price         DOUBLE PRECISION,
    total_volume        DOUBLE PRECISION,
    trade_count         INT,
    vwap                DOUBLE PRECISION,
    price_range         DOUBLE PRECISION,
    price_change_pct    DOUBLE PRECISION,
    is_high_volume      BOOLEAN,
    PRIMARY KEY (candle_time, symbol)
);

CREATE TABLE IF NOT EXISTS wymiar_symboli (
    symbol      VARCHAR(20) PRIMARY KEY,
    nazwa       VARCHAR(50),
    ikona_url   VARCHAR(200)
);

INSERT INTO wymiar_symboli (symbol, nazwa) VALUES
    ('BTCUSDT','Bitcoin'), ('ETHUSDT','Ethereum'), ('SOLUSDT','Solana'),
    ('BNBUSDT','BNB'), ('ADAUSDT','Cardano'), ('XRPUSDT','XRP'),
    ('DOGEUSDT','Dogecoin'), ('AVAXUSDT','Avalanche'),
    ('LINKUSDT','Chainlink'), ('DOTUSDT','Polkadot')
ON CONFLICT (symbol) DO NOTHING;

-- Widok godzinowy z kolumnami granularnosci dzien/miesiac/rok,
-- uzywany przez Field Parameter "Okno czasowe" w Power BI.
DROP VIEW IF EXISTS market_candles_1h;

CREATE VIEW market_candles_1h AS
SELECT
    date_trunc('hour',  candle_time) AS candle_time,
    date_trunc('day',   candle_time) AS dzien,
    date_trunc('month', candle_time) AS miesiac,
    date_trunc('year',  candle_time) AS rok,
    symbol,
    AVG(open_price)                              AS open_price,
    MAX(high_price)                              AS high_price,
    MIN(low_price)                               AS low_price,
    AVG(close_price)                             AS close_price,
    SUM(total_volume)                            AS total_volume,
    SUM(trade_count)                             AS trade_count,
    AVG(vwap)                                    AS vwap,
    MAX(price_range)                             AS price_range,
    AVG(price_change_pct)                        AS price_change_pct,
    MAX(CAST(is_high_volume AS INT))::BOOLEAN    AS is_high_volume
FROM market_candles_1m
GROUP BY
    date_trunc('hour', candle_time),
    date_trunc('day', candle_time),
    date_trunc('month', candle_time),
    date_trunc('year', candle_time),
    symbol
ORDER BY candle_time ASC;
