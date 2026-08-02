-- Sample trades from a specific hour partition
SELECT product_id, trade_id, price, size, maker_side, trade_time
FROM cryptostream_bronze.market_trades
WHERE year = '2026' AND month = '08' AND day = '02' AND hour = '10'
LIMIT 10;

-- Trade count and price range per product across all Bronze data
SELECT
    product_id,
    COUNT(*) AS trade_count,
    MIN(CAST(price AS DOUBLE)) AS min_price,
    MAX(CAST(price AS DOUBLE)) AS max_price
FROM cryptostream_bronze.market_trades
GROUP BY product_id
ORDER BY trade_count DESC;
