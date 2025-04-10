{{ config(
    materialized='table', 
    alias='etf_' ~ var('etf_symbol') ~ '_top_ticker_prices'
    ) 
}}

SELECT
    sp.symbol,
    sp.date,
    sp.close,
    tc.weight_rank
FROM stocks_raw.stock_prices sp
RIGHT JOIN  stocks_refined_dev.etf_{{ var('etf_symbol') }}_tickers_combined tc ON tc.symbol = sp.symbol
WHERE 1=1
AND tc.weight_rank<=20
-- AND date=(SELECT max(date) FROM stocks_raw.stock_prices)
AND date BETWEEN DATE_SUB((SELECT max(date) FROM stocks_raw.stock_prices), INTERVAL 365 DAY) AND (SELECT max(date) FROM stocks_raw.stock_prices)
ORDER BY tc.weight_rank ASC, date DESC