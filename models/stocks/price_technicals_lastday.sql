{{ config(materialized='table') }}

WITH last_30_days AS (
  SELECT
    symbol,
    date,
    close,
    high,
    low,
    open,
    volume
  FROM (
    SELECT
      *,
      MAX(date) OVER (PARTITION BY symbol) AS max_date
    FROM
      stocks_raw.stock_prices
  )
  WHERE date BETWEEN DATE_SUB(max_date, INTERVAL 29 DAY) AND max_date
  ORDER BY symbol asc, date desc
),
moments30 AS
(
SELECT
  symbol,
  -- Aggregate over last 30 days
  AVG(close) AS close_ma30,
  STDDEV(close) AS close_std30,
FROM
  last_30_days
GROUP BY
  symbol
)
SELECT
    pricelast.symbol,
    date,
    close,
    high,
    low,
    open,
    volume,
    close_ma30,
    close_std30,
    close_ma30 + 2*close_std30 as bollinger_up,
    close_ma30 - 2*close_std30 as bollinger_low,
    case 
      when close > (close_ma30 + 2*close_std30) then 'sell'
      when close < (close_ma30 - 2*close_std30) then 'buy'
      else 'hold'
    end as bollinger_recommendation,
    (close-open)/open as percchange
  FROM (
    SELECT
      *,
      MAX(date) OVER (PARTITION BY symbol) AS max_date
    FROM
      stocks_raw.stock_prices
  ) pricelast
  JOIN moments30 ON moments30.symbol=pricelast.symbol 
  WHERE date=max_date
  ORDER BY symbol asc
