{{ config(
    materialized='table',
    labels={'env': target.name}
) }}

{% set lookback_days = var('asof_lookback_days', 10) %}

-- Latest-asof per ticker, then keep all rows from that run
WITH latest_per_ticker AS (
  SELECT
    Ticker,
    MAX(asof) AS asof
  FROM {{ source('silver', 'stock_forecasts') }}
  WHERE asof >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_days }}  DAY)
  GROUP BY Ticker
)
SELECT s.*
FROM {{ source('silver', 'stock_forecasts') }} s
JOIN latest_per_ticker l
  ON s.Ticker = l.Ticker
 AND s.asof   = l.asof
 WHERE s.asof >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ lookback_days }}  DAY)
