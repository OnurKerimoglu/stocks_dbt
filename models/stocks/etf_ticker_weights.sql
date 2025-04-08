{{ config(
    materialized='table', 
    alias='etf_' ~ var('etf_symbol') ~ '_tickers'
    ) 
}}

WITH holding_weights AS(
  select
  ticker as symbol,
  weight
  from stocks_raw.etfs
  where fund_ticker = '{{ var('etf_symbol') }}'
),
etf_tickers_info AS (
 select
 hw.symbol,
 hw.weight,
 company_name,
 si.sector,
 industry,
 market_capitalization,
 overall_risk,
 SAFE_CAST(SPLIT(average_analyst_rating, '-')[OFFSET(0)] AS FLOAT64) AS average_analyst_score
 from stocks_raw.stock_info si
 join holding_weights hw ON hw.symbol = si.symbol
)
select *
from etf_tickers_info
