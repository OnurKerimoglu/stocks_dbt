{{ config(
    materialized='table', 
    alias='etf_' ~ var('etf_symbol') ~ '_tickers_combined'
    ) 
}}

WITH holding_weights AS(
  select
  ticker as symbol,
  weight,
  sector
  from stocks_raw.etfs
  where fund_ticker = '{{ var('etf_symbol') }}'
),
etf_tickers_info AS (
 select
 hw.symbol,
 hw.weight,
 hw.sector,
 company_name,
 industry,
 market_capitalization,
 overall_risk,
 CASE 
  WHEN average_analyst_rating != ''
  THEN SAFE_CAST(SPLIT(average_analyst_rating, '-')[OFFSET(0)] AS FLOAT64)
  ELSE NULL
 END AS average_analyst_score,
 CASE 
  WHEN average_analyst_rating != ''
  THEN SPLIT(average_analyst_rating, '-')[OFFSET(1)]
  ELSE NULL
 END AS average_analyst_recommendation
 from stocks_raw.stock_info si
 join holding_weights hw ON hw.symbol = si.symbol
)
select *
from etf_tickers_info
