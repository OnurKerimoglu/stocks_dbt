{{ config(
    materialized='table', 
    alias='etf_' ~ var('etf_symbol') ~ '_weights'
    ) 
}}

WITH etf_holdings AS(
  select
  ticker,
  weight
  from stocks_raw.etfs
  where fund_ticker = '{{ var('etf_symbol') }}'
)
select ticker, weight
from etf_holdings
