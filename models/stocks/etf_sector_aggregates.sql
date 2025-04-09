{{ config(
    materialized='table', 
    alias='etf_' ~ var('etf_symbol') ~ '_sector_aggregates'
    ) 
}}


WITH bollinger_ranked AS(
SELECT 
sector,
bollinger_recommendation,
count(*) as label_count,
ROW_NUMBER() OVER(partition by sector order by count(*) desc ) as RN 
FROM stocks_refined_dev.etf_{{ var('etf_symbol') }}_tickers_combined
group by sector, bollinger_recommendation
order by sector, RN
),
aggregates AS (
SELECT
tc.sector,
count(*) as holding_count,
sum(weight) as summed_weight,
avg(weight*overall_risk) as weightedavg_risk,
avg(weight*average_analyst_score) as weightedavg_analyst_score,
avg(weight*percchange) as weightedavg_percchange,
FROM stocks_refined_dev.etf_{{ var('etf_symbol') }}_tickers_combined tc
group by sector
)
SELECT
agg.*,
br.bollinger_recommendation,
br.label_count as bollinger_recommendation_count_with_label
FROM aggregates agg
JOIN bollinger_ranked br on br.sector=agg.sector
where RN=1