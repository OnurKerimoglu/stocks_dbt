select
fund_ticker,
count(1) as holding_count
from `stocks-455113`.`stocks_raw`.`etfs`
group by fund_ticker