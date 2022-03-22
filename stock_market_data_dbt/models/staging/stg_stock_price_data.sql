{{ config(
    materialized = "table"
)}}

select
    ticker,
    parse_date('%Y-%m-%d', left(Date,10)) as price_date,
    open as open_price,
    low as low_price,
    high as high_price,
    close as close_price,
    adj_close as adj_close_price,
    volume,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    _airbyte_normalized_at,
    _airbyte_raw_stock_price_data_hashid
from {{source('raw_stock_market_data','raw_stock_price_data')}}