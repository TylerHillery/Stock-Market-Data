{{ config(
    materialized = "table"
)}}

select
    ticker,
    shortName as short_name,
    parse_datetime('%Y-%m-%d %H:%M:%S',left(AsOfDataTime,19)) as as_of_datetime,
    market,
    region,
    exchange,
    marketState as market_state,
    marketCap as market_cap,
    price as current_price,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    _airbyte_normalized_at,
    _airbyte_raw_stock_quote_data_hashid
from {{source('raw_stock_market_data', 'raw_stock_quote_data')}}