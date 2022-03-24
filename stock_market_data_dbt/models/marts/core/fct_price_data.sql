{{ config(
    materialized = "view"
)}} 

with previous_day_close as (
    select
        ticker,
        price_date,
        LAG(close_price) OVER (PARTITION BY ticker ORDER BY price_date) as previous_day_close
    from {{ ref('stg_stock_price_data') }}
)

select
    price.ticker,
    companies.security_name,
    price.price_date,
    price.open_price,
    price.low_price,
    price.high_price,
    price.close_price,
    price.adj_close_price,
    price.volume,
    companies.headquarters_location,
    companies.GICS_sector,
    companies.GICS_sub_industry,
    gics.RowNum as GICS_row_num,
    companies.year_founded,
    companies.date_first_added,
    companies.market,
    companies.region,
    companies.exchange,
    companies.market_cap,
    (close_price - prev.previous_day_close)/prev.previous_day_close as one_day_percent_change
from {{ ref('stg_stock_price_data') }} as price
left join {{ ref('dim_companies') }} as companies
    on price.ticker = companies.ticker
left join {{ref('dim_GICS')}} as gics
    on companies.GICS_sub_industry = gics.GICS_sub_industry
left join previous_day_close as prev
    on price.ticker = prev.ticker 
    and price.price_date = prev.price_date