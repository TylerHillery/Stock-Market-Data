{{ config(
    materialized = "table"
)}} 

select
    c.ticker,
    c.security_name,
    c.headquarters_location,
    c.GICS_sector,
    c.GICS_sub_industry,
    c.year_founded,
    c.date_first_added,
    q.market,
    q.region,
    q.exchange,
    q.market_cap
from {{ ref('stg_snp500_companies') }} as c
left join {{ ref('stg_stock_quote_data') }} as q
    on c.ticker = q.ticker