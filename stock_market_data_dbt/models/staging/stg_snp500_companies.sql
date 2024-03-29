{{ config(
    materialized = "table"
)}}

select
    cast(CIK as int) as CIK,
    regexp_replace(Symbol, r'\.', r'-') as ticker,
    security as security_name,
    Headquarters_Location as headquarters_location,
    GICS_sector,
    GICS_sub_industry,
    cast(left(Founded,4) as int) as year_founded,
    -- To-do handle exceptions where date_first_added only has year
    parse_date('%Y-%m-%d', 
        case 
            when length(date_first_added) = 4 then 
                concat(date_first_added,'-01-01')    
            else
                left(Date_first_added,10)
        end) as date_first_added,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    _airbyte_normalized_at,
    _airbyte_raw_SnP500_companies_hashid
from {{source('raw_stock_market_data', 'raw_SnP500_companies')}}