{{ config(
    materialized = "table"
)}} 

select distinct
    GICS_Sector,
    GICS_sub_industry,
    DENSE_RANK() OVER (ORDER BY  GICS_Sector, GICS_sub_industry) AS RowNum
from {{ ref('dim_companies') }}
order by GICS_sub_industry 