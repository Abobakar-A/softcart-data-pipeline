with source as (
    select * from {{ source('raw', 'products') }}
)

select
    product_id,
    product_name,
    category,
    brand,
    unit_price
from source