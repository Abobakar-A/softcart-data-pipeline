with source as (
    select * from softcart_db.raw.products
)

select
    product_id,
    product_name,
    category,
    brand,
    unit_price
from source