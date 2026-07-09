select
    product_id,
    product_name,
    category,
    brand,
    unit_price
from {{ ref('stg_products') }}