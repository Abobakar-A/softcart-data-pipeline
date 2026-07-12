with source as (
    select * from softcart_db.raw.order_items
)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total
from source