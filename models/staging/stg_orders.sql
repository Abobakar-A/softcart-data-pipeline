with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    order_date,
    payment_method,
    payment_status,
    shipment_status,
    order_total
from source