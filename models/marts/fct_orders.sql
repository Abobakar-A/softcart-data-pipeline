{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='merge'
    )
}}
select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    o.payment_method,
    o.payment_status,
    o.shipment_status,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    o.order_total
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
{% if is_incremental() %}
where o.order_date > (select max(order_date) from {{ this }})
{% endif %}    