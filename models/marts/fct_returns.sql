{{
    config(
        materialized='table'
    )
}}

select
    r.return_id,
    r.order_id,
    o.customer_id,
    oi.product_id,
    r.return_date,
    r.reason,
    r.status,
    r.refund_amount
from {{ ref('stg_returns') }} r
left join {{ ref('stg_orders') }} o
    on r.order_id = o.order_id
left join {{ ref('stg_order_items') }} oi
    on r.order_id = oi.order_id
qualify row_number() over (partition by r.return_id order by oi.product_id) = 1