-- This test fails if any return's refund_amount exceeds the original order's order_total.
-- A refund larger than the order itself is a data quality issue that should never happen.

select
    r.return_id,
    r.order_id,
    r.refund_amount,
    o.order_total
from {{ ref('stg_returns') }} r
left join {{ ref('stg_orders') }} o
    on r.order_id = o.order_id
where r.refund_amount > o.order_total
