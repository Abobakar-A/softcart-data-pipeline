

with product_sales as (
    select
        product_id,
        sum(line_total) as total_sales,
        count(distinct order_id) as total_orders_with_product
    from softcart_db.marts.fct_orders
    group by product_id
),

product_returns as (
    select
        oi.product_id,
        count(distinct r.return_id) as total_returns,
        sum(r.refund_amount) as total_refunded
    from softcart_db.staging.stg_returns r
    left join softcart_db.staging.stg_order_items oi
        on r.order_id = oi.order_id
    group by oi.product_id
)

select
    s.product_id,
    p.product_name,
    s.total_sales,
    s.total_orders_with_product,
    coalesce(r.total_returns, 0) as total_returns,
    coalesce(r.total_refunded, 0) as total_refunded,
    s.total_sales - coalesce(r.total_refunded, 0) as net_revenue,
    round(
        coalesce(r.total_returns, 0) / nullif(s.total_orders_with_product, 0) * 100,
        2
    ) as return_rate_pct
from product_sales s
left join product_returns r
    on s.product_id = r.product_id
left join softcart_db.staging.stg_products p
    on s.product_id = p.product_id
order by return_rate_pct desc