

with customer_first_order as (
    select
        customer_id,
        date_trunc('month', min(order_date)) as cohort_month
    from softcart_db.marts.fct_orders
    group by customer_id
),

customer_orders_with_cohort as (
    select
        fo.customer_id,
        fo.order_id,
        fo.order_date,
        fco.cohort_month,
        datediff('month', fco.cohort_month, date_trunc('month', fo.order_date)) as month_number
    from softcart_db.marts.fct_orders fo
    left join customer_first_order fco
        on fo.customer_id = fco.customer_id
)

select
    cohort_month,
    month_number,
    count(distinct customer_id) as active_customers,
    count(distinct order_id) as total_orders
from customer_orders_with_cohort
group by cohort_month, month_number
order by cohort_month, month_number