

with customer_orders as (
    select
        customer_id,
        max(order_date) as last_order_date,
        count(distinct order_id) as frequency,
        sum(order_total) as monetary
    from softcart_db.marts.fct_orders
    group by customer_id
),

rfm_scores as (
    select
        customer_id,
        last_order_date,
        frequency,
        monetary,
        datediff('day', last_order_date, current_date()) as recency_days,

        -- Score 1-5 for each dimension using quintiles (5 = best)
        ntile(5) over (order by datediff('day', last_order_date, current_date()) desc) as recency_score,
        ntile(5) over (order by frequency asc) as frequency_score,
        ntile(5) over (order by monetary asc) as monetary_score
    from customer_orders
)

select
    customer_id,
    last_order_date,
    recency_days,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    (recency_score + frequency_score + monetary_score) as rfm_total_score,
    case
        when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
        when recency_score >= 3 and frequency_score >= 3 then 'Loyal Customers'
        when recency_score >= 4 and frequency_score <= 2 then 'New Customers'
        when recency_score <= 2 and frequency_score >= 3 then 'At Risk'
        when recency_score <= 2 and frequency_score <= 2 and monetary_score <= 2 then 'Lost'
        else 'Needs Attention'
    end as customer_segment
from rfm_scores
order by rfm_total_score desc