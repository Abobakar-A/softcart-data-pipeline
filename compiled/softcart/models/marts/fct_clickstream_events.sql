

select
    event_id,
    session_id,
    customer_id,
    event_type,
    product_id,
    event_timestamp
from softcart_db.staging.stg_clickstream_events


where event_timestamp > (select max(event_timestamp) from softcart_db.marts.fct_clickstream_events)
