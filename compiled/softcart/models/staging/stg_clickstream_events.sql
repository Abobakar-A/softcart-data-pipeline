select
    raw_event:event_id::string           as event_id,
    raw_event:session_id::string         as session_id,
    raw_event:customer_id::int           as customer_id,
    raw_event:event_type::string         as event_type,
    raw_event:product_id::int            as product_id,
    raw_event:event_timestamp::timestamp as event_timestamp
from softcart_db.raw.clickstream_events