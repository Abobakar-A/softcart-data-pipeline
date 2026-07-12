{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

select
    event_id,
    session_id,
    customer_id,
    event_type,
    product_id,
    event_timestamp
from {{ ref('stg_clickstream_events') }}

{% if is_incremental() %}
where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
