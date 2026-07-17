with source as (
    select * from {{ source('raw', 'returns') }}
)
select
    return_id,
    order_id,
    return_date,
    reason,
    status,
    refund_amount
from source
qualify row_number() over (partition by return_id order by return_date desc) = 1
