with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    city,
    country
from source