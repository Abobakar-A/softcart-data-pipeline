with source as (
    select * from softcart_db.raw.customers
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