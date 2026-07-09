select
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    city,
    country
from {{ ref('stg_customers') }}