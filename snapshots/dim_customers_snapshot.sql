{% snapshot dim_customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'city', 'country']
    )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    city,
    country
from {{ ref('stg_customers') }}

{% endsnapshot %}
