{% snapshot dim_products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_name', 'category', 'brand', 'unit_price']
    )
}}

select
    product_id,
    product_name,
    category,
    brand,
    unit_price
from {{ ref('stg_products') }}

{% endsnapshot %}
