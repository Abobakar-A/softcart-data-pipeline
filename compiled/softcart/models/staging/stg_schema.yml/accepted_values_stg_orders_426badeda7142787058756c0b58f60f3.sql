
    
    

with all_values as (

    select
        shipment_status as value_field,
        count(*) as n_records

    from softcart_db.staging.stg_orders
    group by shipment_status

)

select *
from all_values
where value_field not in (
    'delivered','shipped','processing','cancelled'
)


