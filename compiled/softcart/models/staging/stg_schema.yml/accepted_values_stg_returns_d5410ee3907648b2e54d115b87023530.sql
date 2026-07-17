
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from softcart_db.staging.stg_returns
    group by status

)

select *
from all_values
where value_field not in (
    'requested','approved','rejected','refunded'
)


