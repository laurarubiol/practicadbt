with source as (
    select * from {{ source('practica', 'customers')}}
)

select 
    *
from source