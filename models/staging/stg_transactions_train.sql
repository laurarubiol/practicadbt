with source as (
    select * from {{ source('practica', 'transactions')}}
)

select 
    *
from source