with source as (
    select * from {{ source('practica', 'uszips2')}}
)

select 
    *
from source