with source as (
    select * from {{ source('practica', 'articles')}}
)

select 
    *
from source