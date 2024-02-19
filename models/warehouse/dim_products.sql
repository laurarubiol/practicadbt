with source as (

    select * from {{ ref('stg_articles') }}

),

product as (
select 
    product_code,
    prod_name --(no me sale que haya IDs repetidos)
from source
),

unique_source as (
    select *,
        row_number() over (partition by product_code, prod_name) as row_number
    from product
)

select *
    except (row_number),
from unique_source 
where row_number = 1



