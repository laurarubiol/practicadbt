with source as (

    select * from {{ ref('stg_articles') }}

),

product as (
select 
    product_type_no,
    product_type_name
from source
),

unique_source as (
    select *,
        row_number() over (partition by product_type_no, product_type_name) as row_number
    from product
)

select *
    except (row_number),
from unique_source 
where row_number = 1