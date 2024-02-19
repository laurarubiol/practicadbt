with source as (

    select * from {{ ref('stg_articles') }}

),

indexes as ( 
select 
    index_group_no,
    index_group_name,
    index_code,
    index_name
from source
),

unique_source as (
    select *,
        row_number() over (partition by index_group_no, index_group_name, index_code, index_name) as row_number
    from indexes
)

select *
    except (row_number),
from unique_source 
where row_number = 1





