with source as (

    select * from {{ ref('stg_articles') }}

),

section as ( 
select 
    section_no,
    section_name,
    department_no,
    department_name
from source
),

unique_source as (
    select *,
        row_number() over (partition by section_no, section_name, department_no, department_name) as row_number
    from section
)

select *
    except (row_number),
from unique_source 
where row_number = 1


