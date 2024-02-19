with source as (

    select * from {{ ref('stg_articles') }}

),

article as (
select 
    article_id,
    detail_desc,
    ifnull(product_code,-1) as product_code,
    ifnull(index_code, '-1') as index_code,
    ifnull(department_no, -1) as department_no,
    ifnull(product_type_no, -1) as product_type_no
from source
),

unique_source as (
    select *,
        row_number() over (partition by article_id, detail_desc, product_code, index_code, department_no, product_type_no) as row_number
    from article
)

select *
    except (row_number),
from unique_source 
where row_number = 1

