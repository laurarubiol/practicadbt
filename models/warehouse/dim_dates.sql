with source as (
    select * from {{ref('fct_sales')}}
),

fechas as (
select 
    t_dat,
    month,
    year, 
    FORMAT_DATETIME("%B %Y", t_dat) as year_month,
    cast(FORMAT_DATETIME('%Y%m',t_dat) as INT64) as year_month_num
from source
),

unique_source as (
    select *,
        row_number() over (partition by t_dat, month, year, year_month, year_month_num) as row_number
    from fechas
)

select *
    except (row_number),
from unique_source 
where row_number = 1