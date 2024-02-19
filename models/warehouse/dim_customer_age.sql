with source as (

    select * from {{ ref('stg_customers') }}

),

age as (
select 
    age as customer_age,
    case 
    when age < 18 then '<18'
    when (age >= 18 and age < 30) then '>= 18 y < 30'
    when (age >= 30 and age < 50) then '>= 30 y < 50'
    when (age >= 50 and age < 65) then '>= 50 y < 65'
    when age >= 65 then '>= 65'
    else 'Unknown'
    end as age_range
from source
),

unique_source as (
    select *,
        row_number() over (partition by customer_age) as row_number
    from age
)

select *
    except (row_number),
from unique_source 
where row_number = 1