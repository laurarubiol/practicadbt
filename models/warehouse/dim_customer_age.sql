with source as (

    select * from {{ ref('stg_customer_age') }}

)

select 
    age as customer_age,
    case 
        when customer_age < 18 then '<18'
        when customer_age between 18 and 30 then '>=18 y <30'
        when customer_age between 30 and 50 then '>=30 y <50'
        when customer_age between 50 and 65 then '>=50 y <65'
        when customer_age >= 65 then '>=65'
        else 'Unknown'
    end as age_range
from source