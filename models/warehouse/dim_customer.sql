with source_customers as (
    select * from {{ ref('stg_customers') }}
),

source_zips as (
    select * from {{ ref('dim_zip_grouped') }}
),

customer as (
    select 
        customer_id,
        case 
            when club_member_status is null then '2'
            when club_member_status = 'ACTIVE' then '1'
            when club_member_status = '2' then '2'
            when club_member_status = 'PRE-CREATE' then '3'
            when club_member_status = 'LEFT CLUB' then '4'
            else '-1'
        end as club_member_status_id,
        ifnull(age, -1) as customer_age, 
        postal_code
    from source_customers
),

zips as (
    select 
        ifnull(zip, -1) as zip_code,
        postal_code 
    from source_zips
),

final as (

select customer_id, club_member_status_id, customer_age, zip_code

from customer left join zips on (zips.postal_code = customer.postal_code)
),

unique_source as (
    select *,
        row_number() over (partition by customer_id, club_member_status_id, customer_age, zip_code) as row_number
    from final
)

select *
    except (row_number),
from unique_source 
where row_number = 1