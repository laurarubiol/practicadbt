with source as (

    select * from {{ ref('stg_club_member_status') }}

)

select 
    customer_id as club_member_status_id,
    club_member_status
from source