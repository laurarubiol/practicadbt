with source as (
    select * from {{ ref('dim_zip_grouped') }}
),

zip as

(SELECT DISTINCT
    lat,
    lng,
    city,
    state_id,
    state_name,
    zip
from source)

select * from zip