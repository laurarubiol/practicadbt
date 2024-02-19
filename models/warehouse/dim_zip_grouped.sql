with source_zips as (

    select * from {{ ref('stg_uszips2') }}

),

source_customers as (

    select * from {{ ref('stg_customers') }}

),

zips as

(SELECT

    lat,
    lng,
    city,
    state_id,
    state_name,
    zip,
    ntile(33788) over (order by zip) ranking_zip

FROM source_zips),

hashes as

(select postal_code,

ntile(33788) over (order by postal_code) ranking_hash from (

SELECT distinct postal_code FROM source_customers )

)

select zip, postal_code, lat, lng, city, state_id, state_name

from zips join hashes on (zips.ranking_zip = hashes.ranking_hash)