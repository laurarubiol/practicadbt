with source_sales as (
    select * from {{ref('fct_sales')}}
),

source_articles as (
    select * from {{ref('dim_articles')}}
),

source_customer_age as (
    select * from {{ref('dim_customer_age')}}
),

source_zips as (
    select * from {{ref('dim_zip_grouped')}}
),

source_customer as (
    select * from {{ref('dim_customer')}}
)

select 
    source_sales.sales_channel_id as sales_channel_id,
    source_sales.year as year,
    source_sales.month as month,
    source_zips.zip as zip,
    source_customer_age.age_range as age_range,
    source_articles.department_no as department_no,
    source_articles.index_code as index_code,
    sum(source_sales.price) as sum_price
from source_sales
left join source_articles
on source_sales.article_id = source_articles.article_id
left join source_customer
on source_customer.customer_id = source_sales.customer_id
left join source_customer_age 
on source_customer_age.customer_age = source_customer.customer_age
left join source_zips 
on source_zips.zip = source_customer.zip_code
group by 
    source_sales.sales_channel_id,
    source_sales.year,
    source_sales.month,
    source_zips.zip,
    source_customer_age.age_range,
    source_articles.department_no,
    source_articles.index_code