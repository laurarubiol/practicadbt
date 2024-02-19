with source_transactions_train as (
    select * from {{ref('stg_transactions_train')}}
),

source_articles as (
    select * from {{ref('dim_articles')}}
),

source_customer as (
    select * from {{ref('dim_customer')}}
)

select 
    source_transactions_train.t_dat,
    extract(month from source_transactions_train.t_dat) as month, 
    extract(year from source_transactions_train.t_dat) as year,
    ifnull(source_customer.customer_id, '-1') as customer_id,
    ifnull(source_articles.article_id, -1) as article_id,
    source_transactions_train.sales_channel_id,
    source_transactions_train.price
from source_transactions_train
left join source_articles
on source_transactions_train.article_id = source_articles.article_id
left join source_customer
on source_customer.customer_id = source_transactions_train.customer_id