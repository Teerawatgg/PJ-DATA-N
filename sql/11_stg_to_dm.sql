-- dim_date
insert into dm.dim_date (date_key, date_value, year, month, day)
select distinct
  (extract(year from o.order_purchase_ts)::int * 10000
   + extract(month from o.order_purchase_ts)::int * 100
   + extract(day from o.order_purchase_ts)::int) as date_key,
  o.order_purchase_ts::date,
  extract(year from o.order_purchase_ts)::int,
  extract(month from o.order_purchase_ts)::int,
  extract(day from o.order_purchase_ts)::int
from stg.orders o
where o.order_purchase_ts is not null
on conflict (date_key) do nothing;

-- dim_customer
insert into dm.dim_customer (customer_id, customer_city, customer_state)
select customer_id, customer_city, customer_state
from stg.customers
on conflict (customer_id) do update set
  customer_city = excluded.customer_city,
  customer_state = excluded.customer_state;

-- dim_seller
insert into dm.dim_seller (seller_id, seller_city, seller_state)
select seller_id, seller_city, seller_state
from stg.sellers
on conflict (seller_id) do update set
  seller_city = excluded.seller_city,
  seller_state = excluded.seller_state;

-- dim_product (+ translation)
insert into dm.dim_product (product_id, category_pt, category_en)
select
  p.product_id,
  p.product_category_name,
  ct.product_category_name_english
from stg.products p
left join stg.category_translation ct
  on p.product_category_name = ct.product_category_name
on conflict (product_id) do update set
  category_pt = excluded.category_pt,
  category_en = excluded.category_en;

-- fact_order_items
insert into dm.fact_order_items (order_id, order_item_id, product_id, seller_id, price, freight_value)
select order_id, order_item_id, product_id, seller_id, price, freight_value
from stg.order_items
on conflict (order_id, order_item_id) do update set
  product_id = excluded.product_id,
  seller_id = excluded.seller_id,
  price = excluded.price,
  freight_value = excluded.freight_value;

-- fact_orders (รวมยอด+ส่งของ+รีวิว)
with item_agg as (
  select
    order_id,
    count(*) as total_items,
    sum(price) as gmv,
    sum(freight_value) as freight_total
  from stg.order_items
  group by order_id
),
review_agg as (
  select
    order_id,
    avg(review_score)::numeric(5,2) as review_score_avg
  from stg.order_reviews
  group by order_id
)
insert into dm.fact_orders (
  order_id, customer_id, order_date_key, order_status,
  total_items, gmv, freight_total, review_score_avg,
  delivery_delay_days, delivery_time_days
)
select
  o.order_id,
  o.customer_id,
  (extract(year from o.order_purchase_ts)::int * 10000
   + extract(month from o.order_purchase_ts)::int * 100
   + extract(day from o.order_purchase_ts)::int) as order_date_key,
  o.order_status,
  coalesce(ia.total_items,0),
  coalesce(ia.gmv,0),
  coalesce(ia.freight_total,0),
  ra.review_score_avg,
  case
    when o.order_delivered_customer_ts is not null and o.order_estimated_delivery_ts is not null
      then (o.order_delivered_customer_ts::date - o.order_estimated_delivery_ts::date)
    else null end,
  case
    when o.order_delivered_customer_ts is not null and o.order_purchase_ts is not null
      then (o.order_delivered_customer_ts::date - o.order_purchase_ts::date)
    else null end
from stg.orders o
left join item_agg ia on o.order_id = ia.order_id
left join review_agg ra on o.order_id = ra.order_id
where o.order_purchase_ts is not null
on conflict (order_id) do update set
  customer_id = excluded.customer_id,
  order_date_key = excluded.order_date_key,
  order_status = excluded.order_status,
  total_items = excluded.total_items,
  gmv = excluded.gmv,
  freight_total = excluded.freight_total,
  review_score_avg = excluded.review_score_avg,
  delivery_delay_days = excluded.delivery_delay_days,
  delivery_time_days = excluded.delivery_time_days;