insert into stg.orders (
  order_id, customer_id, order_status,
  order_purchase_ts, order_delivered_customer_ts, order_estimated_delivery_ts
)
select
  order_id,
  customer_id,
  coalesce(nullif(order_status,''), 'unknown'),
  nullif(order_purchase_timestamp,'')::timestamptz,
  nullif(order_delivered_customer_date,'')::timestamptz,
  nullif(order_estimated_delivery_date,'')::timestamptz
from raw.orders
where order_id is not null
on conflict (order_id) do update set
  customer_id = excluded.customer_id,
  order_status = excluded.order_status,
  order_purchase_ts = excluded.order_purchase_ts,
  order_delivered_customer_ts = excluded.order_delivered_customer_ts,
  order_estimated_delivery_ts = excluded.order_estimated_delivery_ts,
  updated_at = now();

insert into stg.order_items (order_id, order_item_id, product_id, seller_id, price, freight_value)
select
  order_id,
  nullif(order_item_id,'')::int,
  product_id,
  seller_id,
  nullif(price,'')::numeric,
  nullif(freight_value,'')::numeric
from raw.order_items
where order_id is not null
on conflict (order_id, order_item_id) do update set
  product_id = excluded.product_id,
  seller_id = excluded.seller_id,
  price = excluded.price,
  freight_value = excluded.freight_value,
  updated_at = now();

insert into stg.products (product_id, product_category_name)
select product_id, product_category_name
from raw.products
where product_id is not null
on conflict (product_id) do update set
  product_category_name = excluded.product_category_name,
  updated_at = now();

insert into stg.customers (customer_id, customer_city, customer_state)
select customer_id, customer_city, customer_state
from raw.customers
where customer_id is not null
on conflict (customer_id) do update set
  customer_city = excluded.customer_city,
  customer_state = excluded.customer_state,
  updated_at = now();

insert into stg.sellers (seller_id, seller_city, seller_state)
select seller_id, seller_city, seller_state
from raw.sellers
where seller_id is not null
on conflict (seller_id) do update set
  seller_city = excluded.seller_city,
  seller_state = excluded.seller_state,
  updated_at = now();

insert into stg.order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
select
  order_id,
  nullif(payment_sequential,'')::int,
  payment_type,
  nullif(payment_installments,'')::int,
  nullif(payment_value,'')::numeric
from raw.order_payments
where order_id is not null
on conflict (order_id, payment_sequential) do update set
  payment_type = excluded.payment_type,
  payment_installments = excluded.payment_installments,
  payment_value = excluded.payment_value,
  updated_at = now();

insert into stg.order_reviews (review_id, order_id, review_score, review_creation_date)
select
  review_id,
  order_id,
  nullif(review_score,'')::int,
  nullif(review_creation_date,'')::date
from raw.order_reviews
where review_id is not null
on conflict (review_id) do update set
  order_id = excluded.order_id,
  review_score = excluded.review_score,
  review_creation_date = excluded.review_creation_date,
  updated_at = now();

insert into stg.category_translation (product_category_name, product_category_name_english)
select product_category_name, product_category_name_english
from raw.category_translation
where product_category_name is not null
on conflict (product_category_name) do update set
  product_category_name_english = excluded.product_category_name_english,
  updated_at = now();