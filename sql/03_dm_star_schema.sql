create table if not exists dm.dim_date (
  date_key int primary key,       -- yyyymmdd
  date_value date,
  year int, month int, day int
);

create table if not exists dm.dim_customer (
  customer_id text primary key,
  customer_city text,
  customer_state text
);

create table if not exists dm.dim_product (
  product_id text primary key,
  category_pt text,
  category_en text
);

create table if not exists dm.dim_seller (
  seller_id text primary key,
  seller_city text,
  seller_state text
);

create table if not exists dm.fact_order_items (
  order_id text,
  order_item_id int,
  product_id text,
  seller_id text,
  price numeric(12,2),
  freight_value numeric(12,2),
  primary key (order_id, order_item_id)
);

create table if not exists dm.fact_orders (
  order_id text primary key,
  customer_id text,
  order_date_key int,
  order_status text,
  total_items int,
  gmv numeric(14,2),
  freight_total numeric(14,2),
  review_score_avg numeric(5,2),
  delivery_delay_days int,
  delivery_time_days int
);