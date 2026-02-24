create table if not exists stg.orders (
  order_id text primary key,
  customer_id text,
  order_status text,
  order_purchase_ts timestamptz,
  order_delivered_customer_ts timestamptz,
  order_estimated_delivery_ts timestamptz,
  updated_at timestamptz default now()
);

create table if not exists stg.order_items (
  order_id text,
  order_item_id int,
  product_id text,
  seller_id text,
  price numeric(12,2),
  freight_value numeric(12,2),
  updated_at timestamptz default now(),
  primary key (order_id, order_item_id)
);

create table if not exists stg.products (
  product_id text primary key,
  product_category_name text,
  updated_at timestamptz default now()
);

create table if not exists stg.customers (
  customer_id text primary key,
  customer_city text,
  customer_state text,
  updated_at timestamptz default now()
);

create table if not exists stg.sellers (
  seller_id text primary key,
  seller_city text,
  seller_state text,
  updated_at timestamptz default now()
);

create table if not exists stg.order_payments (
  order_id text,
  payment_sequential int,
  payment_type text,
  payment_installments int,
  payment_value numeric(12,2),
  updated_at timestamptz default now(),
  primary key (order_id, payment_sequential)
);

create table if not exists stg.order_reviews (
  review_id text primary key,
  order_id text,
  review_score int,
  review_creation_date date,
  updated_at timestamptz default now()
);

create table if not exists stg.category_translation (
  product_category_name text primary key,
  product_category_name_english text,
  updated_at timestamptz default now()
);