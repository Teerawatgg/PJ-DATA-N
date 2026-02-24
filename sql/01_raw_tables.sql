create table if not exists raw.orders (
  order_id text,
  customer_id text,
  order_status text,
  order_purchase_timestamp text,
  order_approved_at text,
  order_delivered_carrier_date text,
  order_delivered_customer_date text,
  order_estimated_delivery_date text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);

create table if not exists raw.order_items (
  order_id text,
  order_item_id text,
  product_id text,
  seller_id text,
  shipping_limit_date text,
  price text,
  freight_value text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);

create table if not exists raw.products (
  product_id text,
  product_category_name text,
  product_weight_g text,
  product_length_cm text,
  product_height_cm text,
  product_width_cm text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);

create table if not exists raw.customers (
  customer_id text,
  customer_unique_id text,
  customer_zip_code_prefix text,
  customer_city text,
  customer_state text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);

create table if not exists raw.sellers (
  seller_id text,
  seller_zip_code_prefix text,
  seller_city text,
  seller_state text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);

create table if not exists raw.order_payments (
  order_id text,
  payment_sequential text,
  payment_type text,
  payment_installments text,
  payment_value text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);

create table if not exists raw.order_reviews (
  review_id text,
  order_id text,
  review_score text,
  review_creation_date text,
  review_answer_timestamp text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);

create table if not exists raw.category_translation (
  product_category_name text,
  product_category_name_english text,
  ingested_at timestamptz default now(),
  source_file text,
  batch_id text
);