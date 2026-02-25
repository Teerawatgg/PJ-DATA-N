-- 10_create_dm_tables.sql
CREATE SCHEMA IF NOT EXISTS dm;

-- Dimensions
CREATE TABLE IF NOT EXISTS dm.dim_customer (
  customer_id TEXT PRIMARY KEY,
  customer_unique_id TEXT,
  customer_zip_code_prefix TEXT,
  customer_city TEXT,
  customer_state TEXT
);

CREATE TABLE IF NOT EXISTS dm.dim_seller (
  seller_id TEXT PRIMARY KEY,
  seller_zip_code_prefix TEXT,
  seller_city TEXT,
  seller_state TEXT
);

CREATE TABLE IF NOT EXISTS dm.dim_product (
  product_id TEXT PRIMARY KEY,
  product_category_name TEXT,
  product_category_name_english TEXT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

-- Date dimension (created as table you can repopulate)
CREATE TABLE IF NOT EXISTS dm.dim_date (
  date_key DATE PRIMARY KEY,
  year INT,
  month INT,
  day INT,
  dow INT
);

-- Facts
-- Fact at order-item level = best for GMV/top products/shipping
CREATE TABLE IF NOT EXISTS dm.fact_order_items (
  order_id TEXT,
  order_item_id INT,
  customer_id TEXT,
  seller_id TEXT,
  product_id TEXT,
  order_status TEXT,
  purchase_ts TIMESTAMP,
  approved_ts TIMESTAMP,
  delivered_carrier_ts TIMESTAMP,
  delivered_customer_ts TIMESTAMP,
  estimated_delivery_ts TIMESTAMP,
  shipping_limit_ts TIMESTAMP,
  price NUMERIC,
  freight_value NUMERIC,
  payment_value NUMERIC,
  review_score INT,
  delivery_days INT,
  is_late_delivery BOOLEAN,
  PRIMARY KEY (order_id, order_item_id)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_dm_fact_purchase_ts ON dm.fact_order_items(purchase_ts);
CREATE INDEX IF NOT EXISTS idx_dm_fact_product_id ON dm.fact_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_dm_fact_customer_id ON dm.fact_order_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_dm_fact_seller_id ON dm.fact_order_items(seller_id);