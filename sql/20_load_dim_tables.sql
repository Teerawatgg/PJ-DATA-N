-- 20_load_dim_tables.sql

-- Customers
TRUNCATE dm.dim_customer;
INSERT INTO dm.dim_customer
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM raw.customers;

-- Sellers
TRUNCATE dm.dim_seller;
INSERT INTO dm.dim_seller
SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM raw.sellers;

-- Products + translation
TRUNCATE dm.dim_product;
INSERT INTO dm.dim_product
SELECT
  p.product_id,
  p.product_category_name,
  t.product_category_name_english,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM raw.products p
LEFT JOIN raw.category_translation t
  ON p.product_category_name = t.product_category_name;

-- Date dimension from order purchase dates (distinct)
TRUNCATE dm.dim_date;
INSERT INTO dm.dim_date (date_key, year, month, day, dow)
SELECT
  d::date AS date_key,
  EXTRACT(YEAR FROM d)::int AS year,
  EXTRACT(MONTH FROM d)::int AS month,
  EXTRACT(DAY FROM d)::int AS day,
  EXTRACT(DOW FROM d)::int AS dow
FROM (
  SELECT DISTINCT order_purchase_timestamp::date AS d
  FROM raw.orders
  WHERE order_purchase_timestamp IS NOT NULL
) x
ORDER BY 1;