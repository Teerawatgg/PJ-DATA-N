-- ---------------------------------------------------------
-- 05) LOAD DM DIMS
-- ---------------------------------------------------------

-- 5.1 dim_date จากช่วงวันที่ใน orders
WITH bounds AS (
  SELECT
    MIN(DATE(order_purchase_timestamp)) AS min_d,
    MAX(DATE(order_purchase_timestamp)) AS max_d
  FROM stg.orders
  WHERE order_purchase_timestamp IS NOT NULL
),
ds AS (
  SELECT generate_series((SELECT min_d FROM bounds),
                         (SELECT max_d FROM bounds),
                         INTERVAL '1 day')::date AS d
)
INSERT INTO dm.dim_date (date_id, full_date, year, month, day, quarter, month_name, day_name, is_weekend)
SELECT
  (EXTRACT(YEAR FROM d)::int * 10000 + EXTRACT(MONTH FROM d)::int * 100 + EXTRACT(DAY FROM d)::int) AS date_id,
  d AS full_date,
  EXTRACT(YEAR FROM d)::int,
  EXTRACT(MONTH FROM d)::int,
  EXTRACT(DAY FROM d)::int,
  EXTRACT(QUARTER FROM d)::int,
  TO_CHAR(d, 'Mon'),
  TO_CHAR(d, 'Dy'),
  (EXTRACT(ISODOW FROM d)::int IN (6,7)) AS is_weekend
FROM ds
ON CONFLICT (date_id) DO NOTHING;

-- 5.2 dim_customer
INSERT INTO dm.dim_customer (customer_id, customer_unique_id, customer_city, customer_state)
SELECT customer_id, customer_unique_id, customer_city, customer_state
FROM stg.customers
ON CONFLICT (customer_id) DO NOTHING;

-- 5.3 dim_seller
INSERT INTO dm.dim_seller (seller_id, seller_city, seller_state)
SELECT seller_id, seller_city, seller_state
FROM stg.sellers
ON CONFLICT (seller_id) DO NOTHING;

-- 5.4 dim_product (join translation)
INSERT INTO dm.dim_product (product_id, category_name, category_english)
SELECT
  p.product_id,
  p.product_category_name,
  t.product_category_name_english
FROM stg.products p
LEFT JOIN stg.category_translation t
  ON t.product_category_name = p.product_category_name
ON CONFLICT (product_id) DO NOTHING;

-- 5.5 dim_order_status
INSERT INTO dm.dim_order_status (order_status)
SELECT DISTINCT order_status
FROM stg.orders
WHERE order_status IS NOT NULL
ON CONFLICT (order_status) DO NOTHING;

-- 5.6 dim_payment_type
INSERT INTO dm.dim_payment_type (payment_type)
SELECT DISTINCT payment_type
FROM stg.order_payments
WHERE payment_type IS NOT NULL
ON CONFLICT (payment_type) DO NOTHING;