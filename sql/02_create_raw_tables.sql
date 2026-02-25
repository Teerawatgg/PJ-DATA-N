-- 02_create_raw_tables.sql
-- RAW tables mirror CSV structure (as close as practical)

-- 1) customers
CREATE TABLE IF NOT EXISTS raw.customers (
  customer_id TEXT,
  customer_unique_id TEXT,
  customer_zip_code_prefix TEXT,
  customer_city TEXT,
  customer_state TEXT
);

-- 2) geolocation (ใหญ่มาก ไม่มี PK ชัดเจน)
CREATE TABLE IF NOT EXISTS raw.geolocation (
  geolocation_zip_code_prefix TEXT,
  geolocation_lat NUMERIC,
  geolocation_lng NUMERIC,
  geolocation_city TEXT,
  geolocation_state TEXT
);

-- 3) orders
CREATE TABLE IF NOT EXISTS raw.orders (
  order_id TEXT,
  customer_id TEXT,
  order_status TEXT,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

-- 4) order_items
CREATE TABLE IF NOT EXISTS raw.order_items (
  order_id TEXT,
  order_item_id INT,
  product_id TEXT,
  seller_id TEXT,
  shipping_limit_date TIMESTAMP,
  price NUMERIC,
  freight_value NUMERIC
);

-- 5) order_payments
CREATE TABLE IF NOT EXISTS raw.order_payments (
  order_id TEXT,
  payment_sequential INT,
  payment_type TEXT,
  payment_installments INT,
  payment_value NUMERIC
);

-- 6) order_reviews
CREATE TABLE IF NOT EXISTS raw.order_reviews (
  review_id TEXT,
  order_id TEXT,
  review_score INT,
  review_comment_title TEXT,
  review_comment_message TEXT,
  review_creation_date TIMESTAMP,
  review_answer_timestamp TIMESTAMP
);

-- 7) products
CREATE TABLE IF NOT EXISTS raw.products (
  product_id TEXT,
  product_category_name TEXT,
  product_name_lenght INT,
  product_description_lenght INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

-- 8) sellers
CREATE TABLE IF NOT EXISTS raw.sellers (
  seller_id TEXT,
  seller_zip_code_prefix TEXT,
  seller_city TEXT,
  seller_state TEXT
);

-- 9) product_category_translation
CREATE TABLE IF NOT EXISTS raw.category_translation (
  product_category_name TEXT,
  product_category_name_english TEXT
);

-- Helpful indexes (optional but recommended)
CREATE INDEX IF NOT EXISTS idx_raw_orders_order_id ON raw.orders(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer_id ON raw.orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_raw_items_order_id ON raw.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_items_product_id ON raw.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_raw_items_seller_id ON raw.order_items(seller_id);

CREATE INDEX IF NOT EXISTS idx_raw_payments_order_id ON raw.order_payments(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_reviews_order_id ON raw.order_reviews(order_id);

CREATE INDEX IF NOT EXISTS idx_raw_customers_customer_id ON raw.customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_products_product_id ON raw.products(product_id);
CREATE INDEX IF NOT EXISTS idx_raw_sellers_seller_id ON raw.sellers(seller_id);