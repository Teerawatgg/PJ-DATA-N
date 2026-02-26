-- ---------------------------------------------------------
-- 02) STG TABLES (RDBMS จริง / PK-FK)
--     * ใส่ constraint ที่ stg เพื่อทำ Advanced SQL ให้สะอาด
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.customers (
  customer_id TEXT PRIMARY KEY,
  customer_unique_id TEXT,
  customer_zip_code_prefix TEXT,
  customer_city TEXT,
  customer_state TEXT
);

CREATE TABLE IF NOT EXISTS stg.sellers (
  seller_id TEXT PRIMARY KEY,
  seller_zip_code_prefix TEXT,
  seller_city TEXT,
  seller_state TEXT
);

CREATE TABLE IF NOT EXISTS stg.products (
  product_id TEXT PRIMARY KEY,
  product_category_name TEXT,
  product_name_lenght INT,
  product_description_lenght INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

CREATE TABLE IF NOT EXISTS stg.category_translation (
  product_category_name TEXT PRIMARY KEY,
  product_category_name_english TEXT
);

CREATE TABLE IF NOT EXISTS stg.orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  order_status TEXT,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP,
  CONSTRAINT fk_stg_orders_customer
    FOREIGN KEY (customer_id) REFERENCES stg.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS stg.order_items (
  order_id TEXT NOT NULL,
  order_item_id INT NOT NULL,
  product_id TEXT NOT NULL,
  seller_id TEXT NOT NULL,
  shipping_limit_date TIMESTAMP,
  price NUMERIC,
  freight_value NUMERIC,
  CONSTRAINT pk_stg_order_items PRIMARY KEY (order_id, order_item_id),
  CONSTRAINT fk_stg_items_order
    FOREIGN KEY (order_id) REFERENCES stg.orders(order_id) ON DELETE CASCADE,
  CONSTRAINT fk_stg_items_product
    FOREIGN KEY (product_id) REFERENCES stg.products(product_id),
  CONSTRAINT fk_stg_items_seller
    FOREIGN KEY (seller_id) REFERENCES stg.sellers(seller_id)
);

CREATE TABLE IF NOT EXISTS stg.order_payments (
  order_id TEXT NOT NULL,
  payment_sequential INT NOT NULL,
  payment_type TEXT,
  payment_installments INT,
  payment_value NUMERIC,
  CONSTRAINT pk_stg_payments PRIMARY KEY (order_id, payment_sequential),
  CONSTRAINT fk_stg_payments_order
    FOREIGN KEY (order_id) REFERENCES stg.orders(order_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stg.order_reviews (
  review_id TEXT PRIMARY KEY,
  order_id TEXT NOT NULL,
  review_score INT,
  review_comment_title TEXT,
  review_comment_message TEXT,
  review_creation_date TIMESTAMP,
  review_answer_timestamp TIMESTAMP,
  CONSTRAINT fk_stg_reviews_order
    FOREIGN KEY (order_id) REFERENCES stg.orders(order_id) ON DELETE CASCADE
);

-- Index ช่วยเรื่อง join (ทำ dashboard เร็วขึ้น)
CREATE INDEX IF NOT EXISTS idx_stg_orders_customer_id ON stg.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_items_product_id   ON stg.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_items_seller_id    ON stg.order_items(seller_id);
CREATE INDEX IF NOT EXISTS idx_stg_payments_order_id  ON stg.order_payments(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_reviews_order_id   ON stg.order_reviews(order_id);