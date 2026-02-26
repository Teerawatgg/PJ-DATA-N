-- ---------------------------------------------------------
-- 04) DM TABLES (STAR SCHEMA)
--     * ออกแบบเพื่อ dashboard: join น้อย + query เร็ว
--     * Grain ของ fact = 1 row ต่อ 1 order_item
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dm.dim_date (
  date_id INT PRIMARY KEY,        -- yyyymmdd
  full_date DATE UNIQUE NOT NULL,
  year INT,
  month INT,
  day INT,
  quarter INT,
  month_name TEXT,
  day_name TEXT,
  is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS dm.dim_customer (
  customer_sk BIGSERIAL PRIMARY KEY,
  customer_id TEXT UNIQUE NOT NULL,
  customer_unique_id TEXT,
  customer_city TEXT,
  customer_state TEXT
);

CREATE TABLE IF NOT EXISTS dm.dim_seller (
  seller_sk BIGSERIAL PRIMARY KEY,
  seller_id TEXT UNIQUE NOT NULL,
  seller_city TEXT,
  seller_state TEXT
);

CREATE TABLE IF NOT EXISTS dm.dim_product (
  product_sk BIGSERIAL PRIMARY KEY,
  product_id TEXT UNIQUE NOT NULL,
  category_name TEXT,
  category_english TEXT
);

CREATE TABLE IF NOT EXISTS dm.dim_order_status (
  order_status_sk BIGSERIAL PRIMARY KEY,
  order_status TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dm.dim_payment_type (
  payment_type_sk BIGSERIAL PRIMARY KEY,
  payment_type TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dm.fact_sales (
  order_id TEXT NOT NULL,
  order_item_id INT NOT NULL,

  order_date_id INT NOT NULL,
  customer_sk BIGINT NOT NULL,
  seller_sk BIGINT NOT NULL,
  product_sk BIGINT NOT NULL,
  order_status_sk BIGINT,
  payment_type_sk BIGINT,

  item_price NUMERIC,
  freight_value NUMERIC,
  payment_value NUMERIC,
  payment_installments INT,
  review_score INT,

  order_purchase_ts TIMESTAMP,
  delivered_customer_ts TIMESTAMP,
  estimated_delivery_ts TIMESTAMP,

  CONSTRAINT pk_fact_sales PRIMARY KEY (order_id, order_item_id),
  CONSTRAINT fk_fact_date FOREIGN KEY (order_date_id) REFERENCES dm.dim_date(date_id),
  CONSTRAINT fk_fact_customer FOREIGN KEY (customer_sk) REFERENCES dm.dim_customer(customer_sk),
  CONSTRAINT fk_fact_seller FOREIGN KEY (seller_sk) REFERENCES dm.dim_seller(seller_sk),
  CONSTRAINT fk_fact_product FOREIGN KEY (product_sk) REFERENCES dm.dim_product(product_sk),
  CONSTRAINT fk_fact_status FOREIGN KEY (order_status_sk) REFERENCES dm.dim_order_status(order_status_sk),
  CONSTRAINT fk_fact_payment FOREIGN KEY (payment_type_sk) REFERENCES dm.dim_payment_type(payment_type_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON dm.fact_sales(order_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON dm.fact_sales(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_seller   ON dm.fact_sales(seller_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON dm.fact_sales(product_sk);