-- ---------------------------------------------------------
-- 07) KPI VIEWS (Dashboard Query ที่ Flask เรียก)
-- ---------------------------------------------------------

-- 7.1 Monthly GMV / Orders / Avg Review
CREATE OR REPLACE VIEW dm.v_kpi_monthly AS
SELECT
  d.year,
  d.month,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)) AS gmv,
  AVG(f.review_score)::numeric(10,2) AS avg_review
FROM dm.fact_sales f
JOIN dm.dim_date d ON d.date_id = f.order_date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 7.2 Sales by Customer State
CREATE OR REPLACE VIEW dm.v_kpi_sales_by_state AS
SELECT
  c.customer_state,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)) AS gmv
FROM dm.fact_sales f
JOIN dm.dim_customer c ON c.customer_sk = f.customer_sk
GROUP BY c.customer_state
ORDER BY gmv DESC;

-- 7.3 Top Categories
CREATE OR REPLACE VIEW dm.v_kpi_top_categories AS
SELECT
  COALESCE(p.category_english, p.category_name, 'unknown') AS category,
  COUNT(*) AS items_sold,
  SUM(COALESCE(f.item_price,0)) AS revenue_items
FROM dm.fact_sales f
JOIN dm.dim_product p ON p.product_sk = f.product_sk
GROUP BY 1
ORDER BY revenue_items DESC;

-- 7.4 Delivery SLA: avg days late (negative = early)
CREATE OR REPLACE VIEW dm.v_kpi_sla AS
SELECT
  d.year,
  d.month,
  COUNT(DISTINCT f.order_id) AS delivered_orders,
  AVG(
    CASE
      WHEN f.delivered_customer_ts IS NULL OR f.estimated_delivery_ts IS NULL THEN NULL
      ELSE (f.delivered_customer_ts::date - f.estimated_delivery_ts::date)
    END
  )::numeric(10,2) AS avg_days_late
FROM dm.fact_sales f
JOIN dm.dim_date d ON d.date_id = f.order_date_id
WHERE f.delivered_customer_ts IS NOT NULL
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 7.5 Payment Mix
CREATE OR REPLACE VIEW dm.v_kpi_payment_mix AS
SELECT
  pt.payment_type,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(COALESCE(f.payment_value,0)) AS total_paid
FROM dm.fact_sales f
LEFT JOIN dm.dim_payment_type pt ON pt.payment_type_sk = f.payment_type_sk
GROUP BY pt.payment_type
ORDER BY total_paid DESC;

-- ---------------------------------------------------------
-- 40_create_kpi_views.sql  (FIXED)
-- KPI VIEWS for Dashboard / API (Flask)
-- Base tables:
--   dm.fact_sales
--   dm.dim_date (date_id, year, month)
--   dm.dim_customer (customer_sk, customer_state)
--   dm.dim_product (product_sk, category_english, category_name)
--   dm.dim_payment_type (payment_type_sk, payment_type)
-- ---------------------------------------------------------

-- 1) Monthly KPI: Orders + GMV + Avg Review
CREATE OR REPLACE VIEW dm.v_kpi_monthly AS
SELECT
  d.year,
  d.month,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)) AS gmv,
  AVG(f.review_score)::numeric(10,2) AS avg_review
FROM dm.fact_sales f
JOIN dm.dim_date d ON d.date_id = f.order_date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2) Sales by Customer State
CREATE OR REPLACE VIEW dm.v_kpi_sales_by_state AS
SELECT
  c.customer_state,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)) AS gmv
FROM dm.fact_sales f
JOIN dm.dim_customer c ON c.customer_sk = f.customer_sk
GROUP BY c.customer_state
ORDER BY gmv DESC;

-- 3) Top Categories (by Item Revenue)
CREATE OR REPLACE VIEW dm.v_kpi_top_categories AS
SELECT
  COALESCE(p.category_english, p.category_name, 'unknown') AS category,
  COUNT(*) AS items_sold,
  SUM(COALESCE(f.item_price,0)) AS revenue_items
FROM dm.fact_sales f
JOIN dm.dim_product p ON p.product_sk = f.product_sk
GROUP BY 1
ORDER BY revenue_items DESC;

-- 4) Delivery SLA (avg days late; negative = early)
CREATE OR REPLACE VIEW dm.v_kpi_sla AS
SELECT
  d.year,
  d.month,
  COUNT(DISTINCT f.order_id) AS delivered_orders,
  AVG(
    CASE
      WHEN f.delivered_customer_ts IS NULL OR f.estimated_delivery_ts IS NULL THEN NULL
      ELSE (f.delivered_customer_ts::date - f.estimated_delivery_ts::date)
    END
  )::numeric(10,2) AS avg_days_late
FROM dm.fact_sales f
JOIN dm.dim_date d ON d.date_id = f.order_date_id
WHERE f.delivered_customer_ts IS NOT NULL
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 5) Payment Mix
CREATE OR REPLACE VIEW dm.v_kpi_payment_mix AS
SELECT
  COALESCE(pt.payment_type, 'unknown') AS payment_type,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(COALESCE(f.payment_value,0)) AS total_paid
FROM dm.fact_sales f
LEFT JOIN dm.dim_payment_type pt ON pt.payment_type_sk = f.payment_type_sk
GROUP BY 1
ORDER BY total_paid DESC;

-- 6) Monthly GMV + Orders + AOV (for charts)
CREATE OR REPLACE VIEW dm.v_monthly_gmv AS
SELECT
  make_date(d.year, d.month, 1) AS month,
  SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric(18,2) AS gmv_brl,
  COUNT(DISTINCT f.order_id)::bigint AS orders_cnt,
  (
    SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))
    / NULLIF(COUNT(DISTINCT f.order_id),0)
  )::numeric(18,2) AS aov_brl
FROM dm.fact_sales f
JOIN dm.dim_date d ON d.date_id = f.order_date_id
GROUP BY 1
ORDER BY 1;

-- 7) Review score distribution
CREATE OR REPLACE VIEW dm.v_review_distribution AS
WITH base AS (
  SELECT review_score, COUNT(*) AS cnt
  FROM dm.fact_sales
  WHERE review_score IS NOT NULL
  GROUP BY review_score
),
tot AS (
  SELECT SUM(cnt) AS total_cnt FROM base
)
SELECT
  b.review_score,
  b.cnt::bigint AS cnt,
  (100.0 * b.cnt / NULLIF(t.total_cnt,0))::numeric(10,2) AS pct
FROM base b
CROSS JOIN tot t
ORDER BY b.review_score;

-- 8) Delivery performance by state (ใช้ ETA เป็นฐาน)
CREATE OR REPLACE VIEW dm.v_delivery_by_state AS
SELECT
  c.customer_state,
  AVG((f.delivered_customer_ts::date - f.estimated_delivery_ts::date))::numeric(10,2) AS avg_delivery_days,
  (100.0 * AVG(CASE WHEN f.delivered_customer_ts::date > f.estimated_delivery_ts::date THEN 1 ELSE 0 END))::numeric(10,2) AS late_rate_pct,
  COUNT(*)::bigint AS items_cnt
FROM dm.fact_sales f
JOIN dm.dim_customer c ON c.customer_sk = f.customer_sk
WHERE f.delivered_customer_ts IS NOT NULL
  AND f.estimated_delivery_ts IS NOT NULL
GROUP BY c.customer_state;

-- 9) Top categories (GMV + share)
CREATE OR REPLACE VIEW dm.v_top_categories AS
WITH base AS (
  SELECT
    COALESCE(p.category_english, p.category_name, 'unknown') AS category,
    SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric(18,2) AS gmv_brl,
    COUNT(*)::bigint AS items_cnt
  FROM dm.fact_sales f
  JOIN dm.dim_product p ON p.product_sk = f.product_sk
  GROUP BY 1
),
tot AS (
  SELECT SUM(gmv_brl) AS total_gmv FROM base
)
SELECT
  b.category,
  b.gmv_brl,
  (100.0 * b.gmv_brl / NULLIF(t.total_gmv,0))::numeric(10,2) AS revenue_share_pct,
  b.items_cnt
FROM base b
CROSS JOIN tot t;