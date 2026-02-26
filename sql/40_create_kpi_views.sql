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