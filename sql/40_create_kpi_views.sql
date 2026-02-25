-- 40_create_kpi_views.sql
-- Views for dashboard / API

-- 1) Monthly GMV (ยอดขายรวมต่อเดือน)
CREATE OR REPLACE VIEW dm.v_monthly_gmv AS
SELECT
  DATE_TRUNC('month', purchase_ts)::date AS month,
  SUM(price + COALESCE(freight_value,0)) AS gmv,
  COUNT(DISTINCT order_id) AS orders
FROM dm.fact_order_items
WHERE purchase_ts IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 2) Top products by GMV (ใช้ category ภาษาอังกฤษถ้ามี)
CREATE OR REPLACE VIEW dm.v_top_products AS
SELECT
  p.product_category_name_english AS category_en,
  p.product_category_name AS category,
  SUM(f.price + COALESCE(f.freight_value,0)) AS gmv,
  COUNT(*) AS items
FROM dm.fact_order_items f
JOIN dm.dim_product p
  ON f.product_id = p.product_id
GROUP BY 1,2
ORDER BY gmv DESC;

-- 3) Delivery performance by customer state
CREATE OR REPLACE VIEW dm.v_delivery_by_state AS
SELECT
  c.customer_state,
  AVG(f.delivery_days) AS avg_delivery_days,
  AVG(CASE WHEN f.is_late_delivery THEN 1 ELSE 0 END) AS late_rate,
  COUNT(*) AS items
FROM dm.fact_order_items f
JOIN dm.dim_customer c
  ON f.customer_id = c.customer_id
WHERE f.delivery_days IS NOT NULL
GROUP BY 1
ORDER BY avg_delivery_days DESC;

-- 4) Review score distribution
CREATE OR REPLACE VIEW dm.v_review_distribution AS
SELECT
  review_score,
  COUNT(*) AS cnt
FROM dm.fact_order_items
WHERE review_score IS NOT NULL
GROUP BY 1
ORDER BY 1;