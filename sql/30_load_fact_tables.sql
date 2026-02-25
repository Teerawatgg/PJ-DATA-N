-- 30_load_fact_tables.sql
-- Build fact at order_item grain with joins to orders/payments/reviews

TRUNCATE dm.fact_order_items;

WITH payments AS (
  SELECT
    order_id,
    SUM(payment_value) AS payment_value
  FROM raw.order_payments
  GROUP BY order_id
),
reviews AS (
  SELECT
    order_id,
    MAX(review_score) AS review_score
  FROM raw.order_reviews
  GROUP BY order_id
)
INSERT INTO dm.fact_order_items (
  order_id, order_item_id,
  customer_id, seller_id, product_id,
  order_status,
  purchase_ts, approved_ts, delivered_carrier_ts, delivered_customer_ts, estimated_delivery_ts,
  shipping_limit_ts,
  price, freight_value,
  payment_value,
  review_score,
  delivery_days,
  is_late_delivery
)
SELECT
  oi.order_id,
  oi.order_item_id,
  o.customer_id,
  oi.seller_id,
  oi.product_id,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  oi.shipping_limit_date,
  oi.price,
  oi.freight_value,
  p.payment_value,
  r.review_score,
  CASE
    WHEN o.order_delivered_customer_date IS NULL OR o.order_purchase_timestamp IS NULL THEN NULL
    ELSE (o.order_delivered_customer_date::date - o.order_purchase_timestamp::date)
  END AS delivery_days,
  CASE
    WHEN o.order_delivered_customer_date IS NULL OR o.order_estimated_delivery_date IS NULL THEN NULL
    ELSE (o.order_delivered_customer_date::date > o.order_estimated_delivery_date::date)
  END AS is_late_delivery
FROM raw.order_items oi
JOIN raw.orders o
  ON oi.order_id = o.order_id
LEFT JOIN payments p
  ON oi.order_id = p.order_id
LEFT JOIN reviews r
  ON oi.order_id = r.order_id;