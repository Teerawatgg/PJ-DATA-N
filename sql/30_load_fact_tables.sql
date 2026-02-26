-- ---------------------------------------------------------
-- 06) LOAD FACT
-- ---------------------------------------------------------
TRUNCATE TABLE dm.fact_sales;

WITH pay_one AS (
  -- เลือก payment ต่อ order (ถ้ามีหลายแถว เอา sequential มากสุด)
  SELECT DISTINCT ON (order_id)
    order_id, payment_type, payment_installments, payment_value
  FROM stg.order_payments
  ORDER BY order_id, payment_sequential DESC
),
rev_one AS (
  -- เลือก review ต่อ order (เอา answer ล่าสุด)
  SELECT DISTINCT ON (order_id)
    order_id, review_score
  FROM stg.order_reviews
  ORDER BY order_id, review_answer_timestamp DESC NULLS LAST
)
INSERT INTO dm.fact_sales (
  order_id, order_item_id,
  order_date_id, customer_sk, seller_sk, product_sk,
  order_status_sk, payment_type_sk,
  item_price, freight_value, payment_value, payment_installments, review_score,
  order_purchase_ts, delivered_customer_ts, estimated_delivery_ts
)
SELECT
  i.order_id,
  i.order_item_id,

  (EXTRACT(YEAR FROM o.order_purchase_timestamp)::int * 10000
   + EXTRACT(MONTH FROM o.order_purchase_timestamp)::int * 100
   + EXTRACT(DAY FROM o.order_purchase_timestamp)::int) AS order_date_id,

  dc.customer_sk,
  ds.seller_sk,
  dp.product_sk,

  dos.order_status_sk,
  dpt.payment_type_sk,

  i.price,
  i.freight_value,
  pay.payment_value,
  pay.payment_installments,
  rev.review_score,

  o.order_purchase_timestamp,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date
FROM stg.order_items i
JOIN stg.orders o ON o.order_id = i.order_id
JOIN dm.dim_customer dc ON dc.customer_id = o.customer_id
JOIN dm.dim_seller   ds ON ds.seller_id = i.seller_id
JOIN dm.dim_product  dp ON dp.product_id = i.product_id
LEFT JOIN dm.dim_order_status dos ON dos.order_status = o.order_status
LEFT JOIN pay_one pay ON pay.order_id = i.order_id
LEFT JOIN dm.dim_payment_type dpt ON dpt.payment_type = pay.payment_type
LEFT JOIN rev_one rev ON rev.order_id = i.order_id
WHERE o.order_purchase_timestamp IS NOT NULL;