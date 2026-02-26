-- ---------------------------------------------------------
-- 03) LOAD STG (raw -> stg) : clean + dedup + FK-safe
--     * ใช้ TRUNCATE เพื่อให้รีเฟรชข้อมูลทุก run ใน Airflow
-- ---------------------------------------------------------
BEGIN;

TRUNCATE TABLE
  stg.order_reviews,
  stg.order_payments,
  stg.order_items,
  stg.orders,
  stg.products,
  stg.sellers,
  stg.customers,
  stg.category_translation
RESTART IDENTITY;

-- 3.1 category translation
INSERT INTO stg.category_translation (product_category_name, product_category_name_english)
SELECT DISTINCT ON (product_category_name)
  product_category_name,
  product_category_name_english
FROM raw.category_translation
WHERE product_category_name IS NOT NULL
ORDER BY product_category_name;

-- 3.2 customers
INSERT INTO stg.customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
SELECT DISTINCT ON (customer_id)
  customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM raw.customers
WHERE customer_id IS NOT NULL
ORDER BY customer_id;

-- 3.3 sellers
INSERT INTO stg.sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
SELECT DISTINCT ON (seller_id)
  seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM raw.sellers
WHERE seller_id IS NOT NULL
ORDER BY seller_id;

-- 3.4 products
INSERT INTO stg.products (
  product_id, product_category_name,
  product_name_lenght, product_description_lenght, product_photos_qty,
  product_weight_g, product_length_cm, product_height_cm, product_width_cm
)
SELECT DISTINCT ON (product_id)
  product_id, product_category_name,
  product_name_lenght, product_description_lenght, product_photos_qty,
  product_weight_g, product_length_cm, product_height_cm, product_width_cm
FROM raw.products
WHERE product_id IS NOT NULL
ORDER BY product_id;

-- 3.5 orders (ต้องมี customer ถึงจะผ่าน FK)
INSERT INTO stg.orders (
  order_id, customer_id, order_status,
  order_purchase_timestamp, order_approved_at,
  order_delivered_carrier_date, order_delivered_customer_date,
  order_estimated_delivery_date
)
SELECT DISTINCT ON (o.order_id)
  o.order_id, o.customer_id, o.order_status,
  o.order_purchase_timestamp, o.order_approved_at,
  o.order_delivered_carrier_date, o.order_delivered_customer_date,
  o.order_estimated_delivery_date
FROM raw.orders o
JOIN stg.customers c ON c.customer_id = o.customer_id
WHERE o.order_id IS NOT NULL
ORDER BY o.order_id;

-- 3.6 items (ต้องมี order/product/seller)
INSERT INTO stg.order_items (
  order_id, order_item_id, product_id, seller_id,
  shipping_limit_date, price, freight_value
)
SELECT DISTINCT ON (i.order_id, i.order_item_id)
  i.order_id, i.order_item_id, i.product_id, i.seller_id,
  i.shipping_limit_date, i.price, i.freight_value
FROM raw.order_items i
JOIN stg.orders   o ON o.order_id = i.order_id
JOIN stg.products p ON p.product_id = i.product_id
JOIN stg.sellers  s ON s.seller_id = i.seller_id
WHERE i.order_id IS NOT NULL AND i.order_item_id IS NOT NULL
ORDER BY i.order_id, i.order_item_id;

-- 3.7 payments (ต้องมี order)
INSERT INTO stg.order_payments (
  order_id, payment_sequential, payment_type, payment_installments, payment_value
)
SELECT DISTINCT ON (p.order_id, p.payment_sequential)
  p.order_id, p.payment_sequential, p.payment_type, p.payment_installments, p.payment_value
FROM raw.order_payments p
JOIN stg.orders o ON o.order_id = p.order_id
WHERE p.order_id IS NOT NULL AND p.payment_sequential IS NOT NULL
ORDER BY p.order_id, p.payment_sequential;

-- 3.8 reviews (ต้องมี order)
INSERT INTO stg.order_reviews (
  review_id, order_id, review_score,
  review_comment_title, review_comment_message,
  review_creation_date, review_answer_timestamp
)
SELECT DISTINCT ON (r.review_id)
  r.review_id, r.order_id, r.review_score,
  r.review_comment_title, r.review_comment_message,
  r.review_creation_date, r.review_answer_timestamp
FROM raw.order_reviews r
JOIN stg.orders o ON o.order_id = r.order_id
WHERE r.review_id IS NOT NULL
ORDER BY r.review_id;

COMMIT;
