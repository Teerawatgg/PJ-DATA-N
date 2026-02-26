from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, render_template, request

app = Flask(__name__, template_folder="templates")

DB_HOST = os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "db"))
DB_PORT = int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")))
DB_NAME = os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "MYDB"))
DB_USER = os.getenv("POSTGRES_USER", os.getenv("DB_USER", "postgres"))
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", "password123"))


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


def q_one(sql: str, params: Optional[Tuple[Any, ...]] = None) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return dict(row) if row else {}


def q_all(sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def fnum(x, digits=2):
    try:
        return round(float(x or 0), digits)
    except Exception:
        return 0.0


@app.get("/health")
def health():
    try:
        q_one("SELECT 1 AS ok;")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.get("/")
def index():
    return render_template("index.html")

# ---------------------------
# Serve CSS/JS from templates folder (เพราะไฟล์คุณอยู่ใน templates/)
# ---------------------------
from flask import send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

@app.get("/styles.css")
def serve_styles():
    return send_from_directory(TEMPLATES_DIR, "styles.css", mimetype="text/css")

@app.get("/app.js")
def serve_app_js():
    return send_from_directory(TEMPLATES_DIR, "app.js", mimetype="application/javascript")

# ---------------------------
# KPI Overview (top cards)
# ---------------------------
@app.get("/api/kpi-overview")
def api_kpi_overview():
    row = q_one(
        """
        SELECT
          COUNT(DISTINCT f.order_id)::bigint AS orders_cnt,
          COALESCE(SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)),0)::numeric(18,2) AS gmv_brl,
          COUNT(DISTINCT c.customer_unique_id)::bigint AS unique_customers,
          COALESCE(AVG(f.review_score),0)::numeric(10,3) AS avg_review
        FROM dm.fact_sales f
        JOIN dm.dim_customer c ON c.customer_sk = f.customer_sk
        WHERE f.order_purchase_ts IS NOT NULL;
        """
    )
    return jsonify(
        {
            "orders_cnt": int(row.get("orders_cnt") or 0),
            "gmv_brl": fnum(row.get("gmv_brl"), 2),
            "unique_customers": int(row.get("unique_customers") or 0),
            "avg_review": fnum(row.get("avg_review"), 3),
        }
    )


# ---------------------------
# Sales Trend: Day / Hour (กราฟใหญ่ซ้าย)
# ---------------------------
@app.get("/api/sales-trend")
def api_sales_trend():
    grain = (request.args.get("grain") or "day").lower()
    if grain not in ("day", "hour"):
        grain = "day"

    if grain == "hour":
        sql = """
        SELECT
          to_char(date_trunc('hour', f.order_purchase_ts), 'YYYY-MM-DD HH24:00') AS label,
          COUNT(DISTINCT f.order_id)::bigint AS orders,
          COALESCE(SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)),0)::numeric(18,2) AS gmv_brl
        FROM dm.fact_sales f
        WHERE f.order_purchase_ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
        """
    else:
        sql = """
        SELECT
          to_char(date_trunc('day', f.order_purchase_ts), 'YYYY-MM-DD') AS label,
          COUNT(DISTINCT f.order_id)::bigint AS orders,
          COALESCE(SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)),0)::numeric(18,2) AS gmv_brl
        FROM dm.fact_sales f
        WHERE f.order_purchase_ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
        """

    rows = q_all(sql)
    return jsonify(
        {
            "grain": grain,
            "labels": [r["label"] for r in rows],
            "orders": [int(r["orders"] or 0) for r in rows],
            "gmv_brl": [float(r["gmv_brl"] or 0) for r in rows],
        }
    )


# ---------------------------
# Transaction Value by Year (ล่างซ้าย)
# ---------------------------
@app.get("/api/transaction-by-year")
def api_transaction_by_year():
    rows = q_all(
        """
        SELECT
          EXTRACT(YEAR FROM f.order_purchase_ts)::int AS year,
          COALESCE(SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)),0)::numeric(18,2) AS gmv_brl
        FROM dm.fact_sales f
        WHERE f.order_purchase_ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
        """
    )
    return jsonify(
        {"labels": [str(r["year"]) for r in rows], "gmv_brl": [float(r["gmv_brl"] or 0) for r in rows]}
    )


# ---------------------------
# Best Selling Category by Orders (ขวาบน)
# ---------------------------
@app.get("/api/best-category-by-orders")
def api_best_category_by_orders():
    # ✅ Top categories by orders (default: Top 10)
    try:
        limit = int(request.args.get("limit", 10))
    except Exception:
        limit = 10
    limit = max(1, min(limit, 30))

    rows = q_all(
        """
        SELECT
          COALESCE(p.category_english, p.category_name, 'unknown') AS category,
          COUNT(DISTINCT f.order_id)::bigint AS orders
        FROM dm.fact_sales f
        JOIN dm.dim_product p ON p.product_sk = f.product_sk
        WHERE f.order_purchase_ts IS NOT NULL
        GROUP BY 1
        ORDER BY orders DESC
        LIMIT %s;
        """,
        (limit,),
    )
    return jsonify(
        {"labels": [r["category"] for r in rows], "orders": [int(r["orders"] or 0) for r in rows], "limit": limit}
    )



# ---------------------------
# Payment Mix (โดนัท)
# ---------------------------
@app.get("/api/payment-mix")
def api_payment_mix():
    rows = q_all(
        """
        SELECT
          COALESCE(NULLIF(TRIM(pt.payment_type), ''), 'unknown') AS payment_type,
          COUNT(*)::bigint AS rows_cnt
        FROM dm.fact_sales f
        LEFT JOIN dm.dim_payment_type pt ON pt.payment_type_sk = f.payment_type_sk
        GROUP BY 1
        ORDER BY rows_cnt DESC;
        """
    )
    return jsonify({"labels": [r["payment_type"] for r in rows], "counts": [int(r["rows_cnt"] or 0) for r in rows]})


# =========================================================
# Key Insights (เหมือนรูปที่ 1)
# =========================================================

@app.get("/api/orders-revenue-by-dow")
def api_orders_revenue_by_dow():
    rows = q_all(
        """
        SELECT
          EXTRACT(ISODOW FROM f.order_purchase_ts)::int AS dow,
          COUNT(DISTINCT f.order_id)::bigint AS orders,
          COALESCE(SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)),0)::numeric(18,2) AS gross_revenue
        FROM dm.fact_sales f
        WHERE f.order_purchase_ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
        """
    )
    dow_map = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
    return jsonify(
        {
            "labels": [dow_map.get(int(r["dow"]), str(r["dow"])) for r in rows],
            "orders": [int(r["orders"] or 0) for r in rows],
            "gross_revenue": [float(r["gross_revenue"] or 0) for r in rows],
        }
    )


@app.get("/api/orders-revenue-by-hour")
def api_orders_revenue_by_hour():
    rows = q_all(
        """
        SELECT
          EXTRACT(HOUR FROM f.order_purchase_ts)::int AS hour,
          COUNT(DISTINCT f.order_id)::bigint AS orders,
          COALESCE(SUM(COALESCE(f.item_price,0) + COALESCE(f.freight_value,0)),0)::numeric(18,2) AS gross_revenue
        FROM dm.fact_sales f
        WHERE f.order_purchase_ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1;
        """
    )
    return jsonify(
        {
            "labels": [str(int(r["hour"])) for r in rows],
            "orders": [int(r["orders"] or 0) for r in rows],
            "gross_revenue": [float(r["gross_revenue"] or 0) for r in rows],
        }
    )


@app.get("/api/state-key-metrics")
def api_state_key_metrics():
    rows = q_all(
        """
        WITH base AS (
          SELECT
            c.customer_state AS state,
            f.order_id,
            (COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric AS gmv
          FROM dm.fact_sales f
          JOIN dm.dim_customer c ON c.customer_sk = f.customer_sk
          WHERE f.order_purchase_ts IS NOT NULL
        ),
        agg AS (
          SELECT
            state,
            COUNT(DISTINCT order_id)::bigint AS total_orders,
            COALESCE(SUM(gmv),0)::numeric(18,2) AS gross_revenue
          FROM base
          GROUP BY 1
        )
        SELECT
          state,
          total_orders,
          gross_revenue,
          CASE WHEN total_orders = 0 THEN 0 ELSE (gross_revenue / total_orders)::numeric(18,2) END AS aov
        FROM agg
        ORDER BY total_orders DESC;
        """,
    )

    tot = q_one(
        """
        WITH base AS (
          SELECT
            f.order_id,
            (COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric AS gmv
          FROM dm.fact_sales f
          WHERE f.order_purchase_ts IS NOT NULL
        )
        SELECT
          COUNT(DISTINCT order_id)::bigint AS total_orders,
          COALESCE(SUM(gmv),0)::numeric(18,2) AS gross_revenue
        FROM base;
        """
    )

    total_orders = int(tot.get("total_orders") or 0)
    gross_revenue = float(tot.get("gross_revenue") or 0)
    aov = 0 if total_orders == 0 else round(gross_revenue / total_orders, 2)

    return jsonify(
        {
            "rows": [
                {
                    "state": r["state"],
                    "total_orders": int(r["total_orders"] or 0),
                    "gross_revenue": float(r["gross_revenue"] or 0),
                    "aov": float(r["aov"] or 0),
                }
                for r in rows
            ],
            "total": {"state": "Total", "total_orders": total_orders, "gross_revenue": gross_revenue, "aov": aov},
        }
    )


# ✅ FIXED: category metrics (ไม่พัง แม้ไม่มี order_status)
@app.get("/api/category-key-metrics")
def api_category_key_metrics():

    has_order_status = False
    try:
        chk = q_one(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.columns
              WHERE table_schema='dm'
                AND table_name='fact_sales'
                AND column_name='order_status'
            ) AS ok;
            """
        )
        has_order_status = bool(chk.get("ok"))
    except Exception:
        has_order_status = False

    if has_order_status:
        sql = """
        WITH item_base AS (
          SELECT
            COALESCE(p.category_english, p.category_name, 'unknown') AS category,
            f.order_id,
            (COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric AS gmv,
            COALESCE(NULLIF(TRIM(f.order_status),''), 'unknown') AS order_status
          FROM dm.fact_sales f
          JOIN dm.dim_product p ON p.product_sk = f.product_sk
          WHERE f.order_purchase_ts IS NOT NULL
        ),
        item_agg AS (
          SELECT
            category,
            COUNT(*)::bigint AS items_cnt,
            COUNT(DISTINCT order_id)::bigint AS total_orders,
            COALESCE(SUM(gmv),0)::numeric(18,2) AS gross_revenue,
            COUNT(DISTINCT CASE WHEN order_status = 'canceled' THEN order_id END)::bigint AS canceled_orders
          FROM item_base
          GROUP BY 1
        )
        SELECT
          category,
          total_orders,
          gross_revenue,
          CASE WHEN total_orders = 0 THEN 0 ELSE (gross_revenue / total_orders)::numeric(18,2) END AS aov,
          CASE WHEN total_orders = 0 THEN 0 ELSE (items_cnt::numeric / total_orders)::numeric(18,2) END AS avg_basket_size,
          CASE WHEN total_orders = 0 THEN 0 ELSE (canceled_orders::numeric / total_orders)::numeric(18,4) END AS cancel_rate
        FROM item_agg
        ORDER BY total_orders DESC;
        """
        rows = q_all(sql)

        tot = q_one(
            """
            WITH item_base AS (
              SELECT
                f.order_id,
                (COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric AS gmv,
                COALESCE(NULLIF(TRIM(f.order_status),''), 'unknown') AS order_status
              FROM dm.fact_sales f
              WHERE f.order_purchase_ts IS NOT NULL
            )
            SELECT
              COUNT(*)::bigint AS items_cnt,
              COUNT(DISTINCT order_id)::bigint AS total_orders,
              COALESCE(SUM(gmv),0)::numeric(18,2) AS gross_revenue,
              COUNT(DISTINCT CASE WHEN order_status='canceled' THEN order_id END)::bigint AS canceled_orders
            FROM item_base;
            """
        )

        items_cnt = float(tot.get("items_cnt") or 0)
        total_orders = float(tot.get("total_orders") or 0)
        gross_revenue = float(tot.get("gross_revenue") or 0)
        canceled_orders = float(tot.get("canceled_orders") or 0)

        total = {
            "category": "Total",
            "total_orders": int(total_orders),
            "gross_revenue": gross_revenue,
            "aov": 0 if total_orders == 0 else round(gross_revenue / total_orders, 2),
            "avg_basket_size": 0 if total_orders == 0 else round(items_cnt / total_orders, 2),
            "cancel_rate": 0 if total_orders == 0 else round(canceled_orders / total_orders, 4),
        }

        return jsonify(
            {
                "rows": [
                    {
                        "category": r["category"],
                        "total_orders": int(r["total_orders"] or 0),
                        "gross_revenue": float(r["gross_revenue"] or 0),
                        "aov": float(r["aov"] or 0),
                        "avg_basket_size": float(r["avg_basket_size"] or 0),
                        "cancel_rate": float(r["cancel_rate"] or 0),
                    }
                    for r in rows
                ],
                "total": total,
                "note": "cancel_rate computed from dm.fact_sales.order_status",
            }
        )

    # fallback: no order_status
    sql = """
    WITH item_base AS (
      SELECT
        COALESCE(p.category_english, p.category_name, 'unknown') AS category,
        f.order_id,
        (COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric AS gmv
      FROM dm.fact_sales f
      JOIN dm.dim_product p ON p.product_sk = f.product_sk
      WHERE f.order_purchase_ts IS NOT NULL
    ),
    item_agg AS (
      SELECT
        category,
        COUNT(*)::bigint AS items_cnt,
        COUNT(DISTINCT order_id)::bigint AS total_orders,
        COALESCE(SUM(gmv),0)::numeric(18,2) AS gross_revenue
      FROM item_base
      GROUP BY 1
    )
    SELECT
      category,
      total_orders,
      gross_revenue,
      CASE WHEN total_orders = 0 THEN 0 ELSE (gross_revenue / total_orders)::numeric(18,2) END AS aov,
      CASE WHEN total_orders = 0 THEN 0 ELSE (items_cnt::numeric / total_orders)::numeric(18,2) END AS avg_basket_size,
      NULL::numeric AS cancel_rate
    FROM item_agg
    ORDER BY total_orders DESC;
    """
    rows = q_all(sql)

    tot = q_one(
        """
        WITH item_base AS (
          SELECT
            f.order_id,
            (COALESCE(f.item_price,0) + COALESCE(f.freight_value,0))::numeric AS gmv
          FROM dm.fact_sales f
          WHERE f.order_purchase_ts IS NOT NULL
        )
        SELECT
          COUNT(*)::bigint AS items_cnt,
          COUNT(DISTINCT order_id)::bigint AS total_orders,
          COALESCE(SUM(gmv),0)::numeric(18,2) AS gross_revenue
        FROM item_base;
        """
    )

    items_cnt = float(tot.get("items_cnt") or 0)
    total_orders = float(tot.get("total_orders") or 0)
    gross_revenue = float(tot.get("gross_revenue") or 0)

    total = {
        "category": "Total",
        "total_orders": int(total_orders),
        "gross_revenue": gross_revenue,
        "aov": 0 if total_orders == 0 else round(gross_revenue / total_orders, 2),
        "avg_basket_size": 0 if total_orders == 0 else round(items_cnt / total_orders, 2),
        "cancel_rate": None,
    }

    return jsonify(
        {
            "rows": [
                {
                    "category": r["category"],
                    "total_orders": int(r["total_orders"] or 0),
                    "gross_revenue": float(r["gross_revenue"] or 0),
                    "aov": float(r["aov"] or 0),
                    "avg_basket_size": float(r["avg_basket_size"] or 0),
                    "cancel_rate": None,
                }
                for r in rows
            ],
            "total": total,
            "note": "cancel_rate is null because dm.fact_sales.order_status not found",
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)