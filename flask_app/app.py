from __future__ import annotations

import os
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, render_template


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


DB_CONFIG = {
    "host": env("DB_HOST", "postgres"),
    "port": int(env("DB_PORT", "5432")),
    "dbname": env("DB_NAME", "MYDB"),
    "user": env("DB_USER", "postgres"),
    "password": env("DB_PASSWORD", "password123"),
}

app = Flask(__name__, template_folder="templates")


def query_all(sql: str, params: tuple | None = None) -> List[Dict[str, Any]]:
    conn = psycopg2.connect(cursor_factory=RealDictCursor, **DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


def query_one(sql: str, params: tuple | None = None) -> Dict[str, Any]:
    rows = query_all(sql, params)
    return rows[0] if rows else {}


# ---------- Pages ----------
@app.route("/")
def index():
    # Summary: รวมจาก dm.v_kpi_monthly
    summary = query_one(
        """
        SELECT
            COALESCE(SUM(orders), 0)::bigint AS total_orders,
            COALESCE(SUM(gmv), 0)::numeric   AS total_gmv,
            COALESCE(AVG(avg_review), 0)::numeric AS avg_review
        FROM dm.v_kpi_monthly;
        """
    )

    # Preview tables: Top states + Top categories
    top_states = query_all(
        """
        SELECT customer_state, orders, gmv
        FROM dm.v_kpi_sales_by_state
        ORDER BY gmv DESC
        LIMIT 10;
        """
    )

    top_categories = query_all(
        """
        SELECT category, items_sold, revenue_items
        FROM dm.v_kpi_top_categories
        ORDER BY revenue_items DESC
        LIMIT 10;
        """
    )

    return render_template(
        "index.html",
        summary=summary,
        top_states=top_states,
        top_categories=top_categories,
    )


@app.route("/health")
def health():
    try:
        _ = query_one("SELECT 1 AS ok;")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


# ---------- APIs (JSON for charts) ----------
@app.route("/api/monthly")
def api_monthly():
    rows = query_all(
        """
        SELECT year, month, orders, gmv, avg_review
        FROM dm.v_kpi_monthly
        ORDER BY year, month;
        """
    )
    labels = [f"{r['year']}-{int(r['month']):02d}" for r in rows]
    gmv = [float(r["gmv"] or 0) for r in rows]
    orders = [int(r["orders"] or 0) for r in rows]
    avg_review = [float(r["avg_review"] or 0) for r in rows]
    return jsonify({"labels": labels, "gmv": gmv, "orders": orders, "avg_review": avg_review})


@app.route("/api/sales_by_state")
def api_sales_by_state():
    rows = query_all(
        """
        SELECT customer_state, orders, gmv
        FROM dm.v_kpi_sales_by_state
        ORDER BY gmv DESC
        LIMIT 15;
        """
    )
    labels = [r["customer_state"] or "NA" for r in rows]
    gmv = [float(r["gmv"] or 0) for r in rows]
    orders = [int(r["orders"] or 0) for r in rows]
    return jsonify({"labels": labels, "gmv": gmv, "orders": orders})


@app.route("/api/top_categories")
def api_top_categories():
    rows = query_all(
        """
        SELECT category, items_sold, revenue_items
        FROM dm.v_kpi_top_categories
        ORDER BY revenue_items DESC
        LIMIT 12;
        """
    )
    labels = [r["category"] or "unknown" for r in rows]
    revenue = [float(r["revenue_items"] or 0) for r in rows]
    items = [int(r["items_sold"] or 0) for r in rows]
    return jsonify({"labels": labels, "revenue": revenue, "items": items})


@app.route("/api/payment_mix")
def api_payment_mix():
    rows = query_all(
        """
        SELECT COALESCE(payment_type,'unknown') AS payment_type,
               orders,
               total_paid
        FROM dm.v_kpi_payment_mix
        ORDER BY total_paid DESC;
        """
    )
    labels = [r["payment_type"] or "unknown" for r in rows]
    paid = [float(r["total_paid"] or 0) for r in rows]
    orders = [int(r["orders"] or 0) for r in rows]
    return jsonify({"labels": labels, "total_paid": paid, "orders": orders})


@app.route("/api/sla")
def api_sla():
    rows = query_all(
        """
        SELECT year, month, delivered_orders, avg_days_late
        FROM dm.v_kpi_sla
        ORDER BY year, month;
        """
    )
    labels = [f"{r['year']}-{int(r['month']):02d}" for r in rows]
    delivered = [int(r["delivered_orders"] or 0) for r in rows]
    avg_days_late = [float(r["avg_days_late"] or 0) for r in rows]
    return jsonify({"labels": labels, "delivered_orders": delivered, "avg_days_late": avg_days_late})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(env("PORT", "5000")), debug=True)