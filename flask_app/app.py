import os
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, request
import psycopg2


# ===============================
# Config (อ่านจาก docker-compose)
# ===============================
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "MYDB")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password123")


app = Flask(__name__)


# ===============================
# DB Connection Helper
# ===============================
def conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ===============================
# Filter Helpers
# ===============================
def parse_dates():
    from_d = request.args.get("from")
    to_d = request.args.get("to")
    state = request.args.get("state")

    from_date = datetime.fromisoformat(from_d) if from_d else None
    to_date = datetime.fromisoformat(to_d) if to_d else None

    return from_date, to_date, state


def where_clause(from_d, to_d, state):
    clauses = []
    params = []

    if from_d:
        clauses.append("d.date_value >= %s")
        params.append(from_d)

    if to_d:
        clauses.append("d.date_value <= %s")
        params.append(to_d)

    if state:
        clauses.append("c.customer_state = %s")
        params.append(state)

    if not clauses:
        return "1=1", []

    return " AND ".join(clauses), params


# ===============================
# Routes
# ===============================
@app.route("/")
def index():
    return render_template("index.html")


# -------------------------------
# KPI Summary
# -------------------------------
@app.get("/api/kpi_summary")
def kpi_summary():
    from_d, to_d, state = parse_dates()
    w, p = where_clause(from_d, to_d, state)

    sql = f"""
    select
      sum(o.gmv) as gmv,
      count(distinct o.order_id) as orders,
      avg(o.delivery_delay_days) as avg_delay,
      avg(o.delivery_time_days) as avg_delivery_time,
      avg(o.review_score) as avg_review,
      avg(case when o.delivery_delay_days <= 0 then 1 else 0 end) as on_time_rate
    from dm.fact_orders o
    join dm.dim_customer c on o.customer_id = c.customer_id
    join dm.dim_date d on o.order_date_key = d.date_key
    where {w};
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql, p)
        r = cur.fetchone()

    return jsonify({
        "gmv": float(r[0] or 0),
        "orders": int(r[1] or 0),
        "avg_delay_days": float(r[2] or 0),
        "avg_delivery_time_days": float(r[3] or 0),
        "avg_review": float(r[4] or 0),
        "on_time_rate": float(r[5] or 0),
        # delta placeholders (คุณจะเพิ่ม logic growth ภายหลังได้)
        "gmv_growth_pct": None,
        "orders_growth_pct": None,
        "on_time_rate_delta": None,
        "avg_delay_days_delta": None,
        "avg_review_delta": None,
    })


# -------------------------------
# GMV Trend
# -------------------------------
@app.get("/api/gmv_trend_monthly")
def gmv_trend():
    from_d, to_d, state = parse_dates()
    w, p = where_clause(from_d, to_d, state)

    sql = f"""
    select
      to_char(d.date_value, 'YYYY-MM') as period,
      sum(o.gmv)
    from dm.fact_orders o
    join dm.dim_customer c on o.customer_id = c.customer_id
    join dm.dim_date d on o.order_date_key = d.date_key
    where {w}
    group by 1
    order by 1;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql, p)
        rows = cur.fetchall()

    return jsonify([{"period": r[0], "gmv": float(r[1] or 0)} for r in rows])


# -------------------------------
# Orders Trend
# -------------------------------
@app.get("/api/orders_trend_monthly")
def orders_trend():
    from_d, to_d, state = parse_dates()
    w, p = where_clause(from_d, to_d, state)

    sql = f"""
    select
      to_char(d.date_value, 'YYYY-MM') as period,
      count(distinct o.order_id)
    from dm.fact_orders o
    join dm.dim_customer c on o.customer_id = c.customer_id
    join dm.dim_date d on o.order_date_key = d.date_key
    where {w}
    group by 1
    order by 1;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql, p)
        rows = cur.fetchall()

    return jsonify([{"period": r[0], "orders": int(r[1] or 0)} for r in rows])


# -------------------------------
# Delivery Performance
# -------------------------------
@app.get("/api/delivery_performance")
def delivery_perf():
    from_d, to_d, state = parse_dates()
    w, p = where_clause(from_d, to_d, state)

    sql = f"""
    select
      to_char(d.date_value, 'YYYY-MM') as period,
      avg(case when o.delivery_delay_days <= 0 then 1 else 0 end) as on_time_rate,
      avg(o.delivery_delay_days) as avg_delay
    from dm.fact_orders o
    join dm.dim_customer c on o.customer_id = c.customer_id
    join dm.dim_date d on o.order_date_key = d.date_key
    where {w}
    group by 1
    order by 1;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql, p)
        rows = cur.fetchall()

    return jsonify([
        {
            "period": r[0],
            "on_time_rate": float(r[1] or 0),
            "avg_delay_days": float(r[2] or 0),
        }
        for r in rows
    ])


# -------------------------------
# Top Categories
# -------------------------------
@app.get("/api/top_categories")
def top_categories():
    limit = request.args.get("limit", 10)

    sql = f"""
    select
      category_name,
      sum(gmv) as sales
    from dm.fact_order_items
    group by 1
    order by sales desc
    limit %s;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    return jsonify([{"category": r[0], "sales": float(r[1] or 0)} for r in rows])


# -------------------------------
# Review vs Delay
# -------------------------------
@app.get("/api/review_vs_delay")
def review_vs_delay():
    sql = """
    select
      case
        when delivery_delay_days <= 0 then 'On Time'
        when delivery_delay_days <= 3 then '1-3 days'
        when delivery_delay_days <= 7 then '4-7 days'
        else '7+ days'
      end as bucket,
      avg(review_score)
    from dm.fact_orders
    group by 1
    order by 1;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return jsonify([
        {"delay_bucket": r[0], "avg_review": float(r[1] or 0)}
        for r in rows
    ])


# -------------------------------
# Worst States
# -------------------------------
@app.get("/api/worst_states")
def worst_states():
    limit = request.args.get("limit", 8)

    sql = """
    select
      c.customer_state,
      avg(o.delivery_delay_days) as avg_delay,
      avg(case when o.delivery_delay_days <= 0 then 1 else 0 end) as on_time_rate
    from dm.fact_orders o
    join dm.dim_customer c on o.customer_id = c.customer_id
    group by 1
    order by avg_delay desc
    limit %s;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    return jsonify([
        {
            "state": r[0],
            "avg_delay_days": float(r[1] or 0),
            "on_time_rate": float(r[2] or 0),
        }
        for r in rows
    ])


# -------------------------------
# Top Sellers Risk
# -------------------------------
@app.get("/api/top_sellers_risk")
def top_sellers():
    limit = request.args.get("limit", 10)

    sql = """
    select
      seller_id,
      avg(delivery_delay_days) as avg_delay,
      avg(review_score) as avg_review
    from dm.fact_order_items
    group by 1
    order by avg_delay desc
    limit %s;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    return jsonify([
        {
            "seller_id": r[0],
            "avg_delay_days": float(r[1] or 0),
            "avg_review": float(r[2] or 0),
        }
        for r in rows
    ])


# -------------------------------
# Executive Insights
# -------------------------------
@app.get("/api/executive_insights")
def executive_insights():
    sql = """
    select
      sum(gmv),
      count(distinct order_id),
      avg(delivery_delay_days),
      avg(review_score),
      avg(case when delivery_delay_days <= 0 then 1 else 0 end)
    from dm.fact_orders;
    """

    with conn() as c, c.cursor() as cur:
        cur.execute(sql)
        r = cur.fetchone()

    gmv = float(r[0] or 0)
    orders = int(r[1] or 0)
    avg_delay = float(r[2] or 0)
    avg_review = float(r[3] or 0)
    on_time = float(r[4] or 0)

    if avg_delay > 2:
        driver = "Delivery Delay"
        action = "Improve logistics partner performance"
    else:
        driver = "Sales Growth"
        action = "Focus on high-performing categories"

    return jsonify({
        "gmv": gmv,
        "orders": orders,
        "avg_delay_days": avg_delay,
        "avg_review": avg_review,
        "on_time_rate": on_time,
        "key_driver": driver,
        "recommended_action": action,
    })


# -------------------------------
# States dropdown
# -------------------------------
@app.get("/api/states")
def states():
    sql = "select distinct customer_state from dm.dim_customer order by 1;"
    with conn() as c, c.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return jsonify([r[0] for r in rows])


# ===============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)