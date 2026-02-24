from __future__ import annotations

from datetime import datetime, timedelta
import os
import time

import psycopg2
from psycopg2 import OperationalError

from airflow import DAG
from airflow.operators.python import PythonOperator


# ===== Config (อ่านจาก env ที่ docker-compose ส่งมา) =====
DB_HOST = os.getenv("POSTGRES_HOST", "db")          # docker-compose: service ชื่อ db
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "MYDB")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")

SQL_DIR = os.getenv("SQL_DIR", "/opt/airflow/sql")


# ===== Helpers =====
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5,
    )


def wait_for_db(max_wait_seconds: int = 120) -> None:
    """Wait until Postgres is ready (healthcheck กันพลาด)."""
    start = time.time()
    while True:
        try:
            conn = get_conn()
            conn.close()
            print("✅ Postgres is ready")
            return
        except OperationalError as e:
            elapsed = int(time.time() - start)
            if elapsed >= max_wait_seconds:
                raise RuntimeError(f"❌ Postgres not ready after {max_wait_seconds}s: {e}") from e
            print(f"⏳ Waiting for Postgres... ({elapsed}s) {e}")
            time.sleep(5)


def run_sql_file(filename: str) -> None:
    path = os.path.join(SQL_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL file not found: {path} (check volume mount ./sql -> {SQL_DIR})")

    with open(path, "r", encoding="utf-8") as f:
        sql = f.read().strip()

    if not sql:
        print(f"⚠️ Empty SQL file: {filename} (skip)")
        return

    conn = get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            print(f"▶ Running {filename} ...")
            cur.execute(sql)
            print(f"✅ Done {filename}")
    finally:
        conn.close()


# ===== DAG =====
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="olist_etl_daily",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",   # ตี 2 ทุกวัน (ถ้าเดโม แนะนำเปลี่ยนเป็น None)
    catchup=False,
    default_args=default_args,
    tags=["olist", "etl"],
) as dag:

    t_wait = PythonOperator(
        task_id="wait_for_db",
        python_callable=wait_for_db,
        op_kwargs={"max_wait_seconds": 180},
    )

    t0 = PythonOperator(task_id="00_schemas", python_callable=run_sql_file, op_args=["00_schemas.sql"])
    t1 = PythonOperator(task_id="01_raw_tables", python_callable=run_sql_file, op_args=["01_raw_tables.sql"])
    t2 = PythonOperator(task_id="02_stg_tables", python_callable=run_sql_file, op_args=["02_stg_tables.sql"])
    t3 = PythonOperator(task_id="03_dm_star_schema", python_callable=run_sql_file, op_args=["03_dm_star_schema.sql"])
    t4 = PythonOperator(task_id="10_raw_to_stg", python_callable=run_sql_file, op_args=["10_raw_to_stg.sql"])
    t5 = PythonOperator(task_id="11_stg_to_dm", python_callable=run_sql_file, op_args=["11_stg_to_dm.sql"])

    # flow
    t_wait >> t0 >> [t1, t2, t3] >> t4 >> t5