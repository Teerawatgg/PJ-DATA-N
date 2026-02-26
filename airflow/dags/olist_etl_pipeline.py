from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import psycopg2

# ====== CONFIG ======
SQL_DIR = "/opt/airflow/sql"

PG = {
    "host": "db",          # ถ้าใน docker-compose ชื่อ service เป็น postgres ให้เปลี่ยนเป็น "postgres"
    "port": 5432,
    "dbname": "MYDB",      # database name
    "user": "postgres",
    "password": "password123",
}

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def run_sql_file(sql_path: str) -> None:
    """Read a .sql file and execute on Postgres (autocommit)."""
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    conn = psycopg2.connect(**PG)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


with DAG(
    dag_id="olist_etl_pipeline",
    start_date=datetime(2026, 2, 25),
    schedule=None,     # ถ้าจะรันทุกวันใช้ "@daily"
    catchup=False,
    default_args=default_args,
    tags=["olist", "etl", "raw", "stg", "dm"],
) as dag:

    # ----------------------
    # DDL (schemas + raw tables)
    # ----------------------
    with TaskGroup(group_id="ddl_raw") as ddl_raw:
        create_schemas = PythonOperator(
            task_id="01_create_schemas",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/01_create_schemas.sql"},
        )

        create_raw_tables = PythonOperator(
            task_id="02_create_raw_tables",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/02_create_raw_tables.sql"},
        )

        create_schemas >> create_raw_tables

    # ----------------------
    # STG (RDBMS PK/FK + load from raw)
    # ----------------------
    with TaskGroup(group_id="stg_rdbms") as stg_rdbms:
        create_stg_tables = PythonOperator(
            task_id="03_create_stg_tables",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/03_create_stg_tables.sql"},
        )

        load_stg_tables = PythonOperator(
            task_id="04_load_stg_tables",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/04_load_stg_tables.sql"},
        )

        create_stg_tables >> load_stg_tables

    # ----------------------
    # DM (Star schema + load dims/facts + KPI views)
    # ----------------------
    with TaskGroup(group_id="dm_star_schema") as dm_star:
        create_dm_tables = PythonOperator(
            task_id="10_create_dm_tables",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/10_create_dm_tables.sql"},
        )

        load_dim_tables = PythonOperator(
            task_id="20_load_dim_tables",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/20_load_dim_tables.sql"},
        )

        load_fact_tables = PythonOperator(
            task_id="30_load_fact_tables",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/30_load_fact_tables.sql"},
        )

        create_kpi_views = PythonOperator(
            task_id="40_create_kpi_views",
            python_callable=run_sql_file,
            op_kwargs={"sql_path": f"{SQL_DIR}/40_create_kpi_views.sql"},
        )

        create_dm_tables >> load_dim_tables >> load_fact_tables >> create_kpi_views

    # ----------------------
    # PIPELINE ORDER
    # ----------------------
    ddl_raw >> stg_rdbms >> dm_star