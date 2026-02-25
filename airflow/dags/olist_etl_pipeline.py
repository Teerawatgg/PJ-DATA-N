from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

PG = {
    "host": "db",
    "port": 5432,
    "dbname": "MYDB",
    "user": "postgres",
    "password": "password123",
}

def run_sql_file_direct(sql_path: str) -> None:
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
    schedule=None,
    catchup=False,
    tags=["olist", "etl"],
) as dag:

    create_schemas = PythonOperator(
        task_id="01_create_schemas",
        python_callable=run_sql_file_direct,
        op_kwargs={"sql_path": "/opt/airflow/sql/01_create_schemas.sql"},
    )

    create_raw = PythonOperator(
        task_id="02_create_raw_tables",
        python_callable=run_sql_file_direct,
        op_kwargs={"sql_path": "/opt/airflow/sql/02_create_raw_tables.sql"},
    )

    create_dm = PythonOperator(
        task_id="10_create_dm_tables",
        python_callable=run_sql_file_direct,
        op_kwargs={"sql_path": "/opt/airflow/sql/10_create_dm_tables.sql"},
    )

    load_dim = PythonOperator(
        task_id="20_load_dim",
        python_callable=run_sql_file_direct,
        op_kwargs={"sql_path": "/opt/airflow/sql/20_load_dim_tables.sql"},
    )

    load_fact = PythonOperator(
        task_id="30_load_fact",
        python_callable=run_sql_file_direct,
        op_kwargs={"sql_path": "/opt/airflow/sql/30_load_fact_tables.sql"},
    )

    kpi_views = PythonOperator(
        task_id="40_kpi_views",
        python_callable=run_sql_file_direct,
        op_kwargs={"sql_path": "/opt/airflow/sql/40_create_kpi_views.sql"},
    )

    create_schemas >> create_raw >> create_dm >> load_dim >> load_fact >> kpi_views