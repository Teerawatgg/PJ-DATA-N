-- Airflow metadata DB + user
DO
$$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
  END IF;
END
$$;

CREATE DATABASE airflow_db OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;