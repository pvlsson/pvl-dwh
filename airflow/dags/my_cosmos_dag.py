from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import pandas as pd
import os
from datetime import datetime

airflow_home = os.environ["AIRFLOW_HOME"]

# Load CSV files into PostgreSQL database
def load_csv_to_postgres():
    folder_path = f'{airflow_home}/dags/dbt/pvl_dwh/seeds'
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(file_path)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    ),
)

with DAG(
    dag_id="pvl_dwh",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    load_csv_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )

    dbt_task = DbtDag(
        project_config=ProjectConfig(
            f"{airflow_home}/dags/dbt/pvl_dwh",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
    )

    extract_task = PostgresOperator(
        task_id="extract_to_postgres",
        postgres_conn_id="airflow_db",
        sql="""
        CREATE TABLE IF NOT EXISTS transformed_data AS
        SELECT * FROM {{ dbt_task.project_config.project_dir }}.output_table;
        """,
    )

    load_csv_task >> dbt_task >> extract_task