from airflow import DAG
from airflow.utils.dates import days_ago
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

from datetime import timedelta
import os

airflow_home = os.environ["AIRFLOW_HOME"]

profile_config = ProfileConfig(
    profile_name = "default",
    target_name = "dev",
    profile_mapping = GoogleCloudServiceAccountDictProfileMapping(
        conn_id = 'bigquery_pvl_dwh',
        profile_args = { 
            "project": "pvlsson",
            "dataset": "pvl_dwh_mart",
            "keyfile_json": "/usr/local/airflow/secrets/pvlsson-34e37e7a3ab1.json",
        },
    ),
)

with DAG(
    dag_id="bq_dag_run",
    start_date=days_ago(1),
    schedule_interval=None,  # or set to daily/hourly if needed
    catchup=False,
    tags=["dbt", "bigquery"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    dbt_task = DbtDag(
        project_config=ProjectConfig(
            f"{airflow_home}/dags/dbt/pvl_dwh",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
    )
