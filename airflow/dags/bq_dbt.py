from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from datetime import datetime
import os

airflow_home = os.environ["AIRFLOW_HOME"]

profile_config = ProfileConfig(
    profile_name = "default",
    target_name = "dev",
    profile_mapping = GoogleCloudServiceAccountFileProfileMapping(
        conn_id = 'pvl_bigquery',
        profile_args = { 
            "project": "pvlsson",
            "dataset": "pvl_dwh_mart",
            "keyfile": f"{airflow_home}/secrets/pvlsson-34e37e7a3ab1.json"
        },
    )
)

bq_dbt = DbtDag(
    dag_id="bq_dbt",
    project_config=ProjectConfig(
        f"{airflow_home}/dags/dbt/pvl_dwh",
    ),
    profile_config=profile_config,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
    ),
)