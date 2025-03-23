from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

from cosmos.providers.dbt.core.operators import (
    DbtDepsOperator,
    DbtRunOperationOperator,
    DbtSeedOperator,
)

with DAG(
    dag_id="import-seeds",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    project_seeds = [
        {
            "project": "pvl-dwh",
            "seeds": ["imdb_ratings", "kp_ratings", "updated_imdb_ratings"],
        }
    ]

    deps_install = DbtDepsOperator(
        task_id="pvl_dwh_install_deps",
        project_dir=f"/usr/local/airflow/dbt/pvl-dwh",
        schema="public",
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        conn_id="postgres",
    )

    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for project in project_seeds:
            for seed in project["seeds"]:
                DbtRunOperationOperator(
                    task_id=f"drop_{seed}_if_exists",
                    macro_name="drop_table",
                    args={"table_name": seed},
                    project_dir=f"/usr/local/airflow/dbt/{project['project']}",
                    schema="public",
                    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                    conn_id="postgres",
                )

    create_seeds = DbtSeedOperator(
        task_id=f"pvl_dwh_seed",
        project_dir=f"/usr/local/airflow/dbt/pvl-dwh",
        schema="public",
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        conn_id="postgres",
        outlets=[Dataset(f"SEED://PVL-DWH")],
     )

    deps_install >> drop_seeds >> create_seeds