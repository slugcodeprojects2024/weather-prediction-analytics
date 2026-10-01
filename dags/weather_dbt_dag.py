"""Weather Prediction Analytics Lab: dbt transform leg (run -> test -> snapshot).

This DAG has schedule=None: it never fires on its own clock. It is started
only by `weather_etl_to_snowflake`'s TriggerDagRunOperator, once that ETL
DAG has verified a fresh load into RAW.WEATHER_FORECAST. That is what keeps
the dbt DAG scheduled strictly after the ETL DAG rather than racing it.

Uses a BashOperator + Snowflake keypair-auth pattern: dbt's
Snowflake connection details are pulled from the `snowflake_conn` Airflow
Connection at DAG-parse time and passed to dbt as DBT_* environment
variables, so no credentials are duplicated in this file or in dbt's own
profiles.yml.

The private key file itself must exist on disk at dbt/rsa_key.p8 in this
repo (never commit it -- see .gitignore), since dbt/ is the one extra
directory docker-compose.yaml already mounts into the Airflow containers,
at /opt/airflow/dbt.
"""

from pendulum import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"

conn = BaseHook.get_connection("snowflake_conn")

with DAG(
    dag_id="weather_dbt_transform",
    start_date=datetime(2026, 1, 1),
    description="dbt run/test/snapshot for the weather analytics models. Triggered by weather_etl_to_snowflake.",
    schedule=None,
    catchup=False,
    tags=["weather", "dbt"],
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_PRIVATE_KEY_PASSPHRASE": conn.password,
            "DBT_PRIVATE_KEY_PATH": "/opt/airflow/dbt/rsa_key.p8",
            "DBT_SCHEMA": "analytics",
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role", "ACCOUNTADMIN"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake",
        }
    },
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test >> dbt_snapshot
