import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="snowflake_reading",
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval="0 11 * * *",
    catchup=False,
):
    snowflake_reader = BashOperator(
        task_id="snowflake_reader",
        bash_command="python3 /usr/local/app/spark_applications/read_snowflake_data.py",
    )

    snowflake_reader
