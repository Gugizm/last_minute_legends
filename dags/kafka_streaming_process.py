import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="kafka_streaming_process_pipeline",
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval="0 11 * * *",
    catchup=False,
):
    batch_processor = BashOperator(
        task_id="batch_processor",
        bash_command="python3 /usr/local/app/services/spark_batch_processor.py"
    )

    stream_processor = BashOperator(
        task_id="stream_processor",
        bash_command="python3 /usr/local/app/services/spark_streaming_processor.py"
    )

    batch_processor
    # [batch_processor, stream_processor]