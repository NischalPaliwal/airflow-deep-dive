from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="DAG005",
    default_args=default_args,
    start_date=datetime(2025, 7, 1, 15),
    schedule=timedelta(days=4),
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id="T1",
        bash_command="echo This is a bash terminal command!"
    )

    # airflow backfill create --dag-id DAG005 --from-date 2025-07-01 --to-date 2025-07-08