from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="DAG007",
    default_args=default_args,
    start_date=datetime(2025, 7, 1, 15),
    schedule='@daily',
    catchup=False
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id="T1",
        conn_id="my_postgres",
        sql='''
        CREATE TABLE IF NOT EXISTS dag_runs(
            dt date,
            dag_id VARCHAR(20),
            PRIMARY KEY (dt, dag_id)
        );
        ''',
        autocommit=True
    )

    task1