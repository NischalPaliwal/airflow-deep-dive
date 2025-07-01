from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 2,
    'retry_delay': timedelta(minutes=4)
}

@dag()
def pipeline():
    pass