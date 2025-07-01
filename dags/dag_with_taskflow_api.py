from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 2,
    'retry_delay': timedelta(minutes=4)
}

@dag(dag_id="DAG_taskflowAPI",
     default_args=default_args,
     start_date=datetime(2025, 7, 1, 14),
     schedule=timedelta(weeks=2)
    )
def pipeline():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Lakshit',
            'last_name': 'Paliwal'
        }
    
    @task()
    def get_age():
        return 15
    
    @task()
    def greet(first_name, last_name, age):
        print(f"My name is {first_name} {last_name} and I am {age} years old.")
    
    name = get_name()
    age = get_age()
    greet(first_name=name['first_name'], last_name=name['last_name'], age=age)

dag = pipeline()