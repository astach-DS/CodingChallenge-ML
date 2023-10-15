from airflow.decorators import dag, task
from datetime import datetime

@dag(
    'xcom_dag', 
    start_date = datetime(2023, 9, 28),
    schedule_interval=None
)
def taskflow():

    @task
    def peter_task(ti=None):
        ti.xcom_push(key='mobile_phone', value='iphone')

    @task
    def pepi_task(ti=None):
        phone = ti.xcom_pull(task_ids='peter_task', key='mobile_phone')
        print(phone)
    
    peter_task() >> pepi_task()

taskflow()