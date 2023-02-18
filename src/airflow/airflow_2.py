from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def my_func():
    """
    print
    :return:
    """
    print('Hello from my_func')


with DAG('python_dag', description='Python DAG', schedule_interval='*/5 * * * *', start_date=datetime(2023, 2, 19),
         catchup=False) as dag:
    dummy_task = EmptyOperator(task_id='dummy_task', retries=3)
    python_task = PythonOperator(task_id='python_task', python_callable=my_func)
dummy_task >> python_task
