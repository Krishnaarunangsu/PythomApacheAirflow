from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]

def print_hello_krishna():
    """

    :return:
    """
    print("Hello Krishna")

# [START instantiate_dag]
with DAG(
        "tutorial-krishna",
        # [START default_args]
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        # [END default_args]
        description="A simple tutorial DAG",
        #schedule=timedelta(days=1),
        schedule="@hourly",
        start_date=datetime(2023, 10, 16),
        catchup=False,
        tags=["example"],
) as dag:
    task_1=PythonOperator(
        task_id="Hello_World",
        python_callable=print_hello_krishna()
    )
task_1
# [END tutorial]
