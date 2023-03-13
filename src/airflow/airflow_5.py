import json
from datetime import datetime, timedelta
from textwrap import dedent

import pandas as pd
import requests
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]

def extract_cats_data(url:str,ti)->None:
    """
    xcom_push will save the fetched results to Airflow’s database
    :param path
    :param ti
    :return:
    """
    res=requests.get(url)
    json_data=json.loads(res.content)
    ti.xcom_push(key='extracted_cats_data', value=json_data)

def transform_cats_data(ti)->None:
    """
    a dummy function to do xcom_pull which will get the data and transform it to our requirements
    :param ti:
    :return:
    """
    cats_data=ti.xcom_pull(key='extracted_cats_data',task_ids=['extracted_cats_data'])[0]
    transformed_cats_data=[]
    for cat in cats_data:
        transformed_cats_data.append({
            'fact':cat['fact'],
            'length':cat['length']
        })
    ti.xcom_push(key='transformed_cats_data', value=transformed_cats_data)

def load_cats_data(path:str,ti)->None:
    """
    xcom_pull to get the transformed cats data and save it to a CSV file
    :param path:
    :param ti:
    :return:
    """
    cats_data=ti.xcom_pull(key='transformed_cats_data', task_ids=['transform_cats_data'])
    cats_data_df=pd.DataFrame(cats_data[0])
    cats_data_df.to_csv(path,index=None)

# [START instantiate_dag]
with DAG(
        dag_id="etl_cats_data",
        # [START default_args]
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            'start_date': datetime(year=2023,month=3, day=14),
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        # [END default_args]
        description="ETL Pipeline for processing Cats data",
        #schedule=timedelta(days=1),
        schedule_interval="@daily",
        # start_date=datetime(2023, 2, 19),
        catchup=False,
        tags=["example"],
) as dag:
    task_extract_cats_data=PythonOperator(
        task_id="extract_cats_data",
        python_callable=extract_cats_data,
        op_kwargs={'url':'https://catfact.ninja/fact'}
    )

    task_transform_cats_data = PythonOperator(
        task_id="transform_cats_data",
        python_callable=transform_cats_data
    )

    task_load_cats_data = PythonOperator(
    task_id="load_cats_data",
    python_callable=load_cats_data,
    op_kwargs = {'path': 'home/airflow/airflow/data/cats_data.csv'}
    )
    task_extract_cats_data>>task_transform_cats_data>>task_load_cats_data

# [END tutorial]
