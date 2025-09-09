from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

BASE_PATH = '/opt/airflow/dags'

default_args = {
    'owner': 'rudenko',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'rudenko_data_lake_pipeline',
    default_args=default_args,
    description='Batch Data Lake Pipeline: Landing → Bronze → Silver → Gold',
    schedule_interval=None,
    catchup=False,
    tags=['rudenko', 'data_lake', 'batch'],
    max_active_runs=1,
)

landing_to_bronze = BashOperator(
    task_id='landing_to_bronze',
    bash_command=f'cd /opt/airflow && python {BASE_PATH}/landing_to_bronze.py',
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command=f'cd /opt/airflow && python {BASE_PATH}/bronze_to_silver.py',
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command=f'cd /opt/airflow && python {BASE_PATH}/silver_to_gold.py',
    dag=dag,
)

landing_to_bronze >> bronze_to_silver >> silver_to_gold
