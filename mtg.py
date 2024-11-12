from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from datetime import datetime, timedelta
import requests
import json
import os

args = {
    'owner': 'airflow'
}

dag = DAG('MTG', default_args=args, description='MTG API', schedule_interval=None, start_date=datetime(2024, 11, 10), catchup=False, max_active_runs=1)

def call_api():
    url = "https://api.magicthegathering.io/v1/cards"
    response = requests.get(url)
    data = response.json()
    output_dir = '/home/airflow/mtg/raw'
    os.makedirs(output_dir, exist_ok=True) # Ensure directory exists
    output_file = os.path.join(output_dir, "mtg_cards_raw.json")
    with open(output_file, "w") as file:
        json.dump(data, file)

create_local_mtg_dir = CreateDirectoryOperator(
    task_id='create_mtg_dir',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
)

create_local_raw_dir = CreateDirectoryOperator(
    task_id='create_raw_dir',
    path='/home/airflow/mtg',
    directory='raw',
    dag=dag,
)

create_local_final_dir = CreateDirectoryOperator(
    task_id='create_final_dir',
    path='/home/airflow/mtg',
    directory='final',
    dag=dag,
)

clear_local_raw_dir = ClearDirectoryOperator(
    task_id='clear_raw_dir',
    directory='/home/airflow/mtg/raw',
    pattern='*',
    dag=dag,
)

clear_local_final_dir = ClearDirectoryOperator(
    task_id='clear_final_dir',
    directory='/home/airflow/mtg/final',
    pattern='*',
    dag=dag,
)

api_call_task = PythonOperator(
    task_id='api_call_task',
    python_callable=call_api,
    dag=dag,
)

create_local_mtg_dir >> create_local_raw_dir >> create_local_final_dir >> clear_local_raw_dir >> clear_local_final_dir  >> api_call_task
