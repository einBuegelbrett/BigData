from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from datetime import datetime, timedelta
import requests
import json

args = {
    'owner': 'airflow'
}

dag = DAG('MTG', default_args=args, description='MTG API',

schedule_interval='56 18 * * *',
start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

def call_api():
    url = "https://api.magicthegathering.io/v1/cards"
    response = requests.get(url)
    data = response.json()
    output_dir = '/home/airflow/mtg/raw'
    os.makedirs(output_dir, exist_ok=True) # Ensure directory exists
    output_file = os.path.join(output_dir, "mtg_cards_raw.json")
    with open(output_file, "w") as file:
        json.dump(data, file)

def output_json_file():
    input_file = '/home/airflow/mtg/mtg_cards_raw.json'
    print("test")
    with open(input_file, "r") as file:
        data = json.load(file)
        # Extract the first element from the list of cards
        first_card = data['cards'][0] if 'cards' in data and data['cards'] else None
        print("First card data:", first_card)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/mtg',
    pattern='*',
    dag=dag,
)

api_call_task = PythonOperator(
    task_id='api_call_task',
    python_callable=call_api,
    dag=dag,
)

output_json_task = PythonOperator(
    task_id='output_json_task',
    python_callable=output_json_file,
    dag=dag,
)

create_local_import_dir >> clear_local_import_dir >> api_call_task >> output_json_task
