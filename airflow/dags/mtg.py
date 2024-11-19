from airflow import DAG
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from datetime import datetime

args = {
    'owner': 'airflow'
}

dag = DAG('MTG', default_args=args, description='MTG API', schedule_interval=None, start_date=datetime(2024, 11, 10), catchup=False, max_active_runs=1)

hiveQL_create_table_mtg_raw_data = '''
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_raw_data (
    id STRING, 
    name STRING, 
    manaCost STRING, 
    colors ARRAY<STRING>, 
    type STRING, 
    rarity STRING, 
    setName STRING, 
    text STRING
)
STORED AS TEXTFILE
LOCATION '/user/hadoop/raw/mtg_cards';
'''

create_mtg_names_table_sql = '''
CREATE TABLE IF NOT EXISTS mtg_names (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
'''

insert_mtg_names_sql = '''
INSERT INTO mtg_names (name)
SELECT name
FROM mtg_raw_data;
'''

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

download_mtg_cards = HttpDownloadOperator(
    task_id='download_mtg_cards',
    download_uri='https://api.magicthegathering.io/v1/cards',
    save_to='/home/airflow/mtg/raw/cards_{{ ds }}.json',
    dag=dag,
)

hdfs_create_cards_raw_dir = HdfsMkdirFileOperator(
    task_id='hdfs_mkdir_raw_cards',
    directory='/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_create_cards_final_dir = HdfsMkdirFileOperator(
    task_id='hdfs_mkdir_final_cards',
    directory='/user/hadoop/mtg/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_mtg_data = HdfsPutFileOperator(
    task_id='upload_mtg_data_to_hdfs',
    local_file='/home/airflow/mtg/raw/cards_{{ ds }}.json',
    remote_file='/user/hadoop/mtg/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/cards_{{ ds }}.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

pyspark_mtg_important_data = SparkSubmitOperator(
    task_id='pyspark_mtg_important_data_to_final',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_mtg_important_data.py',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='1g',
    num_executors='1',
    driver_memory='2g',
    name='spark_get_mtg_important_data',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/mtg/raw', '--hdfs_target_dir', '/user/hadoop/mtg/final/mtg_cards_final', '--hdfs_target_format', 'json'],
    dag = dag
)

pyspark_export_cards = SparkSubmitOperator(
    task_id='pyspark_export_cards_to_postgresql',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_export_cards_db.py',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='1g',
    num_executors='1',
    driver_memory='2g',
    name='spark_export_cards_to_postgresql',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
)

create_local_mtg_dir >> create_local_raw_dir >> create_local_final_dir >> clear_local_raw_dir >> clear_local_final_dir >> download_mtg_cards >> hdfs_create_cards_raw_dir >> hdfs_create_cards_final_dir >> hdfs_put_mtg_data >> pyspark_mtg_important_data >> pyspark_export_cards