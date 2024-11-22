from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from datetime import datetime

args = {
    'owner': 'airflow'
}

dag = DAG('MTG', default_args=args, description='MTG API', schedule_interval=None, start_date=datetime(2024, 11, 10), catchup=False, max_active_runs=1)

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

pyspark_get_cards = SparkSubmitOperator(
    task_id='pyspark_download_cards',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_get_cards.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    driver_memory='2g',
    name='spark_download_cards',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
)

pyspark_mtg_important_data = SparkSubmitOperator(
    task_id='pyspark_mtg_important_data_to_final',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_mtg_important_data.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    driver_memory='2g',
    name='spark_get_mtg_important_data',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
)

pyspark_export_cards = SparkSubmitOperator(
    task_id='pyspark_export_cards_to_postgresql',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_export_cards_db.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    driver_memory='2g',
    name='spark_export_cards_to_postgresql',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}'],
    dag = dag
)

hdfs_create_cards_raw_dir >> hdfs_create_cards_final_dir >> pyspark_get_cards >> pyspark_mtg_important_data >> pyspark_export_cards