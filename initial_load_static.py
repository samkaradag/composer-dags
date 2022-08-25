import json
from datetime import timedelta
import airflow
from airflow import DAG
import sys
import requests
import yaml
from google.cloud import bigquery
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

schedule_interval = "00 01 * * *"

dag = DAG('initial_load_all',catchup=False, default_args=default_args, schedule_interval=schedule_interval)

start_initial_load = DummyOperator(
    task_id='start_daily_initial_load',
    dag=dag
)

finish_initial_load = DummyOperator(
    task_id='finish_daily_initial_load',
    trigger_rule='all_done',
    dag=dag
)


table1_gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='table1_gcs_to_bq_load',
    bucket='elt-demo-source',
    source_objects=['table1.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table='staging.table1',
    dag=dag)

table2_gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='table2_gcs_to_bq_load',
    bucket='elt-demo-source',
    source_objects=['table2.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table='staging.table2',
    dag=dag)

table3_gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='table3_gcs_to_bq_load',
    bucket='elt-demo-source',
    source_objects=['table3.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table='staging.table3',
    dag=dag)


table1_gcs_to_bq_load.set_upstream(start_initial_load)
finish_initial_load.set_upstream(table1_gcs_to_bq_load)

table3_gcs_to_bq_load.set_upstream(start_initial_load)
finish_initial_load.set_upstream(table3_gcs_to_bq_load)

table2_gcs_to_bq_load.set_upstream(start_initial_load)
finish_initial_load.set_upstream(table2_gcs_to_bq_load)
