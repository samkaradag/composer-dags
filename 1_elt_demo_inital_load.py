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


skt_mart_gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='skt_mart_gcs_to_bq_load',
    bucket='elt-demo-source',
    source_objects=['skt_mart.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table='staging.skt_mart',
    dag=dag)

skt_fall_gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='skt_fall_gcs_to_bq_load',
    bucket='elt-demo-source',
    source_objects=['skt_fall.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table='staging.skt_fall',
    dag=dag)

skt_res_zahl_gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='skt_res_zahl_gcs_to_bq_load',
    bucket='elt-demo-source',
    source_objects=['skt_res_zahl.csv'],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table='staging.skt_res_zahl',
    dag=dag)


skt_mart_gcs_to_bq_load.set_upstream(start_initial_load)
finish_initial_load.set_upstream(skt_mart_gcs_to_bq_load)

skt_res_zahl_gcs_to_bq_load.set_upstream(start_initial_load)
finish_initial_load.set_upstream(skt_res_zahl_gcs_to_bq_load)

skt_fall_gcs_to_bq_load.set_upstream(start_initial_load)
finish_initial_load.set_upstream(skt_fall_gcs_to_bq_load)

# truncate_load = BigQueryOperator(
#     task_id='bq_ingest_signal_actions_current',
#     use_legacy_sql=False,
#     write_disposition='WRITE_TRUNCATE',
#     allow_large_results=True,
#     bql='''
#     #standardSQL
#     SELECT *, date('{{ ds }}') LOAD_DATE FROM `be-signalread-prd-3097.signal_reporting_be.actions_current`
#     ''',    destination_dataset_table='psa_signal.actions_current',
#     dag=dag)



