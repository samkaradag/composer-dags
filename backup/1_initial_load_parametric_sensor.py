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
from airflow.models import Variable



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

dag = DAG('sensor_initial_load_parametric',catchup=False, default_args=default_args, schedule_interval=schedule_interval)

start_initial_load = DummyOperator(
    task_id='start_initial_load',
    dag=dag
)

finish_initial_load = DummyOperator(
    task_id='finish_initial_load',
    trigger_rule='all_done',
    dag=dag
)



def set_params(ti,**kwargs):
    if 'SourceCSV' in kwargs['dag_run'].conf:
        SourceCSV = kwargs['dag_run'].conf['SourceCSV']
    else:
        SourceCSV = "skt_mart.csv"

    if 'TargetTable' in kwargs['dag_run'].conf:
        TargetTable = kwargs['dag_run'].conf['TargetTable']
    else:
        TargetTable = "skt_mart"

    if 'TargetTable' in kwargs['dag_run'].conf:
        TargetTable = kwargs['dag_run'].conf['TargetTable']
    else:
        TargetTable = "skt_mart"
    
    ti.xcom_push(key='SourceCSV', value=SourceCSV)
    ti.xcom_push(key='TargetTable', value=TargetTable)


config = PythonOperator(
    task_id='config',
    python_callable=set_params,
    dag=dag,
)

gcs_sensor_task = GCSObjectExistenceSensor(
    task_id="gcs_object_sensor",
    bucket={{var.value.GCS_BUCKET}},
    object='my_data_01082020.csv',
    dag=dag
)

gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_load',
    bucket={{var.value.GCS_BUCKET}},
    source_objects=["{{ti.xcom_pull(key='SourceCSV', task_ids='config')}}"],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table="{{var.value.STAGING_DATASET}}.{{ti.xcom_pull(key='TargetTable', task_ids='config')}}",
    dag=dag)


config.set_upstream(start_initial_load)
gcs_to_bq_load.set_upstream(config)
finish_initial_load.set_upstream(gcs_to_bq_load)

