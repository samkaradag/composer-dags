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

dag = DAG('full_load_curate_parametric',catchup=False, default_args=default_args, schedule_interval=schedule_interval)

start_initial_load = DummyOperator(
    task_id='start_initial_load',
    dag=dag
)

finish_initial_load = DummyOperator(
    task_id='finish_initial_load',
    trigger_rule='all_done',
    dag=dag
)

def set_params(ti, **kwargs):
    # Set source CSV file
    if 'SourceCSV' in kwargs['dag_run'].conf:
        SourceCSV = kwargs['dag_run'].conf['SourceCSV']
    else:
        SourceCSV = "skt_mart.csv"
    ti.xcom_push(key='SourceCSV', value=SourceCSV)

    # Set target table
    if 'TargetTable' in kwargs['dag_run'].conf:
        TargetTable = kwargs['dag_run'].conf['TargetTable']
    else:
        TargetTable = "skt_mart"
    ti.xcom_push(key='TargetTable', value=TargetTable)


    # Set table schema
    if 'TableSchema' in kwargs['dag_run'].conf:
        TableSchema = kwargs['dag_run'].conf['TableSchema']
    else:
        TableSchema = {
            'DUMMY_ID':'INTEGER',
        }
    ti.xcom_push(key='TableSchema', value=TableSchema)

    # set table keys
    if 'TableKeys' in kwargs['dag_run'].conf:
        TableKeys = kwargs['dag_run'].conf['TableKeys']
    else:
        TableKeys = {'DUMMY_KEY1','DUMMY_KEY2'}
    ti.xcom_push(key='TableKeys', value=TableKeys)

    # Set staging and curated tables
    PROJECT_ID = Variable.get('PROJECT_ID')
    STAGING_DATASET = Variable.get('STAGING_DATASET')
    CURATED_DATASET = Variable.get('CURATED_DATASET')

    table_bq_staging = PROJECT_ID + '.' + STAGING_DATASET + '.' + TargetTable
    ti.xcom_push(key='StagingTable', value=table_bq_staging)

    table_bq_curated = PROJECT_ID + '.' + CURATED_DATASET + '.' + TargetTable
    ti.xcom_push(key='CuratedTable', value=table_bq_curated)


config = PythonOperator(
    task_id='config',
    python_callable=set_params,
    dag=dag,
)

gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_load',
    bucket="{{var.value.GCS_BUCKET}}",
    source_objects=["{{ti.xcom_pull(key='SourceCSV', task_ids='config')}}"],
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    destination_project_dataset_table="{{var.value.STAGING_DATASET}}.{{ti.xcom_pull(key='TargetTable', task_ids='config')}}",
    dag=dag)


config.set_upstream(start_initial_load)
gcs_to_bq_load.set_upstream(config)
finish_initial_load.set_upstream(gcs_to_bq_load)




# Start Curation with Merge
start_curation = DummyOperator(
    task_id='start_curation',
    dag=dag
)

finish_curation = DummyOperator(
    task_id='finish_curation',
    trigger_rule='all_done',
    dag=dag
)

#LOAD SKT_RES 
def _create_merge_sql( ti, **kwargs):
    source = ti.xcom_pull(key='StagingTable', task_ids='config')
    target = ti.xcom_pull(key='CuratedTable', task_ids='config')
    schema = ti.xcom_pull(key='TableSchema', task_ids='config')
    keys = ti.xcom_pull(key='TableKeys', task_ids='config')

    columns = [item for item in schema]
    columns = ",".join(columns)

    keys_list = [item for item in keys]
    keys_list = ",".join(keys_list)

    # nonkey_columns = schema.remove(keys)
    nonkey_columns = {}
    for k, v in schema.items():
        if k not in keys:
            nonkey_columns[k] = v

    # Build upper key aliasses and join condition based on the keys 
    key_join_cond = ['T.' + key + ' = S.' + key for key in keys]
    key_join_cond = " AND ".join(key_join_cond)

    # Build join condition based on the entities (non key columns) to track updated records and build update clause
    nonkey_join_cond = []
    update_cond = []
    for key, value in nonkey_columns.items():
        nullvalue = '0'
        if value == 'STRING':
            nullvalue = "'NULL'"
        elif value == 'INTEGER' or value == 'FLOAT': 
            nullvalue = '0'
        elif value == 'DATE' or value == 'DATETIME': 
            nullvalue = "'01/01/1900'"
        nonkey_join_cond.append('IFNULL(T.' + key + "," + nullvalue + ") <> IFNULL(S." + key + "," + nullvalue + ")")
        update_cond.append('T.' + key + ' = S.' + key)


    nonkey_join_cond = " OR ".join(nonkey_join_cond)
    nonkey_join_cond = "(" + nonkey_join_cond + ")"
    update_cond = ", ".join(update_cond)

    #Build update clause


    merge_sql = f"""
        MERGE INTO `{target}` T
        USING `{source}` S
        ON {key_join_cond}
        WHEN MATCHED AND {nonkey_join_cond} THEN UPDATE
        SET {update_cond}
        WHEN NOT MATCHED THEN 
          INSERT ({columns})
          VALUES ({columns})
    """

    ti.xcom_push(key='MergeSQL', value=merge_sql)

# merge

prep_merge_sql = PythonOperator(
    task_id='prep_merge_sql',
    python_callable=_create_merge_sql,
    dag=dag,
)

curate_table = BigQueryOperator(
    task_id=f"merge_table_into_curated",
    sql="{{ti.xcom_pull(key='MergeSQL', task_ids='prep_merge_sql')}}",
    use_legacy_sql=False,
    dag=dag
)
###########Curate FINISH

#skt_res_zahl
start_curation.set_upstream(finish_initial_load)
prep_merge_sql.set_upstream(start_curation)
curate_table.set_upstream(prep_merge_sql)
finish_curation.set_upstream(curate_table)