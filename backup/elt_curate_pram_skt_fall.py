import json
from datetime import timedelta
import airflow
from airflow import DAG
import sys
import requests
import yaml
from google.cloud import bigquery
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import  DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# Set Project and dataset vairables
PROJECT_ID='elt-demo-354311'
STAGING_DS='staging'
CURATED_DS='curated'

# Set default load mode
P_MODE='INITIAL'
SKT_FALL_LOAD_MODE='SCD2-HISTORY'
SKT_MART_LOAD_MODE='WRITE_TRUNCATE'
SKT_RES_ZAHL_LOAD_MODE='WRITE_TRUNCATE'

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

dag = DAG('curate',catchup=False, default_args=default_args, schedule_interval=schedule_interval)

# Update load mode if there is an input config
def get_parameters(**kwargs):
    dag_run = kwargs.get('dag_run')
    # parameters = dag_run.conf['key']
    # return parameters

    if dag_run:
        P_MODE=dag_run.conf['P_MODE']

        if P_MODE == 'INITIAL':
            SKT_FALL_LOAD_MODE='WRITE_TRUNCATE'
            SKT_MART_LOAD_MODE='WRITE_TRUNCATE'
            SKT_RES_ZAHL_LOAD_MODE='WRITE_TRUNCATE'
        elif P_MODE == 'DELTA':
            SKT_MART_LOAD_MODE='WRITE_APPEND'
            SKT_FALL_LOAD_MODE='SCD2-HISTORY'
            SKT_RES_ZAHL_LOAD_MODE='MERGE'

set_params = PythonOperator(
    task_id='set_params',
    provide_context=True,
    python_callable=get_parameters,
    dag=dag,
)

start_curation = DummyOperator(
    task_id='start_curation',
    dag=dag
)

finish_curation = DummyOperator(
    task_id='finish_curation',
    trigger_rule='all_done',
    dag=dag
)

def _create_SCD2_merge_sql(source, target, schema, keys, **context):
    columns = [item for item in schema]
    columns = ",".join(columns)

    keys_list = [item for item in keys]
    keys_list = ",".join(keys_list)

    # nonkey_columns = schema.remove(keys)
    nonkey_columns = {}
    for k, v in schema.items():
        if k not in keys:
            nonkey_columns[k] = v

    # Build INNER join condition based on the keys
    inner_key_join_cond = ['T.' + key + ' = S.' + key for key in keys]
    inner_key_join_cond = " AND ".join(inner_key_join_cond)

    # Build upper key aliasses and join condition based on the keys 
    i = 0
    keyalias = []
    nullkeys = []
    key_join_cond = []
    for key in keys:
        keyalias.append(key + ' AS JK' + str(i))
        nullkeys.append('NULL' + ' AS JK' + str(i))
        key_join_cond.append('T.' + key + ' = S.JK' + str(i))
        i += 1
    keyalias = " , ".join(keyalias)
    nullkeys = " , ".join(nullkeys)

    # key_join_cond = ['T.' + key + ' = S.' + key for key in keys]
    key_join_cond = " AND ".join(key_join_cond)

    # Build join condition based on the entities (non key columns) to track updated records
    nonkey_join_cond = []
    for key, value in nonkey_columns.items():
        nullvalue = '0'
        if value == 'STRING':
            nullvalue = "'NULL'"
        elif value == 'INTEGER' or value == 'FLOAT': 
            nullvalue = '0'
        elif value == 'DATE' or value == 'DATETIME': 
            nullvalue = "'01/01/1900'"
        nonkey_join_cond.append('IFNULL(T.' + key + "," + nullvalue + ") <> IFNULL(S." + key + "," + nullvalue + ")")

    nonkey_join_cond = " OR ".join(nonkey_join_cond)
    nonkey_join_cond = "(" + nonkey_join_cond + ")"
    nonkey_join_cond_w_end_date =  nonkey_join_cond + " AND T.end_date IS NULL"

    return f"""
        MERGE INTO `{target}` T
        USING (
            SELECT {keyalias}, * FROM `{source}` S
            UNION ALL
            SELECT {nullkeys},S.* FROM `{source}` S
            JOIN `{target}` T
            ON {inner_key_join_cond} AND ({nonkey_join_cond_w_end_date}) 
        ) S
        ON {key_join_cond}
        WHEN MATCHED AND {nonkey_join_cond} THEN UPDATE
        SET end_date = CURRENT_DATETIME()
        WHEN NOT MATCHED THEN 
          INSERT ({columns}, start_date, end_date)
          VALUES ({columns}, CURRENT_DATETIME(), NULL)
    """

# Load SKT_FALL from staging to curated
# Initial load
initial_load_skt_fall = BigQueryOperator(
    task_id='initial_load_skt_fall',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql=f'''
    #standardSQL
    SELECT *, CURRENT_DATETIME() AS start_date, DATETIME "9999-12-31 23:59:59"  AS end_date FROM `{PROJECT_ID}.{STAGING_DS}.skt_fall`''', destination_dataset_table=f'{CURATED_DS}.skt_fall',
    dag=dag)

# incremental load SCD2
table_name='skt_fall'
schema = {'SCHADEN_NR':'INTEGER','STICH_DAT':'STRING','SCHADEN_DAT':'STRING','SCHAD_ANL_DAT':'STRING','SCHAD_ERL_DAT':'STRING','SPARTE_HGB':'INTEGER','SCHAD_RES_BETR':'FLOAT','ZAHL_MONAT_BETR':'FLOAT'}
keys = {'SCHADEN_NR','STICH_DAT'}
bq_staging=PROJECT_ID + '.' + STAGING_DS + '.' + table_name
bq_curated=PROJECT_ID + '.' + CURATED_DS + '.' + table_name

curate_skt_fall = BigQueryOperator(
    task_id=f"scd2_merge_skt_fall_into_curated",
    sql=_create_SCD2_merge_sql(bq_staging, bq_curated, schema,keys),
    use_legacy_sql=False,
    dag=dag
)

def set_skt_fall_task( **kwargs):
    if 'P_MODE' in kwargs['dag_run'].conf:
        load_mode = kwargs['dag_run'].conf['P_MODE']
    else:
        load_mode = 'DELTA'
    
    if load_mode == 'INITIAL':
        return 'initial_load_skt_fall'
    elif load_mode == 'DELTA':
        return 'scd2_merge_skt_fall_into_curated'

    

branch_skt_fall = BranchPythonOperator(
    task_id='branch_skt_fall',
    python_callable=set_skt_fall_task,
    # op_kwargs={"load_mode": "{{ dag_run.conf['P_MODE'] }}"},
    dag=dag,
)
#######

# Load SKT_MART from staging to curated
curate_skt_mart = BigQueryOperator(
    task_id='curate_skt_mart',
    use_legacy_sql=False,
    write_disposition=SKT_MART_LOAD_MODE,
    allow_large_results=True,
    sql=f'''
    #standardSQL
    SELECT * FROM `{PROJECT_ID}.{STAGING_DS}.skt_mart`''', destination_dataset_table=f'{CURATED_DS}.skt_mart',
    dag=dag)


set_params.set_upstream(start_curation)

curate_skt_mart.set_upstream(set_params)
finish_curation.set_upstream(curate_skt_mart)

branch_skt_fall.set_upstream(set_params)
initial_load_skt_fall.set_upstream(branch_skt_fall)
curate_skt_fall.set_upstream(branch_skt_fall)

finish_curation.set_upstream(initial_load_skt_fall)
finish_curation.set_upstream(curate_skt_fall)

# if SKT_FALL_LOAD_MODE == 'WRITE_TRUNCATE':
#     initial_load_skt_fall.set_upstream(set_params)
#     finish_curation.set_upstream(initial_load_skt_fall)
# elif SKT_FALL_LOAD_MODE == 'SCD2-HISTORY':
#     curate_skt_fall.set_upstream(set_params)
#     finish_curation.set_upstream(curate_skt_fall)


