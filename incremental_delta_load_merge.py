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
TABLE2_LOAD_MODE='SCD2-HISTORY'
TABLE1_LOAD_MODE='WRITE_TRUNCATE'
TABLE3_LOAD_MODE='WRITE_TRUNCATE'

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

dag = DAG('delta_load',catchup=False, default_args=default_args, schedule_interval=schedule_interval)


start_curation = DummyOperator(
    task_id='start_curation',
    dag=dag
)

finish_curation = DummyOperator(
    task_id='finish_curation',
    trigger_rule='all_done',
    dag=dag
)



# Load TABLE2 from staging to curated
# Initial load table2
initial_load_table2 = BigQueryOperator(
    task_id='initial_load_table2',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql=f'''
    #standardSQL
    SELECT *, CURRENT_DATETIME() AS start_date, DATETIME "9999-12-31 23:59:59"  AS end_date FROM `{PROJECT_ID}.{STAGING_DS}.table2`''', destination_dataset_table=f'{CURATED_DS}.table2',
    dag=dag)


# prepare merge statement for table2
def _create_SCD2_merge_sql(source, target, schema, keys, ti, **kwargs):

    if 'P_STICH_DAT' in kwargs['dag_run'].conf:
        filter_mode=True
        filter_value = kwargs['dag_run'].conf['P_STICH_DAT']
        filter_clause = "WHERE S.STICH_DAT='" + filter_value +"' "
    else:
        filter_mode=False
        filter_clause = ""

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
        key_join_cond.append('T.' + key + ' = S.JK' + str(i))

        nullvalue = '0'
        if schema[key] == 'STRING':
            nullvalue = "'NULL'"
        elif schema[key] == 'INTEGER' or schema[key] == 'FLOAT': 
            nullvalue = '0'
        elif schema[key] == 'DATE' or schema[key] == 'DATETIME': 
            nullvalue = "'01/01/1900'"
        nullkeys.append(nullvalue + ' AS JK' + str(i))

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
    nonkey_join_cond_w_end_date =  nonkey_join_cond + " AND T.end_date = DATETIME \"9999-12-31 23:59:59\""

    merge_sql =  f"""
        MERGE INTO `{target}` T
        USING (
            SELECT {keyalias}, * FROM `{source}` S {filter_clause}
            UNION ALL
            SELECT {nullkeys},S.* FROM `{source}` S 
            JOIN `{target}` T
            ON {inner_key_join_cond} AND ({nonkey_join_cond_w_end_date}) 
            {filter_clause}
        ) S
        ON {key_join_cond}
        WHEN MATCHED AND {nonkey_join_cond} THEN UPDATE
        SET end_date = CURRENT_DATETIME()
        WHEN NOT MATCHED THEN 
          INSERT ({columns}, start_date, end_date)
          VALUES ({columns}, CURRENT_DATETIME(), DATETIME "9999-12-31 23:59:59")
    """

    ti.xcom_push(key='merge_sql', value=merge_sql)

table_name='table2'
schema = {'SCHADEN_NR':'INTEGER','STICH_DAT':'STRING','SCHADEN_DAT':'STRING','SCHAD_ANL_DAT':'STRING','SCHAD_ERL_DAT':'STRING','SPARTE_HGB':'INTEGER','SCHAD_RES_BETR':'FLOAT','ZAHL_MONAT_BETR':'FLOAT'}
keys = {'SCHADEN_NR','STICH_DAT'}
bq_staging=PROJECT_ID + '.' + STAGING_DS + '.' + table_name
bq_curated=PROJECT_ID + '.' + CURATED_DS + '.' + table_name

prep_merge = PythonOperator(
    task_id='prep_merge',
    python_callable=_create_SCD2_merge_sql,
    op_kwargs={"source": bq_staging, "target": bq_curated, "schema":schema, "keys":keys},
    dag=dag,
)

# incremental load SCD2
curate_table2 = BigQueryOperator(
    task_id=f"scd2_merge_table2_into_curated",
    sql="{{ti.xcom_pull(key='merge_sql', task_ids='prep_merge')}}" ,
    use_legacy_sql=False,
    dag=dag
)

def set_table2_task( **kwargs):
    if 'P_MODE' in kwargs['dag_run'].conf:
        load_mode = kwargs['dag_run'].conf['P_MODE']
    else:
        load_mode = 'DELTA'
    
    if load_mode == 'INITIAL':
        return 'initial_load_table2'
    elif load_mode == 'DELTA':
        return 'prep_merge'

branch_table2 = BranchPythonOperator(
    task_id='branch_table2',
    python_callable=set_table2_task,
    # op_kwargs={"load_mode": "{{ dag_run.conf['P_MODE'] }}"},
    dag=dag,
)
#######

# Load TABLE1 from staging to curated
curate_table1 = BigQueryOperator(
    task_id='curate_table1',
    use_legacy_sql=False,
    write_disposition=TABLE1_LOAD_MODE,
    allow_large_results=True,
    sql=f'''
    #standardSQL
    SELECT * FROM `{PROJECT_ID}.{STAGING_DS}.table1`''', destination_dataset_table=f'{CURATED_DS}.table1',
    dag=dag)


###############

#LOAD SKT_RES 
def _create_normal_merge_sql(source, target, schema, keys, **context):
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


    return f"""
        MERGE INTO `{target}` T
        USING `{source}` S
        ON {key_join_cond}
        WHEN MATCHED AND {nonkey_join_cond} THEN UPDATE
        SET {update_cond}
        WHEN NOT MATCHED THEN 
          INSERT ({columns})
          VALUES ({columns})
    """
# merge 
table3_table_name='table3'

table3_schema = {
'SCHADEN_NR':'INTEGER',
'SPARTE_HGB':'INTEGER',
'sparte_lob':'INTEGER',			
'GJ':'INTEGER',		
'SCHADEN_JAHR':'INTEGER',			
'SCHAD_RES_BETR':'FLOAT',			
'IS_RES_BETR':'FLOAT',		
'SS_RES_BETR':'FLOAT',		
'SCHAD_RES_ANZ':'INTEGER',			
'ZAHL_GJ_BETR':'FLOAT',		
'IS_ZAHL_GJ_BETR':'FLOAT',			
'SS_ZAHL_GJ_BETR':'FLOAT',
}

table3_keys = {'SCHADEN_NR','GJ'}

table3_bq_staging=PROJECT_ID + '.' + STAGING_DS + '.' + table3_table_name
table3_bq_curated=PROJECT_ID + '.' + CURATED_DS + '.' + table3_table_name

curate_table3 = BigQueryOperator(
    task_id=f"merge_table3_into_curated",
    sql=_create_normal_merge_sql(table3_bq_staging, table3_bq_curated, table3_schema, table3_keys),
    use_legacy_sql=False,
    dag=dag
)
###########TABLE3 FINISH

#table1
curate_table1.set_upstream(start_curation)
finish_curation.set_upstream(curate_table1)
#table3
curate_table3.set_upstream(start_curation)
finish_curation.set_upstream(curate_table3)

#table2
branch_table2.set_upstream(start_curation)
initial_load_table2.set_upstream(branch_table2)

prep_merge.set_upstream(branch_table2)
curate_table2.set_upstream(prep_merge)

finish_curation.set_upstream(initial_load_table2)
finish_curation.set_upstream(curate_table2)




