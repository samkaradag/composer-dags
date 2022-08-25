# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example HTTP operator and sensor"""

import json
from datetime import timedelta

import airflow
from airflow import DAG
import sys
import requests
import json
import yaml
from google.cloud import bigquery
from airflow.operators import PythonOperator, BranchPythonOperator, DummyOperator
from airflow.operators.sensors import HttpSensor

###########################################################
# Read the argvals (provided from the command line)
###########################################################
# Define the database [duo or minerva]


inp="""CDAP:
  IP: "172.29.132.31"
  PORT: "11015"
  NAMESPACE: "default"

DUO:
  PROJECT_ID: "be-pocingest-acc-1aae"
  SID: "duors"
  ORACLE_IP: "172.26.80.199"
  ORACLE_PORT: "2017"
  USERNAME: "POC_AWS_GCP"
  PASSWORD: "PCG_SWA_COP_4321"
  TABLE_OWNER: "BBX_OWNER"
  DATASET_NAME: "ingest_duo"
  TEMP_BUCKET_NAME: "be-pocingest-temp"
  ROWNUM_LIMIT: "1=1"

MINERVA:
  PROJECT_ID: "be-pocingest-acc-1aae"
  SID: "minerva"
  ORACLE_IP: "172.26.81.45"
  ORACLE_PORT: "2018"
  USERNAME: "POC_AWS_GCP"
  PASSWORD: "UIEURHl233Rj"
  TABLE_OWNER: "MINERVA_DAO"
  DATASET_NAME: "ingest_minerva"
  TEMP_BUCKET_NAME: "be-pocingest-temp"
  ROWNUM_LIMIT: "1=1"
"""
  
params = yaml.load(inp)

duo_tables = {
    'DUO_JN_ACT_ACCOUNTS': {'database':'duo', 'table_name':'JN_ACT_ACCOUNTS', 'cdap_app':'INCREMENTAL_LOAD_JOURNAL_JN_ACT_ACCOUNTS_v1'},
    'DUO_JN_ACT_LEVENSCYCLUS_STATUSES': {'database':'duo', 'table_name':'JN_ACT_LEVENSCYCLUS_STATUSES', 'cdap_app':'INCREMENTAL_LOAD_JOURNAL_JN_ACT_LEVENSCYCLUS_STATUSES_v1'},
    'DUO_JN_ACT_PRODUCTLINE_FOCUSSEN': {'database':'duo', 'table_name':'JN_ACT_PRODUCTLINE_FOCUSSEN', 'cdap_app':'INCREMENTAL_LOAD_JOURNAL_JN_ACT_PRODUCTLINE_FOCUSSEN_v1'},
    'DUO_JN_ACT_SALES_STATUSSEN': {'database':'duo', 'table_name':'JN_ACT_SALES_STATUSSEN', 'cdap_app':'INCREMENTAL_LOAD_JOURNAL_JN_ACT_SALES_STATUSSEN_v1'},
    'DUO_JN_OGE_UNITS': {'database':'duo', 'table_name':'JN_OGE_UNITS', 'cdap_app':'INCREMENTAL_LOAD_JOURNAL_JN_OGE_UNITS_v1'}
}

minerva_tables = {
    'MINERVA_ACT_ACCOUNTS_JN': {'database':'minerva', 'table_name':'ACT_ACCOUNTS_JN', 'cdap_app':'INCREMENTAL_LOAD_MINERVA_JOURNAL_ACT_ACCOUNTS_JN'},
    'MINERVA_FIN_GROOTBOEKREKENING_JN': {'database':'minerva', 'table_name':'FIN_GROOTBOEKREKENING_JN', 'cdap_app':'INCREMENTAL_LOAD_MINERVA_JOURNAL_FIN_GROOTBOEKREKENING_JN'},
    'MINERVA_OGE_BOEKPERIODES_JN': {'database':'minerva', 'table_name':'OGE_BOEKPERIODES_JN', 'cdap_app':'INCREMENTAL_LOAD_MINERVA_JOURNAL_OGE_BOEKPERIODES_JN'},
    'MINERVA_OGE_UNITS_JN': {'database':'minerva', 'table_name':'OGE_UNITS_JN', 'cdap_app':'INCREMENTAL_LOAD_MINERVA_JOURNAL_OGE_UNITS_JN'}
#    'MINERVA_JN_OGE_UNITS': {'database':'minerva', 'table_name':'JN_OGE_UNITS', 'cdap_app':'INCREMENTAL_LOAD_MINERVA_JOURNAL_FIN_BOEKINGEN_JN'}
}

#PARAMS
# CDAP parameters
IP=params["CDAP"]["IP"]
PORT=params["CDAP"]["PORT"]
NAMESPACE=params["CDAP"]["NAMESPACE"]

def read_max_value_from_bq(PROJECT_ID,DATASET_NAME,TABLE_NAME):
  client = bigquery.Client()
  max_date = 'sysdate'
  query = (
      "SELECT string(MAX_JN_DATE) MAX_JN_DATE  FROM `" + PROJECT_ID + "." + DATASET_NAME + "." + "." + TABLE_NAME + "_MAX_JN_DATE_v`"
  )
  query_job = client.query(
      query,
      # Location must match that of the dataset(s) referenced in the query.
      location="EU",
  )  # API request - starts the query

  for row in query_job:  # API request - fetches results
      # Row values can be accessed by field name or index
      assert row[0] == row.MAX_JN_DATE == row["MAX_JN_DATE"]
      print(row[0])
      max_date=row[0][:-3]
  return max_date


def start_incremental_load_job(IP,PORT,NAMESPACE,PROJECT_ID,TEMP_BUCKET_NAME,DATASET_NAME,ORACLE_SID,ORACLE_PORT,ORACLE_IP,ORACLE_USERNAME,ORACLE_PASSWORD,TABLE_OWNER,TABLE_NAME,ROWNUM_LIMIT,MAX_DATE,APP_ID):
  '''
  Given set of parameters (below), starts a job in CDAP.
  '''

  url = 'http://{IP}:{PORT}/v3/namespaces/{NAMESPACE}/start'.format(IP=IP, PORT=PORT, NAMESPACE=NAMESPACE)
  data = '''[{{
     "appId": "{APP_ID}",
     "programType": "Workflow",
     "programId": "DataPipelineWorkflow",
     "runtimeargs":
       {{ "ORACLE_SID"="{ORACLE_SID}",
          "TABLE_NAME"="{TABLE_NAME}",
          "ORACLE_PORT"="{ORACLE_PORT}",
          "TEMP_BUCKET_NAME"="{TEMP_BUCKET_NAME}",
          "ORACLE_IP"="{ORACLE_IP}",
          "DATASET_NAME"="{DATASET_NAME}",
          "ROWNUM_LIMIT"="{ROWNUM_LIMIT}",
          "ORACLE_PASSWORD"="{ORACLE_PASSWORD}",
          "TABLE_OWNER"="{TABLE_OWNER}",
          "PROJECT_ID"="{PROJECT_ID}",
          "JN_DATE"="{MAX_DATE}",
          "ORACLE_USERNAME"="{ORACLE_USERNAME}"
        }}
  }}]'''.format(APP_ID=APP_ID, ORACLE_SID=ORACLE_SID, TABLE_NAME=TABLE_NAME,
               ORACLE_PORT=ORACLE_PORT, TEMP_BUCKET_NAME=TEMP_BUCKET_NAME, ORACLE_IP=ORACLE_IP,
               DATASET_NAME=DATASET_NAME, ROWNUM_LIMIT=ROWNUM_LIMIT, ORACLE_PASSWORD=ORACLE_PASSWORD,
               TABLE_OWNER=TABLE_OWNER, PROJECT_ID=PROJECT_ID, ORACLE_USERNAME=ORACLE_USERNAME,MAX_DATE=MAX_DATE)

  response = requests.post(url, data=data)
  print(response)


#############################################################
# Start the job for each table in the [table_names_file].txt
#############################################################
def start_cdap_pipeline(database,APP_ID,TABLE_NAME):
    '''
    Given set of parameters (below), starts a job in CDAP.
    '''

    # Row limit is just for testing purposes it only ingests first four rows
    # TODO: for full ingest change the limit
    #ROWNUM_LIMIT="1=1"

    # Database parameters
    PROJECT_ID=params[database.upper()]["PROJECT_ID"]
    ORACLE_SID=params[database.upper()]["SID"]
    ORACLE_IP=params[database.upper()]["ORACLE_IP"]
    ORACLE_PORT=params[database.upper()]["ORACLE_PORT"]
    ORACLE_USERNAME=params[database.upper()]["USERNAME"]
    ORACLE_PASSWORD=params[database.upper()]["PASSWORD"]
    TABLE_OWNER=params[database.upper()]["TABLE_OWNER"]
    DATASET_NAME=params[database.upper()]["DATASET_NAME"]
    TEMP_BUCKET_NAME=params[database.upper()]["TEMP_BUCKET_NAME"]

    # Row limit is just for testing purposes it only ingests first four rows
    # TODO: for full ingest change the limit
    ROWNUM_LIMIT=params[database.upper()]["ROWNUM_LIMIT"]



    MAX_DATE=read_max_value_from_bq(PROJECT_ID,DATASET_NAME,TABLE_NAME)
    print(MAX_DATE)
    #APP_ID="INCREMENTAL_LOAD_JOURNAL_JN_ACT_LEVENSCYCLUS_STATUSES"
    start_incremental_load_job(IP,PORT,NAMESPACE,PROJECT_ID,TEMP_BUCKET_NAME,DATASET_NAME,ORACLE_SID,ORACLE_PORT,ORACLE_IP,ORACLE_USERNAME,ORACLE_PASSWORD,TABLE_OWNER,TABLE_NAME,ROWNUM_LIMIT,MAX_DATE,APP_ID)



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

dag = DAG('incremental_load_duo_all',catchup=False, default_args=default_args, schedule_interval=schedule_interval)

dag.doc_md = __doc__

start_duo_daily_incremental_load = DummyOperator(
    task_id='start_duo_daily_incremental_load',
    dag=dag
)

finish_duo_daily_incremental_load = DummyOperator(
    task_id='finish_duo_daily_incremental_load',
    trigger_rule='all_done',
    dag=dag
)

start_minerva_daily_incremental_load = DummyOperator(
    task_id='start_minerva_daily_incremental_load',
    dag=dag
)

finish_minerva_daily_incremental_load = DummyOperator(
    task_id='finish_minerva_daily_incremental_load',
    trigger_rule='all_done',
    dag=dag
)

for table,table_attr in duo_tables.items():
  incremental_load_act_accounts_start = PythonOperator(
  task_id='start_duo_incremental_load_{table}'.format(table=table),
  python_callable=start_cdap_pipeline,
  op_kwargs={'database': table_attr['database'], 'APP_ID':table_attr['cdap_app'], 'TABLE_NAME':table_attr['table_name']},
  dag=dag
  )
  
  incremental_load_act_accounts_monitor =  HttpSensor(
      task_id='incremental_load_monitor_{table}'.format(table=table),
      http_conn_id='cdap',
      endpoint='v3/namespaces/{NAMESPACE}/apps/{APP_ID}/workflows/DataPipelineWorkflow/status'.format(IP=IP, PORT=PORT, NAMESPACE=NAMESPACE,APP_ID=table_attr['cdap_app']),
      request_params={},
      response_check=lambda response: """{"status":"STOPPED"}""" in response.text,
      poke_interval=1,
      dag=dag,
  )
  incremental_load_act_accounts_monitor.set_downstream(finish_duo_daily_incremental_load)
  incremental_load_act_accounts_start.set_downstream(incremental_load_act_accounts_monitor)
  incremental_load_act_accounts_start.set_upstream(start_duo_daily_incremental_load)
  
for table,table_attr in minerva_tables.items():
  incremental_load_act_accounts_start = PythonOperator(
  task_id='start_duo_incremental_load_{table}'.format(table=table),
  python_callable=start_cdap_pipeline,
  op_kwargs={'database': table_attr['database'], 'APP_ID':table_attr['cdap_app'], 'TABLE_NAME':table_attr['table_name']},
  dag=dag
  )
  
  incremental_load_act_accounts_monitor =  HttpSensor(
      task_id='incremental_load_monitor_{table}'.format(table=table),
      http_conn_id='cdap',
      endpoint='v3/namespaces/{NAMESPACE}/apps/{APP_ID}/workflows/DataPipelineWorkflow/status'.format(IP=IP, PORT=PORT, NAMESPACE=NAMESPACE,APP_ID=table_attr['cdap_app']),
      request_params={},
      response_check=lambda response: """{"status":"STOPPED"}""" in response.text,
      poke_interval=1,
      dag=dag,
  )
  incremental_load_act_accounts_monitor.set_downstream(finish_minerva_daily_incremental_load)
  incremental_load_act_accounts_start.set_downstream(incremental_load_act_accounts_monitor)
  incremental_load_act_accounts_start.set_upstream(start_minerva_daily_incremental_load)
  
# incremental_load_act_accounts_start >> incremental_load_act_accounts_monitor >> dummy


