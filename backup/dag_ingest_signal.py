from datetime import timedelta, datetime
import json

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': seven_days_ago,
    'start_date': datetime(2019, 7, 9),
    'email': ['sametkaradag@google.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Set Schedule: Run pipeline once a day. Runs at end of day at 23:59. 
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "00 01 * * 0"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG('bigquery_signal_ingest_denormalize', catchup=False, default_args=default_args, schedule_interval=schedule_interval)


    
ingest_signal_actions_current = BigQueryOperator(
    task_id='bq_ingest_signal_actions_current',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    SELECT *, date('{{ ds }}') LOAD_DATE FROM `be-signalread-prd-3097.signal_reporting_be.actions_current`
    ''',    destination_dataset_table='psa_signal.actions_current',
    dag=dag)
    
ingest_signal_companies_current = BigQueryOperator(
    task_id='bq_ingest_signal_companies_current',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    SELECT *, date('{{ ds }}') LOAD_DATE FROM `be-signalread-prd-3097.signal_reporting_be.companies_current`
    ''',    destination_dataset_table='psa_signal.companies_current',
    dag=dag)
    
ingest_signal_mycompanies_current = BigQueryOperator(
    task_id='bq_ingest_signal_mycompanies_current',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    SELECT *, date('{{ ds }}') LOAD_DATE FROM `be-signalread-prd-3097.signal_reporting_be.mycompanies_current`
    ''',    destination_dataset_table='psa_signal.mycompanies_current',
    dag=dag)
    
denormalize_signal = BigQueryOperator(
    task_id='bq_denormalize_signal',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardSQL
    select  
      ( case when mc.key_location_id is not null then  mc.key_location_id
            when a.key_location_id is not null then  a.key_location_id
            else null
            end) key_location_id
      ,( case when a.token is not null then  a.token
            when mc.token is not null then  mc.token
            else null
            end) token
    	, a.action_id
    	, a.action
      ,a.key_location_id action_duns
    	, ( case when a.role_id is not null then  a.role_id
            when mc.role_id is not null then  mc.role_id
            else null
            end) role_id
    	, ( case when a.action_status='completed' or a.action_status='done' then  'completed'
            else a.action_status
            end) action_status
    	, a.execution_date
    	, a.planned_date
      , a.last_updated
    	, mc.role_name
    	, mc.token as portfolio_token
      , mc.key_location_id portfolio_duns
    	, c.VAT
    	, mc.client_name_long, 
    	(case 
    		when mc.token is NULL then 'NO_PORTFOLIO'
    	    when a.token!=mc.token then 'EXTERNAL_PORTFOLIO'
    	    when a.token=mc.token then 'MY_PORFTFOLIO'
    	    else NULL end) as PORTFOLIO_STATUS,
        peo.Name,
        peo.Function,
        peo.Unit,
        peo.Area,
        peo.Region,
        peo.OU,
        peo.Company,
        peo.Product_Line
    from `be-pocingest-acc-1aae.psa_signal.mycompanies_current` as mc 
    full outer join 
    (select 
       a.token
    	, a.action_id
    	, a.action
    	, a.key_location_id
    	, a.role_id
    	, a.action_status
    	, a.execution_date
    	, a.planned_date
      , a.last_updated
    	, c.VAT
    	, c.client_name_long 
    from `be-pocingest-acc-1aae.psa_signal.actions_current` as a 
    join `be-pocingest-acc-1aae.psa_signal.companies_current` as c 
    on a.key_location_id=c.key_location_id
    ) a
    on mc.role_id=a.role_id  and a.key_location_id = mc.key_location_id
    left join `be-pocingest-acc-1aae.psa_signal.companies_current` as c 
    on mc.key_location_id=c.key_location_id
    left join `be-pocingest-acc-1aae.psa_signal.people` as peo
    on mc.token = peo.E_mail
    where mc.key_location_id is not null or a.key_location_id is not null
    ''',    destination_dataset_table='be-pocdatamart-acc-2240.dm_portfolio.signal',
    dag=dag)
    
    
	

denormalize_signal.set_upstream(ingest_signal_actions_current)
denormalize_signal.set_upstream(ingest_signal_companies_current)
denormalize_signal.set_upstream(ingest_signal_mycompanies_current)

