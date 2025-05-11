from airflow import DAG
from airflow.utils.email import send_email
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'Brian',
    'start_date': datetime(2025, 4, 2, 12, 0)
}

def send_success_email(context):
    subject = 'Crypto gecko data loaded to snowflake ✅'
    html_content = 'Crypto gecko data successfully fetched from Kafka and loaded to Snowflake for cleanup.'
    send_email(to='marionkoki00@gmail.com', subject=subject, html_content=html_content)

def send_failure_email(context):
    subject = 'Crypto gecko data ⚠ failed to load to snowflake ❄'
    html_content = 'Crypto gecko data failed to be fetched from Kafka or loaded into Snowflake.'
    send_email(to='marionkoki00@gmail.com', subject=subject, html_content=html_content)


dag = DAG(
    dag_id='crypto_table_making',
    default_args=default_args,
    catchup=False,
    schedule='@daily',
)

load_bronze_table = SnowflakeOperator(
    task_id='crypto_bronze_table_making',
    sql='./sqls/bronze_crypto_load.sql',
    snowflake_conn_id='snowflake_conn_id',
    on_success_callback=send_success_email,
    on_failure_callback=send_failure_email,
    dag=dag
)

load_silver_table = SnowflakeOperator(
    task_id='crypto_silver_table_making',
    sql='./sqls/silver_crypto_load.sql',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)

load_gold_table = SnowflakeOperator(
    task_id='crypto_gold_table_making',
    sql='./sqls/gold_crypto_load.sql',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)

load_corrupted_data = SnowflakeOperator(
    task_id='crypto_corrupted_data_making',
    sql='./sqls/corrupted_data.sql',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)

trigger_final_table_to_mail = TriggerDagRunOperator(
    task_id='trigger_final_table_dag',
    trigger_dag_id='gold_crypto_layer',
    wait_for_completion=False,
    dag=dag
)

load_bronze_table >> load_silver_table >> load_gold_table >> load_corrupted_data >> trigger_final_table_to_mail