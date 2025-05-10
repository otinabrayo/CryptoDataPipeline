from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'Brian',
    'start_date': datetime(2025, 4, 2, 12, 0)
}

dag = DAG(
    dag_id='crypto_table_making',
    default_args=default_args,
    catchup=False,
    schedule='@daily',
)

send_mail =  EmailOperator(
    task_id='sending_mail',
    to='marionkoki00@gmail.com',
    subject='Crypto gecko data loaded to snowflake ✅',
    html_content='Crypto gecko data successfully fetched from kafka and loaded to snowflake for cleanup',
    dag=dag
)

load_table = SnowflakeOperator(
    task_id='crypto_table_task',
    sql='./sqls/crypto_load.sql',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)

load_table >> send_mail