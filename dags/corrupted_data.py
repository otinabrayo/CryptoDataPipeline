from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
import boto3
import os
import gzip


default_args = {
    'owner': 'Brian',
    'start_date': datetime(2025, 4, 2),
}

dag = DAG(
    dag_id='corrupted_crypto_layer',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

unload_corrupted_data = """
COPY INTO @my_s3_crypto_stage/coin_gecko/corrupted_crypto_gecko_table.csv.gz
FROM (
    SELECT * FROM CRYPTO_DB.GOLD.crypto_gecko_null_data
)
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    COMPRESSION = GZIP
)
HEADER = TRUE
SINGLE = FALSE
OVERWRITE = TRUE;
"""

def download_and_unzip_csv(**kwargs):
    s3 = boto3.client('s3')
    bucket_name = 'data.engineering.projects'
    s3_key = 'coin_gecko/corrupted_crypto_gecko_table.csv.gz_0_0_0.csv.gz'
    local_gz_path = '/tmp/corrupted_crypto_gecko_table.csv.gz_0_0_0.csv.gz'
    local_csv_path = '/tmp/corrupted_crypto_gecko_table.csv'

    # Download the gzipped file
    s3.download_file(bucket_name, s3_key, local_gz_path)

    # Unzip the file
    with gzip.open(local_gz_path, 'rb') as f_in:
        with open(local_csv_path, 'wb') as f_out:
            f_out.write(f_in.read())

    # Optional: clean up .gz file
    os.remove(local_gz_path)

    # Push the path to next task via XCom
    kwargs['ti'].xcom_push(key='csv_file_path', value=local_csv_path)

def send_email_with_attachment(**kwargs):
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='download_csv', key='csv_file_path')

    send_csv_email = EmailOperator(
        task_id='send_csv_report',
        to='marionkoki00@gmail.com',
        subject='Corrupted ✂ Crypto Data 🗑',
        html_content='Attached is the final corrupted crypto CSV file exported from Snowflake.',
        files=[csv_file_path],
        dag=kwargs['dag']
    )

    send_csv_email.execute(kwargs)


export_corrupted_data_to_s3 = SnowflakeOperator(
    task_id='export_corrupted_data_to_s3',
    sql=unload_corrupted_data,
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)

download_csv = PythonOperator(
    task_id='download_csv',
    python_callable=download_and_unzip_csv,
    dag=dag
)

email_task = PythonOperator(
    task_id='send_final_csv_report',
    python_callable=send_email_with_attachment,
    provide_context=True,
    dag=dag
)

export_corrupted_data_to_s3 >> download_csv >> email_task