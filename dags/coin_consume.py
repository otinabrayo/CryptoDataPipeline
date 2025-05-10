import json, time, logging
from datetime import datetime
from s3fs import S3FileSystem
from kafka import KafkaConsumer
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def consume_data_to_s3():
    s3 = S3FileSystem()
    bucket_name = 'data.engineering.projects'
    prefix = 'coin_gecko/dumps/'

    consumer = KafkaConsumer(
        'coin_data',
        bootstrap_servers='kafka:29092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )

    try:
        messages = consumer.poll(timeout_ms=5000)
        if not messages:
            logging.info("No messages received in this batch")
        else:
            for tp, msgs in messages.items():
                for message in msgs:
                    s3_path = f"s3://{bucket_name}/{prefix}/{message.value['id']}.json"
                    with s3.open(s3_path, 'w') as file:
                        json.dump(message.value, file)
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        try:
            consumer.close()
            logging.info("Kafka consumer closed successfully.")
        except Exception as e:
            logging.error(f"Error while closing consumer: {e}")


default_args = {
    'owner': 'Brian',
    'start_date': datetime(2025, 4, 2, 12, 0)
}

dag = DAG(
    'data_consume',
    default_args=default_args,
    description='Consume coin data from Kafka and save to S3',
    schedule_interval='@daily',
    catchup=False
)

trigger_table_making = TriggerDagRunOperator(
    task_id='trigger_table_making',
    trigger_dag_id='crypto_table_making',
    wait_for_completion=False,
    dag=dag
)

consume_data_to_s3 = PythonOperator(
    task_id='consume_data_to_s3',
    python_callable=consume_data_to_s3,
    dag=dag
)

consume_data_to_s3 >> trigger_table_making
