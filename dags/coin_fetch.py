import requests, json, time, uuid, logging, os
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def extract_data():
    load_dotenv()

    gecko_api_key = os.getenv("GECKO_ACCESS_KEY")

    url = f"https://api.coingecko.com/api/v3/coins/categories?x_cg_demo_api_key={gecko_api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        response_data = response.json()

        return response_data

    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP error occurred: {err}")
        logging.error("Response text:", response.text)
    except requests.exceptions.JSONDecodeError:
        logging.error("Failed to decode JSON:", response.text)
    except Exception as e:
        logging.error("An error occurred:", e)


def transform_data(response_data):
    if not isinstance(response_data, list):
        logging.warning("Expected a list of items, got:", type(response_data))
        return []

    transformed_list = []

    for item in response_data:
        try:
            transformed_item = {
                'key_id': str(uuid.uuid4()),
                'id': item.get('id'),
                'name': item.get('name'),
                'market_cap': item.get('market_cap'),
                'market_cap_change_24h': item.get('market_cap_change_24h'),
                'content': item.get('content'),
                'top_3_coins': item.get('top_3_coins'),
                'top_3_coins_id': item.get('top_3_coins_id'),
                'volume_24h': item.get('volume_24h'),
                'coin_updated_at': item.get('updated_at')
            }
            transformed_list.append(transformed_item)
        except Exception as e:
            logging.error(f"Error transforming item: {e}")
            continue

    return transformed_list


def stream_data():
    producer = KafkaProducer(
        bootstrap_servers='kafka:29092',
        max_block_ms=50000,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    start_time = time.time()
    batch_interval_seconds = 5

    try:
        while time.time() <= start_time + 40:  # Run for 10 seconds
            try:
                response_data = extract_data()
                processed_data = transform_data(response_data)

                for item in processed_data:
                    producer.send('coin_data', value=item)

                # Sleep after processing all items in the batch
                time.sleep(batch_interval_seconds)

            except Exception as e:
                logging.error(f"Error in stream_data: {e}")
                continue
    finally:
        producer.close()
        logging.info("Kafka producer closed.")


default_args = {
    'owner': 'Brian',
    'start_date': datetime(2025, 4, 2, 12, 0)
}

dag = DAG(
    dag_id='crypto_fetch',
    default_args=default_args,
    description='Fetch coin data from Coingecko and stream to Kafka',
    schedule_interval='@daily',
    catchup=False
)

trigger_coin_consume = TriggerDagRunOperator(
    task_id='trigger_data_consume',
    trigger_dag_id='crypto_consume',
    wait_for_completion=False,
    dag=dag
)

upstreaming_data = PythonOperator(
    task_id='upstreaming_data_automation',
    python_callable=stream_data,
    dag=dag
)

upstreaming_data  >> trigger_coin_consume
