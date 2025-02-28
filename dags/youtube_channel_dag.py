import os
import sys
import pendulum
import logging
import polars as pl
import json
from airflow.decorators import task, dag
from confluent_kafka import Producer

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from helper.kafka_client import KafkaClientManager
import helper.constants as CNST
from youtube_data_api import LoadDataYT
import youtube_transform as TRANSFORM

DataAPI = LoadDataYT()

default_args = {
    "owner": "Bharath",
    "depends_on_past": False,
    "retries": 1
}

def save_to_json(data, filename):
    """Utility function to save data to a JSON file."""
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    logging.info(f"Saved data to {filename}")

@dag(
    dag_id="youtube_streams_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=pendulum.now("UTC"),
    catchup=False,
    tags=["youtube", "ETL pipeline", "channels"]
)
def youtube_streams_etl_pipeline():
    """
    ### YouTube Streams Pipeline
    This DAG searches for YouTube video streams using the **YouTube Data API**.

    **Schedule:** Runs Daily (`@daily`)  
    """
    @task()
    def confluent_kafka_producer():
        """Creates a Confluent Kafka producer."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer = kafka_client.create_producer("yt_analytics_producer")
        logging.info("Kafka producer created.")
        return producer

    @task()
    def search_data_api():
        """Fetches data from YouTube Search API."""
        search_params = {
            'part': 'snippet',
            'q': 'machine learning|deep learning -statistics',  
            'type': 'channel',
            'maxResults': 2,
            'order': 'videoCount',
            'publishedAfter': '2018-01-01T00:00:00Z',
        }

        search_results = DataAPI.get_search_results(search_params, max_results=5)
        results = TRANSFORM.transform_youtube_results(search_results)
        return results

    @task()
    def extract_channel_ids(result):
        """Converts JSON-serializable data to Polars DataFrame and extracts channel IDs."""
        df = pl.DataFrame(result)
        print(df.columns)
        return df["channelId"].to_list()

    @task()
    def fetch_channel_info(channel_ids):
        """Fetches detailed channel information from the YouTube Data API."""
        try:
            BATCH_SIZE = 20
            all_channel_data = []

            for i in range(0, len(channel_ids), BATCH_SIZE):
                batch = channel_ids[i: i + BATCH_SIZE]
                params = {
                    "part": "snippet,statistics",
                    "id": ",".join(batch)
                }

                logging.info(f"Fetching details for channels: {params['id']}")
                response = DataAPI.get_channels(params)

                if response and "items" in response:
                    all_channel_data.extend(response["items"])

            logging.info(f"Successfully fetched details for {len(all_channel_data)} channels.")
            transformed_result = TRANSFORM.transform_channel_data({"items": all_channel_data})
            return transformed_result

        except Exception as e:
            logging.error(f"Error fetching channel data: {e}", exc_info=True)
            return None
        
    @task()
    def publish_to_kafka(data):
        """Publishes transformed channel data to a Confluent Kafka topic."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer = kafka_client.create_producer("yt_analytics_producer")

        topic = "yt_video_analytics_topic"
        
        for record in data:
            print(record)
            producer.produce(topic, key=record["channel_id"], value=json.dumps(record))
            producer.poll(0)
        print(f"Published {len(data)} records to Kafka topic: {topic}")
        
        producer.flush()
        logging.info(f"Published {len(data)} records to Kafka topic: {topic}")

    # DAG Flow
    search_results = search_data_api()
    channel_ids = extract_channel_ids(search_results)
    transformed_channel_data = fetch_channel_info(channel_ids)
    publish_to_kafka(transformed_channel_data)

# Instantiating the DAG
dag = youtube_streams_etl_pipeline()