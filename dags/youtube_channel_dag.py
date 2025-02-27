import os
import sys
import pendulum
import logging
import polars as pl
import json
from airflow.decorators import task, dag

# Importing helper functions
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from helper.kafka_client import check_create_topic, create_producer, create_consumer
import helper.constants as CNST

# Importing custom libraries
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
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
    def kafka_setup():
        """Creates a Kafka topic and initializes a Kafka producer."""
        check_create_topic(CNST.KAFKA_TOPIC_NAME)
        producer_name = f"yt_channel_producer_{pendulum.now().format('YYYYMMDDHHMMSS')}"
        consumer_name = f"yt_channel_consumer_{pendulum.now().format('YYYYMMDDHHMMSS')}"
        
        producer = create_producer(producer_name)
        consumer = create_consumer(consumer_name)
        return producer, consumer

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

    # DAG Flow
    producer, consumer = kafka_setup()
    search_results = search_data_api()
    channel_ids = extract_channel_ids(search_results)
    fetch_channel_info(channel_ids)

# Instantiating the DAG
dag = youtube_streams_etl_pipeline()