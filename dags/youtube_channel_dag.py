import os
import sys
import pendulum
import logging
import polars as pl
from airflow.decorators import task, dag

# importing custom libraries
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from youtube_data_api import LoadDataYT
import youtube_transform as TRANSFORM

default_args = {
    "owner": "Bharath",
    "depends_on_past": False,
    "retries": 1
}

@dag(
    dag_id = "youtube_streams_pipeline",
    default_args = default_args,
    schedule_interval = "@daily",
    start_date = pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss"),
    catchup = False,
    tags = ["youtube", "ETL pipeline", "channels"]
)

def youtube_streams_etl_pipeline():
    """
    ### YouTube Streams Pipeline
    This DAG searches for YouTube video streams using the **YouTube Data API**.

    **Schedule:** Runs Daily (`@daily`)  
    """

    @task()
    def search_data_api():
        """fetches Data from Youtube Search API"""
        search_params = {
            'part': 'snippet',
            'q': 'machine learning|Cdeep learning -statistics',  # Searches for "machine learning" OR "deep learning" but NOT "statistics"
            'type': 'channel',
            'maxResults': 2,
            'order': 'videoCount',
            'publishedAfter': '2018-01-01T00:00:00Z',
        }

        search_results =  LoadDataYT.get_search_results(search_params, max_results=5)
        results = TRANSFORM.transform_youtube_results(search_results)

        return results
    
    @task()
    def extract_channel_ids(result):
        """converts JSON-serializable data back to Polars DataFrame and extract channel IDs.
        """
        df = pl.DataFrame(result)
        return df["channelId"].to_list()

    @task()
    def fetch_channel_info(channel_ids):
        try:
            BATCH_SIZE = 20
            all_channel_data = []

            for i in range(0, len(channel_ids), BATCH_SIZE):
                batch = channel_ids[i : i + BATCH_SIZE] 

                params = {
                    "part": "snippet,statistics",
                    "id": ",".join(batch)
                }

                logging.info(f"Fetching details for {len(batch)} channels: {params['id']}")

                response = LoadDataYT.get_channels(params)

                if response and "items" in response:
                    all_channel_data.extend(response["items"])

            logging.info(f"Successfully fetched details for {len(all_channel_data)} channels.")
            transformed_result = TRANSFORM.transform_channel_data({"items" : all_channel_data})
            return transformed_result

        except Exception as e:
            logging.error(f"Error fetching channel data: {e}", exc_info=True)
            return None

    # DAG Flow
    search_results = search_data_api()
    channel_ids = extract_channel_ids(search_results)
    fetch_channel_info(channel_ids)
    
# Instantiating the DAG
dag_instance = youtube_streams_etl_pipeline()