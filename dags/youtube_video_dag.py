import os
import sys
import pendulum
import polars as pl
from airflow.decorators import dag, task

# importing custom libraries
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from youtube_data_api import LoadDataYT
import youtube_transform as TRANSFORM

default_args = {
    "owner": "Bharath",
    "depends_on_past": False,
    "retries": 1,
}

@dag(
    dag_id = "youtube_video_pipeline",
    default_args = default_args,
    schedule_interval = "@daily",
    start_date = pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss"),
    catchup = False,
    tags = ["youtube", "videos"],
)

def youtube_video_pipeline():
    
    @task()
    def search_youtube():
        """fetches search results from YouTube API."""
        params = {"part": "snippet", 
                  "type": "video",
                  "q": "machine learning|Cdeep learning -statistics", 
                  "maxResults": 2,
                  "order": "videoCount",
                  "publishedAfter": "2018-01-01T00:00:00Z",
            }
        
        search_results = LoadDataYT.get_search_results(params, max_results=5)
        results = TRANSFORM.transform_youtube_results(search_results)

        return results
    
    @task()
    def extract_channel_ids(result):
        """converts JSON-serializable data back to Polars DataFrame and extract channel IDs.
        """
        df = pl.DataFrame(result)
        return df["channelId"].to_list()

    @task()
    def extract_video_ids(search_results):
        """extracts video IDs from search results."""
        df = pl.DataFrame(search_results)
        return df["videoId"].to_list()

    @task()
    def fetch_video_info(video_ids):
        """fetches video details from YouTube API."""
        pass

    @task()
    def fetch_comments(video_ids):
        """fetches comments for each video."""
        pass

    @task()
    def fetch_captions(video_ids):
        """fetches captions for each video."""
        pass

    # DAG Flow
    search_results = search_youtube()
    video_ids = extract_video_ids(search_results)
    video_info = fetch_video_info(video_ids)
    comments = fetch_comments(video_ids)
    captions = fetch_captions(video_ids)

# Instantiating the DAG
video_dag_instance = youtube_video_pipeline()