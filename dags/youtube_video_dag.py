import os
import sys
import pendulum
import logging
import json
import polars as pl
from airflow.decorators import dag, task

# importing custom libraries
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from youtube_data_api import LoadDataYT
import youtube_transform as TRANSFORM

DataAPI = LoadDataYT()

# Save data to JSON utility
def save_to_json(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

default_args = {
    "owner": "Bharath",
    "depends_on_past": False,
    "retries": 1,
}

@dag(
    dag_id="youtube_video_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss"),
    catchup=False,
    tags=["youtube", "videos"],
)
def youtube_video_pipeline():

    @task()
    def search_youtube():
        """Fetches search results from YouTube API."""
        params = {
            "part": "snippet",
            "type": "video",
            "q": "machine learning|deep learning -statistics",
            "maxResults": 2,
            "order": "viewCount",
            "publishedAfter": "2018-01-01T00:00:00Z",
        }

        search_results = DataAPI.get_search_results(params, max_results=5)
        results = TRANSFORM.transform_youtube_video_results(search_results)
        return results

    @task()
    def extract_channel_ids(result):
        """Converts JSON data to Polars DataFrame and extracts channel IDs."""
        df = pl.DataFrame(result)
        return df["channelId"].to_list()

    @task()
    def extract_video_ids(search_results):
        """Extracts video IDs from search results."""
        df = pl.DataFrame(search_results)
        return df["videoId"].to_list()

    @task()
    def fetch_video_info(video_ids):
        """Fetches video details from YouTube API."""
        try:
            params = {"part": "snippet,statistics", "id": ",".join(video_ids)}
            logging.info(f"Fetching details for {len(video_ids)} videos.")
            response = DataAPI.get_videos(params, max_videos=50    )
            return response if response else []
        except Exception as e:
            logging.error(f"Error fetching video data: {e}", exc_info=True)
            return None

    @task()
    def fetch_comments(video_ids):
        """Fetches comments for each video."""
        try:
            all_comments = {}
            for video_id in video_ids:
                params = {"part": "snippet", "videoId": video_id, "maxResults": 10}
                logging.info(f"Fetching comments for video: {video_id}")
                comments = DataAPI.get_comments(params, max_comments=20)
                all_comments[video_id] = comments if comments else []
            return all_comments
        except Exception as e:
            logging.error(f"Error fetching comments: {e}", exc_info=True)
            return None

    @task()
    def fetch_captions(video_ids):
        """Fetches captions for each video."""
        try:
            all_captions = {}
            for video_id in video_ids:
                params = {"part": "snippet", "videoId": video_id}
                logging.info(f"Fetching captions for video: {video_id}")
                captions = DataAPI.get_captions(params)
                all_captions[video_id] = captions if captions else []
            return all_captions
        except Exception as e:
            logging.error(f"Error fetching captions: {e}", exc_info=True)
            return None
        
    # can also get top or most popular videos in a certain search category
    # see if we can input data to DAG so we can input params
    # @task()
    # def search_top_youtube():
    #     """Fetches search results from YouTube API."""
    #     params = {
    #         "part": "snippet",
    #         "type": "video",
    #         "chart": "mostPopular",
    #         "q": "machine learning|deep learning -statistics",
    #         "maxResults": 2,
    #         "order": "viewCount",
    #         "publishedAfter": "2018-01-01T00:00:00Z",
    #     }

    #     search_results = DataAPI.get_search_results(params, max_results=5)
    #     results = TRANSFORM.transform_youtube_video_results(search_results)
    #     return results

    # can also get comment threads for a channel id - allThreadsRelatedToChannelId

    # 'part': 'snippet,replies',
    # 'allThreadsRelatedToChannelId': "UC_x5XG1OV2P6uZZ5FSM9Ttw", # comment id ah LoL
    # 'maxResults': 5,
    # 'textFormat': 'plainText'

    # change the youtube comment function to get more results than just comments - as it has a lot other features to such as comment likes, comment replies
    # we have to search using commentthread id I guess for replies - no use comment.list() using the commentThreadID


    # DAG Flow
    search_results = search_youtube()
    video_ids = extract_video_ids(search_results)
    channel_ids = extract_channel_ids(search_results)
    video_info = fetch_video_info(video_ids)
    comments = fetch_comments(video_ids)
    captions = fetch_captions(video_ids)

    search_results >> [video_ids, channel_ids] >> [video_info, comments, captions]


# # Instantiating the DAG for Airflow
# video_dag_instance = youtube_video_pipeline()

# ----------------------- Testing Code -----------------------
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)

#     search_results = DataAPI.get_search_results(
#         {
#             "part": "snippet",
#             "type": "video",
#             "q": "machine learning|deep learning -statistics",
#             "maxResults": 2,
#             "order": "videoCount",
#             "publishedAfter": "2018-01-01T00:00:00Z",
#         },
#         max_results=5,
#     )
#     transformed_search_results = TRANSFORM.transform_youtube_video_results(search_results)
#     save_to_json(transformed_search_results, "../data/search_results.json")

#     df = pl.DataFrame(transformed_search_results)
#     video_ids = df["videoId"].to_list()
#     channel_ids = df["channelId"].to_list()
#     save_to_json(video_ids, "../data/video_ids.json")
#     save_to_json(channel_ids, "../data/channel_ids.json")

#     video_info = DataAPI.get_videos({"part": "snippet,statistics", "id": ",".join(video_ids)})
#     save_to_json(video_info, "../data/video_info.json")

#     all_comments = {
#         video_id: DataAPI.get_comments({"part": "snippet", "videoId": video_id, "maxResults": 100}) or []
#         for video_id in video_ids
#     }
#     save_to_json(all_comments, "../data/comments.json")

#     all_captions = {
#         video_id: DataAPI.get_captions({"part": "snippet", "videoId": video_id}) or []
#         for video_id in video_ids
#     }
#     save_to_json(all_captions, "../data/captions.json")

#     logging.info("Testing completed. JSON files saved in the 'data/' directory.")