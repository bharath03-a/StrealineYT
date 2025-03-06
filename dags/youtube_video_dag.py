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
from helper.kafka_client import KafkaClientManager
import helper.constants as CNST

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
    start_date=pendulum.now("UTC"),
    catchup=False,
    tags=["youtube", "videos"],
)
def youtube_video_pipeline():
    @task()
    def kafka_setup():
        """Creates a Kafka topic and initializes a Kafka producer."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        topics = [
            CNST.KAFKA_TOPIC_VIDEO,
            CNST.KAFKA_TOPIC_CHANNEL,
            CNST.KAFKA_TOPIC_COMMENTS,
            CNST.KAFKA_TOPIC_CAPTIONS
        ]

        for topic in topics:
            kafka_client.check_create_topic(topic)
        return True

    @task()
    def search_youtube():
        """Fetches search results from YouTube API."""
        params = {
            "part": "snippet",
            "type": "video",
            "q": CNST.YT_VIDEO_QUERY,
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
    def extract_video_ids(search_results):
        """Extracts video IDs from search results."""
        df = pl.DataFrame(search_results)
        return df["videoId"].to_list()

    @task()
    def fetch_video_info(video_ids):
        """Fetches video details from YouTube API."""
        try:
            BATCH_SIZE = 20
            all_video_data = []

            for i in range(0, len(video_ids), BATCH_SIZE):
                batch = video_ids[i: i + BATCH_SIZE]
                params = {
                    "part": "snippet,statistics",
                    "id": ",".join(batch)
                }

                logging.info(f"Fetching details for videos: {params['id']}")
                response = DataAPI.get_videos(params)

                if response and "items" in response:
                    all_video_data.extend(response["items"])

            logging.info(f"Successfully fetched details for {len(all_video_data)} channels.")
            return all_video_data if all_video_data else []
        except Exception as e:
            logging.error(f"Error fetching video data: {e}", exc_info=True)
            return None

    @task()
    def fetch_comments(video_ids):
        """Fetches comments for each video in batches."""
        try:
            BATCH_SIZE = 20
            all_comments = {}
            
            for i in range(0, len(video_ids), BATCH_SIZE):
                batch = video_ids[i: i + BATCH_SIZE]
                params = {"part": "snippet", "id": ",".join(batch), "maxResults": 10}
                logging.info(f"Fetching comments for videos: {params['id']}")
                response = DataAPI.get_comments(params, max_comments=20)
                
                if response:
                    for idx, video_id in enumerate(batch):
                        all_comments[video_id] = response[idx] if idx < len(response) else []
            
            return all_comments
        except Exception as e:
            logging.error(f"Error fetching comments: {e}", exc_info=True)
            return None

    @task()
    def fetch_captions(video_ids):
        """Fetches captions for each video in batches."""
        try:
            BATCH_SIZE = 20
            all_captions = {}
            
            for i in range(0, len(video_ids), BATCH_SIZE):
                batch = video_ids[i: i + BATCH_SIZE]
                params = {"part": "snippet", "id": ",".join(batch)}
                logging.info(f"Fetching captions for videos: {params['id']}")
                response = DataAPI.get_captions(params)
                
                if response:
                    for idx, video_id in enumerate(batch):
                        all_captions[video_id] = response[idx] if idx < len(response) else []
            
            return all_captions
        except Exception as e:
            logging.error(f"Error fetching captions: {e}", exc_info=True)
            return None
    
    @task()
    def publish_to_kafka(data, topic_name):
        """Publishes transformed data to a specified Kafka topic."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer_name = f"yt_{topic_name}_producer_{pendulum.now().format('YYYYMMDDHHMMSS')}"

        producer = kafka_client.create_producer(producer_name)

        for key, record in enumerate(data):
            producer.produce(topic_name, key=str(key).encode('utf-8'), value=json.dumps(record))
            producer.poll(0)
        
        producer.flush()
        logging.info(f"Published {len(data)} records to Kafka topic: {topic_name}")

        
    # DAG Flow
    kafka_setup_task = kafka_setup()

    # Fetch Data
    search_results = search_youtube()
    video_ids = extract_video_ids(search_results)
    channel_ids = extract_channel_ids(search_results)

    # Fetching additional information
    transformed_channel_data = fetch_channel_info(channel_ids)
    video_info = fetch_video_info(video_ids)
    comments = fetch_comments(video_ids)
    captions = fetch_captions(video_ids)

    # Publishing data to respective Kafka topics
    publish_channel_info = publish_to_kafka(transformed_channel_data, CNST.KAFKA_TOPIC_CHANNEL)
    publish_video_info = publish_to_kafka(video_info, CNST.KAFKA_TOPIC_VIDEO)
    publish_video_comments = publish_to_kafka(comments, CNST.KAFKA_TOPIC_COMMENTS)
    publish_video_captions = publish_to_kafka(captions, CNST.KAFKA_TOPIC_CAPTIONS)

    # Define dependencies
    kafka_setup_task >> [
        publish_channel_info,
        publish_video_info,
        publish_video_comments,
        publish_video_captions
    ]



# Instantiating the DAG for Airflow
video_dag_instance = youtube_video_pipeline()

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
    # not necessarily a good idea for this BTW
