import os
import sys
import pendulum
import logging
import json
import polars as pl
from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import MongoClient
from airflow.models import Variable
from youtube_transcript_api import YouTubeTranscriptApi
from airflow.models.param import Param

# importing custom libraries
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from youtube_data_api import LoadDataYT
import youtube_transform as TRANSFORM
from helper.kafka_client import KafkaClientManager
from helper.mongo_client import MongoDBClient
import helper.constants as CNST

DataAPI = LoadDataYT()

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
    params={
        "query": CNST.YT_VIDEO_QUERY,
        }
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
            CNST.KAFKA_TOPIC_CAPTIONS,
            CNST.KAFKA_TOPIC_TRANSCRIPTS
        ]

        for topic in topics:
            kafka_client.check_create_topic(topic)
        return True

    @task()
    def mongodb_setup():
        """Creates a Kafka topic and initializes a Kafka producer."""
        mongo_client = MongoDBClient()
        mongo_client.setup_database(CNST.MONGODB_NAME, CNST.MONGODB_COLLECTIONS)
        return True

    @task()
    def search_youtube(**context):
        """Fetches search results from YouTube API."""

        params = {
            "part": "snippet",
            "type": "video",
            "q": context["params"]["query"],
            "maxResults": 15,
            "order": "viewCount",
            "publishedAfter": "2021-01-01T00:00:00Z",
            "relevanceLanguage": "en",
            "videoDuration": "medium"
        }

        search_results = DataAPI.get_search_results(params, max_results=60)
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
            return {"items": all_channel_data}

        except Exception as e:
            logging.error(f"Error fetching channel data: {e}", exc_info=True)
            return None
        
    @task()
    def transform_channel_info(raw_channel_data):
        """Transforms raw channel data into the desired format."""
        if not raw_channel_data:
            logging.error("No channel data to transform")
            return None
        
        try:
            transformed_result = TRANSFORM.transform_channel_data(raw_channel_data)
            logging.info(f"Successfully transformed {len(raw_channel_data['items'])} channel records")
            return transformed_result
        except Exception as e:
            logging.error(f"Error transforming channel data: {e}", exc_info=True)
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
    def transform_video_info(raw_video_data):
        """Transforms raw video data into the desired format."""
        if not raw_video_data:
            logging.error("No video data to transform")
            return None
        
        try:
            transformed_result = TRANSFORM.transform_video_data(raw_video_data)
            logging.info(f"Successfully transformed {len(raw_video_data)} video records")
            return transformed_result
        except Exception as e:
            logging.error(f"Error transforming video data: {e}", exc_info=True)
            return None

    @task()
    def fetch_comments(video_ids):
        """Fetches comments for each video individually."""
        try:
            all_comments = []
            
            for video_id in video_ids:
                params = {"part": "snippet", "videoId": video_id}
                logging.info(f"Fetching comments for video: {video_id}")
                response = DataAPI.get_comments(params, max_comments=50)
                
                if response is not None:
                    all_comments.extend(response)
                else:
                    all_comments.extend([])

            return {"items": all_comments}
        except Exception as e:
            logging.error(f"Error fetching comments: {e}", exc_info=True)
            return None
    
    @task()
    def transform_comment_info(raw_comment_data):
        """Transforms raw comment data into the desired format."""
        if not raw_comment_data or not raw_comment_data.get("items"):
            logging.error("No comment data to transform")
            return []
        
        try:
            transformed_result = TRANSFORM.transform_comment_data(raw_comment_data)
            logging.info(f"Successfully transformed {len(raw_comment_data['items'])} comment records")
            return transformed_result
        except Exception as e:
            logging.error(f"Error transforming comment data: {e}", exc_info=True)
            return []

    @task()
    def fetch_captions(video_ids):
        """Fetches captions for each video individually."""
        try:
            all_captions = []
            
            for video_id in video_ids:
                params = {"part": "snippet", "videoId": video_id}
                logging.info(f"Fetching captions for video: {video_id}")
                response = DataAPI.get_captions(params)
                
                if response is not None:
                    all_captions.extend(response)
                else:
                    all_captions.extend([])
            
            return all_captions
        except Exception as e:
            logging.error(f"Error fetching captions: {e}", exc_info=True)
            return None
        
    @task()
    def transform_caption_info(raw_caption_data):
        """Transforms raw caption data into the desired format."""
        if not raw_caption_data:
            logging.error("No caption data to transform")
            return []
        
        try:
            transformed_result = TRANSFORM.transform_caption_data(raw_caption_data)
            logging.info(f"Successfully transformed {len(raw_caption_data)} caption records")
            return transformed_result
        except Exception as e:
            logging.error(f"Error transforming caption data: {e}", exc_info=True)
            return []
    
    @task()
    def publish_videos_to_kafka(data):
        """Publishes transformed video data to Kafka topic."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer_name = f"yt_video_producer_{pendulum.now().format('YYYYMMDDHHMMSS')}"
        producer = kafka_client.create_producer(producer_name)

        for key, record in enumerate(data):
            print(record)
            producer.produce(CNST.KAFKA_TOPIC_VIDEO, key=str(key).encode('utf-8'), value=json.dumps(record))
            producer.poll(0)
        
        producer.flush()
        logging.info(f"Published {len(data)} video records to Kafka topic: {CNST.KAFKA_TOPIC_VIDEO}")

    @task
    def insert_video_info_mongo(data):
        """Function to insert data into MongoDB collection"""
        try:
            client = MongoClient(Variable.get("MONGODB_URI"))
        
            
            db = client[CNST.MONGODB_NAME]
            collection = db[CNST.MONGODB_COLLECTIONS[1]]
            
            collection.insert_many(data)
            print(f"Inserted {len(data)} records into {CNST.MONGODB_COLLECTIONS[1]}")
        except Exception as e:
            logging.error(f"Error inserting video data into MongoDB: {e}", exc_info=True)

    @task()
    def publish_channels_to_kafka(data):
        """Publishes transformed channel data to Kafka topic."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer_name = f"yt_channel_producer_{pendulum.now().format('YYYYMMDDHHMMSS')}"
        producer = kafka_client.create_producer(producer_name)

        for key, record in enumerate(data):
            print(record)
            producer.produce(CNST.KAFKA_TOPIC_CHANNEL, key=str(key).encode('utf-8'), value=json.dumps(record))
            producer.poll(0)
        
        producer.flush()
        logging.info(f"Published {len(data)} channel records to Kafka topic: {CNST.KAFKA_TOPIC_CHANNEL}")
    
    @task
    def insert_channel_info_mongo(data):
        """Function to insert data into MongoDB collection"""
        try:
            client = MongoClient(Variable.get("MONGODB_URI"))
        
            
            db = client[CNST.MONGODB_NAME]
            collection = db[CNST.MONGODB_COLLECTIONS[0]]
            
            collection.insert_many(data)
            print(f"Inserted {len(data)} records into {CNST.MONGODB_COLLECTIONS[0]}")
        except Exception as e:
            logging.error(f"Error inserting video data into MongoDB: {e}", exc_info=True)

    @task()
    def publish_comments_to_kafka(data):
        """Publishes comment data to Kafka topic."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer_name = f"yt_comments_producer_{pendulum.now().format('YYYYMMDDHHMMSS')}"
        producer = kafka_client.create_producer(producer_name)

        for key, record in enumerate(data):
            print(record)
            producer.produce(CNST.KAFKA_TOPIC_COMMENTS, key=str(key).encode('utf-8'), value=json.dumps(record))
            producer.poll(0)
        
        producer.flush()
        logging.info(f"Published {len(data)} comment records to Kafka topic: {CNST.KAFKA_TOPIC_COMMENTS}")
    
    @task
    def insert_comment_info_mongo(data):
        """Function to insert data into MongoDB collection"""
        try:
            client = MongoClient(Variable.get("MONGODB_URI"))
        
            
            db = client[CNST.MONGODB_NAME]
            collection = db[CNST.MONGODB_COLLECTIONS[2]]
            
            collection.insert_many(data)
            print(f"Inserted {len(data)} records into {CNST.MONGODB_COLLECTIONS[2]}")
        except Exception as e:
            logging.error(f"Error inserting video data into MongoDB: {e}", exc_info=True)

    @task()
    def publish_captions_to_kafka(data):
        """Publishes caption data to Kafka topic."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer_name = f"yt_captions_producer_{pendulum.now().format('YYYYMMDDHHMMSS')}"
        producer = kafka_client.create_producer(producer_name)

        for key, record in enumerate(data):
            print(record)
            producer.produce(CNST.KAFKA_TOPIC_CAPTIONS, key=str(key).encode('utf-8'), value=json.dumps(record))
            producer.poll(0)
        
        producer.flush()
        logging.info(f"Published {len(data)} caption records to Kafka topic: {CNST.KAFKA_TOPIC_CAPTIONS}")
    
    @task
    def insert_captions_info_mongo(data):
        """Function to insert data into MongoDB collection"""
        try:
            client = MongoClient(Variable.get("MONGODB_URI"))
        
            
            db = client[CNST.MONGODB_NAME]
            collection = db[CNST.MONGODB_COLLECTIONS[3]]
            
            collection.insert_many(data)
            print(f"Inserted {len(data)} records into {CNST.MONGODB_COLLECTIONS[3]}")
        except Exception as e:
            logging.error(f"Error inserting video data into MongoDB: {e}", exc_info=True)
    

    @task()
    def fetch_transcripts(video_ids):
        """Fetches transcripts for each video individually using youtube-transcript-api."""
        try:
            all_transcripts = []
            
            for video_id in video_ids:
                logging.info(f"Fetching transcript for video: {video_id}")
                try:
                    transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
                    
                    for segment in transcript_list:
                        segment['videoId'] = video_id
                    
                    all_transcripts.append({
                        'videoId': video_id,
                        'transcript': transcript_list
                    })
                    
                except Exception as e:
                    logging.warning(f"Could not fetch transcript for video {video_id}: {e}")
            
            logging.info(f"Successfully fetched transcripts for {len(all_transcripts)} videos")
            return all_transcripts
        except Exception as e:
            logging.error(f"Error fetching transcripts: {e}", exc_info=True)
            return []

    @task()
    def transform_transcript_info(raw_transcript_data):
        """Transforms raw transcript data into the desired format."""
        if not raw_transcript_data:
            logging.error("No transcript data to transform")
            return []
        
        try:
            transformed_result = []
            
            for transcript_item in raw_transcript_data:
                video_id = transcript_item['videoId']
                transcript = transcript_item['transcript']
                total_duration = sum(segment.get('duration', 0) for segment in transcript)
                word_count = sum(len(segment.get('text', '').split()) for segment in transcript)
                
                transformed_result.append({
                    'videoId': video_id,
                    'transcript': transcript,
                    'transcriptMetrics': {
                        'segmentCount': len(transcript),
                        'totalDuration': total_duration,
                        'wordCount': word_count
                    },
                    'processedDate': pendulum.now().to_iso8601_string()
                })
            
            logging.info(f"Successfully transformed {len(raw_transcript_data)} transcript records")
            return transformed_result
        except Exception as e:
            logging.error(f"Error transforming transcript data: {e}", exc_info=True)
            return []

    @task()
    def publish_transcripts_to_kafka(data):
        """Publishes transformed transcript data to Kafka topic."""
        kafka_client = KafkaClientManager(CNST.KAFKA_CONF)
        producer_name = f"yt_transcript_producer_{pendulum.now().format('YYYYMMDDHHMMSS')}"
        producer = kafka_client.create_producer(producer_name)

        for key, record in enumerate(data):
            print(record)
            producer.produce(CNST.KAFKA_TOPIC_TRANSCRIPTS, key=str(key).encode('utf-8'), value=json.dumps(record))
            producer.poll(0)
        
        producer.flush()
        logging.info(f"Published {len(data)} transcript records to Kafka topic: {CNST.KAFKA_TOPIC_TRANSCRIPTS}")

    @task
    def insert_transcript_info_mongo(data):
        """Function to insert transcript data into MongoDB collection"""
        try:
            client = MongoClient(Variable.get("MONGODB_URI"))
            
            db = client[CNST.MONGODB_NAME]
            collection = db[CNST.MONGODB_COLLECTIONS[4]]
            
            collection.insert_many(data)
            print(f"Inserted {len(data)} records into {CNST.MONGODB_COLLECTIONS[4]}")
        except Exception as e:
            logging.error(f"Error inserting transcript data into MongoDB: {e}", exc_info=True)

        
    # DAG Flow
    kafka_setup_task = kafka_setup()
    mongodb_setup_task = mongodb_setup()
    
    # Search and extract IDs
    search_results = search_youtube()
    video_ids = extract_video_ids(search_results)
    channel_ids = extract_channel_ids(search_results)
    
    # Fetch raw data
    raw_channel_data = fetch_channel_info(channel_ids)
    raw_video_data = fetch_video_info(video_ids)
    raw_comment_data = fetch_comments(video_ids)
    raw_caption_data = fetch_captions(video_ids)
    raw_transcript_data = fetch_transcripts(video_ids)
    
    # Transform data
    transformed_channel_data = transform_channel_info(raw_channel_data)
    transformed_video_data = transform_video_info(raw_video_data)
    transformed_comment_data = transform_comment_info(raw_comment_data)
    transformed_caption_data = transform_caption_info(raw_caption_data)
    transformed_transcript_data = transform_transcript_info(raw_transcript_data)
    
    # Publish to Kafka
    publish_channel_task = publish_channels_to_kafka(transformed_channel_data)
    publish_video_task = publish_videos_to_kafka(transformed_video_data)
    publish_comment_task = publish_comments_to_kafka(transformed_comment_data)
    publish_caption_task = publish_captions_to_kafka(transformed_caption_data)
    publish_transcript_task = publish_transcripts_to_kafka(transformed_transcript_data)

    # mongo insert
    write_channels_mongo = insert_channel_info_mongo(transformed_channel_data)
    write_videos_mongo = insert_video_info_mongo(transformed_video_data)
    write_comments_mongo = insert_comment_info_mongo(transformed_comment_data)
    write_captions_mongo = insert_captions_info_mongo(transformed_caption_data)
    write_transcripts_mongo = insert_transcript_info_mongo(transformed_transcript_data)

    # setting up dependencies
    [kafka_setup_task, mongodb_setup_task] >> search_results
    search_results >> [video_ids, channel_ids]
    
    # Data fetching dependencies
    video_ids >> [raw_video_data, raw_comment_data, raw_caption_data]
    channel_ids >> raw_channel_data
    
    # Transform dependencies
    raw_channel_data >> transformed_channel_data
    raw_video_data >> transformed_video_data
    raw_comment_data >> transformed_comment_data
    raw_caption_data >> transformed_caption_data
    raw_transcript_data >> transformed_transcript_data
    
    # Publishing dependencies
    transformed_channel_data >> [publish_channel_task, write_channels_mongo]
    transformed_video_data >> [publish_video_task, write_videos_mongo]
    transformed_comment_data >> [publish_comment_task, write_comments_mongo]
    transformed_caption_data >> [publish_caption_task, write_captions_mongo]
    transformed_transcript_data >> [publish_transcript_task, write_transcripts_mongo]

# Instantiating the DAG for Airflow
video_dag_instance = youtube_video_pipeline()