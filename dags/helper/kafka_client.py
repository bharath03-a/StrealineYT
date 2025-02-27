import os
import sys
import pendulum
import logging
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

# Loading custom modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import constants as CNST

# Initializing the Kafka AdminClient
admin_client = AdminClient(CNST.KAFKA_CONF)

def check_create_topic(topic_name):
    """check if the topic exists and create it if it does not."""
    topic_metadata = admin_client.list_topics()
    if topic_name not in topic_metadata.topics:
        logging.info(f"Topic '{topic_name}' does not exist. Creating...")
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        
        for topic, f in fs.items():
            try:
                f.result()
                logging.info(f"Topic '{topic}' created successfully.")
            except Exception as e:
                logging.info(f"Failed to create topic '{topic}': {e}")
    else:
        logging.info(f"Topic {topic_name} already exists.")

def create_producer(producer_name="yt_video_analytics_producer"):
    """Create a Kafka producer."""
    producer_conf = CNST.KAFKA_CONF.copy()
    producer_conf['client.id'] = producer_name
    producer = Producer(producer_conf)
    print(f"Producer '{producer_name}' created.")
    return producer

def create_consumer(consumer_name="yt_video_analytics_consumer"):
    """Create a Kafka consumer."""
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': consumer_name,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    print(f"Consumer '{consumer_name}' created.")
    return consumer