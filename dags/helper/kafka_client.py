import os
import sys
import logging
import time
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

# Loading custom modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import constants as CNST

class KafkaClientManager:
    def __init__(self, kafka_conf):
        """Initializes Kafka client manager with Kafka configuration."""
        self.admin_client = AdminClient(kafka_conf)
        self.producer = None
        self.consumer = None

    def check_create_topic(self, topic_name):
        """Check if the topic exists and create it if it does not."""
        topic_metadata = self.admin_client.list_topics().topics

        if topic_name not in topic_metadata:
            logging.info(f"Topic '{topic_name}' does not exist. Creating...")
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            fs = self.admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    logging.info(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    logging.error(f"Failed to create topic '{topic}': {e}")
        else:
            logging.info(f"Topic '{topic_name}' already exists.")

    def create_producer(self, producer_name="yt_video_analytics_producer"):
        """Create a Kafka producer."""
        producer_conf = CNST.KAFKA_CONF.copy()
        producer_conf['client.id'] = producer_name
        self.producer = Producer(producer_conf)
        print(f"Producer '{producer_name}' created.")
        return self.producer

    def create_consumer(self, consumer_name="yt_video_analytics_consumer"):
        """Create a Kafka consumer."""
        consumer_conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': consumer_name,
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(consumer_conf)
        print(f"Consumer '{consumer_name}' created.")
        return self.consumer

    def clean_kafka(self, topic_name):
        """Clean up Kafka by deleting the topic and closing producer/consumer."""
        try:
            fs = self.admin_client.delete_topics([topic_name])
            for topic, f in fs.items():
                try:
                    f.result()
                    logging.info(f"Topic '{topic}' deleted successfully.")
                except Exception as e:
                    logging.error(f"Failed to delete topic '{topic}': {e}")
        except Exception as e:
            logging.error(f"Error while deleting topic: {e}")

        try:
            if self.producer:
                self.producer.flush() 
                logging.info("Kafka producer closed.")
        except Exception as e:
            logging.error(f"Error closing producer: {e}")

        try:
            if self.consumer:
                self.consumer.close()
                logging.info("Kafka consumer closed.")
        except Exception as e:
            logging.error(f"Error closing consumer: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    kafka_manager = KafkaClientManager(CNST.KAFKA_CONF)
    
    topic_name = "yt_video_analytics_topic"
    kafka_manager.check_create_topic(topic_name)

    # producer = kafka_manager.create_producer("yt_video_analytics_test_producer")
    # consumer = kafka_manager.create_consumer("yt_video_analytics_test_consumer")

    # consumer.subscribe([topic_name])

    # # Send a test message
    # test_message = "Hello Kafka! This is a test message."
    
    # def delivery_report(err, msg):
    #     """Callback for message delivery confirmation."""
    #     if err is not None:
    #         logging.error(f"Message delivery failed: {err}")
    #     else:
    #         logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # logging.info(f"Sending message: {test_message}")
    # producer.produce(topic_name, key="test_key", value=test_message, callback=delivery_report)
    # producer.flush()  # Ensure message is sent

    # # Give Kafka a moment to process
    # time.sleep(2)

    # # Consume message
    # logging.info("Reading message from Kafka...")
    # msg = consumer.poll(timeout=5.0)

    # if msg is None:
    #     logging.error("No message received!")
    # elif msg.error():
    #     logging.error(f"Consumer error: {msg.error()}")
    # else:
    #     logging.info(f"Received message: {msg.value().decode('utf-8')}")

    # # Cleanup
    # consumer.close()
    # logging.info("Consumer closed.")