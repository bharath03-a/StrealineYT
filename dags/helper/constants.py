YT_API_SCOPE = ["https://www.googleapis.com/auth/youtube.readonly"]

YT_ANALYTICS_API_SCOPE = ['https://www.googleapis.com/auth/yt-analytics.readonly']

YT_REPORTING_API_SCOPE = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']

SERVICE_ACCOUNT_FILE = "../secrets/youtube_api_secret.json"

# Kafka setup constants
KAFKA_CONF = {
    'bootstrap.servers': 'broker:29092',
}

KAFKA_TOPIC_NAME = 'yt_video_analytics'
