import pendulum
from airflow import DAG
from airflow.decorators import task, dag

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
    tags = ["youtube", "ETL pipeline"]
)

def youtube_streams_etl_pipeline():
    """
    ### YouTube Streams Pipeline
    This DAG searches for YouTube video streams using the **YouTube Data API**.

    **Schedule:** Runs Daily (`@daily`)  
    """

    @task()
    def search_data_api():
        """Fetches Data from Youtube Search API"""
        pass
    
# Instantiating the DAG
dag_instance = youtube_streams_etl_pipeline()