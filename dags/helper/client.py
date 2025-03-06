import os
import sys
import logging
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from airflow.models import Variable

# Google APIs
from google.oauth2 import service_account
import googleapiclient.discovery
import googleapiclient.errors
from airflow.models import Variable

# Loading custom modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import constants as CNST

# Loading environment variables
load_dotenv()

# Pydantic model for settings validation
class Settings(BaseModel):
    google_api_key: str = Field(..., env="GOOGLE_API_KEY")

class YouTubeDataAPI:
    def __init__(self):
        self.settings = None
        try:
            self.settings = Settings(google_api_key=Variable.get("GOOGLE_API_KEY"))
            logging.info("Settings loaded and validated.")
        except ValidationError as e:
            logging.error(f"Error loading settings: - {e}")
            sys.exit(1)
    
    def build_auth_client(self):
        try:
            api_service_name = "youtube"
            api_version = "v3"
            api_key = self.settings.google_api_key

            # Building the Youtube API client
            youtube = googleapiclient.discovery.build(
                api_service_name, 
                api_version, 
                developerKey=api_key
            )
            logging.info(f"Authentication for YouTube DATA API using the API key has been completed.")
        except ValueError as ve:
            youtube = None
            logging.error(f"Configuration Error in YouTube DATA API: {ve}")
        except Exception as e:
            youtube = None
            logging.error(f"Exception has occurred while authenticating using API KEY in YouTube DATA API: {e}")

        return youtube
    
    def build_oauth_client(self, service_account_file_path=CNST.SERVICE_ACCOUNT_FILE):
        try:
            api_service_name = "youtube"
            api_version = "v3"

            # authenticating using the service account key file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_file_path, 
                scopes=CNST.YT_API_SCOPE
            )

            # Building the Youtube API client
            youtube = googleapiclient.discovery.build(
                api_service_name,
                api_version,
                credentials=credentials
            )
            logging.info(f"Oauth for YouTube DATA API using the service account key has been completed.")
        except ValueError as ve:
            youtube = None
            logging.error(f"Configuration Error in YouTube DATA API: {ve}")
        except Exception as e:
            youtube = None
            logging.error(f"Exception has occurred during oauth using service account key in YouTube DATA API: {e}")

        return youtube
    

class YouTubeARAPI:
    def __init__(self):
        self.settings = None
        try:
            self.settings = Settings(google_api_key=os.getenv("GOOGLE_API_KEY"))
            logging.info("Settings loaded and validated.")
        except ValidationError as e:
            logging.error(f"Error loading settings: {e}")
            sys.exit(1)

    def build_analytics_client(self, service_account_file_path=CNST.SERVICE_ACCOUNT_FILE):
        try:
            # authenticating using the service account key file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_file_path, 
                scopes=CNST.YT_ANALYTICS_API_SCOPE
            )

            # Building the Youtube API client
            analytics = googleapiclient.discovery.build(
                "youtubeAnalytics",
                "v2",
                credentials=credentials
            )
            logging.info(f"YouTube Analytics service built successfully.")
        except ValueError as ve:
            analytics = None
            logging.error(f"Configuration Error in YouTube Analytics: {ve}")
        except Exception as e:
            analytics = None
            logging.error(f"Error initiating YouTube Analytics service: {e}")

        return analytics
    
    def build_reporting_client(self, service_account_file_path=CNST.SERVICE_ACCOUNT_FILE):
        try:
            # authenticating using the service account key file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_file_path, 
                scopes=CNST.YT_REPORTING_API_SCOPE
            )

            # Building the Youtube API client
            reporting = googleapiclient.discovery.build(
                "youtubereporting",
                "v1",
                credentials=credentials
            )
            logging.info(f"YouTube Reporting service outh initiated successfully.")
        except ValueError as ve:
            reporting = None
            logging.error(f"Configuration Error initisting YouTube Reporting service: {ve}")
        except Exception as e:
            reporting = None
            logging.error(f"Error initisting YouTube Reporting service: {e}")

        return reporting