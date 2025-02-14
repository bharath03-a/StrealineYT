import os
import sys
import logging
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field

# Google APIs
from google.oauth2 import service_account
import googleapiclient.discovery
import googleapiclient.errors

# Loading custom modules
sys.path.append("./")
import constants as CNST

# Loading environment variables
load_dotenv()

# Pydantic model for settings validation
class Settings(BaseModel):
    google_api_key: str = Field(..., env="GOOGLE_API_KEY")

class YouTubeAPI:
    def __init__(self):
        self.settings = None
        try:
            self.settings = Settings()
            logging.info("Settings loaded and validated.")
        except ValidationError as e:
            logging.error(f"Error loading settings: {e}")
            sys.exit(1)
    
    def build_auth_client(self):
        try:
            api_service_name = "youtube"
            api_version = "v3"
            api_key = CNST.API_SCOPE

            # Building the Youtube API client
            youtube = googleapiclient.discovery.build(
                api_service_name, 
                api_version, 
                developerKey=api_key
            )
            logging.info(f"Authentication using the API key has been completed.")
        except ValueError as ve:
            youtube = None
            logging.error(f"Configuration Error: {ve}")
        except Exception as e:
            youtube = None
            logging.error(f"Exception has occurred while authenticating using API KEY: {e}")

        return youtube
    
    def build_oauth_client(self, service_account_file_path=CNST.SERVICE_ACCOUNT_FILE):
        try:
            api_service_name = "youtube"
            api_version = "v3"

            # authenticating using the service account key file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_file_path, 
                scopes=CNST.API_SCOPE
            )

            # Building the Youtube API client
            youtube = googleapiclient.discovery.build(
                api_service_name,
                api_version,
                credentials=credentials
            )
            logging.info(f"Oauth using the service account key has been completed.")
        except ValueError as ve:
            youtube = None
            logging.error(f"Configuration Error: {ve}")
        except Exception as e:
            youtube = None
            logging.error(f"Exception has occurred during oauth using service account key: {e}")

        return youtube