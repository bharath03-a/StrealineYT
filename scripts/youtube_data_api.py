import os
import sys
import json
import logging

# Loading custom modules
sys.path.append("../helper/")
from helper import client as CL
from helper import constants as CNST

# Class for getting Data from YouTube
class LoadDataYT(CL.YouTubeAPI):
    def __init__(self, settings):
        super().__init__(settings)
        self.youtube_auth = self.build_auth_client()
        self.youtube_oauth = self.build_oauth_client(CNST.SERVICE_ACCOUNT_FILE)

    def get_channels(self, channel_id):
        try:
            request = self.youtube_auth.channels().list(
                part="",
                id=channel_id
            )

            response = request.execute()
            return response
        except Exception as e:
            logging.error(f"Error fetchin channel data: {e}")
            return None

    def get_videos(self):
        try:
            pass
        except Exception as e:
            pass

    def get_search_results(self):
        try:
            pass
        except Exception as e:
            pass

    def get_comments(self):
        try:
            pass
        except Exception as e:
            pass

    def get_captions(self):
        try:
            pass
        except Exception as e:
            pass