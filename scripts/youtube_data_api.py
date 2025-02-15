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
    # TODO: Need to  identify the output types for each of these API calls
    # TODO: Test these outputs, understand what they return
    # TODO: Implement Pydantic

    def __init__(self, settings):
        super().__init__(settings)
        self.youtube_auth = self.build_auth_client()
        self.youtube_oauth = self.build_oauth_client(CNST.SERVICE_ACCOUNT_FILE)

    def get_channels(self, params):
        try:
            logging.info(f"Number of parameters: {len(params)}")

            # passing the params as is
            request = self.youtube_auth.channels().list(**params)

            response = request.execute()

            logging.info(f"Successfully fetched the channel list")
            return response
        except Exception as e:
            logging.error(f"Error fetchin channel data: {e}")
            return None

    def get_videos(self, params, max_videos=50):
        try:
            request = self.youtube_auth.videos().list(**params)

            # setting up pagination and video collection
            all_videos = []
            while request and len(all_videos) < max_videos:
                response = request.execute()

                all_videos.extend(response.get("items", []))

                if len(all_videos) >= max_videos:
                    all_videos = all_videos[:max_videos]
                    break

                request = self.youtube_auth.videos().list_next(request, response)

            logging.info(f"Total videos fetched: {len(all_videos)}")
            return all_videos

        except Exception as e:
            logging.error(f"An error occurred while fetching videos: {e}", exc_info=True)
            return None


    def get_search_results(self, params, max_results=50):
        try:
            request = self.youtube_auth.search().list(**params)

            # store the search results
            search_results = []

            while request and len(search_results) < max_results:
                response = request.execute()
                search_results.extend(response.get("items", []))

                if len(search_results) >= max_results:
                    search_results = search_results[:max_results]
                    break

                request = self.youtube_auth.search().list_next(request, response)

            logging.info(f"Total search results fetched: {len(search_results)}")
            return search_results

        except Exception as e:
            logging.error(f"An error occurred while fetching search results: {e}", exc_info=True)
            return None



    def get_comments(self, params, max_comments=100):
        try:
            request = self.youtube_auth.commentThreads().list(**params)

            all_comments = []

            while request and len(all_comments) < max_comments:
                response = request.execute()
                for item in response.get("items", []):
                    all_comments.append(item["snippet"]["topLevelComment"]["snippet"]["textDisplay"])

                if len(all_comments) >= max_comments:
                    all_comments = all_comments[:max_comments]
                    break

                request = self.youtube_auth.commentThreads().list_next(request, response)

            logging.info(f"Total comments fetched: {len(all_comments)}")
            return all_comments

        except Exception as e:
            logging.error(f"An error occurred while fetching comments: {e}", exc_info=True)
            return None

    def get_captions(self, params):
        try:
            request = self.youtube_auth.captions().list(**params)

            response = request.execute()

            captions = []
            for item in response.get("items", []):
                captions.append({
                    "id": item["id"],
                    "language": item["snippet"]["language"],
                    "name": item["snippet"]["name"]
                })

            logging.info(f"Total captions fetched: {len(captions)}")
            return captions

        except Exception as e:
            logging.error(f"An error occurred while fetching captions: {e}", exc_info=True)
            return None