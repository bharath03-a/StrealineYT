import os
import sys
import logging
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# Loading custom modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from helper import client as CL
from helper import constants as CNST

# Class for getting Data from YouTube
class LoadDataYT(CL.YouTubeDataAPI):
    # TODO: Need to  identify the output types for each of these API calls
    # TODO: Test these outputs, understand what they return
    # TODO: Implement Pydantic

    def __init__(self):
        super().__init__()
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
        # TODO: Change this fuction as it is not the correct implementation of the API retreival
        try:
            request = self.youtube_auth.videos().list(**params)
            response = request.execute()

            logging.info(f"Successfully fetched the video list")
            return response
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
        """"""
        try:
            page_size = min(100, max_comments)
            params['maxResults'] = page_size
            
            all_comments = []
            next_page_token = None
            
            while len(all_comments) < max_comments:
                if next_page_token:
                    params['pageToken'] = next_page_token
                
                response = self.youtube_auth.commentThreads().list(**params).execute()
                items = response.get('items', [])
                
                all_comments.extend(items)
                
                next_page_token = response.get('nextPageToken')
                
                if not next_page_token or len(all_comments) >= max_comments:
                    break
            
            if len(all_comments) > max_comments:
                all_comments = all_comments[:max_comments]
                
            logging.info(f"Total comment threads fetched: {len(all_comments)}")
            return all_comments
        except Exception as e:
            logging.error(f"An error occurred while fetching comment threads: {e}", exc_info=True)
            return None

    def get_captions(self, params):
        """Fetches captions with pagination, similar to get_comment_list."""
        try:
            request = self.youtube_auth.captions().list(**params)
            response = request.execute()

            captions = []
            items = response.get("items", [])
            captions.extend(items)

            logging.info(f"Total captions fetched: {len(captions)}")
            return captions

        except Exception as e:
            logging.error(f"An error occurred while fetching captions: {e}", exc_info=True)
            return None
        

# Class for getting Data from YouTube Analytics and Reporting API
class LoadAR(CL.YouTubeARAPI):
    # TODO: Need to  identify the output types for each of these API calls
    # TODO: Test these outputs, understand what they return
    # TODO: Implement Pydantic
    # TODO: Look at the documentation and check once these look really interesting

    def __init__(self):
        super().__init__()
        self.youtube_analytics = self.build_analytics_client(CNST.SERVICE_ACCOUNT_FILE)
        self.youtube_reporting = self.build_reporting_client(CNST.SERVICE_ACCOUNT_FILE)

    def retrieve_analytics_report(self, params):
        try:
            if not self.youtube_analytics:
                logging.error("YouTube Analytics service is not initialized.")
                return None

            response = self.youtube_analytics.reports().query(**params).execute()
            logging.info("Analytics report retrieved successfully.")
            return response
        except HttpError as e:
            logging.error(f"An HTTP error occurred: {e}")
            return None
        except Exception as e:
            logging.error(f"An error occurred while retrieving analytics report: {e}")
            return None
        
    def create_reporting_job(self, report_type_id, name):
        try:
            if not self.youtube_reporting:
                logging.error("YouTube Reporting service is not initialized.")
                return None

            job_body = {
                'reportTypeId': report_type_id,
                'name': name
            }
            response = self.youtube_reporting.jobs().create(body=job_body).execute()
            logging.info(f"Reporting job '{name}' created successfully.")
            return response
        except HttpError as e:
            logging.error(f"An HTTP error occurred: {e}")
            return None
        except Exception as e:
            logging.error(f"An error occurred while creating reporting job: {e}")
            return None
        
    def list_reporting_jobs(self):
        try:
            if not self.youtube_reporting:
                logging.error("YouTube Reporting service is not initialized.")
                return None

            response = self.youtube_reporting.jobs().list().execute()
            logging.info("Reporting jobs retrieved successfully.")
            return response.get('jobs', [])
        except HttpError as e:
            logging.error(f"An HTTP error occurred: {e}")
            return None
        except Exception as e:
            logging.error(f"An error occurred while listing reporting jobs: {e}")
            return None
        
    def download_report(self, job_id, report_id, download_path):
        try:
            if not self.youtube_reporting:
                logging.error("YouTube Reporting service is not initialized.")
                return None

            request = self.youtube_reporting.media().download(
                resourceName=f'reportTypes/{job_id}/reports/{report_id}'
            )
            with open(download_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    logging.info(f"Download {int(status.progress() * 100)}%.")

            logging.info(f"Report downloaded successfully to {download_path}.")
            return download_path
        except HttpError as e:
            logging.error(f"An HTTP error occurred: {e}")
            return None
        except Exception as e:
            logging.error(f"An error occurred while downloading report: {e}")
            return None