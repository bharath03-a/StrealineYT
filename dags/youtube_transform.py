import polars as pl
import logging

def transform_youtube_results(search_results):
    """
    Transforms raw YouTube API search results into a structured Polars DataFrame.
    """
    try:
        extracted_data = [
            {
                "etag": item.get("etag"),
                "kind": item.get("kind"),
                "channelId": item["id"].get("channelId"),
                "publishedAt": item["snippet"].get("publishedAt"),
                "title": item["snippet"].get("title"),
                "description": item["snippet"].get("description"),
                "channelTitle": item["snippet"].get("channelTitle"),
                "liveBroadcastContent": item["snippet"].get("liveBroadcastContent"),
            }
            for item in search_results
            if "id" in item and item["id"].get("channelId")
        ]

        df = pl.DataFrame(extracted_data)

        logging.info(f"Successfully transformed {len(df)} records into Polars DataFrame.")
        return df.to_dicts()

    except Exception as e:
        logging.error(f"Error during transformation: {e}", exc_info=True)
        return None

def transform_channel_data(response):
    """
    Transforms YouTube API channel response into a structured Polars DataFrame.
    """
    try:
        if "items" not in response or not response["items"]:
            logging.error("No channel data found in response")
            return []

        extracted_data = []
        for item in response["items"]:
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            content_details = item.get("contentDetails", {})
            thumbnails = snippet.get("thumbnails", {}).get("high", {})
            # related_playlists = content_details.get("relatedPlaylists", {})

            extracted_data.append({
                "channel_id": item.get("id"),
                "title": snippet.get("title", "N/A"),
                "description": snippet.get("description", "N/A"),
                "custom_url": snippet.get("customUrl", "N/A"),
                "published_at": snippet.get("publishedAt", "N/A"),
                "country": snippet.get("country", "N/A"),
                "subscriber_count": int(statistics.get("subscriberCount", 0)),
                "view_count": int(statistics.get("viewCount", 0)),
                "video_count": int(statistics.get("videoCount", 0)),
                "hidden_subscriber_count": statistics.get("hiddenSubscriberCount", False),
                # "uploads_playlist": related_playlists.get("uploads", "N/A"),
                # "likes_playlist": related_playlists.get("likes", "N/A"),
                "high_thumbnail": thumbnails.get("url", "N/A"),
            })

        df = pl.DataFrame(extracted_data)
        logging.info(f"Successfully transformed {len(df)} channel records into Polars DataFrame.")
        return df.to_dicts()

    except Exception as e:
        logging.error(f"Error transforming channel data: {e}", exc_info=True)
        return []