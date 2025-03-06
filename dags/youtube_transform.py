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
    
def transform_youtube_video_results(search_results):
    """
    Transforms raw YouTube API search results into a structured Polars DataFrame.
    """
    try:
        extracted_data = [
            {
                "etag": item.get("etag"),
                "kind": item.get("kind"),
                "videoId": item["id"].get("videoId"),
                "channelId": item["snippet"].get("channelId"),
                "publishedTime": item["snippet"].get("publishedAt"),
                "title": item["snippet"].get("title"),
                "channelTitle": item["snippet"].get("channelTitle"),
                "description": item["snippet"].get("description"),
                "liveBroadcastContent": item["snippet"].get("liveBroadcastContent"),
            }
            for item in search_results
            if "id" in item and item["id"].get("videoId")
        ]

        df = pl.DataFrame(extracted_data)

        logging.info(f"Successfully transformed {len(df)} records into Polars DataFrame.")
        return df.to_dicts()

    except Exception as e:
        logging.error(f"Error during transformation: {e}", exc_info=True)
        return None

def transform_video_data(response):
    """
    Transforms YouTube API video response into a structured format with additional features.
    
    Args:
        response (list): List of video data items from YouTube API
        
    Returns:
        list: List of dictionaries with extracted video information
    """
    try:
        if not response:
            logging.error("No video data found in response")
            return []

        extracted_data = []
        for item in response:
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            content_details = item.get("contentDetails", {})
            status = item.get("status", {})
            thumbnails = snippet.get("thumbnails", {})
            
            # Get best available thumbnail
            thumbnail_url = ""
            for quality in ["maxres", "standard", "high", "medium", "default"]:
                if quality in thumbnails and thumbnails[quality].get("url"):
                    thumbnail_url = thumbnails[quality].get("url")
                    thumbnail_width = thumbnails[quality].get("width", 0)
                    thumbnail_height = thumbnails[quality].get("height", 0)
                    break
            
            # Convert statistics to integers
            view_count = int(statistics.get("viewCount", 0)) if statistics.get("viewCount") else 0
            like_count = int(statistics.get("likeCount", 0)) if statistics.get("likeCount") else 0
            comment_count = int(statistics.get("commentCount", 0)) if statistics.get("commentCount") else 0
            favorite_count = int(statistics.get("favoriteCount", 0)) if statistics.get("favoriteCount") else 0
            
            # Get all available tags
            tags = snippet.get("tags", [])
            
            # Build structured video data
            video_data = {
                "video_id": item.get("id", ""),
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "description_summary": snippet.get("description", "")[:200] + "..." if len(snippet.get("description", "")) > 200 else snippet.get("description", ""),
                "channel_id": snippet.get("channelId", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "published_at": snippet.get("publishedAt", ""),
                "published_year": snippet.get("publishedAt", "")[:4] if snippet.get("publishedAt") else "",
                
                # Statistics
                "view_count": view_count,
                "like_count": like_count,
                "comment_count": comment_count,
                "favorite_count": favorite_count,
                
                # Engagement metrics
                "engagement_ratio": (like_count + comment_count) / view_count if view_count > 0 else 0,
                "likes_per_view": like_count / view_count if view_count > 0 else 0,
                "comments_per_view": comment_count / view_count if view_count > 0 else 0,
                
                # Thumbnail info
                "thumbnail_url": thumbnail_url,
                "thumbnail_width": thumbnail_width if 'thumbnail_width' in locals() else 0,
                "thumbnail_height": thumbnail_height if 'thumbnail_height' in locals() else 0,
                
                # Content details
                "duration": content_details.get("duration", ""),
                "definition": content_details.get("definition", ""),  # hd or sd
                "caption": content_details.get("caption", "false") == "true",
                "licensed_content": content_details.get("licensedContent", False),
                
                # Metadata
                "tags": tags,
                "tag_count": len(tags),
                "category_id": snippet.get("categoryId", ""),
                "live_broadcast_content": snippet.get("liveBroadcastContent", ""),
                "default_language": snippet.get("defaultLanguage", ""),
                "default_audio_language": snippet.get("defaultAudioLanguage", ""),
                
                # Status
                "privacy_status": status.get("privacyStatus", ""),
                "upload_status": status.get("uploadStatus", ""),
                "embeddable": status.get("embeddable", True),
                "made_for_kids": status.get("madeForKids", False),
                
                # Text analysis fields
                "title_length": len(snippet.get("title", "")),
                "description_length": len(snippet.get("description", "")),
                "has_hashtags": "#" in snippet.get("title", "") or "#" in snippet.get("description", ""),
            }
            
            extracted_data.append(video_data)

        logging.info(f"Successfully transformed {len(extracted_data)} video records.")
        return extracted_data

    except Exception as e:
        logging.error(f"Error transforming video data: {e}", exc_info=True)
        return []

def transform_comment_data(response):
    """
    Transforms YouTube API comment response into a structured Polars DataFrame.
    """
    try:
        if "items" not in response or not response["items"]:
            logging.error("No comment data found in response")
            return []

        extracted_data = []
        for item in response["items"]:
            snippet = item.get("snippet", {})
            top_level_comment = snippet.get("topLevelComment", {})
            comment_snippet = top_level_comment.get("snippet", {})
            
            extracted_data.append({
                "comment_id": top_level_comment.get("id", "N/A"),
                "video_id": snippet.get("videoId", "N/A"),
                "channel_id": snippet.get("channelId", "N/A"),
                "text_display": comment_snippet.get("textDisplay", "N/A"),
                "text_original": comment_snippet.get("textOriginal", "N/A"),
                "author_display_name": comment_snippet.get("authorDisplayName", "N/A"),
                "author_profile_image_url": comment_snippet.get("authorProfileImageUrl", "N/A"),
                "author_channel_url": comment_snippet.get("authorChannelUrl", "N/A"),
                "author_channel_id": comment_snippet.get("authorChannelId", {}).get("value", "N/A") if comment_snippet.get("authorChannelId") else "N/A",
                "like_count": int(comment_snippet.get("likeCount", 0)),
                "published_at": comment_snippet.get("publishedAt", "N/A"),
                "updated_at": comment_snippet.get("updatedAt", "N/A"),
                "can_reply": snippet.get("canReply", False),
                "total_reply_count": int(snippet.get("totalReplyCount", 0)),
                "is_public": snippet.get("isPublic", False)
            })

        df = pl.DataFrame(extracted_data)
        logging.info(f"Successfully transformed {len(df)} comment records into Polars DataFrame.")
        return df.to_dicts()
    
    except Exception as e:
        logging.error(f"Error transforming comment data: {e}", exc_info=True)
        return []
    
def transform_caption_data(response):
    """
    Transforms YouTube API caption response into a structured Polars DataFrame.
    """
    try:
        # Check if there are items in the response
        if not response:
            logging.error("No caption data found in response")
            return []
            
        extracted_data = []
        for item in response:
            snippet = item.get("snippet", {})
            
            extracted_data.append({
                "caption_id": item.get("id"),
                "kind": item.get("kind", "N/A"),
                "etag": item.get("etag", "N/A"),
                "video_id": snippet.get("videoId", "N/A"),
                "language": snippet.get("language", "N/A"),
                "track_kind": snippet.get("trackKind", "N/A"),
                "last_updated": snippet.get("lastUpdated", "N/A"),
                "name": snippet.get("name", ""),
                "audio_track_type": snippet.get("audioTrackType", "N/A"),
                "is_cc": snippet.get("isCC", False),
                "is_large": snippet.get("isLarge", False),
                "is_easy_reader": snippet.get("isEasyReader", False),
                "is_draft": snippet.get("isDraft", False),
                "is_auto_synced": snippet.get("isAutoSynced", False),
                "status": snippet.get("status", "N/A"),
            })
        
        df = pl.DataFrame(extracted_data)
        logging.info(f"Successfully transformed {len(df)} caption records into Polars DataFrame.")
        return df.to_dicts()
        
    except Exception as e:
        logging.error(f"Error transforming caption data: {e}", exc_info=True)
        return []