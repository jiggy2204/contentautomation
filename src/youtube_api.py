"""
YouTube API client for content automation system.
Handles authentication, video uploads, and metadata management.
"""

import os
import json
import pickle
from typing import Optional, Dict, Any, List
from pathlib import Path
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)

class YouTubeAPI:
    """YouTube Data API v3 client for video uploads and management."""
    
    # YouTube API scopes for uploading and managing videos
    SCOPES = [
        'https://www.googleapis.com/auth/youtube.upload',
        'https://www.googleapis.com/auth/youtube'
    ]
    
    def __init__(self, credentials_file: str = "client_secret.json", token_file: str = "youtube_token.pickle"):
        """
        Initialize YouTube API client.
        
        Args:
            credentials_file: Path to OAuth2 client secrets JSON file
            token_file: Path to store/load authentication tokens
        """
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.service = None
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Handle OAuth2 authentication flow."""
        creds = None
        
        # Load existing token if available
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                creds = pickle.load(token)
        
        # If no valid credentials, start OAuth flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    logger.info("Refreshed YouTube API credentials")
                except Exception as e:
                    logger.warning(f"Failed to refresh credentials: {e}")
                    creds = None
            
            if not creds:
                if not os.path.exists(self.credentials_file):
                    raise FileNotFoundError(f"OAuth credentials file not found: {self.credentials_file}")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
                logger.info("Completed YouTube API OAuth flow")
            
            # Save credentials for future use
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
        
        # Build YouTube service
        self.service = build('youtube', 'v3', credentials=creds)
        logger.info("YouTube API client initialized successfully")
    
    def upload_video(self, 
                    video_path: str,
                    title: str,
                    description: str = "",
                    tags: Optional[List[str]] = None,
                    category_id: str = "20",  # Gaming category
                    privacy_status: str = "private",
                    thumbnail_path: Optional[str] = None) -> Optional[str]:
        """
        Upload a video to YouTube.
        
        Args:
            video_path: Path to video file
            title: Video title (max 100 characters)
            description: Video description (max 5000 characters)
            tags: List of tags (max 500 characters total)
            category_id: YouTube category ID (20 = Gaming)
            privacy_status: private, unlisted, or public
            thumbnail_path: Optional path to thumbnail image
            
        Returns:
            Video ID if successful, None if failed
        """
        if not os.path.exists(video_path):
            logger.error(f"Video file not found: {video_path}")
            return None
        
        # Prepare video metadata
        body = {
            'snippet': {
                'title': title[:100],  # YouTube title limit
                'description': description[:5000],  # YouTube description limit
                'categoryId': category_id
            },
            'status': {
                'privacyStatus': privacy_status
            }
        }
        
        # Add tags if provided
        if tags:
            # YouTube has a 500 character limit for all tags combined
            tags_str = ','.join(tags)
            if len(tags_str) <= 500:
                body['snippet']['tags'] = tags
            else:
                logger.warning("Tags exceed 500 character limit, truncating")
                truncated_tags = []
                current_length = 0
                for tag in tags:
                    if current_length + len(tag) + 1 <= 500:  # +1 for comma
                        truncated_tags.append(tag)
                        current_length += len(tag) + 1
                    else:
                        break
                body['snippet']['tags'] = truncated_tags
        
        try:
            # Create media upload object
            media = MediaFileUpload(
                video_path, 
                chunksize=-1,  # Upload in single request
                resumable=True,
                mimetype='video/*'
            )
            
            # Execute upload
            logger.info(f"Starting upload: {title}")
            request = self.service.videos().insert(
                part=','.join(body.keys()),
                body=body,
                media_body=media
            )
            
            response = request.execute()
            video_id = response['id']
            
            logger.info(f"Video uploaded successfully: {video_id}")
            
            # Upload thumbnail if provided
            if thumbnail_path and os.path.exists(thumbnail_path):
                self.set_thumbnail(video_id, thumbnail_path)
            
            return video_id
            
        except HttpError as e:
            logger.error(f"YouTube upload failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during upload: {e}")
            return None
    
    def set_thumbnail(self, video_id: str, thumbnail_path: str) -> bool:
        """
        Set custom thumbnail for a video.
        
        Args:
            video_id: YouTube video ID
            thumbnail_path: Path to thumbnail image
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(thumbnail_path):
            logger.error(f"Thumbnail file not found: {thumbnail_path}")
            return False
        
        try:
            media = MediaFileUpload(thumbnail_path, mimetype='image/*')
            self.service.thumbnails().set(
                videoId=video_id,
                media_body=media
            ).execute()
            
            logger.info(f"Thumbnail set for video: {video_id}")
            return True
            
        except HttpError as e:
            logger.error(f"Failed to set thumbnail: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error setting thumbnail: {e}")
            return False
    
    def update_video(self, video_id: str, **kwargs) -> bool:
        """
        Update video metadata.
        
        Args:
            video_id: YouTube video ID
            **kwargs: title, description, tags, privacy_status, etc.
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get current video data
            response = self.service.videos().list(
                part='snippet,status',
                id=video_id
            ).execute()
            
            if not response['items']:
                logger.error(f"Video not found: {video_id}")
                return False
            
            video = response['items'][0]
            
            # Update fields
            if 'title' in kwargs:
                video['snippet']['title'] = kwargs['title'][:100]
            if 'description' in kwargs:
                video['snippet']['description'] = kwargs['description'][:5000]
            if 'tags' in kwargs:
                video['snippet']['tags'] = kwargs['tags']
            if 'privacy_status' in kwargs:
                video['status']['privacyStatus'] = kwargs['privacy_status']
            
            # Execute update
            self.service.videos().update(
                part='snippet,status',
                body=video
            ).execute()
            
            logger.info(f"Video updated: {video_id}")
            return True
            
        except HttpError as e:
            logger.error(f"Failed to update video: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating video: {e}")
            return False
    
    def get_video_info(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Get video information.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            Video data dictionary or None if failed
        """
        try:
            response = self.service.videos().list(
                part='snippet,status,statistics',
                id=video_id
            ).execute()
            
            if response['items']:
                return response['items'][0]
            else:
                logger.warning(f"Video not found: {video_id}")
                return None
                
        except HttpError as e:
            logger.error(f"Failed to get video info: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting video info: {e}")
            return None
    
    def make_video_public(self, video_id: str) -> bool:
        """
        Make a video public.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            True if successful, False otherwise
        """
        return self.update_video(video_id, privacy_status='public')
    
    def get_upload_quota(self) -> Optional[Dict[str, Any]]:
        """
        Get current API quota usage (requires quota project setup).
        
        Returns:
            Quota information if available
        """
        # Note: This requires additional setup in Google Cloud Console
        # For now, we'll just return None and monitor via console
        logger.info("Quota monitoring not implemented - check Google Cloud Console")
        return None


# Utility functions for common operations
def create_video_title(stream_title: str, date: str, episode_num: Optional[int] = None) -> str:
    """
    Create YouTube video title from stream data.
    
    Args:
        stream_title: Original Twitch stream title
        date: Stream date (YYYY-MM-DD format)
        episode_num: Optional episode number
        
    Returns:
        Formatted YouTube title (max 100 chars)
    """
    if episode_num:
        title = f"Sir_Kris Stream #{episode_num} - {stream_title} ({date})"
    else:
        title = f"Sir_Kris Stream - {stream_title} ({date})"
    
    return title[:97] + "..." if len(title) > 100 else title


def create_video_description(stream_title: str, 
                           stream_start: str,
                           stream_duration: str,
                           twitch_url: str = "") -> str:
    """
    Create YouTube video description from stream data.
    
    Args:
        stream_title: Original Twitch stream title
        stream_start: Stream start time
        stream_duration: Stream duration
        twitch_url: Optional Twitch VOD URL
        
    Returns:
        Formatted description (max 5000 chars)
    """
    description = f"""🎮 {stream_title}

Stream Details:
Started: {stream_start}
Duration: {stream_duration}

This is an automated upload of Sir_Kris's Twitch stream. Follow for more gaming content!

🔴 Live on Twitch: https://twitch.tv/sir_kris
💬 Join the community! https://discord.gg/9CQFbT7AFx

#Gaming #Twitch #SirKris"""

    if twitch_url:
        description += f"\n\n🎬 Original VOD: {twitch_url}"
    
    return description[:5000]


def get_gaming_tags() -> List[str]:
    """Get default gaming-related tags for videos."""
    return [
        "Gaming",
        "Twitch",
        "Stream",
        "SirKris",
        "LiveStream",
        "Gameplay",
        "Gaming Community"
    ]


# Test function
def test_youtube_api():
    """Test YouTube API connection and authentication."""
    try:
        youtube = YouTubeAPI()
        logger.info("YouTube API test successful!")
        return True
    except Exception as e:
        logger.error(f"YouTube API test failed: {e}")
        return False


if __name__ == "__main__":
    # Basic test
    logging.basicConfig(level=logging.INFO)
    test_youtube_api()