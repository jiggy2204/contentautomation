"""
Twitch API client for Content Automation system.
Handles authentication and API calls to Twitch Helix API.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from twitchAPI.twitch import Twitch
from twitchAPI.helper import first
from twitchAPI.type import AuthScope
from src.config import get_config

logger = logging.getLogger(__name__)

class TwitchAPIClient:
    """Twitch API client with helper methods for stream detection and content management."""
    
    def __init__(self):
        self.config = get_config()
        self.twitch: Optional[Twitch] = None
        self.user_id: Optional[str] = None
        self.user_login: str = self.config.TWITCH_USER_LOGIN.lower()
    
    async def initialize(self):
        """Initialize the Twitch API client with authentication."""
        try:
            self.twitch = await Twitch(
                self.config.TWITCH_CLIENT_ID,
                self.config.TWITCH_CLIENT_SECRET
            )
            
            # Get user info to cache the user ID
            user_info = await self.get_user_info()
            if user_info:
                self.user_id = user_info['id']
                logger.info(f"Twitch API initialized for user: {user_info['display_name']}")
            else:
                raise Exception(f"Could not find Twitch user: {self.user_login}")
                
        except Exception as e:
            logger.error(f"Failed to initialize Twitch API: {e}")
            raise
    
    async def close(self):
        """Close the Twitch API client."""
        if self.twitch:
            await self.twitch.close()
    
    async def get_user_info(self) -> Optional[Dict[str, Any]]:
        """Get user information by login name."""
        if not self.twitch:
            raise Exception("Twitch API not initialized")
        
        try:
            users = self.twitch.get_users(logins=[self.user_login])
            user = await first(users)
            
            if user:
                return {
                    'id': user.id,
                    'login': user.login,
                    'display_name': user.display_name,
                    'description': user.description,
                    'profile_image_url': user.profile_image_url,
                    'view_count': user.view_count,
                    'created_at': user.created_at.isoformat() if user.created_at else None
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting user info: {e}")
            return None
    
    async def get_stream_info(self) -> Optional[Dict[str, Any]]:
        """Get current stream information if user is live."""
        if not self.twitch or not self.user_id:
            raise Exception("Twitch API not initialized or user ID not found")
        
        try:
            streams = self.twitch.get_streams(user_id=[self.user_id])
            stream = await first(streams)
            
            if stream:
                return {
                    'id': stream.id,
                    'user_id': stream.user_id,
                    'user_login': stream.user_login,
                    'user_name': stream.user_name,
                    'game_id': stream.game_id,
                    'game_name': stream.game_name,
                    'type': stream.type,
                    'title': stream.title,
                    'viewer_count': stream.viewer_count,
                    'started_at': stream.started_at.isoformat() if stream.started_at else None,
                    'language': stream.language,
                    'thumbnail_url': stream.thumbnail_url,
                    'tag_ids': stream.tag_ids or [],
                    'is_mature': stream.is_mature
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting stream info: {e}")
            return None
    
    async def is_user_live(self) -> bool:
        """Check if the user is currently streaming."""
        stream_info = await self.get_stream_info()
        return stream_info is not None
    
    async def get_recent_videos(self, count: int = 5) -> List[Dict[str, Any]]:
        """Get recent VODs for the user."""
        if not self.twitch or not self.user_id:
            raise Exception("Twitch API not initialized or user ID not found")
        
        try:
            videos = self.twitch.get_videos(user_id=self.user_id, video_type='archive', first=count)
            video_list = []
            
            async for video in videos:
                video_list.append({
                    'id': video.id,
                    'stream_id': video.stream_id,
                    'user_id': video.user_id,
                    'user_login': video.user_login,
                    'user_name': video.user_name,
                    'title': video.title,
                    'description': video.description,
                    'created_at': video.created_at.isoformat() if video.created_at else None,
                    'published_at': video.published_at.isoformat() if video.published_at else None,
                    'url': video.url,
                    'thumbnail_url': video.thumbnail_url,
                    'viewable': video.viewable,
                    'view_count': video.view_count,
                    'language': video.language,
                    'type': video.type,
                    'duration': video.duration
                })
            
            return video_list
            
        except Exception as e:
            logger.error(f"Error getting recent videos: {e}")
            return []
    
    async def get_clips(self, started_at: Optional[datetime] = None, 
                       ended_at: Optional[datetime] = None, count: int = 20) -> List[Dict[str, Any]]:
        """Get clips for the user within a date range."""
        if not self.twitch or not self.user_id:
            raise Exception("Twitch API not initialized or user ID not found")
        
        try:
            clips = self.twitch.get_clips(
                broadcaster_id=self.user_id,
                started_at=started_at,
                ended_at=ended_at,
                first=count
            )
            
            clip_list = []
            async for clip in clips:
                clip_list.append({
                    'id': clip.id,
                    'url': clip.url,
                    'embed_url': clip.embed_url,
                    'broadcaster_id': clip.broadcaster_id,
                    'broadcaster_name': clip.broadcaster_name,
                    'creator_id': clip.creator_id,
                    'creator_name': clip.creator_name,
                    'video_id': clip.video_id,
                    'game_id': clip.game_id,
                    'language': clip.language,
                    'title': clip.title,
                    'view_count': clip.view_count,
                    'created_at': clip.created_at.isoformat() if clip.created_at else None,
                    'thumbnail_url': clip.thumbnail_url,
                    'duration': clip.duration,
                    'vod_offset': clip.vod_offset
                })
            
            return clip_list
            
        except Exception as e:
            logger.error(f"Error getting clips: {e}")
            return []
    
    def parse_duration_to_seconds(self, duration_str: str) -> int:
        """Parse Twitch duration string (like '1h23m45s') to seconds."""
        try:
            total_seconds = 0
            duration_str = duration_str.lower()
            
            # Parse hours
            if 'h' in duration_str:
                hours = int(duration_str.split('h')[0])
                total_seconds += hours * 3600
                duration_str = duration_str.split('h')[1]
            
            # Parse minutes
            if 'm' in duration_str:
                minutes = int(duration_str.split('m')[0])
                total_seconds += minutes * 60
                duration_str = duration_str.split('m')[1]
            
            # Parse seconds
            if 's' in duration_str:
                seconds = int(duration_str.split('s')[0])
                total_seconds += seconds
            
            return total_seconds
            
        except Exception as e:
            logger.error(f"Error parsing duration '{duration_str}': {e}")
            return 0

# Global Twitch client instance
_twitch_client = None

async def get_twitch_client() -> TwitchAPIClient:
    """Get the global Twitch API client instance."""
    global _twitch_client
    if _twitch_client is None:
        _twitch_client = TwitchAPIClient()
        await _twitch_client.initialize()
    return _twitch_client

async def close_twitch_client():
    """Close the global Twitch API client."""
    global _twitch_client
    if _twitch_client:
        await _twitch_client.close()
        _twitch_client = None