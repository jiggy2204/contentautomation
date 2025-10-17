# src/supabase_client.py
"""
Supabase Database Client
Handles all database operations for the Twitch to YouTube automation
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SupabaseClient:
    """Client for interacting with Supabase database"""
    
    def __init__(self):
        """Initialize Supabase client"""
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env file")
        
        self.client: Client = create_client(self.url, self.key)
        logger.info('✅ Supabase client initialized')
    
    # ==========================================
    # STREAMS TABLE OPERATIONS
    # ==========================================
    
    def create_stream(self, stream_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new stream record
        
        Args:
            stream_data: Dictionary containing stream information
                Required fields: twitch_stream_id, user_login, title, started_at
                Optional fields: game_id, game_name, stream_status
        
        Returns:
            Created stream record
        """
        try:
            result = self.client.table('streams').insert(stream_data).execute()
            logger.info(f'✅ Stream created: {stream_data.get("twitch_stream_id")}')
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error creating stream: {e}')
            raise
    
    def get_stream_by_twitch_id(self, twitch_stream_id: str) -> Optional[Dict[str, Any]]:
        """Get stream by Twitch stream ID"""
        try:
            result = self.client.table('streams')\
                .select('*')\
                .eq('twitch_stream_id', twitch_stream_id)\
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error getting stream: {e}')
            raise
    
    def update_stream(self, stream_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update stream record
        
        Args:
            stream_id: UUID of stream record
            updates: Dictionary of fields to update
        
        Returns:
            Updated stream record
        """
        try:
            result = self.client.table('streams')\
                .update(updates)\
                .eq('id', stream_id)\
                .execute()
            logger.info(f'✅ Stream updated: {stream_id}')
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error updating stream: {e}')
            raise
    
    def get_streams_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all streams with a specific status"""
        try:
            result = self.client.table('streams')\
                .select('*')\
                .eq('stream_status', status)\
                .order('started_at', desc=True)\
                .execute()
            return result.data
        except Exception as e:
            logger.error(f'❌ Error getting streams by status: {e}')
            raise
    
    def mark_stream_ended(self, stream_id: str, ended_at: datetime, 
                         duration_seconds: int, twitch_vod_id: str) -> Dict[str, Any]:
        """Mark a stream as ended and update VOD information"""
        updates = {
            'ended_at': ended_at.isoformat(),
            'duration_seconds': duration_seconds,
            'twitch_vod_id': twitch_vod_id,
            'stream_status': 'vod_available'
        }
        return self.update_stream(stream_id, updates)
    
    # ==========================================
    # VOD DOWNLOADS TABLE OPERATIONS
    # ==========================================
    
    def create_vod_download(self, stream_id: str) -> Dict[str, Any]:
        """Create a new VOD download record"""
        try:
            download_data = {
                'stream_id': stream_id,
                'download_status': 'pending'
            }
            result = self.client.table('vod_downloads').insert(download_data).execute()
            logger.info(f'✅ VOD download created for stream: {stream_id}')
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error creating VOD download: {e}')
            raise
    
    def update_vod_download(self, download_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update VOD download record"""
        try:
            result = self.client.table('vod_downloads')\
                .update(updates)\
                .eq('id', download_id)\
                .execute()
            logger.info(f'✅ VOD download updated: {download_id}')
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error updating VOD download: {e}')
            raise
    
    def get_vod_download_by_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Get VOD download record for a stream"""
        try:
            result = self.client.table('vod_downloads')\
                .select('*')\
                .eq('stream_id', stream_id)\
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error getting VOD download: {e}')
            raise
    
    def get_pending_downloads(self) -> List[Dict[str, Any]]:
        """Get all pending VOD downloads"""
        try:
            result = self.client.table('vod_downloads')\
                .select('*, streams(*)')\
                .eq('download_status', 'pending')\
                .execute()
            return result.data
        except Exception as e:
            logger.error(f'❌ Error getting pending downloads: {e}')
            raise
    
    def mark_download_started(self, download_id: str) -> Dict[str, Any]:
        """Mark download as started"""
        updates = {
            'download_status': 'downloading',
            'download_started_at': datetime.now().isoformat()
        }
        return self.update_vod_download(download_id, updates)
    
    def mark_download_completed(self, download_id: str, file_path: str, 
                               file_size_mb: float) -> Dict[str, Any]:
        """Mark download as completed"""
        updates = {
            'download_status': 'completed',
            'download_completed_at': datetime.now().isoformat(),
            'file_path': file_path,
            'file_size_mb': file_size_mb,
            'download_progress_percent': 100
        }
        return self.update_vod_download(download_id, updates)
    
    def mark_download_failed(self, download_id: str, error_message: str) -> Dict[str, Any]:
        """Mark download as failed"""
        updates = {
            'download_status': 'failed',
            'error_message': error_message
        }
        return self.update_vod_download(download_id, updates)
    
    # ==========================================
    # GAME METADATA TABLE OPERATIONS
    # ==========================================
    
    def get_game_metadata(self, game_name: str) -> Optional[Dict[str, Any]]:
        """Get cached game metadata by name"""
        try:
            result = self.client.table('game_metadata')\
                .select('*')\
                .eq('game_name', game_name)\
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error getting game metadata: {e}')
            raise
    
    def create_game_metadata(self, game_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create or update game metadata cache
        
        Args:
            game_data: Dictionary containing game information
                Required: game_name, source
                Optional: description, tags, steam_app_id, igdb_id, rawg_id, etc.
        
        Returns:
            Created/updated game metadata record
        """
        try:
            # Try to upsert (insert or update if exists)
            result = self.client.table('game_metadata')\
                .upsert(game_data, on_conflict='game_name')\
                .execute()
            logger.info(f'✅ Game metadata cached: {game_data.get("game_name")}')
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error caching game metadata: {e}')
            raise
    
    # ==========================================
    # YOUTUBE UPLOADS TABLE OPERATIONS
    # ==========================================
    
    def create_youtube_upload(self, upload_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create YouTube upload record
        
        Args:
            upload_data: Dictionary containing upload information
                Required: stream_id, vod_download_id, video_title, video_description
                Optional: video_tags, thumbnail_url, privacy_status, etc.
        
        Returns:
            Created upload record
        """
        try:
            result = self.client.table('youtube_uploads').insert(upload_data).execute()
            logger.info(f'✅ YouTube upload record created for stream: {upload_data.get("stream_id")}')
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error creating YouTube upload: {e}')
            raise
    
    def update_youtube_upload(self, upload_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update YouTube upload record"""
        try:
            result = self.client.table('youtube_uploads')\
                .update(updates)\
                .eq('id', upload_id)\
                .execute()
            logger.info(f'✅ YouTube upload updated: {upload_id}')
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f'❌ Error updating YouTube upload: {e}')
            raise
    
    def get_queued_uploads(self) -> List[Dict[str, Any]]:
        """Get all queued YouTube uploads"""
        try:
            result = self.client.table('youtube_uploads')\
                .select('*, streams(*), vod_downloads(*)')\
                .eq('upload_status', 'queued')\
                .execute()
            return result.data
        except Exception as e:
            logger.error(f'❌ Error getting queued uploads: {e}')
            raise
    
    def mark_upload_started(self, upload_id: str) -> Dict[str, Any]:
        """Mark upload as started"""
        updates = {
            'upload_status': 'uploading',
            'upload_started_at': datetime.now().isoformat()
        }
        return self.update_youtube_upload(upload_id, updates)
    
    def mark_upload_completed(self, upload_id: str, youtube_video_id: str, 
                             youtube_url: str) -> Dict[str, Any]:
        """Mark upload as completed"""
        updates = {
            'upload_status': 'completed',
            'upload_completed_at': datetime.now().isoformat(),
            'youtube_video_id': youtube_video_id,
            'youtube_url': youtube_url,
            'upload_progress_percent': 100
        }
        return self.update_youtube_upload(upload_id, updates)
    
    def mark_upload_failed(self, upload_id: str, error_message: str) -> Dict[str, Any]:
        """Mark upload as failed"""
        updates = {
            'upload_status': 'failed',
            'error_message': error_message
        }
        return self.update_youtube_upload(upload_id, updates)
    
    # ==========================================
    # UTILITY FUNCTIONS
    # ==========================================
    
    def get_pipeline_status(self) -> List[Dict[str, Any]]:
        """Get complete pipeline status using the view"""
        try:
            result = self.client.table('stream_pipeline_status')\
                .select('*')\
                .limit(20)\
                .execute()
            return result.data
        except Exception as e:
            logger.error(f'❌ Error getting pipeline status: {e}')
            raise
    
    def get_failed_operations(self) -> List[Dict[str, Any]]:
        """Get all failed operations"""
        try:
            result = self.client.table('failed_operations')\
                .select('*')\
                .execute()
            return result.data
        except Exception as e:
            logger.error(f'❌ Error getting failed operations: {e}')
            raise


# Example usage and testing
if __name__ == '__main__':
    # Initialize client
    db = SupabaseClient()
    
    print('\n' + '='*60)
    print('Testing Supabase Client')
    print('='*60)
    
    # Test 1: Create a test stream
    print('\n1. Creating test stream...')
    test_stream = {
        'twitch_stream_id': 'test_stream_123',
        'user_login': 'sir_kris',
        'title': 'Test Stream - Warframe',
        'game_name': 'Warframe',
        'started_at': datetime.now().isoformat(),
        'stream_status': 'live'
    }
    
    try:
        stream = db.create_stream(test_stream)
        print(f'✅ Stream created with ID: {stream["id"]}')
        
        # Test 2: Get stream by Twitch ID
        print('\n2. Retrieving stream...')
        retrieved = db.get_stream_by_twitch_id('test_stream_123')
        print(f'✅ Retrieved stream: {retrieved["title"]}')
        
        # Test 3: Update stream status
        print('\n3. Updating stream status...')
        updated = db.update_stream(stream['id'], {'stream_status': 'ended'})
        print(f'✅ Stream status updated to: {updated["stream_status"]}')
        
        # Test 4: Get pipeline status
        print('\n4. Getting pipeline status...')
        pipeline = db.get_pipeline_status()
        print(f'✅ Found {len(pipeline)} streams in pipeline')
        
        print('\n' + '='*60)
        print('All tests passed! ✅')
        print('='*60 + '\n')
        
    except Exception as e:
        print(f'\n❌ Test failed: {e}\n')
        raise