"""
Database client for Content Automation system.
Handles all interactions with Supabase PostgreSQL database.
"""

from supabase import create_client, Client
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime, timezone
from src.config import get_config

logger = logging.getLogger(__name__)

class SupabaseClient:
    """Supabase database client with helper methods for content automation."""
    
    def __init__(self):
        config = get_config()
        self.supabase: Client = create_client(
            config.SUPABASE_URL,
            config.SUPABASE_KEY
        )
    
    # Stream operations
    def create_stream(self, twitch_stream_id: str, twitch_user_id: str, 
                     title: str, game_name: str, started_at: datetime) -> Dict[str, Any]:
        """Create a new stream record."""
        try:
            result = self.supabase.table('streams').insert({
                'twitch_stream_id': twitch_stream_id,
                'twitch_user_id': twitch_user_id,
                'title': title,
                'game_name': game_name,
                'started_at': started_at.isoformat(),
                'status': 'live'
            }).execute()
            
            logger.info(f"Created stream record: {twitch_stream_id}")
            return result.data[0] if result.data else {}
            
        except Exception as e:
            logger.error(f"Error creating stream: {e}")
            raise
    
    def update_stream_ended(self, stream_id: str, ended_at: datetime, 
                           duration_seconds: int, vod_url: Optional[str] = None) -> Dict[str, Any]:
        """Update stream when it ends."""
        try:
            update_data = {
                'ended_at': ended_at.isoformat(),
                'duration_seconds': duration_seconds,
                'status': 'ended',
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            if vod_url:
                update_data['vod_url'] = vod_url
            
            result = self.supabase.table('streams').update(update_data).eq('id', stream_id).execute()
            
            logger.info(f"Updated stream ended: {stream_id}")
            return result.data[0] if result.data else {}
            
        except Exception as e:
            logger.error(f"Error updating stream end: {e}")
            raise
    
    def get_stream_by_twitch_id(self, twitch_stream_id: str) -> Optional[Dict[str, Any]]:
        """Get stream record by Twitch stream ID."""
        try:
            result = self.supabase.table('streams').select('*').eq('twitch_stream_id', twitch_stream_id).execute()
            return result.data[0] if result.data else None
            
        except Exception as e:
            logger.error(f"Error getting stream: {e}")
            return None
    
    def get_active_streams(self) -> List[Dict[str, Any]]:
        """Get all active (live) streams."""
        try:
            result = self.supabase.table('streams').select('*').eq('status', 'live').execute()
            return result.data
            
        except Exception as e:
            logger.error(f"Error getting active streams: {e}")
            return []
    
    # Processing job operations
    def create_processing_job(self, stream_id: str, job_type: str, 
                            priority: int = 0, metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Create a new processing job."""
        try:
            job_data = {
                'stream_id': stream_id,
                'job_type': job_type,
                'status': 'pending',
                'priority': priority,
                'attempts': 0,
                'max_attempts': 3
            }
            
            if metadata:
                job_data['metadata'] = metadata
            
            result = self.supabase.table('processing_jobs').insert(job_data).execute()
            
            logger.info(f"Created processing job: {job_type} for stream {stream_id}")
            return result.data[0] if result.data else {}
            
        except Exception as e:
            logger.error(f"Error creating processing job: {e}")
            raise
        
    def update_processing_job(self, job_id: str, status: str, metadata: Optional[Dict] = None, error_message: Optional[str] = None):
        """Update processing job - wrapper for update_job_status"""
        return self.update_job_status(job_id, status, error_message)
    
    def update_job_status(self, job_id: str, status: str, 
                         error_message: Optional[str] = None) -> Dict[str, Any]:
        """Update job status."""
        try:
            update_data = {
                'status': status,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            if status == 'processing':
                update_data['started_at'] = datetime.now(timezone.utc).isoformat()
            elif status in ['completed', 'failed']:
                update_data['completed_at'] = datetime.now(timezone.utc).isoformat()
            
            if error_message:
                update_data['error_message'] = error_message
            
            result = self.supabase.table('processing_jobs').update(update_data).eq('id', job_id).execute()
            
            logger.info(f"Updated job {job_id} status to {status}")
            return result.data[0] if result.data else {}
            
        except Exception as e:
            logger.error(f"Error updating job status: {e}")
            raise
    
    # Configuration operations
    def get_config_value(self, key: str) -> Optional[Any]:
        """Get configuration value by key."""
        try:
            result = self.supabase.table('config').select('value').eq('key', key).execute()
            return result.data[0]['value'] if result.data else None
            
        except Exception as e:
            logger.error(f"Error getting config value {key}: {e}")
            return None
    
    def set_config_value(self, key: str, value: Any, description: Optional[str] = None) -> Dict[str, Any]:
        """Set or update configuration value."""
        try:
            config_data = {
                'key': key,
                'value': value,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            if description:
                config_data['description'] = description
            
            # Use upsert to insert or update
            result = self.supabase.table('config').upsert(config_data).execute()
            
            logger.info(f"Set config value: {key}")
            return result.data[0] if result.data else {}
            
        except Exception as e:
            logger.error(f"Error setting config value {key}: {e}")
            raise
    
    # Health check
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            result = self.supabase.table('config').select('count').execute()
            logger.info("Database connection successful")
            return True
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

# Global database client instance
_db_client = None

def get_db_client() -> SupabaseClient:
    """Get the global database client instance."""
    global _db_client
    if _db_client is None:
        _db_client = SupabaseClient()
    return _db_client