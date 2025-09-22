"""
Stream Detection Service for Content Automation system.
Monitors Twitch streams and manages stream lifecycle in database.
"""

import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from src.twitch_api import get_twitch_client, close_twitch_client
from src.database import get_db_client
from src.config import get_config

logger = logging.getLogger(__name__)

class StreamDetector:
    """Monitors Twitch streams and manages stream state in database."""
    
    def __init__(self):
        self.config = get_config()
        self.db = get_db_client()
        self.twitch_client = None
        self.current_stream_id: Optional[str] = None
        self.last_known_state: Optional[Dict[str, Any]] = None
        self.running = False
    
    async def initialize(self):
        """Initialize the stream detector."""
        try:
            self.twitch_client = await get_twitch_client()
            logger.info("Stream detector initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize stream detector: {e}")
            raise
    
    async def start_monitoring(self):
        """Start the stream monitoring loop."""
        if not self.twitch_client:
            await self.initialize()
        
        self.running = True
        logger.info(f"Starting stream monitoring (polling every {self.config.POLL_INTERVAL_SECONDS} seconds)")
        
        try:
            while self.running:
                await self.check_stream_status()
                await asyncio.sleep(self.config.POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Stream monitoring stopped by user")
        except Exception as e:
            logger.error(f"Stream monitoring error: {e}")
        finally:
            await self.cleanup()
    
    def stop_monitoring(self):
        """Stop the stream monitoring loop."""
        self.running = False
        logger.info("Stream monitoring stop requested")
    
    async def check_stream_status(self):
        """Check current stream status and handle state changes."""
        try:
            current_stream = await self.twitch_client.get_stream_info()
            
            if current_stream and not self.last_known_state:
                # Stream started
                await self.handle_stream_started(current_stream)
            elif current_stream and self.last_known_state:
                # Stream ongoing - check for updates
                await self.handle_stream_update(current_stream)
            elif not current_stream and self.last_known_state:
                # Stream ended
                await self.handle_stream_ended()
            elif not current_stream and not self.last_known_state:
                # No stream, no change
                logger.debug("No active stream detected")
            
        except Exception as e:
            logger.error(f"Error checking stream status: {e}")
    
    async def handle_stream_started(self, stream_info: Dict[str, Any]):
        """Handle when a stream starts."""
        
        # Instead of always creating a new stream, check if it exists first
        existing_stream = self.db.get_stream_by_twitch_id(stream_info['id'])
        if existing_stream:
            logger.info(f"[STREAM ONGOING]: '{stream_info['title']}' already being tracked")
            self.current_stream_id = existing_stream['id']
            self.last_known_state = stream_info
            return

        # Only create if it doesn't exist
        logger.info(f"[STREAM STARTED]: '{stream_info['title']}' playing {stream_info['game_name']}")

        try:
            logger.info(f"STREAM STARTED: '{stream_info['title']}' playing {stream_info['game_name']}")
            
            # Parse started_at timestamp
            started_at = datetime.fromisoformat(stream_info['started_at'].replace('Z', '+00:00'))
            
            # Create stream record in database
            stream_record = self.db.create_stream(
                twitch_stream_id=stream_info['id'],
                twitch_user_id=stream_info['user_id'],
                title=stream_info['title'],
                game_name=stream_info['game_name'] or 'Unknown',
                started_at=started_at
            )
            
            self.current_stream_id = stream_record['id']
            self.last_known_state = stream_info
            
            # Create processing job for VOD processing (for later phases)
            self.db.create_processing_job(
                stream_id=self.current_stream_id,
                job_type='vod_download',
                priority=1,
                metadata={'stream_info': stream_info}
            )
            
            logger.info(f"Created stream record: {self.current_stream_id}")
            
        except Exception as e:
            logger.error(f"Error handling stream start: {e}")
    
    async def handle_stream_update(self, stream_info: Dict[str, Any]):
        """Handle stream updates (title changes, etc.)."""
        try:
            # Check if title or game changed
            title_changed = stream_info['title'] != self.last_known_state.get('title')
            game_changed = stream_info['game_name'] != self.last_known_state.get('game_name')
            
            if title_changed or game_changed:
                logger.info(f"📝 Stream updated - Title: '{stream_info['title']}', Game: {stream_info['game_name']}")
                
                # Update stream record (you could add title/game update logic here if needed)
                # For now, we'll just log the changes
                
            self.last_known_state = stream_info
            
        except Exception as e:
            logger.error(f"Error handling stream update: {e}")
    
    async def handle_stream_ended(self):
        """Handle when a stream ends."""
        try:
            logger.info("🔴 STREAM ENDED")
            
            if not self.current_stream_id:
                logger.warning("Stream ended but no current stream ID found")
                return
            
            # Calculate stream duration
            ended_at = datetime.now(timezone.utc)
            started_at_str = self.last_known_state.get('started_at')
            
            if started_at_str:
                started_at = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                duration_seconds = int((ended_at - started_at).total_seconds())
            else:
                duration_seconds = 0
                logger.warning("Could not calculate stream duration")
            
            # Try to get VOD URL
            vod_url = await self.get_vod_url_for_stream()
            
            # Update stream record
            self.db.update_stream_ended(
                stream_id=self.current_stream_id,
                ended_at=ended_at,
                duration_seconds=duration_seconds,
                vod_url=vod_url
            )
            
            logger.info(f"Stream ended - Duration: {duration_seconds//3600}h {(duration_seconds%3600)//60}m {duration_seconds%60}s")
            
            # Reset state
            self.current_stream_id = None
            self.last_known_state = None
            
        except Exception as e:
            logger.error(f"Error handling stream end: {e}")
    
    async def get_vod_url_for_stream(self) -> Optional[str]:
        """Try to get the VOD URL for the recently ended stream."""
        try:
            # Get recent videos to find the VOD
            recent_videos = await self.twitch_client.get_recent_videos(count=3)
            
            if recent_videos:
                # Return the most recent VOD
                latest_vod = recent_videos[0]
                logger.info(f"Found VOD: {latest_vod['url']}")
                return latest_vod['url']
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting VOD URL: {e}")
            return None
    
    async def cleanup(self):
        """Cleanup resources."""
        try:
            await close_twitch_client()
            logger.info("Stream detector cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Main function for running the detector
async def main():
    """Main entry point for stream detection."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    detector = StreamDetector()
    
    try:
        await detector.start_monitoring()
    except KeyboardInterrupt:
        logger.info("Shutting down stream detector...")
    except Exception as e:
        logger.error(f"Stream detector failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())