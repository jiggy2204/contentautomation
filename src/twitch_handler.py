# src/twitch_handler.py
"""
Twitch Handler Module
Daily VOD collection system - runs once per day between 3-7 AM
to collect VODs from previous day's streams.

Updated to check last 7 days for unprocessed VODs and process them in chronological order.
"""

import traceback
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from twitchAPI.twitch import Twitch
from twitchAPI.helper import first
from dotenv import load_dotenv

from twitchAPI.type import VideoType
from supabase_client import SupabaseClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TwitchHandler:
    """Handler for daily Twitch VOD collection"""
    
    def __init__(self, db_client: Optional[SupabaseClient] = None):
        """
        Initialize Twitch Handler
        
        Args:
            db_client: Optional SupabaseClient instance. If None, creates new one.
        """
        self.client_id = os.getenv('TWITCH_CLIENT_ID')
        self.client_secret = os.getenv('TWITCH_CLIENT_SECRET')
        self.user_login = os.getenv('TWITCH_USER_LOGIN', 'sir_kris')
        
        if not self.client_id or not self.client_secret:
            raise ValueError("TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set in .env")
        
        self.twitch: Optional[Twitch] = None
        self.user_id: Optional[str] = None
        self.db = db_client or SupabaseClient()
        
        logger.info(f'🎮 Twitch Handler initialized for user: {self.user_login}')
    
    async def authenticate(self):
        """Authenticate with Twitch API"""
        try:
            self.twitch = await Twitch(self.client_id, self.client_secret)
            logger.info('✅ Twitch API authenticated')
            
            # Get user ID for the configured user
            user = await first(self.twitch.get_users(logins=[self.user_login]))
            if user:
                self.user_id = user.id
                logger.info(f'✅ Found user: {user.display_name} (ID: {self.user_id})')
            else:
                raise ValueError(f"User {self.user_login} not found on Twitch")
                
        except Exception as e:
            logger.error(f'❌ Twitch authentication failed: {e}')
            raise
    
    async def get_recent_vods(self, hours_back: int = 24) -> List[Dict[str, Any]]:
        """
        Get all VODs from the last X hours
        
        Args:
            hours_back: How many hours back to check (default 24)
        
        Returns:
            List of VOD data dictionaries
        """
        if not self.twitch or not self.user_id:
            await self.authenticate()
        
        try:
            from datetime import timezone
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
            vods = []
            
            logger.info(f'🔍 Fetching VODs from last {hours_back} hours...')
            
            # Get recent VODs (Twitch returns newest first)
            vod_generator = self.twitch.get_videos(
                user_id=self.user_id, 
                video_type=VideoType.ARCHIVE,
                first=20  # Get up to 20 recent VODs
            )
            
            async for vod in vod_generator:
                # Check if VOD is within our time window
                vod_created = vod.created_at
                
                if vod_created and vod_created < cutoff_time:
                    # This VOD is too old, stop checking
                    break
                
                vod_data = {
                    'twitch_vod_id': vod.id,
                    'title': vod.title,
                    'game_id': getattr(vod, 'game_id', None),
                    'game_name': getattr(vod, 'game_name', None),
                    'url': vod.url,
                    'duration': vod.duration,
                    'created_at': vod_created.isoformat() if vod_created else None,
                    'view_count': vod.view_count,
                    'thumbnail_url': vod.thumbnail_url,
                    'description': vod.description or '',
                    'game_id': getattr(vod, 'game_id', None),  
                    'game_name': getattr(vod, 'game_name', None)
                }
                
                vods.append(vod_data)
                logger.info(f'📹 Found VOD: {vod.id} - {vod.title}')
            
            logger.info(f'✅ Found {len(vods)} VODs from last {hours_back} hours')
            return vods
            
        except Exception as e:
            logger.error(f'❌ Error fetching VODs: {e}')
            traceback.print_exc()
            return []
    
    async def get_vod_details(self, vod_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific VOD
        
        Args:
            vod_id: Twitch VOD ID
        
        Returns:
            Dictionary with VOD details
        """
        if not self.twitch:
            await self.authenticate()
        
        try:
            vod_generator = self.twitch.get_videos(video_id=[vod_id])
            vod = await first(vod_generator)
            
            if not vod:
                logger.warning(f'⚠️  VOD {vod_id} not found')
                return None
            
            vod_data = {
                'twitch_vod_id': vod.id,
                'title': vod.title,
                'url': vod.url,
                'duration': vod.duration,
                'created_at': vod.created_at.isoformat() if vod.created_at else None,
                'view_count': vod.view_count,
                'thumbnail_url': vod.thumbnail_url,
                'description': vod.description or '',
                'language': vod.language,
                'game_id': getattr(vod, 'game_id', None),  
                'game_name': getattr(vod, 'game_name', None) 
            }
            
            return vod_data
            
        except Exception as e:
            logger.error(f'❌ Error getting VOD details: {e}')
            traceback.print_exc()
            return None
    
    async def get_game_name_from_id(self, game_id: str) -> Optional[str]:
        """
        Get exact game name from Twitch using game_id
        Uses: https://dev.twitch.tv/docs/api/reference#get-games
        
        Args:
            game_id: Twitch game ID
        
        Returns:
            Game name or None if not found
        """
        if not self.twitch:
            await self.authenticate()
        
        try:
            game_generator = self.twitch.get_games(game_ids=[game_id])
            game = await first(game_generator)
            
            if game:
                logger.info(f'🎮 Found game: {game.name} (ID: {game_id})')
                return game.name
            else:
                logger.warning(f'⚠️  Game ID {game_id} not found')
                return None
                
        except Exception as e:
            logger.error(f'❌ Error getting game name for ID {game_id}: {e}')
            return None
        
    def parse_duration(self, duration) -> int:
        """
        Parse Twitch duration to seconds
        Handles both string format ("2h30m15s") and timedelta objects
        
        Args:
            duration: Duration from Twitch (string or timedelta)
        
        Returns:
            Duration in seconds
        """
        import re
        from datetime import timedelta
        
        # If it's already a timedelta object, convert to seconds
        if isinstance(duration, timedelta):
            return int(duration.total_seconds())
        
        # If it's a string, parse it
        if isinstance(duration, str):
            hours = 0
            minutes = 0
            seconds = 0
            
            # Extract hours
            h_match = re.search(r'(\d+)h', duration)
            if h_match:
                hours = int(h_match.group(1))
            
            # Extract minutes
            m_match = re.search(r'(\d+)m', duration)
            if m_match:
                minutes = int(m_match.group(1))
            
            # Extract seconds
            s_match = re.search(r'(\d+)s', duration)
            if s_match:
                seconds = int(s_match.group(1))
            
            return hours * 3600 + minutes * 60 + seconds
        
        # If it's an integer, return as-is
        if isinstance(duration, int):
            return duration
        
        # Unknown type, log warning and return 0
        logger.warning(f'⚠️  Unknown duration type: {type(duration)}')
        return 0
    
    async def process_new_vods(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Main daily processing function:
        1. Get VODs from last X days (default 7)
        2. Check which are new (not in database)
        3. Process oldest unprocessed VODs first (FIFO)
        
        Args:
            days_back: How many days back to check for unprocessed VODs (default 7)
        
        Returns:
            List of newly processed VODs
        """
        logger.info('🚀 Starting daily VOD processing...')
        
        # Get recent VODs from Twitch (check last 7 days to catch any missed streams)
        hours_to_check = days_back * 24
        vods = await self.get_recent_vods(hours_back=hours_to_check)
        
        if not vods:
            logger.info(f'💤 No VODs found in last {days_back} days')
            return []
        
        # Filter for unprocessed VODs
        unprocessed_vods = []
        for vod in vods:
            existing_stream = self.db.get_stream_by_twitch_id(vod['twitch_vod_id'])
            if not existing_stream:
                unprocessed_vods.append(vod)
            else:
                logger.info(f'⏭️  VOD already processed: {vod["twitch_vod_id"]} - {vod["title"]}')
        
        if not unprocessed_vods:
            logger.info('✅ All VODs already processed')
            return []
        
        # Sort by created_at (oldest first) to process in chronological order
        unprocessed_vods.sort(key=lambda v: v['created_at'])
        
        logger.info(f'📋 Found {len(unprocessed_vods)} unprocessed VODs (processing oldest first):')
        for i, vod in enumerate(unprocessed_vods, 1):
            logger.info(f'   {i}. {vod["title"]} (ID: {vod["twitch_vod_id"]}, Created: {vod["created_at"]})')
        
        new_vods = []
        
        for vod in unprocessed_vods:
            try:
                # Create new stream record
                duration_seconds = self.parse_duration(vod['duration'])
                
                # Calculate stream start time from VOD created time and duration
                vod_created = datetime.fromisoformat(vod['created_at'])
                stream_started = vod_created
                stream_ended = vod_created

                # Get game_id and game_name from VOD
                game_id = vod.get('game_id')
                game_name = vod.get('game_name')

                # If we have game_id but no game_name, look it up
                if game_id and not game_name:
                    game_name = await self.get_game_name_from_id(game_id)
                    logger.info(f'🎮 Looked up game name: {game_name}')
                
                stream_data = {
                    'twitch_stream_id': f"vod_{vod['twitch_vod_id']}",
                    'twitch_vod_id': vod['twitch_vod_id'],
                    'user_login': self.user_login,
                    'title': vod['title'],
                    'game_id': game_id,
                    'game_name': game_name,
                    'started_at': stream_started.isoformat(),
                    'ended_at': stream_ended.isoformat(),
                    'duration_seconds': duration_seconds,
                    'stream_status': 'vod_available'
                }
                
                # Create stream record
                stream_record = self.db.create_stream(stream_data)
                logger.info(f'✅ Stream record created: {stream_record["id"]}')
                
                # Create download task
                download_record = self.db.create_vod_download(stream_record['id'])
                logger.info(f'✅ Download task created: {download_record["id"]}')
                
                new_vods.append({
                    'stream': stream_record,
                    'download': download_record,
                    'vod': vod
                })
                
                logger.info(f'🎉 Successfully processed VOD: {vod["title"]} ({vod["twitch_vod_id"]})')
                
            except Exception as e:
                logger.error(f'❌ Error processing VOD {vod.get("twitch_vod_id")}: {e}')
                traceback.print_exc()
                continue
        
        logger.info(f'🎉 Successfully processed {len(new_vods)} new VODs out of {len(unprocessed_vods)} found')
        return new_vods
    
    async def close(self):
        """Clean up resources"""
        if self.twitch:
            await self.twitch.close()
            logger.info('👋 Twitch client closed')


# Example usage and testing
async def main():
    """Test the Twitch Handler"""
    import asyncio
    
    print('\n' + '='*60)
    print('Testing Twitch Handler - Daily VOD Collection')
    print('='*60)
    
    handler = TwitchHandler()
    
    try:
        # Test 1: Authenticate
        print('\n1. Authenticating with Twitch...')
        await handler.authenticate()
        print(f'✅ Authenticated as {handler.user_login} (ID: {handler.user_id})')
        
        # Test 2: Get recent VODs (check last 7 days)
        print('\n2. Fetching VODs from last 7 days...')
        vods = await handler.get_recent_vods(hours_back=168)  # 7 days
        print(f'✅ Found {len(vods)} VODs')
        
        if vods:
            print('\n📹 VOD Details:')
            for i, vod in enumerate(vods, 1):
                print(f'   {i}. {vod["title"]}')
                print(f'      ID: {vod["twitch_vod_id"]}')
                print(f'      Duration: {vod["duration"]}')
                print(f'      Created: {vod["created_at"]}')
                print()
        
        # Test 3: Process new VODs
        print('3. Processing new VODs...')
        new_vods = await handler.process_new_vods()
        print(f'✅ Processed {len(new_vods)} new VODs')
        
        if new_vods:
            print('\n🆕 Newly Processed VODs:')
            for i, item in enumerate(new_vods, 1):
                print(f'   {i}. {item["vod"]["title"]}')
                print(f'      Stream ID: {item["stream"]["id"]}')
                print(f'      Download ID: {item["download"]["id"]}')
                print()
        
        print('='*60)
        print('All tests completed! ✅')
        print('='*60 + '\n')
        
    except Exception as e:
        print(f'\n❌ Test failed: {e}\n')
        import traceback
        traceback.print_exc()
        raise
    finally:
        await handler.close()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())