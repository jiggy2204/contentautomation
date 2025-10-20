# src/youtube_handler.py
"""
YouTube Handler Module
Builds YouTube metadata (title, description, tags) using game metadata
and Twitch stream information. Creates youtube_uploads records ready for upload.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from dotenv import load_dotenv

from supabase_client import SupabaseClient
from game_metadata_handler import GameMetadataHandler
from email_notifier import EmailNotifier

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YouTubeHandler:
    """Handles YouTube metadata creation and upload preparation"""
    
    def __init__(self, db_client: Optional[SupabaseClient] = None):
        """
        Initialize YouTube Handler
        
        Args:
            db_client: Optional SupabaseClient instance
        """
        self.db = db_client or SupabaseClient()
        self.metadata_handler = GameMetadataHandler(db_client)
        self.email_notifier = EmailNotifier()
        
        # Social media links
        self.bluesky = '@sirkrisofgames.bsky.social'
        self.twitch = 'twitch.tv/sir_kris'
        self.facebook = 'sirkrisofgames'
        
        # YouTube settings
        self.default_privacy = os.getenv('YOUTUBE_DEFAULT_PRIVACY', 'private')
        self.publish_delay_hours = int(os.getenv('UPLOAD_PUBLISH_DELAY_HOURS', '14'))  # Default 14 hours (6 PM same day)
        
        logger.info('📺 YouTube Handler initialized')
        logger.info(f'   Default privacy: {self.default_privacy}')
        logger.info(f'   Publish delay: {self.publish_delay_hours} hours')
    
    def format_game_title(self, game_name: str) -> str:
        """
        Format game name for hashtag (remove spaces, special chars)
        
        Args:
            game_name: Original game name
        
        Returns:
            Formatted hashtag (e.g., "Dead Space" → "DeadSpace")
        """
        # Remove special characters and spaces
        formatted = ''.join(c for c in game_name if c.isalnum() or c == ' ')
        # Remove spaces (CamelCase style)
        formatted = formatted.replace(' ', '')
        return formatted
    
    def build_description(self, stream_title: str, game_name: str, 
                         game_metadata: Optional[Dict[str, Any]],
                         stream_date: datetime, duration_seconds: int) -> str:
        """
        Build YouTube video description
        
        Args:
            stream_title: Twitch stream title
            game_name: Game name
            game_metadata: Game metadata from APIs (or None if failed)
            stream_date: When stream started
            duration_seconds: Stream duration
        
        Returns:
            Formatted description string
        """
        # Start with stream title
        description = f"{stream_title}\n\n"
        
        # Add game description if available
        if game_metadata and game_metadata.get('description'):
            game_desc = game_metadata['description']
            # Limit description length (YouTube has limits)
            if len(game_desc) > 500:
                game_desc = game_desc[:497] + '...'
            description += f"{game_desc}\n\n"
        
        # Add stream info
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        duration_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
        
        description += f"🎮 Streamed live on Twitch: https://{self.twitch}\n"
        description += f"📅 Stream Date: {stream_date.strftime('%B %d, %Y')}\n"
        description += f"⏱️ Duration: {duration_str}\n\n"
        
        # Add social links
        description += "Follow Sir Kris:\n"
        description += f"🦋 BlueSky: {self.bluesky}\n"
        description += f"📺 Twitch: {self.twitch}\n"
        description += f"👥 Facebook: facebook.com/{self.facebook}\n\n"
        
        # Add hashtags
        hashtags = self.build_hashtags(game_name, game_metadata)
        description += " ".join(hashtags)
        
        return description
    
    def build_minimal_description(self, stream_title: str, stream_date: datetime, 
                                  duration_seconds: int) -> str:
        """
        Build minimal description when game metadata is not available
        
        Args:
            stream_title: Twitch stream title
            stream_date: When stream started
            duration_seconds: Stream duration
        
        Returns:
            Minimal description string
        """
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        duration_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
        
        description = f"{stream_title}\n\n"
        description += f"🎮 Streamed live on Twitch: https://{self.twitch}\n"
        description += f"📅 Stream Date: {stream_date.strftime('%B %d, %Y')}\n"
        description += f"⏱️ Duration: {duration_str}\n\n"
        description += "Follow Sir Kris:\n"
        description += f"🦋 BlueSky: {self.bluesky}\n"
        description += f"📺 Twitch: {self.twitch}\n"
        description += f"👥 Facebook: facebook.com/{self.facebook}\n\n"
        description += "⚠️ Video uploaded with minimal metadata - please update manually"
        
        return description
    
    def build_hashtags(self, game_name: str, game_metadata: Optional[Dict[str, Any]]) -> List[str]:
        """
        Build hashtag list for YouTube
        
        Args:
            game_name: Game name
            game_metadata: Game metadata (or None)
        
        Returns:
            List of hashtag strings
        """
        hashtags = []
        
        # Add game name hashtag
        formatted_game = self.format_game_title(game_name)
        hashtags.append(f"#{formatted_game}")
        
        # Add tags from metadata
        if game_metadata and game_metadata.get('tags'):
            for tag in game_metadata['tags'][:3]:  # Top 3 tags
                formatted_tag = self.format_game_title(tag)
                hashtags.append(f"#{formatted_tag}")
        
        # Special case: Add #TennoCreate for Warframe
        if game_name.lower() == 'warframe':
            hashtags.append('#TennoCreate')
        
        return hashtags
    
    def build_tags_list(self, game_name: str, game_metadata: Optional[Dict[str, Any]]) -> List[str]:
        """
        Build tag list for YouTube (without # symbol)
        
        Args:
            game_name: Game name
            game_metadata: Game metadata (or None)
        
        Returns:
            List of tag strings (without #)
        """
        tags = []
        
        # Add game name
        tags.append(game_name)
        
        # Add metadata tags
        if game_metadata and game_metadata.get('tags'):
            tags.extend(game_metadata['tags'][:3])

        # Too many tags will cause YT algorithm to see video as spammy
        # # Add generic gaming tags
        # tags.extend(['gaming', 'twitch', 'stream', 'gameplay'])
        
        # Special case: Warframe
        if game_name.lower() == 'warframe':
            tags.append('TennoCreate')
        
        return tags
    
    def calculate_publish_time(self, upload_time: datetime) -> datetime:
        """
        Calculate when video should be published (6 PM same day)
        
        Args:
            upload_time: When video will be uploaded
        
        Returns:
            Scheduled publish datetime
        """
        # If uploading before 6 PM, publish at 6 PM same day
        # If uploading after 6 PM, publish at 6 PM next day
        
        publish_time = upload_time.replace(hour=18, minute=0, second=0, microsecond=0)
        
        # If we're already past 6 PM, schedule for next day
        if upload_time.hour >= 18:
            publish_time += timedelta(days=1)
        
        return publish_time
    
    async def process_completed_downloads(self) -> Dict[str, int]:
        """
        Process all completed downloads and create YouTube upload records
        
        Returns:
            Stats dictionary
        """
        logger.info('🚀 Processing downloads for YouTube metadata...')
        
        try:
            # Get completed downloads that don't have upload records yet
            result = self.db.client.table('vod_downloads')\
                .select('*, streams(*)')\
                .eq('download_status', 'completed')\
                .execute()
            
            completed_downloads = result.data
            
            if not completed_downloads:
                logger.info('💤 No completed downloads to process')
                return {'processed': 0, 'success': 0, 'failed': 0}
            
            stats = {'processed': 0, 'success': 0, 'failed': 0}
            
            for item in completed_downloads:
                download_record = {k: v for k, v in item.items() if k != 'streams'}
                stream_record = item['streams']
                
                # Check if already has upload record
                existing_upload = self.db.client.table('youtube_uploads')\
                    .select('id')\
                    .eq('vod_download_id', download_record['id'])\
                    .execute()
                
                if existing_upload.data:
                    logger.info(f'⏭️  Download {download_record["id"]} already has upload record')
                    continue
                
                stats['processed'] += 1
                
                try:
                    # Get game name from Twitch metadata
                    game_name = stream_record.get('game_name')
                    if not game_name or game_name.strip() == '':
                        game_name = 'Games + Demos'
                    
                    logger.info(f'📺 Processing: {stream_record["title"]}')
                    logger.info(f'   Game: {game_name}')
                    
                    # Fetch game metadata
                    game_metadata, metadata_status = await self.metadata_handler.fetch_game_metadata(game_name)
                    
                    # Parse stream info
                    stream_started = datetime.fromisoformat(stream_record['started_at'])
                    duration_seconds = stream_record.get('duration_seconds', 0)
                    
                    # Build YouTube metadata
                    video_title = stream_record['title']  # Use Twitch title exactly
                    
                    if game_metadata:
                        # Build full description with game info
                        video_description = self.build_description(
                            stream_record['title'],
                            game_name,
                            game_metadata,
                            stream_started,
                            duration_seconds
                        )
                        video_tags = self.build_tags_list(game_name, game_metadata)
                        metadata_status_db = 'ready'
                        manual_review = False
                        review_reason = None
                    else:
                        # Build minimal description without game info
                        video_description = self.build_minimal_description(
                            stream_record['title'],
                            stream_started,
                            duration_seconds
                        )
                        video_tags = self.build_tags_list(game_name, None)
                        metadata_status_db = 'failed'
                        manual_review = True
                        review_reason = f'Game metadata not found for: {game_name}'
                        
                        logger.warning(f'⚠️  Metadata failed for {game_name}')
                    
                    # Calculate publish time (6 PM same/next day)
                    upload_time = datetime.now()
                    scheduled_publish_at = self.calculate_publish_time(upload_time)
                    
                    # Get thumbnail URL from Twitch VOD
                    # Twitch thumbnail template: https://static-cdn.jtvnw.net/previews-ttv/offset-{twitch_vod_id}-320x180.jpg
                    twitch_vod_id = stream_record.get('twitch_vod_id')
                    thumbnail_url = None
                    if twitch_vod_id:
                        thumbnail_url = f"https://static-cdn.jtvnw.net/previews-ttv/offset-{twitch_vod_id}-1920x1080.jpg"
                    
                    # Create YouTube upload record
                    upload_data = {
                        'stream_id': stream_record['id'],
                        'vod_download_id': download_record['id'],
                        'video_title': video_title,
                        'video_description': video_description,
                        'video_tags': video_tags,
                        'thumbnail_url': thumbnail_url,
                        'privacy_status': self.default_privacy,
                        'category_id': 20,  # Gaming category
                        'upload_status': 'queued',
                        'metadata_status': metadata_status_db,
                        'manual_review_required': manual_review,
                        'review_reason': review_reason,
                        'scheduled_publish_at': scheduled_publish_at.isoformat()
                    }
                    
                    youtube_record = self.db.create_youtube_upload(upload_data)
                    logger.info(f'✅ YouTube upload record created: {youtube_record["id"]}')
                    logger.info(f'   Scheduled publish: {scheduled_publish_at.strftime("%Y-%m-%d %I:%M %p")}')
                    
                    # If metadata failed, send email notification
                    if manual_review:
                        logger.info('📧 Sending manual review notification...')
                        self.email_notifier.send_metadata_failure_alert(
                            stream_title=stream_record['title'],
                            game_name=game_name,
                            twitch_vod_id=twitch_vod_id,
                            youtube_url=None  # Video not uploaded yet
                        )
                    
                    stats['success'] += 1
                    
                except Exception as e:
                    logger.error(f'❌ Error processing download {download_record["id"]}: {e}')
                    stats['failed'] += 1
                    continue
            
            logger.info(f'🎉 YouTube metadata processing complete!')
            logger.info(f'   Processed: {stats["processed"]}')
            logger.info(f'   Success: {stats["success"]}')
            logger.info(f'   Failed: {stats["failed"]}')
            
            return stats
            
        except Exception as e:
            logger.error(f'❌ Error processing downloads: {e}')
            raise


# Example usage and testing
def main():
    """Test the YouTube handler"""
    print('\n' + '='*60)
    print('Testing YouTube Handler')
    print('='*60)
    
    handler = YouTubeHandler()
    
    try:
        # Process completed downloads
        print('\n1. Processing completed downloads...')
        stats = handler.process_completed_downloads()
        
        print('\n' + '='*60)
        print('Processing completed!')
        print(f'Processed: {stats["processed"]}')
        print(f'Success: {stats["success"]}')
        print(f'Failed: {stats["failed"]}')
        print('='*60 + '\n')
        
    except Exception as e:
        print(f'\n❌ Test failed: {e}\n')
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()