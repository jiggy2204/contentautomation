# src/youtube_publisher.py
"""
YouTube Publisher Module
Changes video privacy status from PRIVATE to PUBLIC at scheduled times.
Runs daily at 6:00 PM to publish videos scheduled for that time.
"""

import os
import pickle
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

from src.supabase_client import SupabaseClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YouTubePublisher:
    """Handles publishing YouTube videos (changing privacy status)"""
    
    def __init__(self, db_client: Optional[SupabaseClient] = None):
        """
        Initialize YouTube Publisher
        
        Args:
            db_client: Optional SupabaseClient instance
        """
        self.db = db_client or SupabaseClient()
        
        # YouTube API credentials
        self.credentials_file = os.getenv('YOUTUBE_CREDENTIALS_FILE', 'client_secret.json')
        self.token_file = os.getenv('YOUTUBE_TOKEN_FILE', 'youtube_token.pickle')
        
        # YouTube API service
        self.youtube = None
        self._authenticate()
        
        logger.info('📢 YouTube Publisher initialized')
    
    def _authenticate(self):
        """Authenticate with YouTube API"""
        try:
            creds = None
            
            # Load existing token
            if os.path.exists(self.token_file):
                with open(self.token_file, 'rb') as token:
                    creds = pickle.load(token)
            
            # Refresh if expired
            if creds and creds.expired and creds.refresh_token:
                logger.info('🔄 Refreshing YouTube token...')
                creds.refresh(Request())
                
                # Save refreshed token
                with open(self.token_file, 'wb') as token:
                    pickle.dump(creds, token)
            
            if not creds or not creds.valid:
                raise ValueError('YouTube credentials not valid. Run generate_youtube_token.py first.')
            
            # Build YouTube service
            self.youtube = build('youtube', 'v3', credentials=creds)
            logger.info('✅ YouTube API authenticated')
            
        except Exception as e:
            logger.error(f'❌ YouTube authentication failed: {e}')
            raise
    
    def publish_video(self, video_id: str, upload_record: Dict[str, Any]) -> bool:
        """
        Change video privacy status from private to public
        
        Args:
            video_id: YouTube video ID
            upload_record: Upload record from database
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info(f'📢 Publishing video: {video_id}')
            logger.info(f'   Title: {upload_record["video_title"]}')
            
            # Update video status to public
            request_body = {
                'id': video_id,
                'status': {
                    'privacyStatus': 'public',
                    'selfDeclaredMadeForKids': False
                }
            }
            
            self.youtube.videos().update(
                part='status',
                body=request_body
            ).execute()
            
            logger.info(f'✅ Video published successfully!')
            return True
            
        except HttpError as e:
            logger.error(f'❌ YouTube API error: {e}')
            return False
        except Exception as e:
            logger.error(f'❌ Publish error: {e}')
            return False
    
    def get_videos_to_publish(self, publish_window_minutes: int = 30) -> List[Dict[str, Any]]:
        """
        Get videos scheduled to be published within the time window
        
        Args:
            publish_window_minutes: Time window to check (default 30 minutes)
        
        Returns:
            List of upload records ready to publish
        """
        try:
            # Get current time and window
            now = datetime.now()
            window_start = now - timedelta(minutes=publish_window_minutes)
            window_end = now + timedelta(minutes=publish_window_minutes)
            
            logger.info(f'🔍 Checking for videos to publish...')
            logger.info(f'   Time window: {window_start.strftime("%I:%M %p")} - {window_end.strftime("%I:%M %p")}')
            
            # Query videos that are:
            # 1. Upload status = completed
            # 2. Privacy status = private
            # 3. Scheduled publish time is within window
            # 4. Metadata status = ready (skip manual review videos)
            result = self.db.client.table('youtube_uploads')\
                .select('*')\
                .eq('upload_status', 'completed')\
                .eq('privacy_status', 'private')\
                .eq('metadata_status', 'ready')\
                .gte('scheduled_publish_at', window_start.isoformat())\
                .lte('scheduled_publish_at', window_end.isoformat())\
                .execute()
            
            videos = result.data
            
            if videos:
                logger.info(f'📋 Found {len(videos)} videos to publish')
                for video in videos:
                    scheduled = datetime.fromisoformat(video['scheduled_publish_at'])
                    logger.info(f'   - {video["video_title"][:50]}... (scheduled: {scheduled.strftime("%I:%M %p")})')
            else:
                logger.info('💤 No videos ready to publish')
            
            return videos
            
        except Exception as e:
            logger.error(f'❌ Error querying videos: {e}')
            return []
    
    def process_scheduled_publishes(self, publish_window_minutes: int = 30) -> Dict[str, int]:
        """
        Process all videos scheduled for publishing
        
        Args:
            publish_window_minutes: Time window to check (default 30 minutes)
        
        Returns:
            Stats dictionary
        """
        logger.info('🚀 Processing scheduled video publishes...')
        logger.info(f'   Current time: {datetime.now().strftime("%Y-%m-%d %I:%M %p")}')
        
        # Get videos to publish
        videos = self.get_videos_to_publish(publish_window_minutes)
        
        if not videos:
            return {'processed': 0, 'success': 0, 'failed': 0, 'skipped': 0}
        
        stats = {'processed': 0, 'success': 0, 'failed': 0, 'skipped': 0}
        
        for upload_record in videos:
            stats['processed'] += 1
            upload_id = upload_record['id']
            video_id = upload_record.get('youtube_video_id')
            
            try:
                # Verify we have a video ID
                if not video_id:
                    logger.error(f'❌ No YouTube video ID for upload {upload_id}')
                    stats['failed'] += 1
                    continue
                
                # Skip if manual review required
                if upload_record.get('manual_review_required'):
                    logger.warning(f'⏭️  Skipping video requiring manual review: {upload_id}')
                    stats['skipped'] += 1
                    continue
                
                # Publish video
                success = self.publish_video(video_id, upload_record)
                
                if success:
                    # Update database - change privacy status to public
                    self.db.update_youtube_upload(upload_id, {
                        'privacy_status': 'public',
                        'updated_at': datetime.now().isoformat()
                    })
                    
                    stats['success'] += 1
                    logger.info(f'✅ Published: {upload_record["youtube_url"]}')
                else:
                    stats['failed'] += 1
                    logger.error(f'❌ Failed to publish: {upload_id}')
                
            except Exception as e:
                logger.error(f'❌ Error processing upload {upload_id}: {e}')
                stats['failed'] += 1
                continue
        
        logger.info(f'🎉 Publishing complete!')
        logger.info(f'   Processed: {stats["processed"]}')
        logger.info(f'   Published: {stats["success"]}')
        logger.info(f'   Failed: {stats["failed"]}')
        logger.info(f'   Skipped (manual review): {stats["skipped"]}')
        
        return stats
    
    def publish_video_by_id(self, upload_id: str) -> bool:
        """
        Manually publish a specific video by upload ID
        Useful for testing or manual publishing
        
        Args:
            upload_id: Database upload record ID
        
        Returns:
            bool: True if successful
        """
        try:
            # Get upload record
            result = self.db.client.table('youtube_uploads')\
                .select('*')\
                .eq('id', upload_id)\
                .execute()
            
            if not result.data:
                logger.error(f'❌ Upload record not found: {upload_id}')
                return False
            
            upload_record = result.data[0]
            video_id = upload_record.get('youtube_video_id')
            
            if not video_id:
                logger.error(f'❌ No YouTube video ID for upload {upload_id}')
                return False
            
            # Publish
            success = self.publish_video(video_id, upload_record)
            
            if success:
                # Update database
                self.db.update_youtube_upload(upload_id, {
                    'privacy_status': 'public',
                    'updated_at': datetime.now().isoformat()
                })
            
            return success
            
        except Exception as e:
            logger.error(f'❌ Error publishing video: {e}')
            return False


# Example usage and testing
def main():
    """Test the YouTube publisher"""
    import argparse
    
    parser = argparse.ArgumentParser(description='YouTube Publisher')
    parser.add_argument('--test', action='store_true', help='Run test mode')
    parser.add_argument('--publish-id', type=str, help='Manually publish specific upload ID')
    parser.add_argument('--window', type=int, default=30, help='Time window in minutes (default: 30)')
    
    args = parser.parse_args()
    
    print('\n' + '='*60)
    print('YouTube Publisher')
    print('='*60)
    
    publisher = YouTubePublisher()
    
    try:
        if args.publish_id:
            # Manual publish
            print(f'\n📢 Manually publishing upload: {args.publish_id}')
            success = publisher.publish_video_by_id(args.publish_id)
            if success:
                print('✅ Video published successfully!')
            else:
                print('❌ Failed to publish video')
        else:
            # Normal scheduled publishing
            print(f'\n1. Processing scheduled publishes (window: {args.window} minutes)...')
            stats = publisher.process_scheduled_publishes(publish_window_minutes=args.window)
            
            print('\n' + '='*60)
            print('Publishing completed!')
            print(f'Processed: {stats["processed"]}')
            print(f'Published: {stats["success"]}')
            print(f'Failed: {stats["failed"]}')
            print(f'Skipped: {stats["skipped"]}')
            print('='*60 + '\n')
        
    except Exception as e:
        print(f'\n❌ Error: {e}\n')
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()