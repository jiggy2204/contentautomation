# src/youtube_uploader.py
"""
YouTube Uploader Module
Uploads videos to YouTube as PRIVATE with scheduled publish times.
Uses Google's resumable upload protocol for large files.
"""

import os
import pickle
import logging
from typing import Optional, Dict, Any
from datetime import datetime
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from supabase_client import SupabaseClient
from email_notifier import EmailNotifier

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YouTubeUploader:
    """Handles uploading videos to YouTube"""
    
    def __init__(self, db_client: Optional[SupabaseClient] = None):
        """
        Initialize YouTube Uploader
        
        Args:
            db_client: Optional SupabaseClient instance
        """
        self.db = db_client or SupabaseClient()
        self.email_notifier = EmailNotifier()
        
        # YouTube API credentials
        self.credentials_file = os.getenv('YOUTUBE_CREDENTIALS_FILE', 'client_secret.json')
        self.token_file = os.getenv('YOUTUBE_TOKEN_FILE', 'youtube_token.pickle')
        
        # Digital Ocean Spaces for downloading videos
        self.do_spaces_key = os.getenv('DO_SPACES_KEY')
        self.do_spaces_secret = os.getenv('DO_SPACES_SECRET')
        self.do_spaces_endpoint = os.getenv('DO_SPACES_ENDPOINT')
        self.do_spaces_bucket = os.getenv('DO_SPACES_BUCKET')
        
        # Extract region from endpoint
        if self.do_spaces_endpoint:
            self.do_spaces_region = self.do_spaces_endpoint.split('//')[1].split('.')[0]
        else:
            self.do_spaces_region = 'nyc3'
        
        # Initialize S3 client for DO Spaces
        if self.do_spaces_key and self.do_spaces_secret:
            self.s3_client = boto3.client(
                's3',
                region_name=self.do_spaces_region,
                endpoint_url=self.do_spaces_endpoint,
                aws_access_key_id=self.do_spaces_key,
                aws_secret_access_key=self.do_spaces_secret
            )
        else:
            self.s3_client = None
            logger.warning('⚠️  DO Spaces credentials not found')
        
        # YouTube API service
        self.youtube = None
        self._authenticate()
        
        logger.info('📤 YouTube Uploader initialized')
    
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
    
    def download_from_spaces(self, spaces_url: str, local_path: str) -> bool:
        """
        Download video file from DO Spaces to local storage
        
        Args:
            spaces_url: URL to file in DO Spaces
            local_path: Local path to save file
        
        Returns:
            bool: True if successful
        """
        if not self.s3_client:
            logger.error('❌ DO Spaces client not initialized')
            return False
        
        try:
            # Extract S3 key from URL
            # Format: https://nyc3.digitaloceanspaces.com/bucket/vods/2024/10/file.mp4
            # We need: vods/2024/10/file.mp4
            parts = spaces_url.split(f'{self.do_spaces_bucket}/')
            if len(parts) < 2:
                logger.error(f'❌ Invalid Spaces URL format: {spaces_url}')
                return False
            
            s3_key = parts[1]
            
            logger.info(f'⬇️  Downloading from Spaces: {s3_key}')
            
            # Download file
            self.s3_client.download_file(
                self.do_spaces_bucket,
                s3_key,
                local_path
            )
            
            # Verify file exists and has size
            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path) / (1024 * 1024)  # MB
                logger.info(f'✅ Downloaded {file_size:.2f} MB to {local_path}')
                return True
            
            return False
            
        except ClientError as e:
            logger.error(f'❌ DO Spaces download error: {e}')
            return False
        except Exception as e:
            logger.error(f'❌ Download error: {e}')
            return False
    
    def upload_video(self, upload_record: Dict[str, Any], video_file_path: str) -> Optional[str]:
        """
        Upload video to YouTube
        
        Args:
            upload_record: YouTube upload record from database
            video_file_path: Path to local video file
        
        Returns:
            YouTube video ID if successful, None otherwise
        """
        try:
            logger.info(f'📤 Uploading video to YouTube...')
            logger.info(f'   Title: {upload_record["video_title"]}')
            
            # Prepare request body
            body = {
                'snippet': {
                    'title': upload_record['video_title'],
                    'description': upload_record['video_description'],
                    'tags': upload_record.get('video_tags', []),
                    'categoryId': str(upload_record.get('category_id', 20))
                },
                'status': {
                    'privacyStatus': upload_record.get('privacy_status', 'private'),
                    'selfDeclaredMadeForKids': False
                }
            }
            
            # Add scheduled publish time if available and status is private
            if upload_record.get('scheduled_publish_at') and upload_record.get('privacy_status') == 'private':
                # Convert to RFC 3339 format
                scheduled_time = datetime.fromisoformat(upload_record['scheduled_publish_at'])
                body['status']['publishAt'] = scheduled_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                logger.info(f'   Scheduled publish: {scheduled_time.strftime("%Y-%m-%d %I:%M %p")}')
            
            # Prepare media upload (resumable for large files)
            media = MediaFileUpload(
                video_file_path,
                mimetype='video/*',
                resumable=True,
                chunksize=10 * 1024 * 1024  # 10 MB chunks
            )
            
            # Execute upload request
            request = self.youtube.videos().insert(
                part='snippet,status',
                body=body,
                media_body=media
            )
            
            # Resumable upload with progress
            response = None
            logger.info('   Starting upload...')
            
            while response is None:
                status, response = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    logger.info(f'   Upload progress: {progress}%')
            
            video_id = response['id']
            youtube_url = f'https://www.youtube.com/watch?v={video_id}'
            
            logger.info(f'✅ Upload complete!')
            logger.info(f'   Video ID: {video_id}')
            logger.info(f'   URL: {youtube_url}')
            
            return video_id
            
        except HttpError as e:
            logger.error(f'❌ YouTube API error: {e}')
            return None
        except Exception as e:
            logger.error(f'❌ Upload error: {e}')
            return None
    
    def upload_thumbnail(self, video_id: str, thumbnail_url: str) -> bool:
        """
        Upload custom thumbnail to YouTube video
        
        Args:
            video_id: YouTube video ID
            thumbnail_url: URL to thumbnail image (Twitch thumbnail)
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info(f'🖼️  Uploading custom thumbnail...')
            
            # Download thumbnail from Twitch
            response = requests.get(thumbnail_url, timeout=30)
            response.raise_for_status()
            
            # Save temporarily
            temp_thumbnail = f'/tmp/thumbnail_{video_id}.jpg'
            with open(temp_thumbnail, 'wb') as f:
                f.write(response.content)
            
            # Upload to YouTube
            media = MediaFileUpload(temp_thumbnail, mimetype='image/jpeg')
            
            self.youtube.thumbnails().set(
                videoId=video_id,
                media_body=media
            ).execute()
            
            # Clean up temp file
            os.remove(temp_thumbnail)
            
            logger.info('✅ Thumbnail uploaded')
            return True
            
        except Exception as e:
            logger.error(f'⚠️  Thumbnail upload failed: {e}')
            logger.info('   Video will use auto-generated thumbnail')
            return False
    
    def process_queued_uploads(self) -> Dict[str, int]:
        """
        Process all queued uploads
        
        Returns:
            Stats dictionary
        """
        logger.info('🚀 Processing queued YouTube uploads...')
        
        try:
            # Get queued uploads with vod_downloads and streams data
            queued = self.db.client.table('youtube_uploads')\
                .select('*, vod_downloads(*, streams(*))')\
                .eq('upload_status', 'queued')\
                .execute()
            
            if not queued.data:
                logger.info('💤 No queued uploads')
                return {'processed': 0, 'success': 0, 'failed': 0}
            
            stats = {'processed': 0, 'success': 0, 'failed': 0}
            
            for upload_record in queued.data:
                stats['processed'] += 1
                upload_id = upload_record['id']
                
                try:
                    # Get file path from vod_downloads
                    vod_download = upload_record['vod_downloads']
                    file_path_or_url = vod_download['file_path']
                    
                    # Check if file_path is a DO Spaces URL or local path
                    if file_path_or_url.startswith('http'):
                        # Download from DO Spaces
                        local_file = f'/tmp/vod_{upload_id}.mp4'
                        
                        logger.info(f'📥 Downloading VOD from DO Spaces...')
                        success = self.download_from_spaces(file_path_or_url, local_file)
                        
                        if not success:
                            logger.error('❌ Failed to download from DO Spaces')
                            self.db.mark_upload_failed(upload_id, 'Failed to download from DO Spaces')
                            stats['failed'] += 1
                            continue
                    else:
                        # Use local file path
                        local_file = file_path_or_url
                    
                    # Verify file exists
                    if not os.path.exists(local_file):
                        logger.error(f'❌ Video file not found: {local_file}')
                        self.db.mark_upload_failed(upload_id, f'File not found: {local_file}')
                        stats['failed'] += 1
                        continue
                    
                    # Mark as uploading
                    self.db.mark_upload_started(upload_id)
                    
                    # Upload to YouTube
                    video_id = self.upload_video(upload_record, local_file)
                    
                    if not video_id:
                        logger.error('❌ Upload failed')
                        self.db.mark_upload_failed(upload_id, 'YouTube upload failed')
                        stats['failed'] += 1
                        
                        # Clean up temp file if we downloaded it
                        if file_path_or_url.startswith('http') and os.path.exists(local_file):
                            os.remove(local_file)
                        
                        continue
                    
                    # Upload thumbnail if available
                    if upload_record.get('thumbnail_url'):
                        self.upload_thumbnail(video_id, upload_record['thumbnail_url'])
                    
                    # Generate YouTube URL
                    youtube_url = f'https://www.youtube.com/watch?v={video_id}'
                    
                    # Mark as completed
                    self.db.mark_upload_completed(upload_id, video_id, youtube_url)
                    
                    # Clean up temp file if we downloaded it
                    if file_path_or_url.startswith('http') and os.path.exists(local_file):
                        os.remove(local_file)
                        logger.info('🗑️  Cleaned up temporary file')
                    
                    # If this was flagged for manual review, send email with YouTube link
                    if upload_record.get('manual_review_required'):
                        stream = vod_download['streams']
                        logger.info('📧 Sending manual review notification with YouTube link...')
                        self.email_notifier.send_metadata_failure_alert(
                            stream_title=stream['title'],
                            game_name=stream.get('game_name', 'Unknown'),
                            twitch_vod_id=stream['twitch_vod_id'],
                            youtube_url=youtube_url
                        )
                    
                    stats['success'] += 1
                    logger.info(f'✅ Upload complete: {youtube_url}')
                    
                except Exception as e:
                    logger.error(f'❌ Error processing upload {upload_id}: {e}')
                    self.db.mark_upload_failed(upload_id, str(e))
                    stats['failed'] += 1
                    continue
            
            logger.info(f'🎉 Upload processing complete!')
            logger.info(f'   Processed: {stats["processed"]}')
            logger.info(f'   Success: {stats["success"]}')
            logger.info(f'   Failed: {stats["failed"]}')
            
            return stats
            
        except Exception as e:
            logger.error(f'❌ Error processing uploads: {e}')
            raise


# Example usage and testing
def main():
    """Test the YouTube uploader"""
    print('\n' + '='*60)
    print('Testing YouTube Uploader')
    print('='*60)
    
    uploader = YouTubeUploader()
    
    try:
        # Process queued uploads
        print('\n1. Processing queued uploads...')
        stats = uploader.process_queued_uploads()
        
        print('\n' + '='*60)
        print('Upload processing completed!')
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