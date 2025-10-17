# src/downloader.py
"""
VOD Downloader Module
Downloads Twitch VODs to local/temp storage, uploads to Digital Ocean Spaces,
then cleans up local files. Runs after twitch_handler creates download tasks.
"""

import os
import subprocess
import logging
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

from supabase_client import SupabaseClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VODDownloader:
    """Downloads Twitch VODs and uploads to Digital Ocean Spaces"""
    
    def __init__(self, db_client: Optional[SupabaseClient] = None):
        """
        Initialize VOD Downloader
        
        Args:
            db_client: Optional SupabaseClient instance
        """
        self.download_dir = os.getenv('VOD_DOWNLOAD_DIR', 'downloads')
        self.temp_dir = os.getenv('VOD_TEMP_DIR', 'temp')
        self.max_size_gb = float(os.getenv('VOD_MAX_SIZE_GB', '10'))
        self.target_size_gb = float(os.getenv('VOD_TARGET_SIZE_GB', '8'))
        
        # Digital Ocean Spaces configuration
        self.do_spaces_key = os.getenv('DO_SPACES_KEY')
        self.do_spaces_secret = os.getenv('DO_SPACES_SECRET')
        self.do_spaces_endpoint = os.getenv('DO_SPACES_ENDPOINT')
        self.do_spaces_bucket = os.getenv('DO_SPACES_BUCKET')
        
        # Extract region from endpoint (e.g., nyc3 from https://nyc3.digitaloceanspaces.com)
        if self.do_spaces_endpoint:
            self.do_spaces_region = self.do_spaces_endpoint.split('//')[1].split('.')[0]
        else:
            self.do_spaces_region = 'nyc3'
        
        self.db = db_client or SupabaseClient()
        
        # Initialize boto3 S3 client for DO Spaces
        if self.do_spaces_key and self.do_spaces_secret:
            self.s3_client = boto3.client(
                's3',
                region_name=self.do_spaces_region,
                endpoint_url=self.do_spaces_endpoint,
                aws_access_key_id=self.do_spaces_key,
                aws_secret_access_key=self.do_spaces_secret
            )
            logger.info('✅ Digital Ocean Spaces client initialized')
        else:
            self.s3_client = None
            logger.warning('⚠️  DO Spaces credentials not found - uploads will be disabled')
        
        # Ensure directories exist
        Path(self.download_dir).mkdir(parents=True, exist_ok=True)
        Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
        
        logger.info(f'📥 VOD Downloader initialized')
        logger.info(f'Download directory: {self.download_dir}')
        logger.info(f'Max VOD size: {self.max_size_gb} GB')
        logger.info(f'DO Spaces bucket: {self.do_spaces_bucket}')
    
    def get_vod_url(self, twitch_vod_id: str) -> str:
        """
        Convert Twitch VOD ID to URL
        
        Args:
            twitch_vod_id: Twitch VOD ID (e.g., "1234567890")
        
        Returns:
            Full Twitch VOD URL
        """
        return f"https://www.twitch.tv/videos/{twitch_vod_id}"
    
    def get_output_path(self, twitch_vod_id: str, stream_title: str) -> str:
        """
        Generate output file path for VOD (temporary local storage)
        
        Args:
            twitch_vod_id: Twitch VOD ID
            stream_title: Stream title (for filename)
        
        Returns:
            Full output file path in temp directory
        """
        # Sanitize title for filename (remove special characters)
        safe_title = "".join(c for c in stream_title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title[:50]  # Limit length
        
        # Create filename: vod_ID_title_timestamp.mp4
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"vod_{twitch_vod_id}_{safe_title}_{timestamp}.mp4"
        
        # Use temp directory for downloads (will be deleted after upload)
        return os.path.join(self.temp_dir, filename)
    
    def download_with_streamlink(self, vod_url: str, output_path: str) -> bool:
        """
        Download VOD using streamlink
        
        Args:
            vod_url: Twitch VOD URL
            output_path: Where to save the file
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f'🔽 Downloading with streamlink: {vod_url}')
            
            # Streamlink command
            # best = highest quality available
            # --force = overwrite if exists
            # --hls-segment-threads = faster downloads
            cmd = [
                'streamlink',
                '--force',
                '--hls-segment-threads', '4',
                '--output', output_path,
                vod_url,
                'best'
            ]
            
            # Run streamlink
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Stream output for progress logging
            for line in process.stderr:
                if 'Writing output to' in line or 'MB' in line:
                    logger.info(f'   {line.strip()}')
            
            # Wait for completion
            return_code = process.wait()
            
            if return_code == 0:
                logger.info(f'✅ Download completed: {output_path}')
                return True
            else:
                logger.error(f'❌ Streamlink failed with code {return_code}')
                return False
                
        except FileNotFoundError:
            logger.error('❌ Streamlink not found. Install with: pip install streamlink')
            return False
        except Exception as e:
            logger.error(f'❌ Streamlink error: {e}')
            return False
    
    def download_with_ytdlp(self, vod_url: str, output_path: str) -> bool:
        """
        Download VOD using yt-dlp (fallback method)
        
        Args:
            vod_url: Twitch VOD URL
            output_path: Where to save the file
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f'🔽 Downloading with yt-dlp: {vod_url}')
            
            # yt-dlp command
            cmd = [
                'yt-dlp',
                '--format', 'best',
                '--output', output_path,
                '--no-part',  # Don't use .part files
                '--no-mtime',  # Don't restore file modification time
                vod_url
            ]
            
            # Run yt-dlp
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Stream output for progress logging
            for line in process.stdout:
                if 'Downloading' in line or 'ETA' in line:
                    logger.info(f'   {line.strip()}')
            
            # Wait for completion
            return_code = process.wait()
            
            if return_code == 0:
                logger.info(f'✅ Download completed: {output_path}')
                return True
            else:
                logger.error(f'❌ yt-dlp failed with code {return_code}')
                return False
                
        except FileNotFoundError:
            logger.error('❌ yt-dlp not found. Install with: pip install yt-dlp')
            return False
        except Exception as e:
            logger.error(f'❌ yt-dlp error: {e}')
            return False
    
    def upload_to_spaces(self, local_file_path: str, twitch_vod_id: str) -> Optional[str]:
        """
        Upload file to Digital Ocean Spaces
        
        Args:
            local_file_path: Path to local file
            twitch_vod_id: Twitch VOD ID for naming
        
        Returns:
            Public URL of uploaded file, or None if failed
        """
        if not self.s3_client:
            logger.error('❌ DO Spaces client not initialized')
            return None
        
        try:
            # Generate S3 key (path in bucket): vods/2024/10/vod_12345.mp4
            file_name = os.path.basename(local_file_path)
            date_prefix = datetime.now().strftime('%Y/%m')
            s3_key = f"vods/{date_prefix}/{file_name}"
            
            logger.info(f'☁️  Uploading to DO Spaces: {s3_key}')
            
            # Upload file with progress callback
            file_size = os.path.getsize(local_file_path)
            uploaded = 0
            
            def progress_callback(bytes_uploaded):
                nonlocal uploaded
                uploaded += bytes_uploaded
                percent = (uploaded / file_size) * 100
                if percent % 10 < 1:  # Log every ~10%
                    logger.info(f'   Upload progress: {percent:.1f}%')
            
            # Upload to Spaces
            self.s3_client.upload_file(
                local_file_path,
                self.do_spaces_bucket,
                s3_key,
                Callback=progress_callback,
                ExtraArgs={'ACL': 'private'}  # Keep private, not public
            )
            
            # Generate the Spaces URL
            spaces_url = f"{self.do_spaces_endpoint}/{self.do_spaces_bucket}/{s3_key}"
            
            logger.info(f'✅ Upload complete: {spaces_url}')
            return spaces_url
            
        except ClientError as e:
            logger.error(f'❌ DO Spaces upload failed: {e}')
            return None
        except Exception as e:
            logger.error(f'❌ Upload error: {e}')
            return None
    
    def delete_local_file(self, file_path: str) -> bool:
        """
        Delete local file after successful upload
        
        Args:
            file_path: Path to file to delete
        
        Returns:
            bool: True if deleted successfully
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f'🗑️  Deleted local file: {file_path}')
                return True
            return False
        except Exception as e:
            logger.error(f'❌ Error deleting file: {e}')
            return False
    
    def get_file_size_mb(self, file_path: str) -> float:
        """
        Get file size in megabytes
        
        Args:
            file_path: Path to file
        
        Returns:
            File size in MB
        """
        try:
            size_bytes = os.path.getsize(file_path)
            size_mb = size_bytes / (1024 * 1024)
            return round(size_mb, 2)
        except Exception as e:
            logger.error(f'❌ Error getting file size: {e}')
            return 0.0
    
    def download_vod(self, download_record: Dict[str, Any], stream_record: Dict[str, Any]) -> bool:
        """
        Download a single VOD
        
        Args:
            download_record: Database download record
            stream_record: Database stream record
        
        Returns:
            bool: True if successful, False otherwise
        """
        download_id = download_record['id']
        twitch_vod_id = stream_record['twitch_vod_id']
        stream_title = stream_record['title']
        
        logger.info(f'📥 Starting download for VOD: {twitch_vod_id}')
        logger.info(f'   Title: {stream_title}')
        
        # Mark download as started
        self.db.mark_download_started(download_id)
        
        # Get VOD URL and output path
        vod_url = self.get_vod_url(twitch_vod_id)
        output_path = self.get_output_path(twitch_vod_id, stream_title)
        
        # Try streamlink first, fallback to yt-dlp
        success = self.download_with_streamlink(vod_url, output_path)
        
        if not success:
            logger.warning('⚠️  Streamlink failed, trying yt-dlp...')
            success = self.download_with_ytdlp(vod_url, output_path)
        
        if success and os.path.exists(output_path):
            # Get file size
            file_size_mb = self.get_file_size_mb(output_path)
            
            # Check if file size is reasonable
            if file_size_mb < 10:  # Less than 10 MB is suspicious
                logger.error(f'❌ Downloaded file too small ({file_size_mb} MB), likely failed')
                self.db.mark_download_failed(download_id, f'File too small: {file_size_mb} MB')
                self.delete_local_file(output_path)  # Clean up bad file
                return False
            
            # Check if exceeds max size
            file_size_gb = file_size_mb / 1024
            if file_size_gb > self.max_size_gb:
                logger.warning(f'⚠️  File size ({file_size_gb:.2f} GB) exceeds max ({self.max_size_gb} GB)')
                # Continue anyway, but log warning
            
            # Upload to DO Spaces
            logger.info('☁️  Uploading to Digital Ocean Spaces...')
            spaces_url = self.upload_to_spaces(output_path, twitch_vod_id)
            
            if not spaces_url:
                logger.error('❌ Upload to DO Spaces failed')
                self.db.mark_download_failed(download_id, 'DO Spaces upload failed')
                self.delete_local_file(output_path)  # Clean up local file
                return False
            
            # Mark download as completed with Spaces URL
            self.db.mark_download_completed(download_id, spaces_url, file_size_mb)
            
            # Delete local file after successful upload
            self.delete_local_file(output_path)
            
            logger.info(f'✅ Download and upload successful!')
            logger.info(f'   Spaces URL: {spaces_url}')
            logger.info(f'   Size: {file_size_mb} MB ({file_size_gb:.2f} GB)')
            
            return True
        else:
            error_msg = 'Download failed with both streamlink and yt-dlp'
            logger.error(f'❌ {error_msg}')
            self.db.mark_download_failed(download_id, error_msg)
            return False
    
    def process_pending_downloads(self) -> Dict[str, int]:
        """
        Process all pending downloads
        
        Returns:
            Dictionary with stats: {'successful': X, 'failed': Y, 'total': Z}
        """
        logger.info('🚀 Starting download processing...')
        
        # Get pending downloads with stream data
        pending = self.db.get_pending_downloads()
        
        if not pending:
            logger.info('💤 No pending downloads')
            return {'successful': 0, 'failed': 0, 'total': 0}
        
        logger.info(f'📋 Found {len(pending)} pending downloads')
        
        stats = {'successful': 0, 'failed': 0, 'total': len(pending)}
        
        for item in pending:
            download_record = {k: v for k, v in item.items() if not k == 'streams'}
            stream_record = item['streams']
            
            try:
                success = self.download_vod(download_record, stream_record)
                if success:
                    stats['successful'] += 1
                else:
                    stats['failed'] += 1
            except Exception as e:
                logger.error(f'❌ Error processing download {download_record["id"]}: {e}')
                self.db.mark_download_failed(download_record['id'], str(e))
                stats['failed'] += 1
        
        logger.info(f'🎉 Download processing complete!')
        logger.info(f'   Successful: {stats["successful"]}')
        logger.info(f'   Failed: {stats["failed"]}')
        logger.info(f'   Total: {stats["total"]}')
        
        return stats


# Example usage and testing
def main():
    """Test the downloader"""
    print('\n' + '='*60)
    print('Testing VOD Downloader')
    print('='*60)
    
    downloader = VODDownloader()
    
    try:
        # Process all pending downloads
        print('\n1. Processing pending downloads...')
        stats = downloader.process_pending_downloads()
        
        print('\n' + '='*60)
        print('Download processing completed!')
        print(f'Successful: {stats["successful"]}')
        print(f'Failed: {stats["failed"]}')
        print(f'Total: {stats["total"]}')
        print('='*60 + '\n')
        
    except Exception as e:
        print(f'\n❌ Test failed: {e}\n')
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()