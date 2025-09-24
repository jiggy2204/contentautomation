"""
Upload manager that coordinates VOD processing and YouTube uploads.
Integrates with the existing stream detection and database system.
"""

import os
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path
import threading
from queue import Queue, Empty

# Import your existing modules
from config import *
from src.database import SupabaseClient
from src.youtube_api import YouTubeAPI, create_video_title, create_video_description, get_gaming_tags
from src.vod_processor import VODProcessor, process_stream_vod

logger = logging.getLogger(__name__)

class UploadManager:
    """Manages the complete pipeline from stream detection to YouTube upload."""
    
    def __init__(self):
        """Initialize the upload manager with all required services."""
        self.db = SupabaseClient()
        self.youtube = YouTubeAPI()
        self.vod_processor = VODProcessor()
        self.upload_queue = Queue()
        self.running = False
        self.worker_thread = None
        
        logger.info("Upload manager initialized")
    
    def check_for_completed_streams(self) -> List[Dict[str, Any]]:
        """
        Check database for streams that need processing.
        
        Returns:
            List of stream records ready for processing
        """
        try:
            # Query for ended streams that haven't been processed yet
            response = self.db.supabase.table('streams').select(
                'id, title, started_at, ended_at, user_login, twitch_stream_id'
            ).is_('ended_at', 'null').is_('processed_at', 'null').execute()
            
            completed_streams = []
            
            for stream in response.data:
                # Check if stream has actually ended (no longer live)
                if self._is_stream_ended(stream):
                    completed_streams.append(stream)
            
            if completed_streams:
                logger.info(f"Found {len(completed_streams)} completed streams to process")
            
            return completed_streams
            
        except Exception as e:
            logger.error(f"Failed to check for completed streams: {e}")
            return []
    
    def _is_stream_ended(self, stream: Dict[str, Any]) -> bool:
        """
        Check if a stream has actually ended.
        Could integrate with Twitch API or use time-based heuristic.
        """
        # Simple heuristic: if stream started more than 6 hours ago, consider it ended
        # In production, you'd want to use Twitch API to check actual status
        started_at = datetime.fromisoformat(stream['started_at'].replace('Z', '+00:00'))
        time_since_start = datetime.now().replace(tzinfo=started_at.tzinfo) - started_at
        
        return time_since_start > timedelta(hours=6)
    
    def queue_stream_for_processing(self, stream: Dict[str, Any]) -> bool:
        """
        Add a stream to the processing queue.
        
        Args:
            stream: Stream record from database
            
        Returns:
            True if queued successfully
        """
        try:
            # Create processing job record
            job_data = {
                'stream_id': stream['id'],
                'job_type': 'youtube_upload',
                'status': 'queued',
                'created_at': datetime.utcnow().isoformat(),
                'metadata': {
                    'stream_title': stream['title'],
                    'twitch_stream_id': stream['twitch_stream_id']
                }
            }
            
            response = self.db.supabase.table('processing_jobs').insert(job_data).execute()
            job_id = response.data[0]['id']
            
            # Add to processing queue
            self.upload_queue.put({
                'job_id': job_id,
                'stream': stream
            })
            
            logger.info(f"Stream {stream['id']} queued for processing (job {job_id})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue stream {stream['id']}: {e}")
            return False
    
    def process_upload_job(self, job: Dict[str, Any]) -> bool:
        """
        Process a single upload job.
        
        Args:
            job: Job data from queue
            
        Returns:
            True if successful
        """
        job_id = job['job_id']
        stream = job['stream']
        
        logger.info(f"Processing upload job {job_id} for stream {stream['id']}")
        
        try:
            # Update job status
            self._update_job_status(job_id, 'processing')
            
            # Step 1: Get Twitch VOD URL
            vod_url = self._get_vod_url(stream['twitch_stream_id'])
            if not vod_url:
                logger.error(f"Could not find VOD URL for stream {stream['id']}")
                self._update_job_status(job_id, 'failed', 'VOD URL not found')
                return False
            
            # Step 2: Process VOD
            logger.info("Starting VOD processing...")
            processed_data = process_stream_vod(
                stream['id'], 
                vod_url, 
                self.vod_processor
            )
            
            if not processed_data or not processed_data['ready_for_upload']:
                logger.error("VOD processing failed")
                self._update_job_status(job_id, 'failed', 'VOD processing failed')
                return False
            
            # Step 3: Create YouTube metadata
            title = create_video_title(
                stream['title'],
                datetime.fromisoformat(stream['started_at']).strftime('%Y-%m-%d')
            )
            
            description = create_video_description(
                stream['title'],
                datetime.fromisoformat(stream['started_at']).strftime('%Y-%m-%d %H:%M UTC'),
                self.vod_processor._format_duration(processed_data['vod_info']['duration']),
                vod_url
            )
            
            tags = get_gaming_tags()
            
            # Step 4: Upload to YouTube
            logger.info("Starting YouTube upload...")
            video_id = self.youtube.upload_video(
                video_path=processed_data['processed_file'],
                title=title,
                description=description,
                tags=tags,
                privacy_status='private',  # Start private
                thumbnail_path=processed_data['thumbnail_file']
            )
            
            if not video_id:
                logger.error("YouTube upload failed")
                self._update_job_status(job_id, 'failed', 'YouTube upload failed')
                return False
            
            # Step 5: Create upload record
            upload_data = {
                'stream_id': stream['id'],
                'platform': 'youtube',
                'video_id': video_id,
                'title': title,
                'description': description,
                'status': 'uploaded',
                'privacy_status': 'private',
                'uploaded_at': datetime.utcnow().isoformat(),
                'file_path': processed_data['processed_file'],
                'thumbnail_path': processed_data['thumbnail_file'],
                'metadata': {
                    'original_file': processed_data['original_file'],
                    'vod_info': processed_data['vod_info'],
                    'processing_stats': self.vod_processor.get_processing_stats()
                }
            }
            
            self.db.supabase.table('uploads').insert(upload_data).execute()
            
            # Step 6: Mark stream as processed
            self.db.supabase.table('streams').update({
                'processed_at': datetime.utcnow().isoformat()
            }).eq('id', stream['id']).execute()
            
            # Step 7: Complete job
            self._update_job_status(job_id, 'completed', f'Video uploaded: {video_id}')
            
            logger.info(f"Upload job {job_id} completed successfully! YouTube video: {video_id}")
            
            # Step 8: Cleanup (optional - keep files for now)
            # self._cleanup_processed_files(processed_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Upload job {job_id} failed: {e}")
            self._update_job_status(job_id, 'failed', str(e))
            return False
    
    def _get_vod_url(self, twitch_stream_id: str) -> Optional[str]:
        """
        Get VOD URL from Twitch stream ID.
        This is a placeholder - you'd need to implement actual Twitch API call.
        """
        # TODO: Implement actual Twitch API call to get VOD URL
        # For now, construct expected URL format
        if twitch_stream_id:
            return f"https://www.twitch.tv/videos/{twitch_stream_id}"
        return None
    
    def _update_job_status(self, job_id: int, status: str, message: str = "") -> None:
        """Update job status in database."""
        try:
            update_data = {
                'status': status,
                'updated_at': datetime.utcnow().isoformat()
            }
            
            if message:
                update_data['error_message'] = message
            
            if status == 'completed':
                update_data['completed_at'] = datetime.utcnow().isoformat()
            
            self.db.supabase.table('processing_jobs').update(update_data).eq('id', job_id).execute()
            
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status: {e}")
    
    def start_processing(self) -> None:
        """Start the background processing worker."""
        if self.running:
            logger.warning("Processing already running")
            return
        
        self.running = True
        self.worker_thread = threading.Thread(target=self._process_worker, daemon=True)
        self.worker_thread.start()
        
        logger.info("Upload processing started")
    
    def stop_processing(self) -> None:
        """Stop the background processing worker."""
        self.running = False
        
        if self.worker_thread:
            self.worker_thread.join(timeout=30)
        
        logger.info("Upload processing stopped")
    
    def _process_worker(self) -> None:
        """Background worker that processes upload jobs."""
        logger.info("Upload processing worker started")
        
        while self.running:
            try:
                # Get job from queue (wait up to 5 seconds)
                job = self.upload_queue.get(timeout=5)
                
                # Process the job
                success = self.process_upload_job(job)
                
                if success:
                    logger.info("Job processed successfully")
                else:
                    logger.error("Job processing failed")
                
                # Mark job as done
                self.upload_queue.task_done()
                
                # Small delay between jobs
                time.sleep(10)
                
            except Empty:
                # No jobs in queue, continue loop
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}")
                time.sleep(30)  # Wait before retrying
        
        logger.info("Upload processing worker stopped")
    
    def run_scan_cycle(self) -> None:
        """Run one scan cycle for completed streams."""
        logger.info("Scanning for completed streams...")
        
        completed_streams = self.check_for_completed_streams()
        
        for stream in completed_streams:
            self.queue_stream_for_processing(stream)
        
        logger.info(f"Scan cycle complete: {len(completed_streams)} streams queued")
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue and processing status."""
        return {
            'queue_size': self.upload_queue.qsize(),
            'processing_active': self.running,
            'worker_alive': self.worker_thread.is_alive() if self.worker_thread else False
        }
    
    def make_videos_public(self, hours_after_upload: int = 2) -> None:
        """
        Make uploaded videos public after specified time.
        
        Args:
            hours_after_upload: Hours to wait before making public
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_after_upload)
            
            # Get private videos uploaded before cutoff
            response = self.db.supabase.table('uploads').select(
                'id, video_id, uploaded_at'
            ).eq('platform', 'youtube').eq('privacy_status', 'private').lt(
                'uploaded_at', cutoff_time.isoformat()
            ).execute()
            
            for upload in response.data:
                if self.youtube.make_video_public(upload['video_id']):
                    # Update database
                    self.db.supabase.table('uploads').update({
                        'privacy_status': 'public',
                        'published_at': datetime.utcnow().isoformat()
                    }).eq('id', upload['id']).execute()
                    
                    logger.info(f"Made video public: {upload['video_id']}")
                else:
                    logger.error(f"Failed to make video public: {upload['video_id']}")
        
        except Exception as e:
            logger.error(f"Failed to make videos public: {e}")


# Enhanced main monitoring function that includes upload processing
def run_enhanced_stream_monitor():
    """
    Enhanced version of stream monitor that includes upload processing.
    Combines stream detection with VOD processing and uploads.
    """
    # Import your existing stream detector
    from stream_detector import StreamDetector
    
    # Initialize services
    stream_detector = StreamDetector()
    upload_manager = UploadManager()
    
    # Start upload processing
    upload_manager.start_processing()
    
    logger.info("Enhanced stream monitor started")
    logger.info("- Stream detection: Every 2 minutes")
    logger.info("- Upload processing: Every 30 minutes")
    logger.info("- Auto-publishing: Every 30 minutes")
    
    # Tracking variables
    last_upload_scan = datetime.now()
    last_publish_scan = datetime.now()
    upload_scan_interval = timedelta(minutes=30)
    publish_scan_interval = timedelta(minutes=30)
    
    try:
        while True:
            current_time = datetime.now()
            
            # Regular stream detection (every 2 minutes)
            try:
                stream_detector.check_stream_status()
                time.sleep(120)  # 2 minutes
            except Exception as e:
                logger.error(f"Stream detection error: {e}")
                time.sleep(60)  # Wait 1 minute before retry
            
            # Upload processing scan (every 30 minutes)
            if current_time - last_upload_scan >= upload_scan_interval:
                try:
                    upload_manager.run_scan_cycle()
                    last_upload_scan = current_time
                except Exception as e:
                    logger.error(f"Upload scan error: {e}")
            
            # Publishing scan (every 30 minutes)
            if current_time - last_publish_scan >= publish_scan_interval:
                try:
                    upload_manager.make_videos_public(hours_after_upload=2)
                    last_publish_scan = current_time
                except Exception as e:
                    logger.error(f"Publishing scan error: {e}")
            
            # Log queue status periodically
            if current_time.minute % 10 == 0:  # Every 10 minutes
                status = upload_manager.get_queue_status()
                logger.info(f"Queue status: {status}")
    
    except KeyboardInterrupt:
        logger.info("Shutting down enhanced stream monitor...")
        upload_manager.stop_processing()
        logger.info("Shutdown complete")


# Utility functions for testing and manual operations
def manual_process_stream(stream_id: int) -> bool:
    """
    Manually trigger processing for a specific stream.
    
    Args:
        stream_id: ID of the stream to process
        
    Returns:
        True if processing started successfully
    """
    try:
        db = SupabaseClient()
        upload_manager = UploadManager()
        
        # Get stream record
        response = db.supabase.table('streams').select('*').eq('id', stream_id).execute()
        
        if not response.data:
            logger.error(f"Stream {stream_id} not found")
            return False
        
        stream = response.data[0]
        
        # Queue for processing
        success = upload_manager.queue_stream_for_processing(stream)
        
        if success:
            # Start processing if not already running
            upload_manager.start_processing()
            logger.info(f"Stream {stream_id} queued for manual processing")
        
        return success
        
    except Exception as e:
        logger.error(f"Manual processing failed: {e}")
        return False


def check_upload_status() -> Dict[str, Any]:
    """
    Check current upload and processing status.
    
    Returns:
        Status dictionary with current information
    """
    try:
        db = SupabaseClient()
        upload_manager = UploadManager()
        
        # Get recent jobs
        jobs_response = db.supabase.table('processing_jobs').select(
            'id, status, created_at, completed_at, error_message'
        ).order('created_at', desc=True).limit(10).execute()
        
        # Get recent uploads
        uploads_response = db.supabase.table('uploads').select(
            'id, video_id, title, status, privacy_status, uploaded_at'
        ).order('uploaded_at', desc=True).limit(10).execute()
        
        # Get queue status
        queue_status = upload_manager.get_queue_status()
        
        return {
            'queue': queue_status,
            'recent_jobs': jobs_response.data,
            'recent_uploads': uploads_response.data,
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return {'error': str(e)}


def cleanup_old_files(days: int = 7) -> Dict[str, int]:
    """
    Clean up old downloaded and processed files.
    
    Args:
        days: Number of days to keep files
        
    Returns:
        Cleanup statistics
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Get directories from config
        download_dir = Path(VOD_DOWNLOAD_DIR)
        temp_dir = Path(VOD_TEMP_DIR)
        
        cleaned_files = 0
        freed_bytes = 0
        
        # Clean download directory
        if download_dir.exists():
            for file_path in download_dir.rglob('*'):
                if file_path.is_file():
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_time < cutoff_date:
                        freed_bytes += file_path.stat().st_size
                        file_path.unlink()
                        cleaned_files += 1
        
        # Clean temp directory
        if temp_dir.exists():
            for file_path in temp_dir.rglob('*'):
                if file_path.is_file():
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_time < cutoff_date:
                        freed_bytes += file_path.stat().st_size
                        file_path.unlink()
                        cleaned_files += 1
        
        logger.info(f"Cleanup complete: {cleaned_files} files, {freed_bytes / 1024 / 1024:.1f} MB freed")
        
        return {
            'files_cleaned': cleaned_files,
            'bytes_freed': freed_bytes,
            'mb_freed': freed_bytes / 1024 / 1024
        }
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return {'error': str(e)}


# Testing functions
def test_upload_manager():
    """Test basic upload manager functionality."""
    try:
        upload_manager = UploadManager()
        
        # Test database connectivity
        completed_streams = upload_manager.check_for_completed_streams()
        print(f"Found {len(completed_streams)} completed streams")
        
        # Test queue status
        status = upload_manager.get_queue_status()
        print(f"Queue status: {status}")
        
        print("Upload manager test completed successfully!")
        return True
        
    except Exception as e:
        print(f"Upload manager test failed: {e}")
        return False


def test_full_pipeline():
    """Test the complete pipeline integration."""
    try:
        print("Testing complete pipeline...")
        
        # Test individual components
        from youtube_api import test_youtube_api
        from vod_processor import test_vod_processor
        
        print("1. Testing YouTube API...")
        if not test_youtube_api():
            return False
        
        print("2. Testing VOD processor...")
        if not test_vod_processor():
            return False
        
        print("3. Testing upload manager...")
        if not test_upload_manager():
            return False
        
        print("4. Testing status functions...")
        status = check_upload_status()
        print(f"Status check: {status}")
        
        print("\nFull pipeline test completed successfully!")
        print("Your system is ready to process streams automatically!")
        
        return True
        
    except Exception as e:
        print(f"Full pipeline test failed: {e}")
        return False


if __name__ == "__main__":
    # Run enhanced monitoring when called directly
    run_enhanced_stream_monitor()