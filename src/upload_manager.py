"""
Enhanced Upload Manager with Clips Processing and Optimal Scheduling
Phase 3 Enhancement - Integrated automation system
"""

import os
import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .config import Config
from .database import SupabaseClient
from .youtube_api import YouTubeAPI
from .vod_processor import VODProcessor
from .clips_processor import ClipsProcessor
from .scheduling_optimizer import SchedulingOptimizer

logger = logging.getLogger(__name__)

class UploadManager:
    def __init__(self):
        """Initialize the enhanced upload manager with all Phase 3 components"""
        self.config = Config()
        self.db = SupabaseClient()
        self.youtube = YouTubeAPI()
        self.vod_processor = VODProcessor()
        self.clips_processor = ClipsProcessor()
        self.scheduler = SchedulingOptimizer()
        
        # Processing control
        self.processing_active = True
        self.upload_thread = None
        self.scheduler_thread = None
        
        # Enhanced intervals - use existing config or defaults
        upload_scan_minutes = getattr(self.config, 'UPLOAD_SCAN_INTERVAL_MINUTES', 30)
        self.upload_scan_interval = upload_scan_minutes * 60  # Convert to seconds
        self.scheduler_scan_interval = 300  # 5 minutes for scheduled publishing
        
        logger.info("Enhanced upload manager initialized with clips processing and scheduling")
    
    def start_enhanced_processing(self):
        """Start all enhanced processing workers"""
        if self.upload_thread and self.upload_thread.is_alive():
            logger.info("Enhanced processing already running")
            return
        
        self.processing_active = True
        
        # Start main upload processing worker
        self.upload_thread = threading.Thread(
            target=self._enhanced_upload_worker,
            name="EnhancedUploadWorker",
            daemon=True
        )
        self.upload_thread.start()
        
        # Start scheduler worker for timed publishing
        self.scheduler_thread = threading.Thread(
            target=self._scheduler_worker,
            name="SchedulerWorker", 
            daemon=True
        )
        self.scheduler_thread.start()
        
        logger.info("Enhanced processing workers started")
        logger.info(f"- Upload processing: Every {self.upload_scan_interval // 60} minutes")
        logger.info(f"- Scheduled publishing: Every {self.scheduler_scan_interval // 60} minutes")
    
    def _enhanced_upload_worker(self):
        """Enhanced worker that processes VODs and clips"""
        # Create async event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while self.processing_active:
            try:
                logger.info("🔄 Running enhanced upload scan...")
                
                # Process completed streams
                completed_streams = self._get_completed_streams()
                
                for stream in completed_streams:
                    stream_id = stream['id']
                    logger.info(f"Processing completed stream: {stream['title']}")
                    
                    # Process VOD first
                    loop.run_until_complete(self._process_stream_vod(stream_id, stream))
                    
                    # Process clips for shorts
                    loop.run_until_complete(self._process_stream_clips(stream_id, stream))
                    
                    # Create optimal schedule for all content
                    self._schedule_stream_content(stream_id)
                
                # Upload any content ready for immediate upload
                self._upload_ready_content()
                
                # Clean up old files
                self._cleanup_old_files()
                
                logger.info("✅ Enhanced upload scan completed")
                
            except Exception as e:
                logger.error(f"Error in enhanced upload worker: {e}")
            
            # Wait for next scan
            time.sleep(self.upload_scan_interval)
        
        loop.close()
    
    def _scheduler_worker(self):
        """Worker that handles scheduled publishing"""
        while self.processing_active:
            try:
                logger.debug("🕐 Checking scheduled content...")
                
                # Get content ready to publish
                ready_uploads = self.scheduler.get_ready_to_publish()
                
                for upload in ready_uploads:
                    try:
                        self._publish_scheduled_content(upload)
                    except Exception as e:
                        logger.error(f"Error publishing scheduled content {upload['id']}: {e}")
                
                if ready_uploads:
                    logger.info(f"✅ Published {len(ready_uploads)} scheduled items")
                
            except Exception as e:
                logger.error(f"Error in scheduler worker: {e}")
            
            # Wait for next check
            time.sleep(self.scheduler_scan_interval)
    
    async def _process_stream_vod(self, stream_id: str, stream_data: Dict):
        """Process VOD for a completed stream"""
        try:
            # Check if VOD already processed
            vod_job = self._get_processing_job(stream_id, 'vod_download')
            if vod_job and vod_job.get('status') == 'completed':
                logger.info(f"VOD already processed for stream {stream_id}")
                return
            
            logger.info(f"Processing VOD for stream: {stream_data['title']}")
            
            # Process with existing VOD processor - check if method exists and is async
            if hasattr(self.vod_processor, 'process_stream_vod'):
                # Try to call the method (might be sync or async)
                try:
                    result = self.vod_processor.process_stream_vod(stream_id)
                    # If it returns a coroutine, await it
                    if hasattr(result, '__await__'):
                        success = await result
                    else:
                        success = result
                except TypeError:
                    # Method might not exist or have different signature
                    logger.warning(f"VOD processor method signature mismatch for stream {stream_id}")
                    success = False
            else:
                logger.warning("VOD processor doesn't have process_stream_vod method")
                success = False
            
            if success:
                logger.info(f"✅ VOD processed successfully for stream {stream_id}")
                
                # Update processing job
                if vod_job:
                    self.db.update_processing_job(vod_job['id'], 'completed')
            else:
                logger.error(f"❌ VOD processing failed for stream {stream_id}")
                if vod_job:
                    self.db.update_processing_job(vod_job['id'], 'failed')
                    
        except Exception as e:
            logger.error(f"Error processing VOD for stream {stream_id}: {e}")
    
    async def _process_stream_clips(self, stream_id: str, stream_data: Dict):
        """Process clips for a completed stream"""
        try:
            # Check if clips already processed
            clips_job = self._get_processing_job(stream_id, 'clips_processing')
            if clips_job and clips_job.get('status') == 'completed':
                logger.info(f"Clips already processed for stream {stream_id}")
                return
            
            # Create clips processing job if doesn't exist
            if not clips_job:
                clips_job = self.db.create_processing_job(
                    stream_id=stream_id,
                    job_type='clips_processing',
                    priority=0
                )
            
            logger.info(f"Processing clips for stream: {stream_data['title']}")
            
            # Update job status  
            self.db.update_job_status(clips_job['id'], 'processing')
            
            # Process clips
            processed_clips = await self.clips_processor.process_stream_clips(stream_id)
            
            if processed_clips:
                logger.info(f"✅ Processed {len(processed_clips)} clips for stream {stream_id}")
                
                # Update processing job with results using your database method
                self.db.update_job_status(clips_job['id'], 'completed')
            else:
                logger.info(f"No clips found for stream {stream_id}")
                self.db.update_job_status(clips_job['id'], 'completed')
                
        except Exception as e:
            logger.error(f"Error processing clips for stream {stream_id}: {e}")
            if clips_job:
                self.db.update_job_status(clips_job['id'], 'failed', error_message=str(e))
    
    def _schedule_stream_content(self, stream_id: str):
        """Create optimal schedule for all content from a stream"""
        try:
            logger.info(f"Creating schedule for stream content: {stream_id}")
            
            # Get optimal schedule from scheduler
            schedule = self.scheduler.schedule_content_uploads(stream_id)
            
            # Apply schedule to VOD uploads (all uploads from this stream)
            vod_uploads = self._get_stream_uploads(stream_id)
            vod_schedule = schedule.get('vod', [])
            
            for i, upload in enumerate(vod_uploads):
                if i < len(vod_schedule) and upload['status'] == 'ready_for_upload':
                    slot = vod_schedule[i]
                    self.scheduler.update_upload_schedule(upload['id'], slot.datetime)
            
            logger.info(f"✅ Created schedule for {len(vod_uploads)} uploads from stream {stream_id}")
            
        except Exception as e:
            logger.error(f"Error scheduling content for stream {stream_id}: {e}")
    
    def _publish_scheduled_content(self, upload_data: Dict):
        """Publish scheduled content and make it public"""
        try:
            upload_id = upload_data['id']
            content_type = upload_data.get('content_type', 'unknown')
            
            logger.info(f"Publishing scheduled {content_type}: {upload_data.get('youtube_title', 'Untitled')}")
            
            # If content isn't uploaded yet, upload it first
            if upload_data['status'] == 'scheduled' and not upload_data.get('youtube_video_id'):
                success = self._upload_content_to_youtube(upload_data)
                if not success:
                    return
                
                # Refresh upload data
                upload_data = self.db.get_upload(upload_id)
            
            # Make the video public
            if upload_data.get('youtube_video_id'):
                success = self.youtube.update_video_privacy(
                    upload_data['youtube_video_id'], 
                    'public'
                )
                
                if success:
                    # Update upload record
                    self.db.supabase.table('uploads').update({
                        'youtube_privacy_status': 'public',
                        'published_at': datetime.now().isoformat(),
                        'status': 'published'
                    }).eq('id', upload_id).execute()
                    
                    logger.info(f"✅ Published {content_type}: {upload_data['youtube_title']}")
                else:
                    logger.error(f"Failed to publish {content_type}: {upload_id}")
            
        except Exception as e:
            logger.error(f"Error publishing scheduled content {upload_data['id']}: {e}")
    
    def _upload_content_to_youtube(self, upload_data: Dict) -> bool:
        """Upload content to YouTube"""
        try:
            file_path = upload_data['file_path']
            if not file_path or not os.path.exists(file_path):
                logger.error(f"File not found for upload {upload_data['id']}: {file_path}")
                return False
            
            # Prepare metadata
            metadata = {
                'title': upload_data.get('youtube_title', 'Untitled'),
                'description': upload_data.get('youtube_description', ''),
                'privacy_status': upload_data.get('youtube_privacy_status', 'private'),
                'category_id': '20',  # Gaming category
                'tags': upload_data.get('metadata', {}).get('tags', [])
            }
            
            # Upload to YouTube
            video_id = self.youtube.upload_video(file_path, metadata)
            
            if video_id:
                # Update upload record
                self.db.supabase.table('uploads').update({
                    'youtube_video_id': video_id,
                    'status': 'uploaded'
                }).eq('id', upload_data['id']).execute()
                
                logger.info(f"✅ Uploaded to YouTube: {video_id}")
                return True
            else:
                logger.error(f"Failed to upload {upload_data['id']}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading content {upload_data['id']}: {e}")
            return False
    
    def _upload_ready_content(self):
        """Upload any content marked as ready for immediate upload"""
        try:
            # Get uploads ready for immediate upload (no schedule)
            ready_uploads = self.db.supabase.table('uploads').select('*').eq('status', 'ready_for_upload').is_('scheduled_publish_at', 'null').execute()
            
            for upload in ready_uploads.data:
                try:
                    self._upload_content_to_youtube(upload)
                except Exception as e:
                    logger.error(f"Error uploading ready content {upload['id']}: {e}")
                    
        except Exception as e:
            logger.error(f"Error uploading ready content: {e}")
    
    def _get_completed_streams(self) -> List[Dict]:
        """Get streams that are completed but not fully processed"""
        try:
            # Get streams that ended but haven't been fully processed
            streams = self.db.supabase.table('streams').select('*').not_.is_('ended_at', 'null').execute()
            
            completed_streams = []
            for stream in streams.data:
                # Check if this stream needs processing
                if self._stream_needs_processing(stream['id']):
                    completed_streams.append(stream)
            
            return completed_streams
            
        except Exception as e:
            logger.error(f"Error getting completed streams: {e}")
            return []
    
    def _stream_needs_processing(self, stream_id: str) -> bool:
        """Check if a stream needs processing"""
        try:
            # Check for existing processing jobs
            vod_job = self._get_processing_job(stream_id, 'vod_download')
            clips_job = self._get_processing_job(stream_id, 'clips_processing')
            
            # Stream needs processing if:
            # 1. No VOD job exists or VOD job is not completed
            # 2. No clips job exists or clips job is not completed
            needs_vod = not vod_job or vod_job.get('status') != 'completed'
            needs_clips = not clips_job or clips_job.get('status') != 'completed'
            
            return needs_vod or needs_clips
            
        except Exception as e:
            logger.error(f"Error checking if stream needs processing: {e}")
            return True  # Err on side of caution
    
    def _get_processing_job(self, stream_id: str, job_type: str) -> Optional[Dict]:
        """Get processing job for a stream"""
        try:
            jobs = self.db.supabase.table('processing_jobs').select('*').eq('stream_id', stream_id).eq('job_type', job_type).execute()
            return jobs.data[0] if jobs.data else None
        except Exception as e:
            logger.error(f"Error getting processing job: {e}")
            return None
    
    def _get_stream_uploads(self, stream_id: str) -> List[Dict]:
        """Get uploads for a specific stream - simplified since no content_type filtering"""
        try:
            # Get clips associated with this stream from clips table
            clips = self.db.supabase.table('clips').select('*').eq('stream_id', stream_id).eq('processed', True).execute()
            
            # For each clip, find corresponding upload (this is a workaround)
            uploads = []
            for clip in clips.data:
                # Try to find upload by matching title or other identifier
                upload_results = self.db.supabase.table('uploads').select('*').eq('status', 'ready_for_upload').execute()
                
                # Filter uploads that might be from this stream (basic heuristic)
                for upload in upload_results.data:
                    if upload.get('file_path') and clip['download_url'] in upload['file_path']:
                        uploads.append(upload)
                        break
            
            return uploads
            
        except Exception as e:
            logger.error(f"Error getting stream uploads: {e}")
            return []
    
    def _cleanup_old_files(self):
        """Clean up old files"""
        try:
            # Clean up VOD files - use existing method if available
            if hasattr(self.vod_processor, 'cleanup_old_files'):
                cleanup_days = getattr(self.config, 'CLEANUP_KEEP_DAYS', 7)
                self.vod_processor.cleanup_old_files(cleanup_days)
            
            # Clean up clip files  
            if hasattr(self.clips_processor, 'cleanup_clip_files'):
                cleanup_days = getattr(self.config, 'CLEANUP_KEEP_DAYS', 7)
                self.clips_processor.cleanup_clip_files(cleanup_days)
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def stop_processing(self):
        """Stop all processing workers"""
        logger.info("Stopping enhanced processing workers...")
        self.processing_active = False
        
        if self.upload_thread:
            self.upload_thread.join(timeout=10)
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
            
        logger.info("Enhanced processing workers stopped")
    
    def get_enhanced_status(self) -> Dict:
        """Get comprehensive status of the enhanced system"""
        try:
            # Get schedule summary
            schedule_summary = self.scheduler.get_schedule_summary(7)
            
            # Get processing status
            pending_jobs = self.db.supabase.table('processing_jobs').select('*').neq('status', 'completed').execute()
            
            # Get upload queue status
            queued_uploads = self.db.supabase.table('uploads').select('*').in_('status', ['ready_for_upload', 'scheduled', 'uploading']).execute()
            
            return {
                'processing_active': self.processing_active,
                'worker_threads': {
                    'upload_worker': self.upload_thread.is_alive() if self.upload_thread else False,
                    'scheduler_worker': self.scheduler_thread.is_alive() if self.scheduler_thread else False
                },
                'pending_jobs': len(pending_jobs.data),
                'queued_uploads': len(queued_uploads.data),
                'schedule_summary': schedule_summary,
                'next_scheduled': min([item['scheduled_time'] for day_items in schedule_summary.values() for item in day_items], default=None)
            }
            
        except Exception as e:
            logger.error(f"Error getting enhanced status: {e}")
            return {'error': str(e)}

# Test functions
async def test_enhanced_system():
    """Test the complete enhanced system"""
    manager = UploadManager()
    
    logger.info("Testing enhanced upload manager...")
    
    # Test clips processor
    await manager.clips_processor.test_clips_processor() 
    
    # Test scheduler
    manager.scheduler.test_scheduling_optimizer()
    
    # Test system status
    status = manager.get_enhanced_status()
    logger.info(f"System status: {status}")
    
    logger.info("Enhanced system test completed")

if __name__ == "__main__":
    asyncio.run(test_enhanced_system())