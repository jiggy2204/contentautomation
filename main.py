"""
Enhanced Content Automation System - Phase 3
Complete automation with clips processing, optimal scheduling, and enhanced monitoring
"""

import asyncio
import logging
import signal
import sys
import threading
import time
from datetime import datetime

from src.config import Config
from src.stream_detector import StreamDetector
from src.enhanced_upload_manager import EnhancedUploadManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('automation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class EnhancedContentAutomation:
    def __init__(self):
        """Initialize the enhanced content automation system"""
        self.config = Config()
        
        # Core components
        self.stream_detector = StreamDetector()
        self.upload_manager = EnhancedUploadManager()
        
        # Control flags
        self.running = False
        self.shutdown_event = threading.Event()
        
        logger.info("🚀 Enhanced Content Automation System initialized")
        logger.info("Features: Stream Detection + VOD Processing + Clips + Scheduling + YouTube Uploads")
    
    async def start_system(self):
        """Start the complete enhanced automation system"""
        try:
            logger.info("🌟 Starting Enhanced Content Automation System...")
            
            # Test all components first
            await self._run_system_tests()
            
            self.running = True
            
            # Start enhanced upload processing
            self.upload_manager.start_enhanced_processing()
            
            # Start stream detection loop
            await self._run_stream_detection()
            
        except Exception as e:
            logger.error(f"❌ Error starting enhanced system: {e}")
            raise
    
    async def _run_system_tests(self):
        """Run comprehensive system tests"""
        logger.info("🔧 Running enhanced system tests...")
        
        try:
            # Test database connectivity
            logger.info("Testing database connection...")
            test_query = self.upload_manager.db.supabase.table('streams').select('*').limit(1).execute()
            logger.info("✅ Database connection successful")
            
            # Test YouTube API
            logger.info("Testing YouTube API...")
            self.upload_manager.youtube.test_youtube_api()
            logger.info("✅ YouTube API connection successful")
            
            # Test Twitch API
            logger.info("Testing Twitch API...")
            await self.upload_manager.clips_processor.test_clips_processor()
            logger.info("✅ Twitch API connection successful")
            
            # Test scheduling system
            logger.info("Testing scheduling optimizer...")
            self.upload_manager.scheduler.test_scheduling_optimizer()
            logger.info("✅ Scheduling system functional")
            
            logger.info("🎉 All system tests passed!")
            
        except Exception as e:
            logger.error(f"❌ System test failed: {e}")
            raise
    
    async def _run_stream_detection(self):
        """Run the stream detection loop with enhanced monitoring"""
        logger.info("👀 Enhanced stream monitoring started")
        logger.info(f"- Stream detection: Every {self.config.POLL_INTERVAL_SECONDS} seconds")
        logger.info(f"- Upload processing: Every {self.config.UPLOAD_SCAN_INTERVAL_MINUTES} minutes") 
        logger.info(f"- Scheduled publishing: Every 5 minutes")
        
        poll_count = 0
        
        while self.running and not self.shutdown_event.is_set():
            try:
                poll_count += 1
                
                # Run stream detection
                await self.stream_detector.check_stream_status()
                
                # Log system status periodically (every 10 polls)
                if poll_count % 10 == 0:
                    await self._log_system_status()
                
                # Wait for next poll
                await asyncio.sleep(self.config.POLL_INTERVAL_SECONDS)
                
            except Exception as e:
                logger.error(f"❌ Error in stream detection loop: {e}")
                await asyncio.sleep(30)  # Wait 30 seconds before retrying
    
    async def _log_system_status(self):
        """Log comprehensive system status"""
        try:
            status = self.upload_manager.get_enhanced_status()
            
            logger.info("📊 Enhanced System Status:")
            logger.info(f"  - Processing Active: {status.get('processing_active', 'Unknown')}")
            logger.info(f"  - Upload Worker: {'✅' if status.get('worker_threads', {}).get('upload_worker') else '❌'}")
            logger.info(f"  - Scheduler Worker: {'✅' if status.get('worker_threads', {}).get('scheduler_worker') else '❌'}")
            logger.info(f"  - Pending Jobs: {status.get('pending_jobs', 0)}")
            logger.info(f"  - Queued Uploads: {status.get('queued_uploads', 0)}")
            
            # Log upcoming scheduled content
            if status.get('next_scheduled'):
                logger.info(f"  - Next Scheduled: {status['next_scheduled']}")
            
            # Log today's schedule
            schedule_summary = status.get('schedule_summary', {})
            today = datetime.now().strftime('%Y-%m-%d')
            
            if today in schedule_summary:
                today_items = schedule_summary[today]
                logger.info(f"  - Today's Schedule: {len(today_items)} items")
                for item in today_items[:3]:  # Show first 3 items
                    logger.info(f"    • {item['scheduled_time']}: {item['type']} - {item['title'][:50]}...")
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
    
    def setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers"""
        def signal_handler(signum, frame):
            logger.info(f"📴 Received signal {signum}, initiating graceful shutdown...")
            self.shutdown()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("Signal handlers configured (Ctrl+C to stop)")
    
    def shutdown(self):
        """Gracefully shutdown the system"""
        logger.info("🛑 Shutting down Enhanced Content Automation System...")
        
        self.running = False
        self.shutdown_event.set()
        
        # Stop upload manager
        if self.upload_manager:
            self.upload_manager.stop_processing()
        
        logger.info("✅ Enhanced Content Automation System stopped")

# CLI Commands
async def check_system_status():
    """Check and display system status"""
    try:
        automation = EnhancedContentAutomation()
        status = automation.upload_manager.get_enhanced_status()
        
        print("\n🔍 Enhanced System Status:")
        print("=" * 50)
        print(f"Processing Active: {status.get('processing_active', 'Unknown')}")
        print(f"Upload Worker: {'Running' if status.get('worker_threads', {}).get('upload_worker') else 'Stopped'}")
        print(f"Scheduler Worker: {'Running' if status.get('worker_threads', {}).get('scheduler_worker') else 'Stopped'}")
        print(f"Pending Jobs: {status.get('pending_jobs', 0)}")
        print(f"Queued Uploads: {status.get('queued_uploads', 0)}")
        
        # Show schedule summary
        schedule_summary = status.get('schedule_summary', {})
        if schedule_summary:
            print(f"\n📅 Upcoming Schedule:")
            for day, items in list(schedule_summary.items())[:3]:  # Next 3 days
                print(f"\n{day}:")
                for item in items:
                    print(f"  • {item['scheduled_time']}: {item['type']} - {item['title'][:50]}...")
        
        print("\n" + "=" * 50)
        
    except Exception as e:
        print(f"❌ Error checking system status: {e}")

async def process_stream_manually(stream_id: str):
    """Manually process a specific stream"""
    try:
        automation = EnhancedContentAutomation()
        
        print(f"🔧 Manually processing stream: {stream_id}")
        
        # Get stream data
        stream_data = automation.upload_manager.db.get_stream(stream_id)
        if not stream_data:
            print(f"❌ Stream not found: {stream_id}")
            return
        
        print(f"Stream: {stream_data['title']}")
        print(f"Started: {stream_data['started_at']}")
        print(f"Ended: {stream_data['ended_at']}")
        
        # Process VOD
        print("\n🎥 Processing VOD...")
        await automation.upload_manager._process_stream_vod(stream_id, stream_data)
        
        # Process clips
        print("\n📹 Processing clips...")
        await automation.upload_manager._process_stream_clips(stream_id, stream_data)
        
        # Create schedule
        print("\n📅 Creating optimal schedule...")
        automation.upload_manager._schedule_stream_content(stream_id)
        
        print("\n✅ Manual processing completed!")
        
    except Exception as e:
        print(f"❌ Error processing stream: {e}")

def main():
    """Main entry point for the enhanced automation system"""
    try:
        # Create and configure the system
        automation = EnhancedContentAutomation()
        automation.setup_signal_handlers()
        
        # Start the system
        asyncio.run(automation.start_system())
        
    except KeyboardInterrupt:
        logger.info("👋 Received keyboard interrupt")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Content Automation System")
    parser.add_argument('--status', action='store_true', help='Check system status')
    parser.add_argument('--process-stream', help='Manually process a specific stream ID')
    
    args = parser.parse_args()
    
    if args.status:
        asyncio.run(check_system_status())
    elif args.process_stream:
        asyncio.run(process_stream_manually(args.process_stream))
    else:
        main()