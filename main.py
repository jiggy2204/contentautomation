# src/main.py
"""
Main Orchestrator for Twitch to YouTube Automation
Coordinates all modules and handles scheduling
"""

import os
import sys
import logging
import asyncio
import argparse
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv
from pathlib import Path

# Import all modules
from src.supabase_client import SupabaseClient
from twitch_handler import TwitchHandler
from downloader import VODDownloader
from game_metadata_handler import GameMetadataHandler
from youtube_handler import YouTubeHandler
from youtube_uploader import YouTubeUploader
from youtube_publisher import YouTubePublisher
from api_clients.token_scheduler import TokenScheduler

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/automation_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class AutomationOrchestrator:
    """Main orchestrator for the entire automation pipeline"""
    
    def __init__(self):
        """Initialize all components"""
        logger.info('='*60)
        logger.info('🤖 Content Automation System Starting')
        logger.info('='*60)
        
        # Initialize database
        self.db = SupabaseClient()
        
        # Initialize all handlers
        self.twitch_handler = TwitchHandler(self.db)
        self.downloader = VODDownloader(self.db)
        self.metadata_handler = GameMetadataHandler(self.db, twitch_handler=self.twitch_handler)
        self.youtube_handler = YouTubeHandler(self.db)
        self.youtube_uploader = YouTubeUploader(self.db)
        self.youtube_publisher = YouTubePublisher(self.db)
        
        # Initialize token scheduler for IGDB
        self.token_scheduler = TokenScheduler()
        
        logger.info('✅ All components initialized')
    
    async def run_vod_collection(self) -> Dict[str, Any]:
        """
        Step 1: Collect VODs from Twitch (3:30 AM)
        
        Returns:
            Stats dictionary
        """
        logger.info('')
        logger.info('='*60)
        logger.info('📺 STEP 1: Twitch VOD Collection')
        logger.info('='*60)
        
        try:
            # Process new VODs
            new_vods = await self.twitch_handler.process_new_vods()
            
            stats = {
                'step': 'vod_collection',
                'success': True,
                'vods_found': len(new_vods),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f'✅ Step 1 Complete: Found {len(new_vods)} new VODs')
            return stats
            
        except Exception as e:
            logger.error(f'❌ Step 1 Failed: {e}')
            return {'step': 'vod_collection', 'success': False, 'error': str(e)}
    
    def run_downloads(self) -> Dict[str, Any]:
        """
        Step 2: Download VODs to DO Spaces (3:45 AM)
        
        Returns:
            Stats dictionary
        """
        logger.info('')
        logger.info('='*60)
        logger.info('⬇️  STEP 2: VOD Downloads')
        logger.info('='*60)
        
        try:
            # Process pending downloads
            stats = self.downloader.process_pending_downloads()
            stats['step'] = 'downloads'
            stats['success'] = True
            stats['timestamp'] = datetime.now().isoformat()
            
            logger.info(f'✅ Step 2 Complete: {stats["successful"]} downloads successful')
            return stats
            
        except Exception as e:
            logger.error(f'❌ Step 2 Failed: {e}')
            return {'step': 'downloads', 'success': False, 'error': str(e)}
    
    async def run_metadata_fetching(self) -> Dict[str, Any]:
        """
        Step 3: Fetch game metadata (4:00 AM)
        
        Returns:
            Stats dictionary
        """
        logger.info('')
        logger.info('='*60)
        logger.info('🎮 STEP 3: Game Metadata Fetching')
        logger.info('='*60)
        
        try:
            # Process completed downloads for metadata
            stats = await self.metadata_handler.process_completed_downloads()
            stats['step'] = 'metadata'
            stats['success'] = True
            stats['timestamp'] = datetime.now().isoformat()
            
            logger.info(f'✅ Step 3 Complete: {stats["success"]} metadata fetched')
            return stats
            
        except Exception as e:
            logger.error(f'❌ Step 3 Failed: {e}')
            return {'step': 'metadata', 'success': False, 'error': str(e)}
    
    async def run_youtube_preparation(self) -> Dict[str, Any]:
        """
        Step 4: Prepare YouTube metadata (4:15 AM)
        
        Returns:
            Stats dictionary
        """
        logger.info('')
        logger.info('='*60)
        logger.info('📝 STEP 4: YouTube Metadata Preparation')
        logger.info('='*60)
        
        try:
            # Process completed downloads
            stats = await self.youtube_handler.process_completed_downloads()
            stats['step'] = 'youtube_prep'
            stats['success'] = True
            stats['timestamp'] = datetime.now().isoformat()
            
            logger.info(f'✅ Step 4 Complete: {stats["success"]} uploads prepared')
            return stats
            
        except Exception as e:
            logger.error(f'❌ Step 4 Failed: {e}')
            return {'step': 'youtube_prep', 'success': False, 'error': str(e)}
    
    def run_youtube_upload(self) -> Dict[str, Any]:
        """
        Step 5: Upload videos to YouTube as PRIVATE (4:30 AM)
        
        Returns:
            Stats dictionary
        """
        logger.info('')
        logger.info('='*60)
        logger.info('📤 STEP 5: YouTube Upload (Private)')
        logger.info('='*60)
        
        try:
            # Process queued uploads
            stats = self.youtube_uploader.process_queued_uploads()
            stats['step'] = 'youtube_upload'
            stats['success'] = True
            stats['timestamp'] = datetime.now().isoformat()
            
            logger.info(f'✅ Step 5 Complete: {stats["success"]} videos uploaded')
            return stats
            
        except Exception as e:
            logger.error(f'❌ Step 5 Failed: {e}')
            return {'step': 'youtube_upload', 'success': False, 'error': str(e)}
    
    def run_youtube_publishing(self) -> Dict[str, Any]:
        """
        Step 6: Publish videos (change PRIVATE → PUBLIC) (6:00 PM)
        
        Returns:
            Stats dictionary
        """
        logger.info('')
        logger.info('='*60)
        logger.info('📢 STEP 6: YouTube Publishing (Public)')
        logger.info('='*60)
        
        try:
            # Process scheduled publishes 
            stats = self.youtube_publisher.process_scheduled_publishes()
            stats['step'] = 'youtube_publish'
            stats['success'] = True
            stats['timestamp'] = datetime.now().isoformat()
            
            logger.info(f'✅ Step 6 Complete: {stats["success"]} videos published')
            return stats
            
        except Exception as e:
            logger.error(f'❌ Step 6 Failed: {e}')
            return {'step': 'youtube_publish', 'success': False, 'error': str(e)}
    
    async def run_morning_pipeline(self) -> Dict[str, Any]:
        """
        Run the complete morning pipeline (3:30 AM - 5:00 AM)
        Steps 1-5: VOD collection through YouTube upload
        
        Returns:
            Combined stats dictionary
        """
        logger.info('')
        logger.info('🌅 Starting Morning Pipeline')
        logger.info(f'   Time: {datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")}')
        
        pipeline_start = datetime.now()
        all_stats = []
        
        # Check IGDB token
        self.token_scheduler.run_pending()
        
        # Step 1: Collect VODs from Twitch
        stats = await self.run_vod_collection()
        all_stats.append(stats)
        
        # Only continue if we found VODs
        if stats.get('vods_found', 0) > 0:
            # Step 2: Download VODs
            stats = self.run_downloads()
            all_stats.append(stats)
            
            # Step 3: Fetch game metadata
            stats = await self.run_metadata_fetching()
            all_stats.append(stats)
            
            # Step 4: Prepare YouTube metadata
            stats = await self.run_youtube_preparation()
            all_stats.append(stats)
            
            # Step 5: Upload to YouTube
            stats = self.run_youtube_upload()
            all_stats.append(stats)
        else:
            logger.info('💤 No new VODs found, skipping remaining steps')
        
        pipeline_end = datetime.now()
        duration = (pipeline_end - pipeline_start).total_seconds() / 60
        
        logger.info('')
        logger.info('='*60)
        logger.info('🎉 Morning Pipeline Complete')
        logger.info(f'   Duration: {duration:.1f} minutes')
        logger.info('='*60)
        
        return {
            'pipeline': 'morning',
            'start_time': pipeline_start.isoformat(),
            'end_time': pipeline_end.isoformat(),
            'duration_minutes': round(duration, 1),
            'steps': all_stats
        }
    
    def run_evening_pipeline(self) -> Dict[str, Any]:
        """
        Run the evening pipeline (6:00 PM)
        Step 6: Publish videos
        
        Returns:
            Stats dictionary
        """
        logger.info('')
        logger.info('🌆 Starting Evening Pipeline')
        logger.info(f'   Time: {datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")}')
        
        pipeline_start = datetime.now()
        
        # Step 6: Publish videos
        stats = self.run_youtube_publishing()
        
        pipeline_end = datetime.now()
        duration = (pipeline_end - pipeline_start).total_seconds() / 60
        
        logger.info('')
        logger.info('='*60)
        logger.info('🎉 Evening Pipeline Complete')
        logger.info(f'   Duration: {duration:.1f} minutes')
        logger.info('='*60)
        
        return {
            'pipeline': 'evening',
            'start_time': pipeline_start.isoformat(),
            'end_time': pipeline_end.isoformat(),
            'duration_minutes': round(duration, 1),
            'steps': [stats]
        }
    
    async def run_full_pipeline_test(self) -> Dict[str, Any]:
        """
        Run entire pipeline end-to-end (for testing only)
        
        Returns:
            Combined stats
        """
        logger.info('')
        logger.info('🧪 RUNNING FULL PIPELINE TEST')
        logger.info('='*60)
        
        # Run morning pipeline
        morning_stats = await self.run_morning_pipeline()
        
        # Run evening pipeline
        evening_stats = self.run_evening_pipeline()
        
        return {
            'test_run': True,
            'morning': morning_stats,
            'evening': evening_stats
        }
    
    async def cleanup(self):
        """Cleanup resources"""
        logger.info('🧹 Cleaning up resources...')
        
        # Close Twitch handler
        if hasattr(self.twitch_handler, 'close'):
            await self.twitch_handler.close()
        
        logger.info('✅ Cleanup complete')


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Content Automation System')
    parser.add_argument('--mode', choices=['morning', 'evening', 'test'], 
                       default='morning',
                       help='Which pipeline to run (default: morning)')
    parser.add_argument('--step', choices=['vod', 'download', 'metadata', 'prep', 'upload', 'publish'],
                       help='Run specific step only')
    
    args = parser.parse_args()
    
    orchestrator = AutomationOrchestrator()
    
    try:
        if args.step:
            # Run specific step
            logger.info(f'🎯 Running specific step: {args.step}')
            
            if args.step == 'vod':
                await orchestrator.run_vod_collection()
            elif args.step == 'download':
                orchestrator.run_downloads()
            elif args.step == 'metadata':
                await orchestrator.run_metadata_fetching()
            elif args.step == 'prep':
                await orchestrator.run_youtube_preparation()
            elif args.step == 'upload':
                orchestrator.run_youtube_upload()
            elif args.step == 'publish':
                orchestrator.run_youtube_publishing()
        
        elif args.mode == 'morning':
            # Run morning pipeline
            await orchestrator.run_morning_pipeline()
        
        elif args.mode == 'evening':
            # Run evening pipeline
            orchestrator.run_evening_pipeline()
        
        elif args.mode == 'test':
            # Run full test
            await orchestrator.run_full_pipeline_test()
        
        # Cleanup
        await orchestrator.cleanup()
        
        logger.info('')
        logger.info('='*60)
        logger.info('✅ Automation Complete')
        logger.info('='*60)
        logger.info('')
        
    except KeyboardInterrupt:
        logger.info('\n⚠️  Interrupted by user')
        await orchestrator.cleanup()
        sys.exit(0)
    
    except Exception as e:
        logger.error(f'\n❌ Fatal error: {e}')
        import traceback
        traceback.print_exc()
        await orchestrator.cleanup()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())