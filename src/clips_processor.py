"""
Twitch Clips Processor for YouTube Shorts Creation
Phase 3 Enhancement - Automated clip processing and shorts creation
"""

import os
import json
import logging
import asyncio
import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path

import ffmpeg
import yt_dlp
from twitchAPI import Twitch
from twitchAPI.helper import first

from .config import Config
from .database import Database
from .youtube_api import YouTubeAPI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClipsProcessor:
    def __init__(self):
        """Initialize the clips processor with required services"""
        self.config = Config()
        self.db = Database()
        self.youtube = YouTubeAPI()
        
        # Clips processing settings
        self.clips_dir = Path(self.config.VOD_DOWNLOAD_DIR) / "clips"
        self.clips_temp_dir = Path(self.config.VOD_TEMP_DIR) / "clips"
        self.processed_clips_dir = Path(self.config.VOD_DOWNLOAD_DIR) / "shorts"
        
        # Create directories
        self.clips_dir.mkdir(parents=True, exist_ok=True)
        self.clips_temp_dir.mkdir(parents=True, exist_ok=True)
        self.processed_clips_dir.mkdir(parents=True, exist_ok=True)
        
        # Clips processing parameters
        self.max_clips_per_stream = 5  # Max clips to process per stream
        self.min_clip_views = 10       # Minimum views to consider a clip
        self.shorts_max_duration = 60  # Max duration for YouTube Shorts
        
        logger.info(f"Clips processor initialized: clips={self.clips_dir}, shorts={self.processed_clips_dir}")
    
    async def get_twitch_client(self) -> Twitch:
        """Get authenticated Twitch API client"""
        twitch = await Twitch(
            app_id=self.config.TWITCH_CLIENT_ID,
            app_secret=self.config.TWITCH_CLIENT_SECRET
        )
        return twitch
    
    async def fetch_stream_clips(self, broadcaster_id: str, started_at: datetime, ended_at: datetime) -> List[Dict]:
        """
        Fetch clips created during a specific stream period
        
        Args:
            broadcaster_id: Twitch user ID
            started_at: Stream start time
            ended_at: Stream end time
            
        Returns:
            List of clip data dictionaries
        """
        try:
            twitch = await self.get_twitch_client()
            
            # Get clips created during the stream period
            clips = []
            async for clip in twitch.get_clips(
                broadcaster_id=broadcaster_id,
                started_at=started_at,
                ended_at=ended_at,
                first=20  # Get up to 20 clips to filter from
            ):
                clip_data = {
                    'id': clip.id,
                    'url': clip.url,
                    'embed_url': clip.embed_url,
                    'broadcaster_id': clip.broadcaster_id,
                    'broadcaster_name': clip.broadcaster_name,
                    'creator_id': clip.creator_id,
                    'creator_name': clip.creator_name,
                    'video_id': clip.video_id,
                    'game_id': clip.game_id,
                    'language': clip.language,
                    'title': clip.title,
                    'view_count': clip.view_count,
                    'created_at': clip.created_at,
                    'thumbnail_url': clip.thumbnail_url,
                    'duration': clip.duration,
                    'vod_offset': clip.vod_offset
                }
                clips.append(clip_data)
            
            await twitch.close()
            
            # Filter clips by view count and duration
            filtered_clips = [
                clip for clip in clips
                if clip['view_count'] >= self.min_clip_views 
                and clip['duration'] <= self.shorts_max_duration
            ]
            
            # Sort by view count (most popular first)
            filtered_clips.sort(key=lambda x: x['view_count'], reverse=True)
            
            # Limit to max clips per stream
            return filtered_clips[:self.max_clips_per_stream]
            
        except Exception as e:
            logger.error(f"Error fetching clips: {e}")
            return []
    
    def download_clip(self, clip_data: Dict) -> Optional[str]:
        """
        Download a clip using yt-dlp
        
        Args:
            clip_data: Clip information dictionary
            
        Returns:
            Path to downloaded clip file or None if failed
        """
        try:
            clip_id = clip_data['id']
            clip_url = clip_data['url']
            
            # Output path
            output_path = self.clips_dir / f"{clip_id}.%(ext)s"
            
            # yt-dlp options for clip download
            ydl_opts = {
                'outtmpl': str(output_path),
                'format': 'best[height<=1080]',  # Get best quality up to 1080p
                'quiet': False,
                'no_warnings': False
            }
            
            logger.info(f"Downloading clip {clip_id}: {clip_data['title']}")
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([clip_url])
            
            # Find the actual downloaded file
            for file_path in self.clips_dir.glob(f"{clip_id}.*"):
                if file_path.suffix in ['.mp4', '.mkv', '.webm', '.flv']:
                    logger.info(f"Successfully downloaded clip: {file_path}")
                    return str(file_path)
            
            logger.error(f"Could not find downloaded file for clip {clip_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error downloading clip {clip_data['id']}: {e}")
            return None
    
    def convert_to_shorts_format(self, input_path: str, clip_data: Dict) -> Optional[str]:
        """
        Convert clip to YouTube Shorts format (9:16 aspect ratio)
        
        Args:
            input_path: Path to input clip file
            clip_data: Clip metadata
            
        Returns:
            Path to processed shorts file or None if failed
        """
        try:
            clip_id = clip_data['id']
            output_path = self.processed_clips_dir / f"short_{clip_id}.mp4"
            
            logger.info(f"Converting clip {clip_id} to Shorts format")
            
            # Get video info
            probe = ffmpeg.probe(input_path)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            
            if not video_stream:
                logger.error(f"No video stream found in {input_path}")
                return None
            
            width = int(video_stream['width'])
            height = int(video_stream['height'])
            
            # Calculate dimensions for 9:16 aspect ratio (Shorts format)
            target_width = 1080
            target_height = 1920
            
            # Create filter complex for centering and cropping/padding to 9:16
            if width / height > 9 / 16:  # Video is too wide
                # Crop width to match 9:16 ratio
                new_width = int(height * 9 / 16)
                x_offset = (width - new_width) // 2
                filter_complex = f"[0:v]crop={new_width}:{height}:{x_offset}:0,scale={target_width}:{target_height}[v]"
            else:  # Video is too tall or already correct ratio
                # Pad height to match 9:16 ratio or scale appropriately
                new_height = int(width * 16 / 9)
                if new_height > height:
                    # Pad with black bars
                    y_offset = (new_height - height) // 2
                    filter_complex = f"[0:v]pad={width}:{new_height}:0:{y_offset}:black,scale={target_width}:{target_height}[v]"
                else:
                    # Scale to fit
                    filter_complex = f"[0:v]scale={target_width}:{target_height}[v]"
            
            # Build ffmpeg command
            stream = ffmpeg.input(input_path)
            stream = ffmpeg.filter(stream, 'scale', target_width, target_height, force_original_aspect_ratio='decrease')
            stream = ffmpeg.filter(stream, 'pad', target_width, target_height, -1, -1, color='black')
            
            # Add audio processing
            audio = ffmpeg.input(input_path)['a']
            
            # Output with optimizations for YouTube
            out = ffmpeg.output(
                stream, audio, str(output_path),
                vcodec='libx264',
                acodec='aac',
                preset='medium',
                crf=23,
                maxrate='8M',
                bufsize='16M',
                movflags='faststart',
                pix_fmt='yuv420p'
            )
            
            # Run conversion
            ffmpeg.run(out, overwrite_output=True, quiet=False)
            
            if output_path.exists():
                logger.info(f"Successfully converted clip to shorts: {output_path}")
                return str(output_path)
            else:
                logger.error(f"Conversion failed - output file not created: {output_path}")
                return None
                
        except Exception as e:
            logger.error(f"Error converting clip {clip_data['id']} to shorts format: {e}")
            return None
    
    def generate_shorts_metadata(self, clip_data: Dict, stream_title: str) -> Dict[str, str]:
        """
        Generate YouTube metadata for a shorts video
        
        Args:
            clip_data: Clip information
            stream_title: Original stream title
            
        Returns:
            Dictionary with title, description, tags
        """
        # Create engaging title for the short
        clip_title = clip_data['title']
        game_context = ""
        
        # Add game context if available
        if 'game_name' in clip_data and clip_data['game_name']:
            game_context = f" - {clip_data['game_name']}"
        
        title = f"{clip_title}{game_context} #Shorts"
        
        # Create description
        description = f"""🎮 {clip_title}

From the stream: "{stream_title}"

Creator: {clip_data['creator_name']}
Originally clipped: {clip_data['created_at'].strftime('%Y-%m-%d')}

#gaming #twitch #shorts #highlights #clips"""
        
        # Generate tags
        tags = [
            "gaming", "twitch", "shorts", "highlights", "clips",
            "sir_kris", "gameplay", "funny", "epic"
        ]
        
        return {
            'title': title[:100],  # YouTube title limit
            'description': description,
            'tags': tags
        }
    
    async def process_stream_clips(self, stream_id: str) -> List[Dict]:
        """
        Process all clips for a completed stream
        
        Args:
            stream_id: Database stream ID
            
        Returns:
            List of processed clip information
        """
        try:
            # Get stream information
            stream_data = self.db.get_stream(stream_id)
            if not stream_data:
                logger.error(f"Stream not found: {stream_id}")
                return []
            
            # Convert Twitch user login to user ID
            twitch = await self.get_twitch_client()
            user = await first(twitch.get_users(logins=[self.config.TWITCH_USER_LOGIN]))
            await twitch.close()
            
            if not user:
                logger.error(f"Could not find Twitch user: {self.config.TWITCH_USER_LOGIN}")
                return []
            
            broadcaster_id = user.id
            
            logger.info(f"Processing clips for stream: {stream_data['title']}")
            
            # Fetch clips from the stream period
            clips = await self.fetch_stream_clips(
                broadcaster_id=broadcaster_id,
                started_at=stream_data['started_at'],
                ended_at=stream_data['ended_at']
            )
            
            if not clips:
                logger.info(f"No clips found for stream {stream_id}")
                return []
            
            logger.info(f"Found {len(clips)} clips to process")
            
            processed_clips = []
            
            for clip_data in clips:
                try:
                    # Download clip
                    downloaded_path = self.download_clip(clip_data)
                    if not downloaded_path:
                        continue
                    
                    # Convert to shorts format
                    shorts_path = self.convert_to_shorts_format(downloaded_path, clip_data)
                    if not shorts_path:
                        continue
                    
                    # Generate metadata
                    metadata = self.generate_shorts_metadata(clip_data, stream_data['title'])
                    
                    # Create upload record
                    upload_data = {
                        'platform': 'youtube',
                        'content_type': 'short',
                        'file_path': shorts_path,
                        'youtube_title': metadata['title'],
                        'youtube_description': metadata['description'],
                        'youtube_privacy_status': 'private',  # Start as private
                        'status': 'ready_for_upload',
                        'metadata': {
                            'clip_id': clip_data['id'],
                            'clip_url': clip_data['url'],
                            'original_views': clip_data['view_count'],
                            'clip_duration': clip_data['duration'],
                            'creator_name': clip_data['creator_name'],
                            'tags': metadata['tags'],
                            'stream_id': stream_id
                        }
                    }
                    
                    upload_id = self.db.create_upload(upload_data)
                    
                    processed_clips.append({
                        'upload_id': upload_id,
                        'clip_data': clip_data,
                        'shorts_path': shorts_path,
                        'metadata': metadata
                    })
                    
                    logger.info(f"Processed clip: {clip_data['title']} -> {upload_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing clip {clip_data['id']}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(processed_clips)} clips for stream {stream_id}")
            return processed_clips
            
        except Exception as e:
            logger.error(f"Error processing clips for stream {stream_id}: {e}")
            return []
    
    def cleanup_clip_files(self, days_old: int = 7):
        """Clean up old clip files"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            for directory in [self.clips_dir, self.clips_temp_dir, self.processed_clips_dir]:
                for file_path in directory.glob("*"):
                    if file_path.is_file():
                        file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_date < cutoff_date:
                            file_path.unlink()
                            logger.info(f"Cleaned up old clip file: {file_path}")
                            
        except Exception as e:
            logger.error(f"Error during clip cleanup: {e}")

# Test functions
async def test_clips_processor():
    """Test the clips processor functionality"""
    processor = ClipsProcessor()
    
    logger.info("Testing Twitch API connection...")
    twitch = await processor.get_twitch_client()
    user = await first(twitch.get_users(logins=[processor.config.TWITCH_USER_LOGIN]))
    await twitch.close()
    
    if user:
        logger.info(f"✅ Found user: {user.display_name} (ID: {user.id})")
    else:
        logger.error("❌ Could not find Twitch user")
    
    logger.info("✅ Clips processor test completed")

if __name__ == "__main__":
    asyncio.run(test_clips_processor())