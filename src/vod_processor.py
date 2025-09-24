"""
VOD processing system for Twitch to YouTube automation.
Handles downloading, processing, and preparing videos for upload.
"""

import os
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import yt_dlp
import ffmpeg

logger = logging.getLogger(__name__)

class VODProcessor:
    """Handles Twitch VOD downloading and processing for YouTube upload."""
    
    def __init__(self, 
                 download_dir: str = "downloads",
                 temp_dir: str = "temp",
                 max_file_size_gb: int = 10):
        """
        Initialize VOD processor.
        
        Args:
            download_dir: Directory for downloaded VODs
            temp_dir: Directory for temporary processing files
            max_file_size_gb: Maximum file size for YouTube (GB)
        """
        self.download_dir = Path(download_dir)
        self.temp_dir = Path(temp_dir)
        self.max_file_size_bytes = max_file_size_gb * 1024 * 1024 * 1024
        
        # Create directories
        self.download_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        
        logger.info(f"VOD processor initialized: downloads={download_dir}, temp={temp_dir}")
    
    def get_vod_info(self, vod_url: str) -> Optional[Dict[str, Any]]:
        """
        Get VOD information without downloading.
        
        Args:
            vod_url: Twitch VOD URL (e.g., https://www.twitch.tv/videos/123456789)
            
        Returns:
            VOD info dictionary or None if failed
        """
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(vod_url, download=False)
                
                # Extract relevant info
                vod_info = {
                    'id': info.get('id'),
                    'title': info.get('title'),
                    'description': info.get('description', ''),
                    'duration': info.get('duration'),  # seconds
                    'upload_date': info.get('upload_date'),
                    'uploader': info.get('uploader'),
                    'view_count': info.get('view_count'),
                    'thumbnail': info.get('thumbnail'),
                    'formats': len(info.get('formats', [])),
                    'file_size': info.get('filesize') or info.get('filesize_approx'),
                }
                
                logger.info(f"VOD info retrieved: {vod_info['title']} ({self._format_duration(vod_info['duration'])})")
                return vod_info
                
        except Exception as e:
            logger.error(f"Failed to get VOD info for {vod_url}: {e}")
            return None
    
    def download_vod(self, 
                    vod_url: str, 
                    quality: str = "best[height<=1080]",
                    audio_only: bool = False) -> Optional[str]:
        """
        Download VOD from Twitch.
        
        Args:
            vod_url: Twitch VOD URL
            quality: Video quality selector
            audio_only: Download audio only (for faster processing)
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            vod_id = vod_url.split('/')[-1]
            
            if audio_only:
                filename = f"vod_{vod_id}_{timestamp}.%(ext)s"
                format_selector = "bestaudio"
            else:
                filename = f"vod_{vod_id}_{timestamp}.%(ext)s"
                format_selector = quality
            
            output_path = self.download_dir / filename
            
            # yt-dlp options
            ydl_opts = {
                'format': format_selector,
                'outtmpl': str(output_path),
                'writeinfojson': True,  # Save metadata
                'writethumbnail': True,  # Save thumbnail
                'ignoreerrors': False,
            }
            
            # Add file size limit
            if not audio_only:
                ydl_opts['max_filesize'] = self.max_file_size_bytes
            
            logger.info(f"Starting VOD download: {vod_url}")
            logger.info(f"Quality: {format_selector}, Output: {output_path}")
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([vod_url])
            
            # Find the downloaded file (yt-dlp changes extension)
            downloaded_files = list(self.download_dir.glob(f"vod_{vod_id}_{timestamp}.*"))
            video_file = None
            
            for file in downloaded_files:
                if file.suffix in ['.mp4', '.mkv', '.flv', '.m4a', '.webm']:
                    video_file = file
                    break
            
            if video_file and video_file.exists():
                logger.info(f"VOD downloaded successfully: {video_file}")
                return str(video_file)
            else:
                logger.error("Downloaded file not found")
                return None
                
        except Exception as e:
            logger.error(f"VOD download failed: {e}")
            return None
    
    def process_for_youtube(self, 
                          input_path: str, 
                          output_path: Optional[str] = None,
                          target_size_gb: float = 8.0) -> Optional[str]:
        """
        Process video for YouTube upload (format conversion, compression).
        
        Args:
            input_path: Path to input video file
            output_path: Optional output path (auto-generated if None)
            target_size_gb: Target file size in GB
            
        Returns:
            Path to processed file or None if failed
        """
        try:
            input_path = Path(input_path)
            if not input_path.exists():
                logger.error(f"Input file not found: {input_path}")
                return None
            
            # Generate output path
            if not output_path:
                output_path = self.temp_dir / f"youtube_{input_path.stem}_processed.mp4"
            else:
                output_path = Path(output_path)
            
            logger.info(f"Processing video for YouTube: {input_path} -> {output_path}")
            
            # Get input video info
            probe = ffmpeg.probe(str(input_path))
            video_info = next(stream for stream in probe['streams'] if stream['codec_type'] == 'video')
            audio_info = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)
            
            duration = float(probe['format']['duration'])
            input_size = int(probe['format']['size'])
            
            logger.info(f"Input: {video_info['width']}x{video_info['height']}, "
                       f"{self._format_duration(duration)}, {input_size / (1024*1024*1024):.1f}GB")
            
            # Calculate target bitrate
            target_size_bytes = target_size_gb * 1024 * 1024 * 1024
            target_bitrate = int((target_size_bytes * 8) / duration * 0.95)  # 95% to leave margin
            
            # YouTube recommended settings
            max_bitrates = {
                2160: 40_000_000,  # 4K
                1440: 16_000_000,  # 1440p
                1080: 8_000_000,   # 1080p
                720: 5_000_000,    # 720p
                480: 2_500_000,    # 480p
            }
            
            height = int(video_info['height'])
            recommended_bitrate = max_bitrates.get(height, 5_000_000)
            final_bitrate = min(target_bitrate, recommended_bitrate)
            
            logger.info(f"Target bitrate: {final_bitrate:,} bps ({final_bitrate/1_000_000:.1f} Mbps)")
            
            # Build ffmpeg command
            input_stream = ffmpeg.input(str(input_path))
            
            # Video processing
            video = input_stream['v'].filter('scale', -2, min(height, 1080))  # Max 1080p height
            
            # Audio processing
            audio = input_stream['a'] if audio_info else None
            
            # Output with YouTube-optimized settings
            output_args = {
                'vcodec': 'libx264',
                'preset': 'medium',
                'crf': 23,
                'maxrate': final_bitrate,
                'bufsize': final_bitrate * 2,
                'pix_fmt': 'yuv420p',
                'movflags': '+faststart',  # Optimize for streaming
            }
            
            if audio:
                output_args.update({
                    'acodec': 'aac',
                    'audio_bitrate': '128k',
                    'ar': 48000,
                })
                output = ffmpeg.output(video, audio, str(output_path), **output_args)
            else:
                output = ffmpeg.output(video, str(output_path), **output_args)
            
            # Run ffmpeg
            logger.info("Starting video processing...")
            ffmpeg.run(output, overwrite_output=True, quiet=False)
            
            if output_path.exists():
                output_size = output_path.stat().st_size
                logger.info(f"Video processed successfully: {output_size / (1024*1024*1024):.1f}GB")
                return str(output_path)
            else:
                logger.error("Processed file not found")
                return None
                
        except Exception as e:
            logger.error(f"Video processing failed: {e}")
            return None
    
    def create_thumbnail(self, video_path: str, timestamp: str = "00:05:00") -> Optional[str]:
        """
        Extract thumbnail from video.
        
        Args:
            video_path: Path to video file
            timestamp: Time position for thumbnail (HH:MM:SS)
            
        Returns:
            Path to thumbnail image or None if failed
        """
        try:
            video_path = Path(video_path)
            thumbnail_path = video_path.parent / f"{video_path.stem}_thumb.jpg"
            
            (
                ffmpeg
                .input(str(video_path), ss=timestamp)
                .output(str(thumbnail_path), vframes=1, q=2)
                .overwrite_output()
                .run(quiet=True)
            )
            
            if thumbnail_path.exists():
                logger.info(f"Thumbnail created: {thumbnail_path}")
                return str(thumbnail_path)
            else:
                logger.error("Thumbnail creation failed")
                return None
                
        except Exception as e:
            logger.error(f"Thumbnail creation failed: {e}")
            return None
    
    def cleanup_temp_files(self, keep_days: int = 1) -> None:
        """
        Clean up old temporary files.
        
        Args:
            keep_days: Keep files newer than this many days
        """
        try:
            cutoff_time = datetime.now() - timedelta(days=keep_days)
            
            for directory in [self.download_dir, self.temp_dir]:
                for file_path in directory.iterdir():
                    if file_path.is_file():
                        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_time < cutoff_time:
                            file_path.unlink()
                            logger.info(f"Cleaned up old file: {file_path}")
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        try:
            download_files = list(self.download_dir.glob("*"))
            temp_files = list(self.temp_dir.glob("*"))
            
            download_size = sum(f.stat().st_size for f in download_files if f.is_file())
            temp_size = sum(f.stat().st_size for f in temp_files if f.is_file())
            
            return {
                'download_files': len(download_files),
                'temp_files': len(temp_files),
                'download_size_gb': download_size / (1024*1024*1024),
                'temp_size_gb': temp_size / (1024*1024*1024),
                'total_size_gb': (download_size + temp_size) / (1024*1024*1024),
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {}
    
    @staticmethod
    def _format_duration(seconds: Optional[int]) -> str:
        """Format duration in seconds to human-readable string."""
        if not seconds:
            return "Unknown"
        
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"


# Integration functions for your existing system
def process_stream_vod(stream_id: int, 
                      vod_url: str,
                      processor: VODProcessor) -> Optional[Dict[str, Any]]:
    """
    Process a stream VOD for YouTube upload.
    
    Args:
        stream_id: Stream ID from your database
        vod_url: Twitch VOD URL
        processor: VODProcessor instance
        
    Returns:
        Dictionary with processed file paths and metadata
    """
    logger.info(f"Processing VOD for stream {stream_id}: {vod_url}")
    
    # Get VOD info
    vod_info = processor.get_vod_info(vod_url)
    if not vod_info:
        logger.error("Failed to get VOD info")
        return None
    
    # Check duration (skip very short or very long streams)
    duration = vod_info.get('duration', 0)
    if duration < 300:  # Less than 5 minutes
        logger.warning(f"Stream too short: {duration}s")
        return None
    
    if duration > 12 * 3600:  # More than 12 hours
        logger.warning(f"Stream too long: {duration}s - consider splitting")
    
    # Download VOD
    video_file = processor.download_vod(vod_url)
    if not video_file:
        logger.error("VOD download failed")
        return None
    
    # Process for YouTube
    processed_file = processor.process_for_youtube(video_file)
    if not processed_file:
        logger.error("Video processing failed")
        return None
    
    # Create thumbnail
    thumbnail_file = processor.create_thumbnail(processed_file)
    
    result = {
        'stream_id': stream_id,
        'vod_info': vod_info,
        'original_file': video_file,
        'processed_file': processed_file,
        'thumbnail_file': thumbnail_file,
        'ready_for_upload': True,
    }
    
    logger.info(f"VOD processing complete for stream {stream_id}")
    return result


# Test function
def test_vod_processor():
    """Test VOD processor with a sample URL."""
    processor = VODProcessor()
    
    # This would need a real Twitch VOD URL to test
    test_url = "https://www.twitch.tv/videos/123456789"  # Replace with actual URL
    
    logger.info("Testing VOD info retrieval...")
    vod_info = processor.get_vod_info(test_url)
    
    if vod_info:
        logger.info(f"Test successful: {vod_info['title']}")
        return True
    else:
        logger.warning("Test requires valid Twitch VOD URL")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_vod_processor()