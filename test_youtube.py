import sys
import os
import logging

# Add src directory to path
sys.path.append('src')

from youtube_api import YouTubeAPI, test_youtube_api

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == "__main__":
    print("Testing YouTube API connection...")
    
    if test_youtube_api():
        print("✅ YouTube API connection successful!")
        
        # Try to get basic service info
        youtube = YouTubeAPI()
        print(f"✅ YouTube service initialized: {youtube.service is not None}")
        
    else:
        print("❌ YouTube API connection failed!")