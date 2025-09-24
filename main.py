import sys
import logging
sys.path.append('src')

from upload_manager import run_enhanced_stream_monitor

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('automation.log'),
            logging.StreamHandler()
        ]
    )
    
    print("🚀 Starting Complete Content Automation System")
    print("Features: Stream Detection + VOD Processing + YouTube Uploads")
    
    run_enhanced_stream_monitor()