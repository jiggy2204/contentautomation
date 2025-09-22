"""
Main entry point for Content Automation system.
"""

import asyncio
import logging
import signal
import sys
from src.stream_detector import StreamDetector
from src.config import get_config

def setup_logging():
    """Set up application logging."""
    config = get_config()
    
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('content_automation.log')
        ]
    )

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    print("\nShutdown signal received. Stopping content automation...")
    sys.exit(0)

async def main():
    """Main application entry point."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Content Automation System")
    
    try:
        detector = StreamDetector()
        await detector.start_monitoring()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))