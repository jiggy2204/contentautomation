# token_scheduler.py
"""
Automated scheduler for IGDB token renewal
This runs as part of your main automation and checks token status daily
"""
import schedule
import time
import logging
from datetime import datetime
from .igdb_token_manager import get_valid_access_token, check_token_status

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def daily_token_check():
    """
    Daily check to ensure IGDB token is valid
    This will automatically renew if expiring within 7 days
    """
    logger.info('🔍 Running daily IGDB token check...')
    try:
        token = get_valid_access_token()
        logger.info('✅ IGDB token check complete')
        return token
    except Exception as e:
        logger.error(f'❌ Token check failed: {e}')
        raise

def start_token_scheduler():
    """
    Start the token renewal scheduler
    Checks token status once per day at 3 AM
    """
    logger.info('🚀 Starting IGDB token renewal scheduler...')
    
    # Run immediately on startup
    daily_token_check()
    
    # Schedule daily checks at 3 AM
    schedule.every().day.at("03:00").do(daily_token_check)
    
    logger.info('✅ Token scheduler started - will check daily at 3:00 AM')

def run_scheduler_loop():
    """
    Run the scheduler loop (blocking)
    Use this if running token scheduler as standalone service
    """
    start_token_scheduler()
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

# For integration into main automation
class TokenScheduler:
    """
    Non-blocking token scheduler that can be integrated into main automation
    """
    def __init__(self):
        self.scheduler = schedule.Scheduler()
        self.setup_schedule()
    
    def setup_schedule(self):
        """Set up the token renewal schedule"""
        logger.info('🚀 Setting up IGDB token renewal schedule...')
        
        # Check token immediately
        daily_token_check()
        
        # Schedule daily checks
        self.scheduler.every().day.at("03:00").do(daily_token_check)
        
        logger.info('✅ Token renewal schedule configured')
    
    def run_pending(self):
        """Check and run any pending scheduled tasks"""
        self.scheduler.run_pending()

# Example integration into main.py
"""
# In your main.py:

from token_scheduler import TokenScheduler

def main():
    # Initialize token scheduler
    token_scheduler = TokenScheduler()
    
    while True:
        # Your main automation logic here
        # ...
        
        # Check for scheduled token renewal
        token_scheduler.run_pending()
        
        # Sleep between iterations
        time.sleep(60)

if __name__ == '__main__':
    main()
"""

if __name__ == '__main__':
    # Run as standalone scheduler
    print('\n' + '='*60)
    print('IGDB Token Renewal Scheduler')
    print('='*60)
    print('This will check token status daily and auto-renew when needed')
    print('Press Ctrl+C to stop\n')
    
    try:
        run_scheduler_loop()
    except KeyboardInterrupt:
        print('\n\n👋 Scheduler stopped by user')
    except Exception as e:
        logger.error(f'❌ Scheduler error: {e}')
        raise