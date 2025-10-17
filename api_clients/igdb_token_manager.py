# igdb_token_manager.py (UPDATED VERSION)
import os
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

TWITCH_CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
TOKEN_FILE = 'igdb_token.json'  # Changed to relative path for better portability

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_new_access_token():
    """Fetch a new access token from Twitch OAuth"""
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': TWITCH_CLIENT_ID,
        'client_secret': TWITCH_CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    
    try:
        response = requests.post(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Calculate expiration date (Twitch tokens expire in ~60 days)
        expires_in = data['expires_in']
        expiration_date = datetime.now() + timedelta(seconds=expires_in)
        
        # Save token info to file
        token_info = {
            'access_token': data['access_token'],
            'expires_at': expiration_date.isoformat(),
            'expires_in_seconds': expires_in,
            'created_at': datetime.now().isoformat()
        }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(TOKEN_FILE) if os.path.dirname(TOKEN_FILE) else '.', exist_ok=True)
        
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_info, f, indent=2)
        
        logger.info('✅ New IGDB access token obtained')
        logger.info(f'Token expires: {expiration_date.strftime("%Y-%m-%d %H:%M:%S")}')
        logger.info(f'Token saved to {TOKEN_FILE}')
        
        return data['access_token']
        
    except requests.exceptions.RequestException as e:
        logger.error(f'❌ Error getting access token: {e}')
        raise

def is_token_valid(token_info):
    """
    Check if token is still valid
    
    Args:
        token_info: Dictionary containing token information
    
    Returns:
        bool: True if token is valid, False otherwise
    """
    try:
        expiration_date = datetime.fromisoformat(token_info['expires_at'])
        
        # Token is valid if it expires more than 7 days from now (safety buffer)
        # This ensures we renew before expiration
        buffer_days = 7
        is_valid = datetime.now() < expiration_date - timedelta(days=buffer_days)
        
        if is_valid:
            days_remaining = (expiration_date - datetime.now()).days
            logger.info(f'✅ Token valid - expires in {days_remaining} days ({expiration_date.strftime("%Y-%m-%d")})')
        else:
            logger.warning(f'⚠️  Token expiring soon or expired - will renew')
        
        return is_valid
    
    except (KeyError, ValueError) as e:
        logger.error(f'❌ Invalid token info format: {e}')
        return False

def get_valid_access_token(force_refresh=False):
    """
    Get a valid access token, automatically refreshing if necessary
    
    Args:
        force_refresh: If True, force token refresh regardless of expiration
    
    Returns:
        str: Valid access token
    """
    # Force refresh if requested
    if force_refresh:
        logger.info('🔄 Forcing token refresh...')
        return get_new_access_token()
    
    # Check if token file exists
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                token_info = json.load(f)
            
            # Check if token is still valid
            if is_token_valid(token_info):
                return token_info['access_token']
            else:
                logger.info('🔄 Token expired or expiring soon, refreshing...')
                return get_new_access_token()
        
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f'❌ Error reading token file: {e}')
            logger.info('🆕 Getting new token...')
            return get_new_access_token()
    else:
        logger.info('🆕 No token found, getting new one...')
        return get_new_access_token()

def check_token_status():
    """
    Check and display current token status
    Useful for monitoring and debugging
    """
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                token_info = json.load(f)
            
            expiration_date = datetime.fromisoformat(token_info['expires_at'])
            created_date = datetime.fromisoformat(token_info['created_at'])
            days_remaining = (expiration_date - datetime.now()).days
            
            print('\n' + '='*60)
            print('IGDB Token Status')
            print('='*60)
            print(f'Token Created: {created_date.strftime("%Y-%m-%d %H:%M:%S")}')
            print(f'Token Expires: {expiration_date.strftime("%Y-%m-%d %H:%M:%S")}')
            print(f'Days Remaining: {days_remaining}')
            print(f'Status: {"✅ Valid" if days_remaining > 7 else "⚠️  Expiring Soon"}')
            print('='*60 + '\n')
            
            return token_info
        
        except Exception as e:
            print(f'❌ Error reading token: {e}')
            return None
    else:
        print('❌ No token file found')
        return None

# Example usage and testing
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='IGDB Token Manager')
    parser.add_argument('--status', action='store_true', help='Check token status')
    parser.add_argument('--refresh', action='store_true', help='Force token refresh')
    parser.add_argument('--test', action='store_true', help='Test token by making API call')
    
    args = parser.parse_args()
    
    if args.status:
        check_token_status()
    elif args.refresh:
        token = get_valid_access_token(force_refresh=True)
        print(f'\n✅ New token obtained: {token[:20]}...\n')
    elif args.test:
        print('🧪 Testing token with IGDB API...')
        token = get_valid_access_token()
        
        # Make a test API call
        headers = {
            'Client-ID': TWITCH_CLIENT_ID,
            'Authorization': f'Bearer {token}'
        }
        response = requests.post(
            'https://api.igdb.com/v4/games',
            headers=headers,
            data='fields name; limit 1;'
        )
        
        if response.status_code == 200:
            print('✅ Token is working! API call successful.')
        else:
            print(f'❌ API call failed: {response.status_code}')
    else:
        # Default: just get/refresh token
        token = get_valid_access_token()
        print(f'\n✅ Token ready: {token[:20]}...\n')
        check_token_status()