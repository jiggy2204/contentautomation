"""
YouTube OAuth Token Generator
Run this script locally to generate youtube_token.pickle
This only needs to be done once, then upload the token file to your server.
"""

import os
import pickle
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Get project root directory (parent of api_clients folder)
PROJECT_ROOT = Path(__file__).parent.parent

# YouTube API scopes - these define what permissions we're requesting
SCOPES = [
    'https://www.googleapis.com/auth/youtube.upload',
    'https://www.googleapis.com/auth/youtube',
    'https://www.googleapis.com/auth/youtube.force-ssl'
]

def generate_youtube_token():
    """
    Generate YouTube OAuth token by walking through the browser authentication flow.
    Saves the token to youtube_token.pickle for future use.
    """
    creds = None
    # Store token files in project root, not in api_clients folder
    token_file = PROJECT_ROOT / 'youtube_token.pickle'
    client_secrets_file = PROJECT_ROOT / 'client_secret.json'
    
    # Check if client_secret.json exists
    if not client_secrets_file.exists():
        print(f"❌ Error: {client_secrets_file} not found!")
        print(f"Please make sure client_secret.json is in the project root: {PROJECT_ROOT}")
        return
    
    # Check if we already have a valid token
    if token_file.exists():
        print(f"📄 Found existing {token_file.name}")
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("🔄 Refreshing expired token...")
            try:
                creds.refresh(Request())
                print("✅ Token refreshed successfully!")
            except Exception as e:
                print(f"⚠️  Token refresh failed: {e}")
                print("Will generate new token...")
                creds = None
        
        if not creds:
            print("\n🔐 Starting OAuth flow...")
            print("A browser window will open for you to authorize the application.")
            print(f"Make sure you're logged into your husband's YouTube account!\n")
            
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(client_secrets_file), 
                    SCOPES
                )
                
                # This will open a browser window for authorization
                creds = flow.run_local_server(
                    port=8080,
                    authorization_prompt_message='Please visit this URL to authorize: {url}',
                    success_message='Authorization complete! You can close this window.',
                    open_browser=True
                )
                
                print("✅ Authorization successful!")
                
            except Exception as e:
                print(f"❌ OAuth flow failed: {e}")
                return
        
        # Save the credentials for future use
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
        
        print(f"\n✅ Token saved to {token_file}")
        print("\n📤 Next steps:")
        print("1. Copy youtube_token.pickle to your Digital Ocean server")
        print("2. Place it in the same directory as your application")
        print("3. Make sure your .env has: YOUTUBE_TOKEN_FILE=youtube_token.pickle")
        print("\n⚠️  Keep this file secure - it grants access to the YouTube account!")
    
    else:
        print("✅ Valid token already exists!")
        print(f"Token file: {token_file}")
        print("No action needed - you're ready to go!")

if __name__ == '__main__':
    print("=" * 60)
    print("YouTube OAuth Token Generator")
    print("=" * 60)
    generate_youtube_token()
    print("=" * 60)