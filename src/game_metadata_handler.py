# src/game_metadata_handler.py
"""
Game Metadata Handler
Fetches game information from IGDB → Steam → RAWG (in that order)
Handles failures with email notifications to Sir_Kris
"""

import sys
import os
import re
import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from dotenv import load_dotenv
import requests

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from twitch_handler import TwitchHandler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase_client import SupabaseClient
from api_clients.igdb_client import IGDBClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GameMetadataHandler:
    """Handles fetching and caching game metadata from multiple sources"""
    
    def __init__(self, db_client: Optional[SupabaseClient] = None, twitch_handler: Optional[TwitchHandler] = None):
        """
        Initialize Game Metadata Handler
        
        Args:
            db_client: Optional SupabaseClient instance
        """
        self.db = db_client or SupabaseClient()
        self.twitch_handler = twitch_handler  
        
        # Initialize API clients
        self.igdb_client = IGDBClient()
        
        # API Keys
        self.rawg_api_key = os.getenv('RAWG_API_KEY')
        self.steam_api_key = os.getenv('STEAM_API_KEY')  # Not available yet
        
        logger.info('🎮 Game Metadata Handler initialized')
        if not self.rawg_api_key:
            logger.warning('⚠️  RAWG API key not found')
        if not self.steam_api_key:
            logger.warning('⚠️  Steam API key not found (will skip Steam)')
    
    def extract_game_name(self, stream_title: str) -> str:
        """
        Extract game name from stream title (FALLBACK ONLY)
        
        This should rarely be used - prefer getting game_name from Twitch metadata
        
        Args:
            stream_title: Stream title from Twitch
        
        Returns:
            Extracted game name
        """
        logger.warning('⚠️  Using fallback game name extraction from title')
        logger.warning('   Prefer using game_name from Twitch stream metadata')
        
        # Remove common streaming phrases
        title = stream_title.lower()
        title = re.sub(r'\b(playing|streaming|live|first playthrough|day \d+)\b', '', title, flags=re.IGNORECASE)
        
        # Extract text before common separators
        separators = [' - ', ' | ', ' : ', '!', '?']
        for sep in separators:
            if sep in stream_title:
                game_name = stream_title.split(sep)[0].strip()
                # Remove brackets if present
                game_name = re.sub(r'[\[\]\(\)]', '', game_name).strip()
                return game_name
        
        # If no separator found, take first 1-3 words (likely game name)
        words = stream_title.split()[:3]
        return ' '.join(words).strip()
    
    def fetch_from_igdb(self, game_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch game metadata from IGDB
        
        Args:
            game_name: Name of the game
        
        Returns:
            Dictionary with game metadata or None
        """
        try:
            logger.info(f'🔍 Searching IGDB for: {game_name}')
            
            # Search for game
            results = self.igdb_client.search_games(game_name, limit=1)
            
            if not results:
                logger.warning(f'⚠️  No results from IGDB for: {game_name}')
                return None
            
            game = results[0]
            
            # Extract metadata
            metadata = {
                'game_name': game.get('name', game_name),
                'source': 'igdb',
                'description': game.get('summary', ''),
                'igdb_id': str(game.get('id')),
                'tags': []
            }
            
            # Extract genre tags
            if 'genres' in game and game['genres']:
                for genre in game['genres']:
                    if isinstance(genre, dict) and 'name' in genre:
                        metadata['tags'].append(genre['name'])
            
            # Limit to top 3 tags
            metadata['tags'] = metadata['tags'][:3]
            
            logger.info(f'✅ Found game in IGDB: {metadata["game_name"]}')
            logger.info(f'   Tags: {", ".join(metadata["tags"])}')
            
            return metadata
            
        except Exception as e:
            logger.error(f'❌ IGDB error: {e}')
            return None
    
    def fetch_from_steam(self, game_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch game metadata from Steam API
        
        Args:
            game_name: Name of the game
        
        Returns:
            Dictionary with game metadata or None
        """
        if not self.steam_api_key:
            logger.info('⏭️  Steam API key not available, skipping')
            return None
        
        try:
            logger.info(f'🔍 Searching Steam for: {game_name}')
            
            # TODO: Implement Steam API search when key is available
            # Steam API doesn't have a direct search endpoint
            # Would need to use SteamSpy API or scrape Steam store
            
            logger.warning('⚠️  Steam integration not yet implemented')
            return None
            
        except Exception as e:
            logger.error(f'❌ Steam error: {e}')
            return None
    
    def fetch_from_rawg(self, game_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch game metadata from RAWG API
        
        Args:
            game_name: Name of the game
        
        Returns:
            Dictionary with game metadata or None
        """
        if not self.rawg_api_key:
            logger.warning('⚠️  RAWG API key not available')
            return None
        
        try:
            logger.info(f'🔍 Searching RAWG for: {game_name}')
            
            # RAWG API search endpoint
            url = 'https://api.rawg.io/api/games'
            params = {
                'key': self.rawg_api_key,
                'search': game_name,
                'page_size': 1
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('results'):
                logger.warning(f'⚠️  No results from RAWG for: {game_name}')
                return None
            
            game = data['results'][0]
            
            # Extract metadata
            metadata = {
                'game_name': game.get('name', game_name),
                'source': 'rawg',
                'description': game.get('description_raw', ''),
                'rawg_id': str(game.get('id')),
                'tags': []
            }
            
            # Extract genre/tag names
            if game.get('genres'):
                for genre in game['genres'][:3]:  # Top 3
                    metadata['tags'].append(genre['name'])
            
            if game.get('tags') and len(metadata['tags']) < 3:
                for tag in game['tags'][:3 - len(metadata['tags'])]:
                    metadata['tags'].append(tag['name'])
            
            logger.info(f'✅ Found game in RAWG: {metadata["game_name"]}')
            logger.info(f'   Tags: {", ".join(metadata["tags"])}')
            
            return metadata
            
        except Exception as e:
            logger.error(f'❌ RAWG error: {e}')
            return None
    
    def fetch_game_metadata(self, game_name: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Fetch game metadata from all available sources (IGDB → Steam → RAWG)
        
        Args:
            game_name: Name of the game
        
        Returns:
            Tuple of (metadata dict or None, status string)
            Status: 'success', 'failed', 'cached'
        """
        # Normalize game name
        game_name = game_name.strip()
        
        # Check cache first
        cached = self.db.get_game_metadata(game_name)
        if cached:
            logger.info(f'💾 Using cached metadata for: {game_name}')
            return cached, 'cached'
        
        logger.info(f'🎮 Fetching metadata for: {game_name}')
        
        # Try IGDB first
        metadata = self.fetch_from_igdb(game_name)
        if metadata:
            logger.info(f'✅ Found metadata from IGDB')
            # Cache and return
            try:
                self.db.create_game_metadata(metadata)
                logger.info(f'💾 Cached metadata for: {game_name}')
            except Exception as e:
                logger.error(f'⚠️  Failed to cache metadata: {e}')
            return metadata, 'success'
        
        # Try Steam if IGDB failed
        metadata = self.fetch_from_steam(game_name)
        if metadata:
            logger.info(f'✅ Found metadata from Steam')
            # Cache and return
            try:
                self.db.create_game_metadata(metadata)
                logger.info(f'💾 Cached metadata for: {game_name}')
            except Exception as e:
                logger.error(f'⚠️  Failed to cache metadata: {e}')
            return metadata, 'success'
        
        # Try RAWG if both IGDB and Steam failed
        metadata = self.fetch_from_rawg(game_name)
        if metadata:
            logger.info(f'✅ Found metadata from RAWG')
            # Cache and return
            try:
                self.db.create_game_metadata(metadata)
                logger.info(f'💾 Cached metadata for: {game_name}')
            except Exception as e:
                logger.error(f'⚠️  Failed to cache metadata: {e}')
            return metadata, 'success'
        
        # If all sources failed
        logger.error(f'❌ All sources failed for: {game_name}')
        return None, 'failed'
    
    def process_completed_downloads(self) -> Dict[str, int]:
        """
        Process all completed downloads and fetch game metadata
        
        Returns:
            Stats dictionary with counts
        """
        logger.info('🚀 Processing completed downloads for metadata...')
        
        # Get completed downloads that don't have metadata yet
        # We need to query downloads with status 'completed' and no youtube_upload record
        try:
            # Get all completed downloads with their stream data
            result = self.db.client.table('vod_downloads')\
                .select('*, streams(*)')\
                .eq('download_status', 'completed')\
                .execute()
            
            completed_downloads = result.data
            
            if not completed_downloads:
                logger.info('💤 No completed downloads to process')
                return {'processed': 0, 'success': 0, 'failed': 0, 'cached': 0}
            
            stats = {'processed': 0, 'success': 0, 'failed': 0, 'cached': 0}
            
            for item in completed_downloads:
                download_record = {k: v for k, v in item.items() if k != 'streams'}
                stream_record = item['streams']
                
                # Check if already has upload record
                existing_upload = self.db.client.table('youtube_uploads')\
                    .select('id')\
                    .eq('vod_download_id', download_record['id'])\
                    .execute()
                
                if existing_upload.data:
                    logger.info(f'⏭️  Download {download_record["id"]} already has upload record')
                    continue
                
                stats['processed'] += 1
                
                # Get game name from Twitch metadata
                # Priority: game_name > "Games + Demos" > extract from title
                game_name = stream_record.get('game_name')
                
                if not game_name or game_name.strip() == '':
                    logger.warning(f'⚠️  No game_name in stream metadata for: {stream_record["title"]}')
                    logger.info('   Using default: Games + Demos')
                    game_name = 'Games + Demos'
                
                logger.info(f'🎮 Processing: {stream_record["title"]}')
                logger.info(f'   Game: {game_name}')
                
                # Fetch metadata
                metadata, status = self.fetch_game_metadata(game_name)
                
                if status == 'cached':
                    stats['cached'] += 1
                elif status == 'success':
                    stats['success'] += 1
                elif status == 'failed':
                    stats['failed'] += 1
                
                # Store metadata status in a way we can track it
                # We'll create a minimal upload record to mark this as processed
                # The youtube_handler will fill in the rest
                
            logger.info(f'🎉 Metadata processing complete!')
            logger.info(f'   Processed: {stats["processed"]}')
            logger.info(f'   Success: {stats["success"]}')
            logger.info(f'   Cached: {stats["cached"]}')
            logger.info(f'   Failed: {stats["failed"]}')
            
            return stats
            
        except Exception as e:
            logger.error(f'❌ Error processing downloads: {e}')
            raise


# Example usage and testing
def main():
    """Test the game metadata handler"""
    print('\n' + '='*60)
    print('Testing Game Metadata Handler')
    print('='*60)
    
    handler = GameMetadataHandler()
    
    # Test game names
    test_games = [
        'Warframe',
        'Dead Space',
        'The Legend of Zelda',
        'Nonexistent Game 12345'  # Should fail
    ]
    
    for game_name in test_games:
        print(f'\n📦 Testing: {game_name}')
        metadata, status = handler.fetch_game_metadata(game_name)
        
        if metadata:
            print(f'✅ Status: {status}')
            print(f'   Name: {metadata["game_name"]}')
            print(f'   Source: {metadata["source"]}')
            print(f'   Tags: {", ".join(metadata["tags"])}')
            print(f'   Description: {metadata["description"][:100]}...')
        else:
            print(f'❌ Status: {status}')
            print(f'   No metadata found')
    
    print('\n' + '='*60)
    print('Testing complete!')
    print('='*60 + '\n')


if __name__ == '__main__':
    main()