# src/game_metadata_handler.py
"""
Game Metadata Handler
Fetches game information from Twitch → IGDB → RAWG (in that order)
Prioritizes Twitch for accurate game names and basic info
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

from src.supabase_client import SupabaseClient
from api_clients.igdb_client import IGDBClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GameMetadataHandler:
    """Handles fetching and caching game metadata from multiple sources"""
    
    def __init__(self, db_client: Optional[SupabaseClient] = None, twitch_handler: Optional['TwitchHandler'] = None):
        """
        Initialize Game Metadata Handler
        
        Args:
            db_client: Optional SupabaseClient instance
            twitch_handler: Optional TwitchHandler instance for Twitch API access
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
    
    async def fetch_from_twitch(self, game_id: str = None, game_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Fetch game metadata from Twitch API (PRIMARY SOURCE)
        
        Args:
            game_id: Twitch game ID (preferred)
            game_name: Game name (fallback if no game_id)
        
        Returns:
            Dictionary with game metadata or None
        """
        if not self.twitch_handler:
            logger.warning('⚠️  No Twitch handler available')
            return None
        
        try:
            # Authenticate if needed
            if not self.twitch_handler.twitch:
                await self.twitch_handler.authenticate()
            
            game = None
            
            # Try by game_id first (most reliable)
            if game_id:
                logger.info(f'🔍 Fetching game from Twitch by ID: {game_id}')
                from twitchAPI.helper import first
                game_generator = self.twitch_handler.twitch.get_games(game_ids=[game_id])
                game = await first(game_generator)
            
            # Fallback to search by name
            elif game_name:
                logger.info(f'🔍 Searching Twitch for: {game_name}')
                from twitchAPI.helper import first
                game_generator = self.twitch_handler.twitch.get_games(names=[game_name])
                game = await first(game_generator)
            
            if not game:
                logger.warning(f'⚠️  Game not found on Twitch')
                return None
            
            # Extract metadata from Twitch
            metadata = {
                'game_name': game.name,
                'source': 'twitch',
                'twitch_game_id': game.id,
                'description': '',  # Twitch doesn't provide descriptions
                'tags': [],  # Will get from IGDB/RAWG if needed
                'box_art_url': game.box_art_url if hasattr(game, 'box_art_url') else None,
                'igdb_id': game.igdb_id if hasattr(game, 'igdb_id') else None
            }
            
            logger.info(f'✅ Found game on Twitch: {metadata["game_name"]}')
            if metadata.get('igdb_id'):
                logger.info(f'   IGDB ID available: {metadata["igdb_id"]}')
            
            return metadata
            
        except Exception as e:
            logger.error(f'❌ Twitch API error: {e}')
            return None
    
    def fetch_from_igdb(self, game_name: str, igdb_id: str = None, require_exact_match: bool = True) -> Optional[Dict[str, Any]]:
        """
        Fetch game metadata from IGDB
        
        Args:
            game_name: Name of the game (from Twitch - this is canonical)
            igdb_id: Optional IGDB ID from Twitch for direct lookup
            require_exact_match: If True, only return if exact match found (prevents wrong game data)
        
        Returns:
            Dictionary with game metadata or None
        """
        try:
            if igdb_id:
                logger.info(f'🔍 Fetching IGDB game by ID: {igdb_id}')
                # TODO: Add direct ID lookup to IGDB client if needed
            
            logger.info(f'🔍 Searching IGDB for: {game_name}')
            
            # Search for game
            results = self.igdb_client.search_games(game_name, limit=10)
            
            if not results:
                logger.warning(f'⚠️  No results from IGDB for: {game_name}')
                return None
            
            # Find EXACT match only
            game = None
            search_name = game_name.lower().strip()
            
            for result in results:
                result_name = result.get('name', '').lower().strip()
                
                # EXACT match only
                if result_name == search_name:
                    game = result
                    logger.info(f'✅ Found EXACT match in IGDB: {result.get("name")}')
                    break
            
            # If require_exact_match and no exact match found, return None
            if require_exact_match and not game:
                logger.warning(f'⚠️  No EXACT match in IGDB for: {game_name}')
                logger.info(f'   Skipping IGDB, will try RAWG instead')
                return None
            
            # If we found an exact match, extract metadata
            if game:
                # Extract metadata - USE TWITCH NAME, not IGDB name
                metadata = {
                    'game_name': game_name,  # Use Twitch's canonical name!
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
                
                logger.info(f'✅ Using IGDB data for: {metadata["game_name"]}')
                logger.info(f'   Tags: {", ".join(metadata["tags"])}')
                
                return metadata
            
            return None
            
        except Exception as e:
            logger.error(f'❌ IGDB error: {e}')
            return None
    
    def fetch_from_rawg(self, game_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch game metadata from RAWG API
        
        Args:
            game_name: Name of the game (from Twitch - this is canonical)
        
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
                'page_size': 10
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('results'):
                logger.warning(f'⚠️  No results from RAWG for: {game_name}')
                return None
            
            # Find EXACT match only
            game = None
            search_name = game_name.lower().strip()
            
            for result in data['results']:
                result_name = result.get('name', '').lower().strip()
                
                # EXACT match
                if result_name == search_name:
                    game = result
                    logger.info(f'✅ Found EXACT match in RAWG: {result.get("name")}')
                    break
            
            # If no exact match, return None (don't guess)
            if not game:
                logger.warning(f'⚠️  No EXACT match in RAWG for: {game_name}')
                return None
            
            # Extract metadata - USE TWITCH NAME
            metadata = {
                'game_name': game_name,  # Use Twitch's canonical name!
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
            
            logger.info(f'✅ Using RAWG data for: {metadata["game_name"]}')
            logger.info(f'   Tags: {", ".join(metadata["tags"])}')
            
            return metadata
            
        except Exception as e:
            logger.error(f'❌ RAWG error: {e}')
            return None
    
    async def fetch_game_metadata(
        self, 
        game_name: str, 
        game_id: str = None
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Fetch game metadata from all available sources (Twitch → IGDB → RAWG)
        
        Args:
            game_name: Name of the game
            game_id: Optional Twitch game ID
        
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
        
        # Try Twitch first (MOST RELIABLE)
        if self.twitch_handler:
            twitch_metadata = await self.fetch_from_twitch(game_id=game_id, game_name=game_name)
            if twitch_metadata:
                # Get additional details from IGDB if we have IGDB ID
                igdb_id = twitch_metadata.get('igdb_id')
                igdb_metadata = self.fetch_from_igdb(
                    twitch_metadata['game_name'], 
                    igdb_id=igdb_id
                )
                
                # Merge Twitch + IGDB data
                if igdb_metadata:
                    # Use Twitch name (most accurate), but add IGDB details
                    final_metadata = {
                        'game_name': twitch_metadata['game_name'],  # Twitch name is canonical
                        'source': 'twitch+igdb',
                        'twitch_game_id': twitch_metadata.get('twitch_game_id'),
                        'description': igdb_metadata.get('description', ''),
                        'tags': igdb_metadata.get('tags', []),
                        'igdb_id': igdb_metadata.get('igdb_id')
                    }
                    
                    logger.info(f'✅ Combined Twitch + IGDB metadata')
                else:
                    # Just use Twitch data
                    final_metadata = twitch_metadata
                    logger.info(f'✅ Using Twitch metadata only')
                
                # Cache and return
                try:
                    self.db.create_game_metadata(final_metadata)
                    logger.info(f'💾 Cached metadata for: {game_name}')
                except Exception as e:
                    logger.error(f'⚠️  Failed to cache metadata: {e}')
                
                return final_metadata, 'success'
        
        # Fallback to IGDB if Twitch unavailable
        metadata = self.fetch_from_igdb(game_name)
        if metadata:
            logger.info(f'✅ Found metadata from IGDB')
            try:
                self.db.create_game_metadata(metadata)
                logger.info(f'💾 Cached metadata for: {game_name}')
            except Exception as e:
                logger.error(f'⚠️  Failed to cache metadata: {e}')
            return metadata, 'success'
        
        # Final fallback to RAWG
        metadata = self.fetch_from_rawg(game_name)
        if metadata:
            logger.info(f'✅ Found metadata from RAWG')
            try:
                self.db.create_game_metadata(metadata)
                logger.info(f'💾 Cached metadata for: {game_name}')
            except Exception as e:
                logger.error(f'⚠️  Failed to cache metadata: {e}')
            return metadata, 'success'
        
        # If all sources failed
        logger.error(f'❌ All sources failed for: {game_name}')
        return None, 'failed'
    
    async def process_completed_downloads(self) -> Dict[str, int]:
        """
        Process all completed downloads and fetch game metadata
        
        Returns:
            Stats dictionary with counts
        """
        logger.info('🚀 Processing completed downloads for metadata...')
        
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
                
                # Get game info from stream metadata
                game_name = stream_record.get('game_name')
                game_id = stream_record.get('game_id')
                
                if not game_name or game_name.strip() == '':
                    logger.warning(f'⚠️  No game_name in stream metadata for: {stream_record["title"]}')
                    logger.info('   Using default: Games + Demos')
                    game_name = 'Games + Demos'
                
                logger.info(f'🎮 Processing: {stream_record["title"]}')
                logger.info(f'   Game: {game_name}')
                if game_id:
                    logger.info(f'   Game ID: {game_id}')
                
                # Fetch metadata
                metadata, status = await self.fetch_game_metadata(game_name, game_id=game_id)
                
                if status == 'cached':
                    stats['cached'] += 1
                elif status == 'success':
                    stats['success'] += 1
                elif status == 'failed':
                    stats['failed'] += 1
            
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
async def main():
    """Test the game metadata handler"""
    import asyncio
    from twitch_handler import TwitchHandler
    
    print('\n' + '='*60)
    print('Testing Game Metadata Handler with Twitch Priority')
    print('='*60)
    
    # Initialize with Twitch handler
    twitch_handler = TwitchHandler()
    await twitch_handler.authenticate()
    
    handler = GameMetadataHandler(twitch_handler=twitch_handler)
    
    # Test game names
    test_games = [
        ('Warframe', '66170'),  # game_name, game_id
        ('Dead Space', None),
        ('The Legend of Zelda', None),
    ]
    
    for game_name, game_id in test_games:
        print(f'\n📦 Testing: {game_name}')
        if game_id:
            print(f'   With Game ID: {game_id}')
        
        metadata, status = await handler.fetch_game_metadata(game_name, game_id=game_id)
        
        if metadata:
            print(f'✅ Status: {status}')
            print(f'   Name: {metadata["game_name"]}')
            print(f'   Source: {metadata["source"]}')
            print(f'   Tags: {", ".join(metadata.get("tags", []))}')
            if metadata.get('description'):
                print(f'   Description: {metadata["description"][:100]}...')
        else:
            print(f'❌ Status: {status}')
            print(f'   No metadata found')
    
    await twitch_handler.close()
    
    print('\n' + '='*60)
    print('Testing complete!')
    print('='*60 + '\n')


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())