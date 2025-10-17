# igdb_client.py
import sys
import os
import requests
from dotenv import load_dotenv
from .igdb_token_manager import get_valid_access_token

load_dotenv()

class IGDBClient:
    """Client for making requests to the IGDB API"""
    
    def __init__(self):
        self.client_id = os.getenv('TWITCH_CLIENT_ID')
        self.base_url = 'https://api.igdb.com/v4'
        self.access_token = None
        self._refresh_token()
    
    def _refresh_token(self):
        """Refresh the access token"""
        self.access_token = get_valid_access_token()
    
    def _get_headers(self):
        """Get headers for IGDB API requests"""
        return {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {self.access_token}'
        }
    
    def query(self, endpoint, query_body):
        """
        Make a query to IGDB API
        
        Args:
            endpoint: API endpoint (e.g., 'games', 'genres')
            query_body: Query string (e.g., 'fields name,rating; limit 10;')
        
        Returns:
            JSON response from API
        """
        url = f'{self.base_url}/{endpoint}'
        
        try:
            response = requests.post(
                url,
                headers=self._get_headers(),
                data=query_body
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                # Token expired, refresh and retry
                print('🔄 Token expired, refreshing...')
                self._refresh_token()
                response = requests.post(
                    url,
                    headers=self._get_headers(),
                    data=query_body
                )
                response.raise_for_status()
                return response.json()
            else:
                raise
    
    def search_games(self, search_term, limit=10):
        """Search for games by name"""
        query = f'search "{search_term}"; fields name,rating,genres.name,summary; limit {limit};'
        return self.query('games', query)
    
    def get_game_by_id(self, game_id):
        """Get detailed info about a specific game"""
        query = f'fields name,rating,genres.name,summary,cover.url; where id = {game_id};'
        return self.query('games', query)
    
    def get_popular_games(self, limit=20):
        """Get popular games sorted by rating"""
        query = f'fields name,rating,genres.name,summary; where rating > 80; sort rating desc; limit {limit};'
        return self.query('games', query)

# Example usage
if __name__ == '__main__':
    client = IGDBClient()
    
    # Search for a game
    print('\n🔍 Searching for "Zelda"...')
    results = client.search_games('Zelda', limit=5)
    for game in results:
        print(f"- {game.get('name')} (Rating: {game.get('rating', 'N/A')})")