import asyncio
from src.twitch_api import get_twitch_client, close_twitch_client

async def test_twitch():
    try:
        client = await get_twitch_client()
        
        # Test getting user info
        user_info = await client.get_user_info()
        print(f"User info: {user_info['display_name']} (ID: {user_info['id']})")
        
        # Test checking if live
        is_live = await client.is_user_live()
        print(f"Currently live: {is_live}")
        
        # If live, get stream info
        if is_live:
            stream_info = await client.get_stream_info()
            print(f"Stream: '{stream_info['title']}' playing {stream_info['game_name']}")
        
        await close_twitch_client()
        print("Twitch API test completed!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_twitch())