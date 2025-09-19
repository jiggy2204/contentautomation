"""
Configuration management for Content Automation system.
Loads environment variables and provides easy access to settings.
"""

import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class that loads and validates environment variables."""
    
    def __init__(self):
        self._validate_required_vars()
    
    # Supabase Configuration
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    
    # Twitch API Configuration
    TWITCH_CLIENT_ID: str = os.getenv("TWITCH_CLIENT_ID", "")
    TWITCH_CLIENT_SECRET: str = os.getenv("TWITCH_CLIENT_SECRET", "")
    TWITCH_USER_LOGIN: str = os.getenv("TWITCH_USER_LOGIN", "")
    
    # DigitalOcean Spaces Configuration
    DO_SPACES_KEY: str = os.getenv("DO_SPACES_KEY", "")
    DO_SPACES_SECRET: str = os.getenv("DO_SPACES_SECRET", "")
    DO_SPACES_ENDPOINT: str = os.getenv("DO_SPACES_ENDPOINT", "")
    DO_SPACES_BUCKET: str = os.getenv("DO_SPACES_BUCKET", "")
    
    # Application Settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))
    
    def _validate_required_vars(self):
        """Validate that all required environment variables are set."""
        required_vars = [
            ("SUPABASE_URL", self.SUPABASE_URL),
            ("SUPABASE_KEY", self.SUPABASE_KEY),
            ("TWITCH_CLIENT_ID", self.TWITCH_CLIENT_ID),
            ("TWITCH_CLIENT_SECRET", self.TWITCH_CLIENT_SECRET),
            ("TWITCH_USER_LOGIN", self.TWITCH_USER_LOGIN),
        ]
        
        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    @property
    def is_production(self) -> bool:
        """Check if we're running in production mode."""
        return os.getenv("ENVIRONMENT", "development").lower() == "production"
    
    @property
    def debug_mode(self) -> bool:
        """Check if debug mode is enabled."""
        return self.LOG_LEVEL.upper() == "DEBUG"
    
    def get_db_config(self) -> dict:
        """Get database configuration as a dictionary."""
        return {
            "url": self.SUPABASE_URL,
            "key": self.SUPABASE_KEY
        }
    
    def get_twitch_config(self) -> dict:
        """Get Twitch API configuration as a dictionary."""
        return {
            "client_id": self.TWITCH_CLIENT_ID,
            "client_secret": self.TWITCH_CLIENT_SECRET,
            "user_login": self.TWITCH_USER_LOGIN
        }

# Create global config instance
config = Config()

# Convenience function for getting config
def get_config() -> Config:
    """Get the global configuration instance."""
    return config