# Content Automation

Automated Twitch-to-YouTube content processing system.

## Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and fill in your credentials
3. Install dependencies: `pip install -r requirements.txt`
4. Run initial setup: `python scripts/setup_config.py`
5. Start the stream detector: `python main.py`

## Features

- [x] Database schema setup
- [x] Twitch API integration
- [x] Stream detection
- [x] Video processing
- [x] YouTube uploads
- [x] Optimal scheduling

## Development

This project uses:
- Python 3.8+
- Supabase for database
- Twitch Helix API
- YouTube Data API v3
- DigitalOcean Spaces for storage
