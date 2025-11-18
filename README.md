# Content Automation - Twitch to YouTube

**Status:** ✅ Running in production (1+ month stable after initial debugging)  
**Type:** Single-user MVP / Proof of Concept  
**Next Version:** [EOSVA](https://github.com/jiggy2204/eosva) (multi-user, multi-platform)

---

## 🎯 The Problem

Content creators spend hours manually managing their content:
- Downloading VODs from Twitch
- Writing descriptions and finding optimal tags
- Uploading to YouTube
- Formatting metadata for discoverability

This is tedious, time-consuming work that should be automated.

---

## ✨ The Solution

Automated pipeline that:
1. **Monitors** Twitch for new VODs (webhook-based)
2. **Downloads** videos when streams end
3. **Generates** metadata automatically from game databases
4. **Optimizes** descriptions with game-specific hashtags and trending tags
5. **Uploads** to YouTube with zero manual intervention

**Current Status:** Running in production for 1+ month with no manual intervention after initial stabilization phase.

---

## 🏗️ Architecture

```
Twitch Stream Ends
    ↓
Webhook Triggers
    ↓
Celery Task: Download VOD
    ↓
Query Game Databases (IGDB/RAWG)
    ↓
Generate Description + Tags
    ↓
Celery Task: Upload to YouTube
    ↓
Cleanup Downloaded Files
```

### Tech Stack

**Backend:**
- Python 3.x
- Django (REST API + Admin)
- Celery + Redis (async task processing)

**APIs:**
- Twitch API (OAuth2 + Webhooks)
- YouTube Data API v3
- IGDB (game metadata)
- RAWG (game database)

**Infrastructure:**
- DigitalOcean (hosting)
- DigitalOcean Spaces (temporary storage)

---

## 🔧 Key Technical Challenges Solved

### 1. Webhook Reliability
**Problem:** Twitch webhooks can arrive out of order or be delayed  
**Solution:** Implemented retry logic with exponential backoff, state tracking in database

### 2. Large File Processing
**Problem:** VODs can be 10-22GB, processing takes 30min-2hrs  
**Solution:** Celery workers handle async processing, allowing concurrent uploads without blocking

### 3. API Rate Limiting
**Problem:** YouTube API has strict quota limits (10,000 units/day)  
**Solution:** Request throttling, retry logic, and efficient batch operations

### 4. Storage Management
**Problem:** Storing 20GB+ files quickly becomes expensive  
**Solution:** Download → Process → Upload → Delete pipeline with automatic cleanup

### 5. Metadata Quality
**Problem:** Generic descriptions don't perform well on YouTube  
**Solution:** Game database integration provides game-specific hashtags and trending tags automatically

---

## 📊 What It Does (Step by Step)

1. **Twitch Stream Detection**
   - Webhook fires when stream ends
   - System verifies VOD is available
   - Queues download task

2. **VOD Download**
   - Downloads from Twitch to temporary storage
   - Validates file integrity
   - Logs download status

3. **Metadata Generation**
   - Extracts game name from stream data
   - Queries IGDB/RAWG for game details
   - Generates description using template system
   - Adds game-specific hashtags
   - Includes top 3 trending tags for that game

4. **YouTube Upload**
   - Uploads video with generated metadata
   - Sets visibility, category, etc.
   - Handles rate limiting gracefully

5. **Cleanup**
   - Deletes temporary files after successful upload
   - Updates database status
   - Logs completion

---

## 🚀 The Journey (Lessons Learned)

### Initial Deployment (October 2024)
- Deployed MVP, immediately hit production issues
- Webhooks arriving in wrong order
- Videos timing out during processing
- Rate limits hitting unexpectedly

### Debugging Phase (Weeks 2-3)
- Added comprehensive error logging
- Implemented retry logic for all external API calls
- Optimized video processing pipeline
- Added monitoring for task queue health

### Stable Production (November 2024 - Present)
- 1+ month running with zero manual intervention
- All VODs processed automatically
- No failed uploads
- System self-recovers from transient errors

**Key Insight:** The difference between "works on my laptop" and "works in production" is 90% error handling and edge case management.

---

## 💡 Why This Architecture?

### Django
- **Pro:** Excellent ORM, built-in admin panel, robust ecosystem
- **Con:** Not the fastest for webhooks (but good enough for this use case)
- **Decision:** Use Django for database, task management, and admin interface

### Celery + Redis
- **Pro:** Reliable async task processing, built-in retry logic
- **Con:** Requires Redis infrastructure
- **Decision:** Video processing is too slow for synchronous handling, Celery is battle-tested

### Webhook-Based (vs Polling)
- **Pro:** Instant detection when stream ends
- **Con:** More complex (need to handle retries, ordering)
- **Decision:** Webhooks are more responsive and scale better than polling

---

## 📈 Metrics (Informal - No Dashboard Yet)

- **Uptime:** 1+ month stable, zero downtime
- **Success Rate:** ~100% (no failed uploads after stabilization)
- **Processing Time:** 30min - 2hrs depending on VOD length
- **Storage Costs:** ~$5/month (DigitalOcean Spaces)
- **Manual Intervention:** Zero (fully automated)

---

## 🎯 What's Next

This MVP proved the concept works. Key learnings:

1. ✅ **Automation is viable** - VOD → YouTube pipeline can be fully automated
2. ✅ **Metadata matters** - Auto-generated tags/hashtags work well
3. ✅ **Reliability is hard** - Most dev time went to error handling, not features
4. ⚠️ **Single-user limits** - Hard-coded for one user, not scalable

### Next Steps
Building **[EOSVA](https://github.com/jiggy2204/eosva)** - the multi-user, multi-platform version with:
- Multi-tenant architecture (serve 50+ users)
- Cross-platform support (YouTube, TikTok, Instagram, Bluesky, Facebook)
- React dashboard for monitoring
- Automatic clip generation
- Scheduled posting per platform
- User-configurable templates

---

## 🛠️ Setup (For Reference)

**Note:** This is a single-user MVP. You'd need to modify configuration for your own use.

### Prerequisites
- Python 3.8+
- Redis
- DigitalOcean account (or alternative hosting)
- API keys: Twitch, YouTube, IGDB, RAWG

### Environment Variables
```bash
# Twitch
TWITCH_CLIENT_ID=your_client_id
TWITCH_CLIENT_SECRET=your_secret
TWITCH_WEBHOOK_SECRET=your_webhook_secret

# YouTube
YOUTUBE_CLIENT_ID=your_youtube_id
YOUTUBE_CLIENT_SECRET=your_youtube_secret

# Game Databases
IGDB_CLIENT_ID=your_igdb_id
IGDB_CLIENT_SECRET=your_igdb_secret
RAWG_API_KEY=your_rawg_key

# Infrastructure
DATABASE_URL=your_db_url
REDIS_URL=your_redis_url
DO_SPACES_KEY=your_spaces_key
DO_SPACES_SECRET=your_spaces_secret
```

### Installation
```bash
# Clone repo
git clone https://github.com/jiggy2204/contentautomation
cd contentautomation

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run migrations
python manage.py migrate

# Start Celery worker
celery -A contentautomation worker -l info

# Start Celery beat (scheduler)
celery -A contentautomation beat -l info

# Start Django server
python manage.py runserver
```

---

## 📝 Project Structure

```
contentautomation/
├── src/
│   ├── twitch_handler.py      # Twitch API + webhook handling
│   ├── youtube_handler.py     # YouTube upload logic
│   ├── downloader.py           # VOD download management
│   ├── game_metadata_handler.py # IGDB/RAWG integration
│   └── supabase_client.py      # Database operations
├── temp/                        # Temporary VOD storage
├── manage.py
└── requirements.txt
```

---

## 🤔 Why Open Source This?

**Three reasons:**

1. **Portfolio proof** - Shows I can ship production systems
2. **Learning in public** - Share lessons learned about production debugging
3. **Foundation for EOSVA** - This MVP validated the approach

**What you won't find here:**
- My API keys (obviously)
- User data
- A polished, production-ready product

**What you will find:**
- Real code that works
- Honest documentation
- Architecture decisions explained

---

## 📄 License

MIT License - See [LICENSE](LICENSE) file

---

## 🔗 Related Projects

- **[EOSVA](https://github.com/jiggy2204/eosva)** - Multi-user, multi-platform version (in development)

---

## 👤 Author

**Jennifer Ignacio**  
Learning Platform Engineer | Full-Stack Developer  
[LinkedIn](https://www.linkedin.com/in/jennifer-ignacio-2204/)

---

## 💭 Reflections

Building this taught me that:
- **Shipping > Planning** - I learned more in 1 month of production than 6 months of planning would teach
- **Error handling is 90% of production code** - The happy path is easy, edge cases are hard
- **Start small, prove it works** - This single-user MVP validated the concept before I invested in the full multi-user system

If you're building something similar, feel free to reach out. Happy to share lessons learned.

---

**Built with ☕ and spousal support!**
