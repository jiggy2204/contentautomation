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
Cron Job: morning.sh (runs daily, e.g., 9am)
    ↓
Check Twitch for VODs from previous day
    ↓
Download VOD with Streamlink
    ↓
Query Game Databases (IGDB/RAWG) for metadata
    ↓
Store metadata for later use

Cron Job: evening.sh (runs daily, e.g., 6pm)
    ↓
Generate YouTube description from stored metadata
    ↓
Upload to YouTube at optimal posting time
    ↓
Delete downloaded VOD files
    ↓
Clean up temporary storage

```

### Tech Stack

**Task Scheduling:**
- Cron (Linux built-in task scheduler)
- Bash shell scripts (morning.sh, evening.sh, cleanup.sh)

**Backend:**
- Python 3.x (VOD processing scripts)
- Supabase (PostgreSQL database)

**APIs:**
- Twitch API (OAuth2 + VOD retrieval)
- YouTube Data API v3
- IGDB (game metadata)
- RAWG (game database)

**Tools:**
- Streamlink (VOD downloading)
- yt-dlp (backup downloader)

**Infrastructure:**
- DigitalOcean Droplet (hosting)
- DigitalOcean Spaces (temporary storage)
- Supabase (PostgreSQL database)

---

## 🔧 Key Technical Challenges Solved

### 1. Webhook Reliability
**Problem:** Twitch webhooks can arrive out of order or be delayed  
**Solution:** Implemented retry logic with exponential backoff, state tracking in database

### 2. Large File Processing
**Problem:** VODs can be 10-22GB, processing takes 30min-2hrs  
**Solution:** Sequential processing with progress tracking. For single-user MVP, this is acceptable - only one stream to process at a time.

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

### Initial Deployment (October 2025)
- Deployed MVP, immediately hit production issues
- Webhooks arriving in wrong order
- Videos timing out during processing
- Rate limits hitting unexpectedly

### Debugging Phase (Weeks 2-3)
- Added comprehensive error logging
- Implemented retry logic for all external API calls
- Optimized video processing pipeline
- Added monitoring for task queue health

### Stable Production (November 2025 - Present)
- 1+ month running with zero manual intervention
- All VODs processed automatically
- No failed uploads
- System self-recovers from transient errors

---

## 💡 Why This Architecture?

### Cron + Shell Scripts (Not Celery, Not Python schedule)
- **Pro:** Built into Linux, zero infrastructure setup
- **Pro:** Dead simple to debug (just look at cron logs)
- **Pro:** Separate processes = easier to fix individual pieces
- **Pro:** Can run tasks at different times (morning download, evening upload)
- **Con:** Not suitable for multi-user systems with complex workflows
- **Decision:** For single-user MVP, use the simplest possible solution - cron is built-in and bulletproof

**Why three separate scripts?**
1. **morning.sh** - Downloads VODs from previous day + gets metadata (runs at 9am)
2. **evening.sh** - Generates description + uploads to YouTube (runs at 6pm, optimal posting time)
3. **cleanup.sh** - Deletes downloaded files after successful upload (saves storage costs)

**Why separate morning/evening jobs?**
- YouTube algorithm favors posts at specific times (6pm is optimal for viewership)
- Separating download from upload means if download fails, don't try to upload
- Easier to debug - if something breaks, you know which step failed

**Why NOT Celery/Redis for MVP?**
- Would need to install, configure, and monitor Redis
- Would need to set up Celery workers and beat scheduler
- Adds complexity for zero benefit when processing one stream
- Cron has been doing this job since 1975 - it's battle-tested

**Why NOT Python `schedule` library?**
- Requires keeping a Python process running 24/7
- If process crashes, tasks don't run until you restart it
- Cron is built into the OS - if server reboots, cron auto-starts

**When to upgrade to Celery?** (EOSVA multi-user version)
- Multiple users = need task queuing and priority management
- Need retry logic for failed uploads
- Need to scale horizontally (multiple worker machines)
- Worth the infrastructure complexity at that point

### Django
- **Pro:** Excellent ORM, built-in admin panel for managing data
- **Con:** Not needed for the shell scripts, but useful for database operations
- **Decision:** Use Django for database models and admin interface

### Streamlink for VOD Download
- **Pro:** Reliable, handles Twitch authentication automatically
- **Con:** Can be slower than direct download
- **Decision:** Reliability > speed for automated system

---

## 🎓 Architecture Philosophy

**This project demonstrates:**

1. **Use Built-In Tools** - Cron is already on every Linux server, why install more?
2. **Separate Concerns** - Three scripts for three jobs (download, upload, cleanup)
3. **Prove Concept** - Validated automation works before adding complexity
4. **Scale When Needed** - Planning Celery + Redis for multi-user (EOSVA)

**Result:** Working system in production with zero infrastructure overhead, clear path to scale.

---

## 🛠️ Setup (For Reference)

**Note:** This is a single-user MVP. You'd need to modify configuration for your own use.

### Prerequisites
- Python 3.8+
- Linux server with cron (DigitalOcean Droplet or similar)
- Supabase account (for database)
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
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
DO_SPACES_KEY=your_spaces_key
DO_SPACES_SECRET=your_spaces_secret
DO_SPACES_BUCKET=your_bucket_name
DO_SPACES_REGION=nyc3
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

# Set up environment variables (create .env file)
# See Environment Variables section above

# Set up cron jobs
crontab -e

# Add these lines (adjust times as needed):
# Download VODs overnight at 3am
0 3 * * * /path/to/contentautomation/morning.sh

# Upload to YouTube every evening at 6pm
0 18 * * * /path/to/contentautomation/evening.sh

# Run token check on IGDB for refresh
0 19 * * * /path/to/contentautomation/run_token_check.sh
```

**Cron Job Explanation:**
- `0 3 * * *` = Run at 3:00 AM every day
- `0 18 * * *` = Run at 6:00 PM every day
- `0 19 * * *` = Run at 7:00 PM every day

**What each script does:**
- **morning.sh** - Checks Twitch for yesterday's VODs, downloads them, fetches game metadata
- **evening.sh** - Generates YouTube description from metadata, uploads video at optimal time
- **run_token_check.sh** - Checks IGDB token to fetch game description and hashtags

**Note:** 
- Cron runs automatically in the background
- No need to keep any process running manually
- Logs are in `/var/log/syslog` (or wherever your cron logs go)
- To test: `bash morning.sh` manually before setting up cron

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

## 👤 Author

**Jennifer Ignacio**  
Learning Platform Engineer | Full-Stack Developer  
[LinkedIn](https://www.linkedin.com/in/jennifer-ignacio-2204/)

---

## 💭 Reflections

Building this taught me that:
- **Shipping > Planning** - I learned more in 1 month of production than 6 months of planning would teach
- **Error handling is 90% of production code** - The happy path is easy, edge cases are hard
- **Use what's already there** - Cron has been solving this problem since 1975. Why reinvent it?
- **Separate concerns** - Three scripts = three responsibilities = easier debugging
- **Don't over-engineer** - Single-user system doesn't need Redis, Celery, or message queues

Cron + shell scripts were a first for me, but I wanted to go with old reliable. It works and it's been running stable for over a month.

If you're building something similar, feel free to reach out. Happy to share lessons learned.

---

**Built with caffeine and spousal support**
