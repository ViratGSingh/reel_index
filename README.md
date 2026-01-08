# Instagram Reel Indexer

A Python-based pipeline for indexing Instagram Reels from creator accounts into a searchable vector database. This system powers **Drissea's** Instagram search feature, enabling semantic search across reel content.

## 🔄 System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              INDEXING PIPELINE                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
     ┌────────────────────────────────┼────────────────────────────────┐
     ▼                                ▼                                ▼
┌─────────────┐              ┌─────────────────┐              ┌──────────────┐
│  EXTRACT    │      →       │    PROCESS      │      →       │    INDEX     │
│  (extract.py)              │  (transcription)│              │  (index.py)  │
└─────────────┘              └─────────────────┘              └──────────────┘
     │                                │                                │
     ▼                                ▼                                ▼
• Fetch reels via              • Transcribe audio              • Create embeddings
  Instagram API                  using Groq Whisper              (OpenAI)
• Extract metadata             • Extract text content           • Store in Upstash
• Upload to R2 CDN                                                Vector DB
• Save to Firestore                                             • Save metadata

                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │     DRISSEA SEARCH API    │
                        │                           │
                        │  Query Upstash Vector DB  │
                        │  Return relevant reels    │
                        └───────────────────────────┘
```

## 📁 Project Structure

| File | Purpose |
|------|---------|
| `extract.py` | Instagram scraper - fetches reels, handles authentication, uploads to R2 |
| `index.py` | Main indexer - orchestrates the full pipeline, creates vector embeddings |
| `transcription.py` | Audio transcription service using Groq Whisper |
| `query.py` | Query utility for testing semantic search |
| `migrate*.py` | Migration scripts for updating existing data |

## 🔧 How It Works

### 1. **Extraction Phase** (`extract.py`)

The `InstagramReelsScraper` class handles all Instagram interactions:

- **User Lookup**: Given a username, fetches the user ID and profile data
- **Reel Fetching**: Uses Instagram's internal API (`/api/v1/clips/user/`) to paginate through all reels
- **Metadata Extraction**: Captures for each reel:
  - Caption text
  - View/like/comment counts
  - Audio information (original vs. music)
  - Collaborator data
  - Thumbnail and video URLs
- **CDN Upload**: Uploads thumbnails to Cloudflare R2 for fast, reliable delivery

### 2. **Transcription Phase** (`transcription.py`)

The `TranscriptionService` converts speech to text:

- Uses **Groq's Whisper Large V3** model for fast, accurate transcription
- Only transcribes reels with original audio (speech content)
- Stores transcription results in Firestore

### 3. **Indexing Phase** (`index.py`)

The `ReelIndexer` creates searchable embeddings:

- **Combined Text**: Merges caption + transcription + audio title + collaborator names
- **Embedding**: Uses **OpenAI's text-embedding-3-small** model
- **Vector Storage**: Indexes into **Upstash Vector DB** with full metadata

### 4. **Sync Modes**

The system supports two sync modes:

| Mode | Trigger | Behavior |
|------|---------|----------|
| **Full Sync** | New creator | Fetches all reels, processes everything |
| **Incremental Sync** | Existing creator | Stops at first known reel, processes only new content |

## 🚀 Usage

### Index a Creator's Reels

```python
from extract import InstagramReelsScraper
from index import ReelIndexer

# Initialize
scraper = InstagramReelsScraper.from_env()
indexer = ReelIndexer()

# Index all reels for a username
reels, is_new = indexer.sync_user_reels("username", scraper)
```

### Query the Index

```python
from query import query_reels

# Semantic search
query_reels("cooking pasta recipe", top_k=5)
```

## 🗄️ Data Storage

### Firestore Collections

| Collection | Purpose |
|------------|---------|
| `ig_creators` | Creator profiles, sync status |
| `ig_reels` | Individual reel metadata, transcriptions |

### Upstash Vector Index

Each reel is indexed as a vector with metadata:

```python
{
    "id": "reel_shortcode",
    "data": "combined searchable text",
    "metadata": {
        "caption": "...",
        "transcription": "...",
        "username": "creator_handle",
        "thumbnail_url": "https://cdn.../...",
        "permalink": "https://instagram.com/reel/...",
        "view_count": 12345,
        "like_count": 678,
        # ... more fields
    }
}
```

## 🔍 Drissea Integration

Drissea's search endpoint queries the Upstash Vector DB:

1. User enters a search query (e.g., "quick pasta recipes")
2. Query is converted to an embedding via OpenAI
3. Upstash returns top-k semantically similar reels
4. Results are displayed with thumbnail, caption, and link

**Key Features:**
- **Semantic Search**: Finds relevant content even without exact keyword matches
- **Multi-modal**: Searches across captions AND transcribed speech
- **Fast**: Vector similarity search returns results in milliseconds

## ⚙️ Environment Variables

Create a `.env` file with:

```env
# Instagram Authentication
SESSION_ID=your_instagram_session_id
CSRF_TOKEN=your_csrf_token  # Optional, auto-generated

# OpenAI (for embeddings)
OPENAI_API_KEY=sk-...

# Groq (for transcription)
GROQ_API_KEY=gsk_...

# Upstash Vector
UPSTASH_VECTOR_REST_URL=https://...
UPSTASH_VECTOR_REST_TOKEN=...

# Cloudflare R2
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_ENDPOINT_URL=https://...
R2_BUCKET_NAME=...
CDN_URL=https://cdn.yourdomain.com
```

## 📝 Notes

- **Rate Limiting**: The scraper includes delays between API calls to avoid Instagram blocks
- **Proxy Support**: Configure proxies in environment for safer scraping
- **Thumbnail Only**: Video uploads are disabled by default to save storage
