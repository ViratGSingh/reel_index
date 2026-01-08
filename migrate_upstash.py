"""
Migration script to index Firestore reels to Upstash Vector.
Only indexes reels that:
1. Are not already in Upstash
2. Have CDN URLs (drissea.com domain) for thumbnail
"""

from pathlib import Path
from dotenv import load_dotenv
import os
import firebase_admin
from firebase_admin import credentials, firestore
from upstash_vector import Index, Vector
from datetime import datetime

# Load .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)
print(f"✓ Loaded .env file from: {env_path}")

# Initialize Firebase (only if not already initialized)
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

# Initialize Upstash Vector
upstash_index = Index(
    url=os.getenv("UPSTASH_VECTOR_REST_URL"),
    token=os.getenv("UPSTASH_VECTOR_REST_TOKEN")
)


def has_cdn_url(url: str) -> bool:
    """Check if a URL is from CDN (drissea domain)."""
    if not url:
        return False
    return "drissea.com" in url.lower()


def is_indexed_in_upstash(reel_id: str) -> bool:
    """Check if a reel is already indexed in Upstash."""
    try:
        result = upstash_index.fetch([reel_id])
        return len(result) > 0 and result[0] is not None
    except Exception as e:
        print(f"⚠ Error checking Upstash for {reel_id}: {e}")
        return False


def to_iso(dt):
    """Convert datetime to ISO string."""
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime):
        return dt.isoformat()
    try:
        return dt.to_datetime().isoformat()
    except Exception:
        return None


def index_reel_to_upstash(reel: dict, reel_id: str) -> bool:
    """Index a single reel to Upstash Vector."""
    caption = reel.get("caption", "") or ""
    transcription = reel.get("transcription", "") or ""
    # framewatch = reel.get("framewatch", "") or ""  # Framewatch disabled
    audio_title = reel.get("audio_title", "") or ""
    
    # Extract collaborators info
    collaborators = reel.get("collaborators", []) or []
    collaborator_names = []
    for collaborator in collaborators:
        username = collaborator.get("username", "")
        full_name = collaborator.get("full_name", "")
        if username or full_name:
            collaborator_names.append(" ".join(filter(None, [username, full_name])))
    collaborators_text = " ".join(collaborator_names) if collaborator_names else ""
    
    # Combined text for semantic search (framewatch disabled)
    combined_text = " ".join(filter(None, [caption, transcription, audio_title, collaborators_text]))
    
    if not combined_text.strip():
        print(f"  ⚠ Skipping {reel_id}, no text content")
        return False
    
    created_at = to_iso(reel.get("created_at")) or datetime.utcnow().isoformat()
    updated_at = to_iso(reel.get("updated_at")) or datetime.utcnow().isoformat()
    
    # Handle taken_at - convert to timestamp if it's a datetime object
    taken_at = reel.get("taken_at")
    if taken_at is not None:
        if hasattr(taken_at, 'timestamp'):
            taken_at = int(taken_at.timestamp())
        elif hasattr(taken_at, 'to_datetime'):
            taken_at = int(taken_at.to_datetime().timestamp())
        elif not isinstance(taken_at, (int, float)):
            taken_at = None
    
    try:
        upstash_index.upsert(
            vectors=[
                Vector(
                    id=str(reel_id),
                    data=combined_text,
                    metadata={
                        "audio_type": reel.get("audio_type", "") or ("original" if reel.get("is_original_audio") else "music"),
                        "caption": caption,
                        "code": reel.get("code", "") or reel_id,
                        "collaborator_count": reel.get("collaborator_count", 0) or len(collaborators),
                        "collaborators": collaborators,
                        "comment_count": reel.get("comment_count", 0) or 0,
                        "created_at": created_at,
                        "framewatch": "",  # Framewatch disabled - always empty
                        "full_name": reel.get("full_name") or "",
                        "has_collaborators": reel.get("has_collaborators", False) or len(collaborators) > 0,
                        "id": reel.get("id", "") or "",
                        "is_framewatched": False,  # Framewatch disabled
                        "is_original_audio": reel.get("is_original_audio", False) or False,
                        "is_transcribed": reel.get("is_transcribed", False) or False,
                        "like_count": reel.get("like_count", 0) or 0,
                        "permalink": reel.get("permalink", "") or "",
                        "play_count": reel.get("play_count", 0) or reel.get("view_count", 0) or 0,
                        "profile_pic_url": reel.get("profile_pic_url") or "",
                        "taken_at": taken_at,
                        "thumbnail_url": reel.get("thumbnail_url", "") or "",
                        "transcription": transcription,
                        "updated_at": updated_at,
                        "user_id": reel.get("user_id", "") or "",
                        "username": reel.get("username") or "",
                        "video_url": "",  # Video URL disabled - always empty
                        "view_count": reel.get("view_count", 0) or reel.get("play_count", 0) or 0,
                    }
                )
            ]
        )
        return True
    except Exception as e:
        print(f"  ❌ Error indexing {reel_id}: {e}")
        return False


def find_reels_to_index():
    """Find all Firestore reels with CDN URLs that are not in Upstash."""
    print("🔍 Scanning ig_reels collection...")
    
    reels_ref = db.collection("ig_reels")
    all_docs = reels_ref.stream()
    
    reels_to_index = []
    total_scanned = 0
    skipped_no_cdn = 0
    skipped_already_indexed = 0
    
    for doc in all_docs:
        total_scanned += 1
        reel = doc.to_dict()
        reel_id = doc.id
        
        # video_url = reel.get("video_url", "") or ""  # Video upload commented out
        thumbnail_url = reel.get("thumbnail_url", "") or ""
        
        # Only check thumbnail URL for CDN, video upload commented out
        if not has_cdn_url(thumbnail_url):
        # if not (has_cdn_url(video_url) and has_cdn_url(thumbnail_url)):
            skipped_no_cdn += 1
            continue
        
        # Check if already indexed in Upstash
        if is_indexed_in_upstash(reel_id):
            skipped_already_indexed += 1
            continue
        
        reels_to_index.append((reel_id, reel))
        
        if total_scanned % 50 == 0:
            print(f"  Scanned {total_scanned} documents...")
    
    print(f"\n✅ Scan complete:")
    print(f"   Total scanned: {total_scanned}")
    print(f"   Skipped (no CDN URL): {skipped_no_cdn}")
    print(f"   Skipped (already indexed): {skipped_already_indexed}")
    print(f"   To index: {len(reels_to_index)}")
    
    return reels_to_index


def run_migration(limit: int = None):
    """Run the full migration process."""
    print("\n" + "=" * 60)
    print("Firestore to Upstash Vector Migration")
    print("=" * 60 + "\n")
    
    # Find reels to index
    reels_to_index = find_reels_to_index()
    
    if not reels_to_index:
        print("\n✨ No reels need indexing. All eligible reels are already in Upstash!")
        return
    
    # Apply limit if specified
    if limit:
        reels_to_index = reels_to_index[:limit]
        print(f"\n📊 Processing first {limit} reels only")
    
    # Index each reel
    success_count = 0
    failed_count = 0
    total = len(reels_to_index)
    
    print(f"\n🚀 Starting indexing of {total} reels...\n")
    
    for i, (reel_id, reel) in enumerate(reels_to_index, 1):
        print(f"[{i}/{total}] Indexing {reel_id}...")
        
        if index_reel_to_upstash(reel, reel_id):
            success_count += 1
            print(f"  ✅ Indexed successfully")
        else:
            failed_count += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("Migration Complete!")
    print("=" * 60)
    print(f"✅ Successfully indexed: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📊 Total processed: {total}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate Firestore reels to Upstash Vector")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of reels to process")
    parser.add_argument("--dry-run", action="store_true", help="Only scan, don't index")
    
    args = parser.parse_args()
    
    if args.dry_run:
        find_reels_to_index()
    else:
        run_migration(limit=args.limit)
