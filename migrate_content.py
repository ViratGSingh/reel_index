"""
Migration script to find reels that have CDN URLs but are missing transcription data.

Finds reels where:
- thumbnail_url is a CDN URL (not Instagram domain)
- transcription is missing or empty

Note: Framewatch is disabled - only transcription is processed.
"""

from pathlib import Path
from dotenv import load_dotenv
import os
import firebase_admin
from firebase_admin import credentials, firestore
from index import ReelIndexer
from transcription import TranscriptionService
# from framewatch import FrameWatchService  # Framewatch disabled
import time

# Load .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)
print(f"✓ Loaded .env file from: {env_path}")

# Initialize Firebase (only if not already initialized)
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

# CDN domain to identify CDN URLs
CDN_DOMAIN = "cdn.drissea.com"

# Instagram domains that indicate non-CDN URLs
INSTAGRAM_DOMAINS = [
    "instagram.com",
    "cdninstagram.com",
    "fbcdn.net",
    "scontent",
]


def is_cdn_url(url: str) -> bool:
    """Check if a URL is a CDN URL (not Instagram domain)."""
    if not url:
        return False
    url_lower = url.lower()
    
    # Check if it's our CDN
    if CDN_DOMAIN in url_lower:
        return True
    
    # Check if it's NOT an Instagram URL (could be other CDNs)
    return not any(domain in url_lower for domain in INSTAGRAM_DOMAINS)


def is_empty_or_missing(value) -> bool:
    """Check if a value is None, empty string, or missing."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def find_reels_missing_content():
    """
    Find reels that have CDN thumbnail URL but are missing transcription.
    
    Returns reels where:
    - thumbnail_url is a CDN URL
    - transcription is missing/empty
    
    If transcription exists, the reel is skipped.
    """
    print("🔍 Scanning ig_reels collection for reels missing transcription...")
    
    reels_ref = db.collection("ig_reels")
    all_docs = reels_ref.stream()
    
    reels_to_process = []
    total_scanned = 0
    skipped_no_cdn = 0
    skipped_has_content = 0
    
    for doc in all_docs:
        total_scanned += 1
        reel = doc.to_dict()
        reel_id = doc.id
        
        # video_url = reel.get("video_url", "") or ""  # Video upload commented out
        thumbnail_url = reel.get("thumbnail_url", "") or ""
        transcription = reel.get("transcription")
        # framewatch = reel.get("framewatch")  # Framewatch disabled
        
        # Check if thumbnail URL is CDN URL (video upload commented out)
        # has_cdn_video = is_cdn_url(video_url)
        has_cdn_thumbnail = is_cdn_url(thumbnail_url)
        
        # Only check thumbnail, video upload commented out
        if not has_cdn_thumbnail:
        # if not has_cdn_video or not has_cdn_thumbnail:
            skipped_no_cdn += 1
            continue
        
        # Check if transcription exists (framewatch disabled)
        has_transcription = not is_empty_or_missing(transcription)
        
        # Skip if transcription exists
        if has_transcription:
            skipped_has_content += 1
            continue
        
        # This reel needs processing
        reel["_doc_id"] = reel_id
        reels_to_process.append(reel)
        
        if total_scanned % 100 == 0:
            print(f"  Scanned {total_scanned} documents...")
    
    print(f"\n✅ Scan complete:")
    print(f"   📊 Total scanned: {total_scanned}")
    print(f"   ⏭ Skipped (no CDN URLs): {skipped_no_cdn}")
    print(f"   ⏭ Skipped (has transcription): {skipped_has_content}")
    print(f"   📋 Need processing: {len(reels_to_process)}")
    
    return reels_to_process


def process_reel_content(indexer: ReelIndexer, reel: dict, 
                         transcription_service: TranscriptionService) -> bool:
    """
    Process a single reel with transcription only (framewatch disabled).
    
    Returns True if processing was successful.
    """
    doc_id = reel.get("_doc_id")
    shortcode = reel.get("code") or reel.get("id") or doc_id
    video_url = reel.get("video_url")
    
    print(f"\n🎬 Processing reel: {shortcode}")
    
    if not video_url:
        print(f"  ⚠ No video URL, skipping")
        return False
    
    update_data = {}
    
    # Always use transcription (framewatch disabled)
    print(f"  🎤 Transcribing...")
    try:
        transcription = indexer._transcribe_reel(transcription_service, video_url)
        if transcription:
            update_data["transcription"] = transcription
            update_data["is_transcribed"] = True
            print(f"  ✅ Transcription complete ({len(transcription)} chars)")
        else:
            print(f"  ⚠ Transcription returned empty")
            return False
    except Exception as e:
        print(f"  ❌ Transcription failed: {e}")
        return False
    
    # Framewatch disabled - always set to empty/False
    update_data["framewatch"] = ""
    update_data["is_framewatched"] = False
    
    # Update Firestore
    if update_data:
        reel_ref = db.collection("ig_reels").document(doc_id)
        reel_ref.update(update_data)
        print(f"  ✅ Firestore updated: {doc_id}")
        
        # Also update in Upstash/index
        try:
            updated_reel = {**reel, **update_data}
            indexer.index_reel(updated_reel)
            print(f"  ✅ Re-indexed in Upstash")
        except Exception as e:
            print(f"  ⚠ Upstash indexing failed: {e}")
        
        return True
    
    return False


def run_content_migration(delay: float = 2.0, limit: int = None):
    """Run the content processing migration."""
    print("\n" + "=" * 60)
    print("Content Analysis Migration Script (Transcription Only)")
    print("=" * 60 + "\n")
    
    # Find reels that need processing
    reels_to_process = find_reels_missing_content()
    
    if not reels_to_process:
        print("\n✨ No reels need content processing!")
        return
    
    # Apply limit if specified
    if limit:
        reels_to_process = reels_to_process[:limit]
        print(f"\n📊 Processing first {limit} reels only")
    
    # Initialize services (framewatch disabled)
    print("\n🔧 Initializing services...")
    indexer = ReelIndexer()
    transcription_service = TranscriptionService()
    # framewatch_service = FrameWatchService()  # Framewatch disabled
    
    # Process each reel
    success_count = 0
    failed_count = 0
    total = len(reels_to_process)
    
    print(f"\n🚀 Starting transcription processing of {total} reels...\n")
    
    for i, reel in enumerate(reels_to_process, 1):
        print(f"\n[{i}/{total}] ", end="")
        
        try:
            if process_reel_content(indexer, reel, transcription_service):
                success_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"  ❌ Error processing reel: {e}")
            failed_count += 1
        
        # Add delay between processing
        if i < total:
            time.sleep(delay)
    
    # Summary
    print("\n" + "=" * 60)
    print("Migration Complete!")
    print("=" * 60)
    print(f"✅ Successfully processed: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📊 Total processed: {total}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate reels - add missing transcription")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between processing in seconds")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of reels to process")
    parser.add_argument("--dry-run", action="store_true", help="Only scan, don't process")
    
    args = parser.parse_args()
    
    if args.dry_run:
        find_reels_missing_content()
    else:
        run_content_migration(delay=args.delay, limit=args.limit)
