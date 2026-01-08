"""
Migration script to find and re-upload reels that still have Instagram domain URLs
instead of CDN URLs for video_url and thumbnail_url.
"""

from pathlib import Path
from dotenv import load_dotenv
import os
import firebase_admin
from firebase_admin import credentials, firestore
from extract import InstagramReelsScraper
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

# Instagram domains to look for
INSTAGRAM_DOMAINS = [
    "instagram.com",
    "cdninstagram.com",
    "fbcdn.net",
    "scontent",
]


def has_instagram_url(url: str) -> bool:
    """Check if a URL contains an Instagram domain."""
    if not url:
        return False
    url_lower = url.lower()
    return any(domain in url_lower for domain in INSTAGRAM_DOMAINS)


def find_reels_with_instagram_urls():
    """Find all reels that have Instagram domain URLs in thumbnail_url."""
    print("🔍 Scanning ig_reels collection for Instagram domain URLs...")
    
    reels_ref = db.collection("ig_reels")
    all_docs = reels_ref.stream()
    
    reels_to_migrate = []
    total_scanned = 0
    
    for doc in all_docs:
        total_scanned += 1
        reel = doc.to_dict()
        reel_id = doc.id
        
        # video_url = reel.get("video_url", "") or ""  # Video upload commented out
        thumbnail_url = reel.get("thumbnail_url", "") or ""
        
        # Only check thumbnail URL, video upload commented out
        needs_migration = has_instagram_url(thumbnail_url)
        # needs_migration = has_instagram_url(video_url) or has_instagram_url(thumbnail_url)
        
        if needs_migration:
            reel["_doc_id"] = reel_id
            reels_to_migrate.append(reel)
            
        if total_scanned % 100 == 0:
            print(f"  Scanned {total_scanned} documents...")
    
    print(f"✅ Scanned {total_scanned} documents")
    print(f"📋 Found {len(reels_to_migrate)} reels with Instagram URLs that need migration")
    
    return reels_to_migrate


def migrate_reel_urls(scraper: InstagramReelsScraper, reel: dict) -> bool:
    """Download and upload a single reel's media to R2 and update Firestore.
    
    Fetches fresh URLs from Instagram API using get_reel_info, then downloads
    and uploads to R2.
    """
    doc_id = reel.get("_doc_id")
    shortcode = reel.get("code") or reel.get("id") or doc_id
    
    print(f"\n📤 Migrating reel: {shortcode}")
    
    # Fetch fresh URLs from Instagram API using shortcode
    print(f"  ⏳ Fetching fresh URLs from Instagram API...")
    reel_info = scraper.get_reel_info(shortcode)
    
    if not reel_info:
        print(f"  ❌ Could not fetch reel info from Instagram API")
        return False
    
    # Only require thumbnail URL, video upload commented out
    # if not reel_info.get("video_url") and not reel_info.get("thumbnail_url"):
    if not reel_info.get("thumbnail_url"):
        print(f"  ❌ No thumbnail URL returned from Instagram API")
        return False
    
    print(f"  ✓ Got fresh URLs from Instagram API")
    
    # Use the fresh reel_info data for upload - video upload disabled
    cdn_urls = scraper.download_and_upload_reel(reel_info, upload_video=False, upload_thumbnail=True)
    
    # Prepare update data
    update_data = {}
    
    # Video upload commented out
    # if cdn_urls.get("video_cdn_url"):
    #     update_data["video_url"] = cdn_urls["video_cdn_url"]
    #     print(f"  ✓ Video URL updated: {cdn_urls['video_cdn_url']}")
    
    if cdn_urls.get("thumbnail_cdn_url"):
        update_data["thumbnail_url"] = cdn_urls["thumbnail_cdn_url"]
        print(f"  ✓ Thumbnail URL updated: {cdn_urls['thumbnail_cdn_url']}")
    
    # Update Firestore if we have new URLs
    if update_data:
        reel_ref = db.collection("ig_reels").document(doc_id)
        reel_ref.update(update_data)
        print(f"  ✅ Firestore document updated: {doc_id}")
        return True
    else:
        print(f"  ⚠ No CDN URLs obtained, skipping Firestore update")
        return False


def run_migration(delay: float = 1.0, limit: int = None):
    """Run the full migration process."""
    print("\n" + "=" * 60)
    print("Instagram URL to CDN Migration Script")
    print("=" * 60 + "\n")
    
    # Find reels that need migration
    reels_to_migrate = find_reels_with_instagram_urls()
    
    if not reels_to_migrate:
        print("\n✨ No reels need migration. All URLs are already using CDN!")
        return
    
    # Apply limit if specified
    if limit:
        reels_to_migrate = reels_to_migrate[:limit]
        print(f"📊 Processing first {limit} reels only")
    
    # Initialize scraper
    print("\n🔧 Initializing Instagram scraper...")
    scraper = InstagramReelsScraper.from_env()
    
    if not scraper.r2_client:
        print("❌ R2 client not configured. Cannot proceed with migration.")
        print("   Please ensure R2 credentials are set in .env file.")
        return
    
    # Process each reel
    success_count = 0
    failed_count = 0
    total = len(reels_to_migrate)
    
    print(f"\n🚀 Starting migration of {total} reels...\n")
    
    for i, reel in enumerate(reels_to_migrate, 1):
        print(f"\n[{i}/{total}] ", end="")
        
        try:
            if migrate_reel_urls(scraper, reel):
                success_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"  ❌ Error migrating reel: {e}")
            failed_count += 1
        
        # Add delay between uploads
        if i < total:
            time.sleep(delay)
    
    # Summary
    print("\n" + "=" * 60)
    print("Migration Complete!")
    print("=" * 60)
    print(f"✅ Successfully migrated: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📊 Total processed: {total}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate Instagram URLs to CDN URLs")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between uploads in seconds")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of reels to process")
    parser.add_argument("--dry-run", action="store_true", help="Only scan, don't migrate")
    
    args = parser.parse_args()
    
    if args.dry_run:
        find_reels_with_instagram_urls()
    else:
        run_migration(delay=args.delay, limit=args.limit)
