from openai import OpenAI
from upstash_vector import Index, Vector
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone
import firebase_admin
import os
from firebase_admin import credentials, firestore
from transcription import TranscriptionService
# from framewatch import FrameWatchService  # Framewatch disabled

class ReelIndexer:
    def __init__(self):
        # Load .env file automatically
        try:
            # Load .env from the same directory as this script
            env_path = Path(__file__).parent / '.env'
            load_dotenv(dotenv_path=env_path)
            print(f"✓ Loaded .env file from: {env_path}")
        except ImportError:
            print("⚠ python-dotenv not installed. Install with: pip install python-dotenv")
            print("  Falling back to system environment variables")
        except Exception as e:
            print(f"⚠ Could not load .env file: {e}")
            print("  Falling back to system environment variables")

        # Initialize OpenAI client
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Initialize Upstash Vector
        self.index = Index(
            url=os.getenv("UPSTASH_VECTOR_REST_URL"),
            token=os.getenv("UPSTASH_VECTOR_REST_TOKEN")
        )
        self.creator_data = {}

        # Initialize Firebase (only if not already initialized)
        if not firebase_admin._apps:
            cred = credentials.Certificate("serviceAccountKey.json")
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def create_embedding(self, text: str):
        response = self.openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return response.data[0].embedding

    def index_reel(self, reel: dict):
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

        def to_iso(dt):
            if dt is None:
                return None
            if isinstance(dt, datetime):
                return dt.isoformat()
            try:
                # Firestore timestamp has .to_datetime() method
                return dt.to_datetime().isoformat()
            except Exception:
                return None

        created_at = to_iso(reel.get("created_at")) or datetime.utcnow().isoformat()
        updated_at = to_iso(reel.get("updated_at")) or datetime.utcnow().isoformat()

        reel_id = reel.get("code") or reel.get("id") or reel.get("reel_id") or ""

        # Upsert to Upstash Vector with comprehensive metadata
        self.index.upsert(
            vectors=[
                Vector(
                    id=str(reel_id),
                    data=combined_text,
                    metadata={
                        "audio_type": reel.get("audio_type", "") or ("original" if reel.get("is_original_audio") else "music"),
                        "caption": caption,
                        "code": reel.get("code", "") or "",
                        "collaborator_count": reel.get("collaborator_count", 0) or len(collaborators),
                        "collaborators": collaborators,
                        "comment_count": reel.get("comment_count", 0) or 0,
                        "created_at": created_at,
                        "framewatch": "",  # Framewatch disabled - always empty
                        "full_name": reel.get("full_name") or self.creator_data.get("full_name"),
                        "has_collaborators": reel.get("has_collaborators", False) or len(collaborators) > 0,
                        "id": reel.get("id", "") or "",
                        "is_framewatched": False,  # Framewatch disabled
                        "is_original_audio": reel.get("is_original_audio", False) or False,
                        "is_transcribed": reel.get("is_transcribed", False) or False,
                        "like_count": reel.get("like_count", 0) or 0,
                        "permalink": reel.get("permalink", "") or "",
                        "play_count": reel.get("play_count", 0) or reel.get("view_count", 0) or 0,
                        "profile_pic_url": reel.get("profile_pic_url") or self.creator_data.get("profile_pic_url"),
                        "taken_at": reel.get("taken_at"),
                        "thumbnail_url": reel.get("thumbnail_url", "") or "",
                        "transcription": transcription,
                        "updated_at": updated_at,
                        "user_id": reel.get("user_id", "") or "",
                        "username": reel.get("username") or self.creator_data.get("username"),
                        "video_url": "",  # Video URL disabled - always empty
                        "view_count": reel.get("view_count", 0) or reel.get("play_count", 0) or 0,
                    }
                )
            ]
        )
        print(f"📊 Indexed reel {reel_id} in Upstash")

    def fetch_creator_data(self, user_id):
        doc_ref = self.db.collection("ig_creators").document(user_id)
        doc = doc_ref.get()
        if doc.exists:
            self.creator_data = doc.to_dict()
        else:
            self.creator_data = {}

    def get_reels_by_user(self, user_id):
        reels_ref = self.db.collection("ig_reels")
        query = reels_ref.where("user_id", "==", user_id)
        docs = query.stream()
        return [doc.to_dict() for doc in docs]

    def update_reel_data(self, reel_id, updated_data):
        reel_ref = self.db.collection("ig_reels").document(reel_id)
        reel_ref.set(updated_data, merge=True)
        print(f"✅ Updated reel with ID {reel_id} in Firestore")

    def update_new_reels_for_user(self, user_id, scraper, delay: float = 2.0):
        """
        Fetch only new reels for a user by stopping when an existing reel is found.
        
        Args:
            user_id: Instagram user ID
            scraper: InstagramReelsScraper instance to fetch reels
            delay: Delay between API requests in seconds (default 2.0)
            
        Returns:
            List of new reel dictionaries that were saved
        """
        import time
        
        new_reels = []
        max_id = None
        page_num = 1
        found_existing = False
        
        print(f"🔍 Checking for new reels for user {user_id}...")
        
        while not found_existing:
            print(f"Fetching page {page_num}...")
            
            result = scraper.get_reels_clips_api(user_id, page_size=50, max_id=max_id)
            
            if not result or not result['items']:
                print("No more reels to fetch")
                break
            
            for reel in result['items']:
                reel_id = reel.get('code') or reel.get('id')
                
                # Check if this reel already exists in Firestore
                reel_ref = self.db.collection("ig_reels").document(str(reel_id))
                if reel_ref.get().exists:
                    print(f"✋ Found existing reel {reel_id}, stopping fetch")
                    found_existing = True
                    break
                
                # This is a new reel, add it to our list
                new_reels.append(reel)
                print(f"  ✨ Found new reel: {reel_id}")
            
            if found_existing:
                break
                
            print(f"  Processed {len(result['items'])} reels (new so far: {len(new_reels)})")
            
            # Check if there are more pages
            if not result['has_more']:
                print("No more pages available")
                break
            
            # Get next page cursor
            max_id = result['next_max_id']
            if not max_id:
                print("No pagination cursor found")
                break
            
            page_num += 1
            time.sleep(delay)
        
        print(f"✅ Found {len(new_reels)} new reels for user {user_id}")
        return new_reels

    def update_all_reels_for_user(self, user_id):
        reels = self.get_reels_by_user(user_id)
        additional_data = {"profile_pic_url": self.creator_data.get("profile_pic_url", "") or ""}
        def to_iso_taken_at(value):
            print(value)
            if value is None:
                return None
            if isinstance(value, str):
                try:
                    print("Parsing string date:", datetime.fromisoformat(value))
                    datetime.fromisoformat(value).astimezone(timezone.utc)
                    return value
                except Exception:
                    return None
            if isinstance(value, datetime):
                print("Parsing date:", value.isoformat().astimezone(timezone.utc))
                return value.isoformat().astimezone(timezone.utc)
            # Check for Firestore timestamp object
            if hasattr(value, "to_datetime") and callable(getattr(value, "to_datetime")):
                print("Parsing datetime:", value.to_datetime().isoformat())
                return value.to_datetime().isoformat()
            if isinstance(value, (int, float)):
                if value > 1e10:
                    dt = datetime.fromtimestamp(value / 1000)
                else:
                    dt = datetime.fromtimestamp(value)
                print("Parsing iso datetime:", dt.astimezone(timezone.utc))
                return dt.astimezone(timezone.utc)
            return None

        for reel in reels:
            taken_at_iso = to_iso_taken_at(reel.get("taken_at"))
            updated_reel = {**reel}
            if taken_at_iso:
                updated_reel["taken_at"] = taken_at_iso
            self.update_reel_data(reel["id"], updated_reel)
            print(f"🔄 Applied update function to reel ID {reel['id']}")

    def process_reel_content(self, reels: list, transcription_service: TranscriptionService, framewatch_service=None):
        """
        Process reels with transcription only (framewatch disabled).
        
        Args:
            reels: List of reel dictionaries
            transcription_service: TranscriptionService instance
            framewatch_service: Deprecated - not used (framewatch disabled)
            
        Returns:
            List of reels with transcription data added
        """
        print(f"\n🎬 Processing content for {len(reels)} reels (transcription only, framewatch disabled)...")
        
        for i, reel in enumerate(reels, 1):
            reel_id = reel.get('code') or reel.get('id')
            video_url = reel.get('video_url')
            
            if not video_url:
                print(f"  [{i}/{len(reels)}] ⚠ Skipping {reel_id}, no video URL")
                continue
            
            print(f"  [{i}/{len(reels)}] Processing {reel_id}...")
            
            # Always use transcription (framewatch disabled)
            print(f"    🎤 Transcribing...")
            try:
                transcription = self._transcribe_reel(transcription_service, video_url)
                if transcription:
                    reel['transcription'] = transcription
                    reel['is_transcribed'] = True
                    print(f"    ✅ Transcription complete")
            except Exception as e:
                print(f"    ❌ Transcription failed: {e}")
                reel['is_transcribed'] = False
            
            # Framewatch disabled - always set to empty/False
            reel['framewatch'] = ""
            reel['is_framewatched'] = False
        
        print(f"✅ Content processing complete\n")
        return reels
    
    def _transcribe_reel(self, transcription_service: TranscriptionService, video_url: str) -> str:
        """Transcribe a single reel's video."""
        import requests
        try:
            video_bytes = requests.get(video_url).content
            transcription = transcription_service.client.audio.transcriptions.create(
                model="whisper-large-v3",
                file=("input.mp4", video_bytes, "video/mp4"),
                language="en",
            )
            return transcription.text
        except Exception as e:
            print(f"      Error: {e}")
            return None
    
    # Framewatch disabled - method commented out
    # def _analyze_reel_frames(self, framewatch_service, video_url: str) -> str:
    #     """Analyze frames from a single reel's video."""
    #     import requests
    #     import cv2
    #     import numpy as np
    #     import base64
    #     import tempfile
    #     
    #     try:
    #         video_bytes = requests.get(video_url).content
    #         
    #         with tempfile.NamedTemporaryFile(suffix=".mp4") as tmp:
    #             tmp.write(video_bytes)
    #             tmp.flush()
    #             cap = cv2.VideoCapture(tmp.name)
    #             
    #             frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    #             if frame_count <= 0:
    #                 return None
    #             
    #             indices = np.linspace(0, frame_count - 1, 5, dtype=int)
    #             image_data_urls = []
    #             
    #             for idx in indices:
    #                 cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
    #                 ret, frame = cap.read()
    #                 if not ret:
    #                     continue
    #                 _, jpeg = cv2.imencode(".jpg", frame)
    #                 b64 = base64.b64encode(jpeg.tobytes()).decode("utf-8")
    #                 image_data_urls.append(f"data:image/jpeg;base64,{b64}")
    #             
    #             cap.release()
    #         
    #         if len(image_data_urls) == 0:
    #             return None
    #         
    #         messages = [
    #             {
    #                 "role": "user",
    #                 "content": [
    #                     {
    #                         "type": "text",
    #                         "text": (
    #                             "Analyze the following 5 images taken from different times in a video. "
    #                             "Write a concise description under 1000 characters of what is happening overall. "
    #                             "Do not use phrases like 'this video is of'; simply describe the events, actions, "
    #                             "objects, and context visible in the frames."
    #                         ),
    #                     }
    #                 ]
    #                 + [
    #                     {"type": "image_url", "image_url": {"url": url}}
    #                     for url in image_data_urls
    #                 ],
    #             }
    #         ]
    #         
    #         completion = framewatch_service.client.chat.completions.create(
    #             model="meta-llama/llama-4-maverick-17b-128e-instruct",
    #             messages=messages,
    #             temperature=0.8,
    #             max_completion_tokens=1024,
    #             top_p=1,
    #             stream=False,
    #         )
    #         
    #         return completion.choices[0].message.content.strip()
    #     except Exception as e:
    #         print(f"      Error: {e}")
    #         return None

    def sync_user_reels(self, username: str, scraper, delay: float = 2.0):
        """
        Sync reels for a user - full sync if new, incremental if existing.
        Includes transcription processing (framewatch disabled).
        
        Args:
            username: Instagram username
            scraper: InstagramReelsScraper instance to fetch reels
            delay: Delay between API requests in seconds (default 2.0)
            
        Returns:
            Tuple of (reels_list, is_new_user)
        """
        # Initialize services (framewatch disabled)
        transcription_service = TranscriptionService()
        # framewatch_service = FrameWatchService()  # Framewatch disabled
        
        # Get user ID from username and save/update creator data
        print(f"🔍 Looking up user: {username}")
        user_id = scraper.get_user_id(username)
        
        if not user_id:
            print(f"❌ Could not find user: {username}")
            return [], False
        
        # Fetch creator data for use in reel indexing
        self.fetch_creator_data(user_id)
        
        # Check if user already has reels in ig_reels collection
        creator_ref = self.db.collection("ig_creators").document(str(user_id))
        creator_doc = creator_ref.get()
        
        if creator_doc.exists and creator_doc.to_dict().get('status') != 'initial':
            # Existing user with reels - fetch only new reels
            print(f"👤 User {username} (ID: {user_id}) exists, fetching only new reels...")
            new_reels = self.update_new_reels_for_user(user_id, scraper, delay)
            
            if not new_reels:
                print(f"✅ No new reels found for user {username}")
                return new_reels, False
            
            # Download and upload to R2 (get CDN URLs)
            new_reels = scraper.download_and_upload_all_reels(new_reels)
            
            # Process transcription (framewatch disabled)
            new_reels = self.process_reel_content(new_reels, transcription_service)
            
            # Save new reels to Firestore
            for reel in new_reels:
                scraper.save_reel_to_firestore(reel, skip_if_exists=True)
            
            # Index new reels
            for reel in new_reels:
                self.index_reel(reel)
            
            print(f"✅ Synced {len(new_reels)} new reels for existing user {username}")
            return new_reels, False
        else:
            # New user or initial status - fetch all reels
            print(f"🆕 User {username} (ID: {user_id}) needs full sync, fetching all reels...")
            self.fetch_creator_data(user_id)
            
            # Fetch all reels
            all_reels = scraper.get_all_reels_clips_api(user_id, delay=delay)
            
            # Download and upload to R2 (get CDN URLs)
            all_reels = scraper.download_and_upload_all_reels(all_reels)
            
            # Process transcription (framewatch disabled)
            all_reels = self.process_reel_content(all_reels, transcription_service)
            
            # Save all reels to Firestore
            scraper.save_all_reels_to_firestore(all_reels, user_id=user_id, skip_if_exists=True)
            
            # Index all reels
            for reel in all_reels:
                self.index_reel(reel)
            
            # Update/create creator status (use set with merge to handle missing doc)
            creator_ref.set({"status": "indexed"}, merge=True)
            
            print(f"✅ Fully synced {len(all_reels)} reels for new user {user_id}")
            return all_reels, True


if __name__ == "__main__":
    from extract import InstagramReelsScraper
    
    username = "akkicooks"  # Replace with target username
    indexer = ReelIndexer()
    scraper = InstagramReelsScraper.from_env()
    
    # Smart sync - full for new users, incremental for existing
    reels, is_new = indexer.sync_user_reels(username, scraper)
    # reels = indexer.get_reels_by_user(user_id)
    # for reel in reels:
    #     indexer.index_reel(reel)
    # print(f"✅ Indexed {len(reels)} reels for user ID {user_id}")
    # if len(reels) > 0:
    #     creator_ref = indexer.db.collection("ig_creators").document(user_id)
    #     creator_ref.update({"status": "indexed"})
    #     print(f"✅ Updated status to 'indexed' for creator with user ID {user_id}")