import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
from datetime import datetime
from dotenv import load_dotenv
from upstash_vector import Index, Vector
import json

# Load environment variables
load_dotenv()

UPSTASH_VECTOR_REST_URL = os.getenv("UPSTASH_VECTOR_REST_URL")
UPSTASH_VECTOR_REST_TOKEN = os.getenv("UPSTASH_VECTOR_REST_TOKEN")

def fetch_ig_reels():
    # Check if serviceAccountKey.json exists
    if not os.path.exists('serviceAccountKey.json'):
        print("Error: serviceAccountKey.json not found.")
        return

    if not UPSTASH_VECTOR_REST_URL or not UPSTASH_VECTOR_REST_TOKEN:
        print("Error: UPSTASH_VECTOR_REST_URL or UPSTASH_VECTOR_REST_TOKEN not found in .env")
        return

    # Initialize Firebase Admin SDK
    cred = credentials.Certificate('serviceAccountKey.json')
    try:
        firebase_admin.initialize_app(cred)
    except ValueError:
        # App already initialized
        pass

    db = firestore.client()
    
    # Initialize Upstash Index
    index = Index(url=UPSTASH_VECTOR_REST_URL, token=UPSTASH_VECTOR_REST_TOKEN)

    # Fetch documents from ig_reels collection
    collection_ref = db.collection('ig_reels')
    docs = collection_ref.stream()

    print("Fetching and processing documents from 'ig_reels' collection:")
    count = 0
    vectors = []

    def serialize_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, 'isoformat'): # Handle DatetimeWithNanoseconds
            return obj.isoformat()
        return obj

    for doc in docs:
        data = doc.to_dict()
        
        # Map fields based on user request
        processed_doc = {
            "audio_type": data.get("audio_type"),
            "caption": data.get("caption"),
            "code": data.get("code"),
            "collaborator_count": data.get("collaborator_count"),
            "collaborators": data.get("collaborators"),
            "comment_count": data.get("comment_count"),
            "created_at": serialize_datetime(data.get("created_at")),
            "framewatch": data.get("framewatch"),
            "full_name": data.get("full_name"),
            "has_collaborators": data.get("has_collaborators"),
            "id": data.get("id"),
            "is_framewatched": data.get("is_framewatched"),
            "is_original_audio": data.get("is_original_audio"),
            "is_transcribed": data.get("is_transcribed"),
            "like_count": data.get("like_count"),
            "permalink": data.get("permalink"),
            "play_count": data.get("play_count"),
            "profile_pic_url": data.get("profile_pic_url"),
            "taken_at": serialize_datetime(data.get("taken_at")),
            "thumbnail_url": data.get("thumbnail_url"),
            "updated_at": serialize_datetime(data.get("updated_at")),
            "user_id": data.get("user_id"),
            "username": data.get("username"),
            "video_url": "",  # Video URL disabled - always empty
            "view_count": data.get("view_count")
        }
        
        # Prepare vector for Upstash
        # Combine fields for embedding: caption, username, transcription (framewatch disabled)
        caption = processed_doc.get("caption") or ""
        username = processed_doc.get("username") or ""
        transcription = data.get("transcription") or "" 
        # framewatch = processed_doc.get("framewatch") or ""  # Framewatch disabled
        
        text_to_embed = f"Caption: {caption}\nUsername: {username}\nTranscription: {transcription}"
        
        vector_id = processed_doc.get("code")
        
        if vector_id:
            vectors.append(Vector(
                id=vector_id,
                data=text_to_embed,
                metadata=processed_doc
            ))

        count += 1
        print(f"Processed {count} documents...", end='\r')
    
    print(f"\nTotal documents processed: {count}")
    
    # Upsert to Upstash in batches
    batch_size = 100
    for i in range(0, len(vectors), batch_size):
        batch = vectors[i:i + batch_size]
        print(f"Upserting batch {i//batch_size + 1} ({len(batch)} vectors)...")
        
        try:
            index.upsert(vectors=batch)
            print(f"Batch {i//batch_size + 1} success")
        except Exception as e:
            print(f"Error upserting batch {i//batch_size + 1}: {e}")

if __name__ == '__main__':
    fetch_ig_reels()
