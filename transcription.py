import requests
from google.cloud import firestore
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore
from groq import Groq
# Assuming GROQ_API_KEY is set in your environment
import os
from dotenv import load_dotenv
from pathlib import Path


# Initialize Firebase Admin
try:
    
    # Try to initialize Firebase if not already initialized
    if not firebase_admin._apps:
        # Look for service account key file
        service_account_path = Path(__file__).parent / 'serviceAccountKey.json'
        
        if service_account_path.exists():
            cred = credentials.Certificate(str(service_account_path))
            firebase_admin.initialize_app(cred)
            print(f"✓ Firebase initialized with: {service_account_path}")
        else:
            print(f"⚠ Firebase service account key not found at: {service_account_path}")
            print("  Firebase features will be disabled")
except ImportError:
    print("⚠ firebase-admin not installed. Install with: pip install firebase-admin")
    print("  Firebase features will be disabled")
    firebase_admin = None
    firestore = None
except Exception as e:
    print(f"⚠ Error initializing Firebase: {e}")
    firebase_admin = None
    firestore = None

class TranscriptionService:
    def __init__(self):
        load_dotenv()
        self.GROQ_API_KEY = os.getenv("GROQ_API_KEY")
        self.client = Groq(api_key=self.GROQ_API_KEY)
        # Setup Firestore client
        self.db = None
        if firestore:
            try:
                self.db = firestore.client()
                print(f"✓ Firestore client initialized")
            except Exception as e:
                print(f"⚠ Error initializing Firestore client: {e}")

    def transcribe_video(self, doc_id: str, video_url: str):
        """
        Transcribes a video using Groq Whisper and updates Firestore document.

        Args:
            doc_id (str): Firestore document ID.
            video_url (str): URL to the video file.
        """
        doc_ref = self.db.collection("ig_reels").document(doc_id)

        # Check is_original_audio field before transcription
        doc_snapshot = doc_ref.get()
        if doc_snapshot.exists:
            doc_data = doc_snapshot.to_dict()
            transcription_field = doc_data.get("transcription")
            if transcription_field and transcription_field.strip():
                print(f"⏭ Skipping {doc_id}, transcription already exists")
                return
        else:
            print(f"⚠ Document {doc_id} does not exist.")
            return

        try:
            # Step 1: Fetch video bytes (Groq API expects a file)
            video_bytes = requests.get(video_url).content

            # Step 2: Send to Groq Whisper model for transcription with built-in translation to English
            transcription = self.client.audio.transcriptions.create(
                model="whisper-large-v3",
                file=("input.mp4", video_bytes, "video/mp4"),
                language="en",
            )
            transcription_text = transcription.text

            # Step 3: Update Firestore document
            update_data = {
                "transcription": transcription_text,
                "is_transcribed": True,
                "updated_at": firestore.SERVER_TIMESTAMP,
            }
            doc_ref.update(update_data)

            print(f"✅ Transcription saved for {doc_id}")

        except Exception as e:
            print(f"❌ Error transcribing video {doc_id}: {e}")
            # Optional: log error or set is_transcribed=False
            doc_ref.update({
                "is_transcribed": False,
                "updated_at": firestore.SERVER_TIMESTAMP,
                "transcription": None
            })

    def get_untranscribed_docs(self, user_id: str):
        """
        Queries Firestore for documents in 'ig_reels' where user_id matches and is_transcribed is False.

        Args:
            user_id (str): The user ID to filter documents.

        Returns:
            List of tuples (document reference, document data) for matching documents.
        """
        query = (
            self.db.collection("ig_reels")
            .where("user_id", "==", user_id)
            .where("is_transcribed", "==", False)
            .where("is_original_audio", "==", True)
        )
        results = query.get()
        return [(doc.reference, doc.to_dict()) for doc in results]




# Example usage
if __name__ == "__main__":
    # ===== Fetch Reels =====
    user_id = "36599340756"  # Replace with target username
    service = TranscriptionService()
    untranscribed_docs = service.get_untranscribed_docs(user_id)
    docs_list = list(untranscribed_docs)
    print(f"Found {len(docs_list)} untranscribed documents for user '{user_id}'.")
    for doc_ref, doc_data in docs_list:
        doc_id = doc_ref.id
        video_url = doc_data.get("video_url")
        is_original_audio = doc_data.get("is_original_audio", True)
        if not is_original_audio:
            print(f"⏭ Skipping {doc_id}, is_original_audio is False")
            continue
        if video_url:
            service.transcribe_video(doc_id, video_url)
        else:
            print(f"⚠️ Document {doc_id} missing 'video_url', skipping transcription.")
    
    print("✅ Transcription process completed.")
    if service.db:
        creator_doc_ref = service.db.collection("ig_creators").document(user_id)
        try:
            creator_doc_ref.update({"status": "transcribed", "updated_at": firestore.SERVER_TIMESTAMP})
            print(f"✅ Updated status to 'transcribed' for creator {user_id}")
        except Exception as e:
            print(f"❌ Failed to update status for creator {user_id}: {e}")