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
import cv2
import numpy as np
import io

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

class FrameWatchService:
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

    def analyze_video(self, doc_id: str, video_url: str):
        """
        Analyzes a video by extracting 5 frames and describing what is happening using Groq's multimodal model.
        """
        doc_ref = self.db.collection("ig_reels").document(doc_id)

        doc_snapshot = doc_ref.get()
        if doc_snapshot.exists:
            doc_data = doc_snapshot.to_dict()
            if doc_data.get("framewatch") and doc_data["framewatch"].strip():
                print(f"⏭ Skipping {doc_id}, framewatch description already exists")
                return
        else:
            print(f"⚠ Document {doc_id} does not exist.")
            return

        try:
            # Step 1: Download video
            video_bytes = requests.get(video_url).content

            # Step 2: Extract 5 evenly spaced frames
            import tempfile, base64
            with tempfile.NamedTemporaryFile(suffix=".mp4") as tmp:
                tmp.write(video_bytes)
                tmp.flush()
                cap = cv2.VideoCapture(tmp.name)

                frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                if frame_count <= 0:
                    print(f"❌ Video {doc_id} has no frames.")
                    return

                indices = np.linspace(0, frame_count - 1, 5, dtype=int)
                image_data_urls = []

                for idx in indices:
                    cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
                    ret, frame = cap.read()
                    if not ret:
                        continue
                    _, jpeg = cv2.imencode(".jpg", frame)
                    b64 = base64.b64encode(jpeg.tobytes()).decode("utf-8")
                    image_data_urls.append(f"data:image/jpeg;base64,{b64}")

                cap.release()

            if len(image_data_urls) == 0:
                print(f"❌ No frames extracted for {doc_id}")
                return

            # Step 3: Ask Groq model to describe what’s happening
            print(f"🧠 Sending 5 frames from {doc_id} to Groq model...")
            messages = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Analyze the following 5 images taken from different times in a video. "
                                "Write a concise description under 1000 characters of what is happening overall. "
                                "Do not use phrases like 'this video is of'; simply describe the events, actions, "
                                "objects, and context visible in the frames."
                            ),
                        }
                    ]
                    + [
                        {"type": "image_url", "image_url": {"url": url}}
                        for url in image_data_urls
                    ],
                }
            ]

            completion = self.client.chat.completions.create(
                model="meta-llama/llama-4-maverick-17b-128e-instruct",
                messages=messages,
                temperature=0.8,
                max_completion_tokens=1024,
                top_p=1,
                stream=False,
            )

            summary_text = completion.choices[0].message.content.strip()

            # Step 4: Update Firestore
            update_data = {
                "framewatch": summary_text,
                "is_framewatched": True,
                "updated_at": firestore.SERVER_TIMESTAMP,
            }
            doc_ref.update(update_data)
            print(f"✅ Framewatch analysis saved for {doc_id}")

        except Exception as e:
            print(f"❌ Error analyzing video {doc_id}: {e}")
            doc_ref.update({
                "is_framewatched": False,
                "updated_at": firestore.SERVER_TIMESTAMP,
                "framewatch": None
            })

    def get_non_framewatched_docs(self, user_id: str):
        """
        Queries Firestore for documents in 'ig_reels' where user_id matches and is_analyzed is False.

        Args:
            user_id (str): The user ID to filter documents.

        Returns:
            List of tuples (document reference, document data) for matching documents.
        """
        query = (
            self.db.collection("ig_reels")
            .where("user_id", "==", user_id)
            .where("is_original_audio", "==", False)
            .where("is_framewatched", "==", False)
        )
        results = query.get()
        return [(doc.reference, doc.to_dict()) for doc in results]




# Example usage
if __name__ == "__main__":
    # ===== Fetch Reels =====
    user_id = "36599340756"  # Replace with target username
    service = FrameWatchService()
    untranscribed_docs = service.get_non_framewatched_docs(user_id)
    docs_list = list(untranscribed_docs)
    print(f"Found {len(docs_list)} unanalyzed documents for user '{user_id}'.")
    for doc_ref, doc_data in docs_list:
        doc_id = doc_ref.id
        video_url = doc_data.get("video_url")
        if video_url:
            service.analyze_video(doc_id, video_url)
        else:
            print(f"⚠️ Document {doc_id} missing 'video_url', skipping analysis.")
    
    print("✅ Framewatch process completed.")
    if service.db:
        creator_doc_ref = service.db.collection("ig_creators").document(user_id)
        try:
            creator_doc_ref.update({"status": "framewatched", "updated_at": firestore.SERVER_TIMESTAMP})
            print(f"✅ Updated status to 'analyzed' for creator {user_id}")
        except Exception as e:
            print(f"❌ Failed to update status for creator {user_id}: {e}")