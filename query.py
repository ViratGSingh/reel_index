import os
from dotenv import load_dotenv
from upstash_vector import Index

# Load environment variables
load_dotenv()

UPSTASH_VECTOR_REST_URL = os.getenv("UPSTASH_VECTOR_REST_URL")
UPSTASH_VECTOR_REST_TOKEN = os.getenv("UPSTASH_VECTOR_REST_TOKEN")

def query_reels(query_text, top_k=5):
    if not UPSTASH_VECTOR_REST_URL or not UPSTASH_VECTOR_REST_TOKEN:
        print("Error: UPSTASH_VECTOR_REST_URL or UPSTASH_VECTOR_REST_TOKEN not found in .env")
        return

    # Initialize Upstash Index
    index = Index(url=UPSTASH_VECTOR_REST_URL, token=UPSTASH_VECTOR_REST_TOKEN)

    print(f"Querying for: '{query_text}'")
    
    try:
        result = index.query(
            data=query_text,
            top_k=top_k,
            include_metadata=True,
            include_vectors=False
        )
        
        print(f"\nFound {len(result)} results:")
        for i, res in enumerate(result):
            print(f"\nResult {i+1}:")
            print(f"ID: {res.id}")
            print(f"Score: {res.score}")
            if res.metadata:
                print(f"Caption: {res.metadata.get('caption', 'N/A')[:100]}...")
                print(f"Username: {res.metadata.get('username', 'N/A')}")
                print(f"Video URL: {res.metadata.get('video_url', 'N/A')}")
            else:
                print("No metadata found.")
                
    except Exception as e:
        print(f"Error querying index: {e}")

if __name__ == "__main__":
    # Sample query
    query_reels("perplexity cloudfare issue")
