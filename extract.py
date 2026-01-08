import requests
import json
import os
from typing import Optional, List, Dict
from urllib.parse import urlencode
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import time
import firebase_admin
from firebase_admin import credentials, firestore
import re

# Load .env file automatically
try:
    from dotenv import load_dotenv
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

class InstagramReelsScraper:
    """
    Scraper for Instagram reels using internal API with proxy support
    WARNING: This is against Instagram's ToS. Use at your own risk.
    """
    
    def __init__(self, session_id: Optional[str] = None, csrf_token: Optional[str] = None, 
                 proxy_config: Optional[Dict] = None, r2_config: Optional[Dict] = None):
        """
        Initialize scraper with authentication credentials and optional proxy
        
        Args:
            session_id: Your Instagram sessionid cookie (optional if generating CSRF)
            csrf_token: Your Instagram csrftoken cookie (will auto-generate if not provided)
            proxy_config: Dict with keys: username, password, host, port
                         Example: {'username': 'user', 'password': 'pass', 
                                  'host': 'proxy.com', 'port': 8080}
        """
        self.session_id = session_id
        self.session = requests.Session()
        
        # Setup proxy if provided
        self.proxies = None
        if proxy_config:
            self.proxies = self._setup_proxy(proxy_config)
            print(f"✓ Proxy configured: {proxy_config['host']}:{proxy_config['port']}")

        # Setup R2 client if provided
        self.r2_client = None
        self.r2_bucket = None
        self.cdn_url = None
        if r2_config:
            self.r2_client = self._setup_r2_client(r2_config)
            self.r2_bucket = r2_config.get('bucket_name', 'drissea')
            self.cdn_url = r2_config.get('cdn_url', 'https://cdn.drissea.com')
            print(f"✓ R2 configured: bucket={self.r2_bucket}, cdn={self.cdn_url}")
        
        # Setup Firestore client
        self.db = None
        if firestore:
            try:
                self.db = firestore.client()
                print(f"✓ Firestore client initialized")
            except Exception as e:
                print(f"⚠ Error initializing Firestore client: {e}")

        # Get CSRF token (auto-generate if not provided)
        if csrf_token:
            self.csrf_token = csrf_token
            print("✓ Using provided CSRF token")
        else:
            print("⏳ Generating CSRF token...")
            self.csrf_token = self._get_csrf_token()
            print(f"✓ CSRF token generated: {self.csrf_token[:20]}...")
        
        # Set up headers
        self._setup_headers()

    def _setup_r2_client(self, config: Dict):
        """
        Setup Cloudflare R2 S3-compatible client
        
        Args:
            config: R2 configuration dict with access_key_id, secret_access_key, endpoint_url
            
        Returns:
            boto3 S3 client configured for R2
        """
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=config['endpoint_url'],
                aws_access_key_id=config['access_key_id'],
                aws_secret_access_key=config['secret_access_key'],
                region_name='auto'  # R2 uses 'auto' for region
            )
            return s3_client
        except Exception as e:
            print(f"⚠ Error setting up R2 client: {e}")
            return None
    
    def _setup_proxy(self, config: Dict) -> Dict:
        """
        Setup proxy configuration
        
        Args:
            config: Proxy configuration dict
            
        Returns:
            Proxies dict for requests library
        """
        proxy_url = f"http://{config['username']}:{config['password']}@{config['host']}:{config['port']}"
        return {
            'http': proxy_url,
            'https': proxy_url
        }
    
    def _get_csrf_token(self) -> str:
        """
        Automatically generate CSRF token by visiting Instagram homepage
        
        Returns:
            CSRF token string
            
        Raises:
            Exception if token cannot be obtained
        """
        try:
            response = self.session.get(
                'https://www.instagram.com/',
                proxies=self.proxies,
                timeout=30
            )
            response.raise_for_status()
            
            # Extract CSRF token from cookies
            if 'csrftoken' in response.cookies:
                return response.cookies['csrftoken']
            
            # Try to extract from Set-Cookie header
            set_cookie_header = response.headers.get('set-cookie', '')
            if 'csrftoken=' in set_cookie_header:
                for cookie in set_cookie_header.split(';'):
                    if 'csrftoken=' in cookie:
                        return cookie.split('csrftoken=')[1].split(';')[0]
            
            raise Exception("CSRF token not found in response")
            
        except Exception as e:
            raise Exception(f"Failed to obtain CSRF token: {str(e)}")
    
    def _setup_headers(self):
        """Setup request headers with authentication"""
        cookies = f'csrftoken={self.csrf_token};'
        if self.session_id:
            cookies += f' sessionid={self.session_id};'
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'X-IG-App-ID': '936619743392459',
            'X-CSRFToken': self.csrf_token,
            'X-Instagram-AJAX': '1',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://www.instagram.com/',
            'Cookie': cookies
        })
    
    def _extract_collaborators(self, media: Dict) -> List[Dict]:
        """
        Extract collaborator information from media object
        
        Args:
            media: Media object from API response
            
        Returns:
            List of collaborator dictionaries with username, user_id, full_name, profile_pic
        """
        collaborators = []
        
        # Check for invited_coauthor_producers (main collaborators field)
        invited_coauthors = media.get('invited_coauthor_producers', [])
        for user in invited_coauthors:
            collaborators.append({
                'user_id': user.get('pk') or user.get('id'),
                'username': user.get('username'),
                'full_name': user.get('full_name'),
                'profile_pic_url': user.get('profile_pic_url'),
                'is_verified': user.get('is_verified', False),
                'type': 'invited_coauthor'
            })
        
        # Check for coauthor_producers (accepted collaborators)
        coauthor_producers = media.get('coauthor_producers', [])
        for user in coauthor_producers:
            # Avoid duplicates
            if not any(c['user_id'] == (user.get('pk') or user.get('id')) for c in collaborators):
                collaborators.append({
                    'user_id': user.get('pk') or user.get('id'),
                    'username': user.get('username'),
                    'full_name': user.get('full_name'),
                    'profile_pic_url': user.get('profile_pic_url'),
                    'is_verified': user.get('is_verified', False),
                    'type': 'coauthor'
                })
        
        # Check for sponsor_tags (branded content/sponsorships)
        sponsor_tags = media.get('sponsor_tags', [])
        for sponsor in sponsor_tags:
            sponsor_user = sponsor.get('sponsor', {})
            if sponsor_user:
                collaborators.append({
                    'user_id': sponsor_user.get('pk') or sponsor_user.get('id'),
                    'username': sponsor_user.get('username'),
                    'full_name': sponsor_user.get('full_name'),
                    'profile_pic_url': sponsor_user.get('profile_pic_url'),
                    'is_verified': sponsor_user.get('is_verified', False),
                    'type': 'sponsor'
                })
        
        # Check for usertags (tagged users - not all are collaborators but good to include)
        usertags = media.get('usertags', {}).get('in', [])
        for tag in usertags:
            user = tag.get('user', {})
            user_id = user.get('pk') or user.get('id')
            # Avoid duplicates and only add if not already in list
            if user_id and not any(c['user_id'] == user_id for c in collaborators):
                collaborators.append({
                    'user_id': user_id,
                    'username': user.get('username'),
                    'full_name': user.get('full_name'),
                    'profile_pic_url': user.get('profile_pic_url'),
                    'is_verified': user.get('is_verified', False),
                    'type': 'tagged_user'
                })
        
        return collaborators
    
    @staticmethod
    def from_env(session_id: Optional[str] = None) -> 'InstagramReelsScraper':
        """
        Create scraper instance from environment variables
        
        Environment variables:
            - SESSION_ID: Instagram session ID (optional)
            - CSRF_TOKEN: Instagram CSRF token (optional, will auto-generate)
            - PROXY_USERNAME: Proxy username (optional)
            - PROXY_PASSWORD: Proxy password (optional)
            - PROXY_HOST: Proxy host (optional)
            - PROXY_PORT: Proxy port (optional)
            - R2_ACCESS_KEY_ID: Cloudflare R2 access key (optional)
            - R2_SECRET_ACCESS_KEY: Cloudflare R2 secret key (optional)
            - R2_ENDPOINT_URL: R2 endpoint URL (optional)
            - R2_BUCKET_NAME: R2 bucket name (optional, default: drissea)
            - R2_CDN_URL: CDN URL (optional, default: https://cdn.drissea.com)
        
        Args:
            session_id: Override session_id from env
            
        Returns:
            InstagramReelsScraper instance
        """
        session_id = session_id or os.getenv('SESSION_ID')
        csrf_token = os.getenv('CSRF_TOKEN')
        
        proxy_config = None
        if all([os.getenv('PROXY_USERNAME'), os.getenv('PROXY_PASSWORD'), 
                os.getenv('PROXY_HOST'), os.getenv('PROXY_PORT')]):
            proxy_config = {
                'username': os.getenv('PROXY_USERNAME'),
                'password': os.getenv('PROXY_PASSWORD'),
                'host': os.getenv('PROXY_HOST'),
                'port': int(os.getenv('PROXY_PORT'))
            }
        
        r2_config = None
        if all([os.getenv('R2_ACCESS_KEY_ID'), os.getenv('R2_SECRET_ACCESS_KEY'), 
                os.getenv('R2_ENDPOINT_URL')]):
            r2_config = {
                'access_key_id': os.getenv('R2_ACCESS_KEY_ID'),
                'secret_access_key': os.getenv('R2_SECRET_ACCESS_KEY'),
                'endpoint_url': os.getenv('R2_ENDPOINT_URL'),
                'bucket_name': os.getenv('R2_BUCKET_NAME', 'drissea'),
                'cdn_url': os.getenv('R2_CDN_URL', 'https://cdn.drissea.com')
            }
        
        return InstagramReelsScraper(session_id, csrf_token, proxy_config, r2_config)
    
    def search_users(self, query: str, limit: int = 30) -> List[Dict]:
        """
        Search for Instagram users/creators
        
        Args:
            query: Search query (username, name, or keywords)
            limit: Maximum number of results to return (default: 30)
            
        Returns:
            List of user dictionaries with basic info
        """
        url = 'https://www.instagram.com/api/v1/web/search/topsearch/'
        params = {
            'query': query,
            'context': 'blended',  # Search for all types
        }
        
        try:
            response = self.session.get(url, params=params, proxies=self.proxies, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            users = []
            for item in data.get('users', [])[:limit]:
                user = item.get('user', {})
                users.append({
                    'user_id': user.get('pk'),
                    'username': user.get('username'),
                    'full_name': user.get('full_name'),
                    'profile_pic_url': user.get('profile_pic_url'),
                    'is_verified': user.get('is_verified', False),
                    'follower_count': user.get('follower_count', 0),
                })
            
            return users
            
        except Exception as e:
            print(f"Error searching users for '{query}': {e}")
            return []
    
    def search_reels(self, query: str, limit: int = 30) -> List[Dict]:
        """
        Search for Instagram reels by keywords/hashtags
        
        Args:
            query: Search query (keywords or hashtags without #)
            limit: Maximum number of results to return (default: 30)
            
        Returns:
            List of reel dictionaries with metadata
        """
        # Method 1: Search via hashtag (more reliable)
        if not query.startswith('#'):
            query_hashtag = query.replace(' ', '')
        else:
            query_hashtag = query[1:]
        
        url = f'https://www.instagram.com/api/v1/tags/{query_hashtag}/sections/'
        
        try:
            response = self.session.get(url, proxies=self.proxies, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            reels = []
            sections = data.get('sections', [])
            
            for section in sections:
                layout_content = section.get('layout_content', {})
                medias = layout_content.get('medias', [])
                
                for media_item in medias[:limit]:
                    media = media_item.get('media', {})
                    
                    # Only process video content (reels)
                    if media.get('media_type') != 2:  # 2 = video
                        continue
                    
                    # Extract audio information
                    audio_info = self._parse_audio_info_clips(media)
                    
                    reel_data = {
                        'id': media.get('id'),
                        'code': media.get('code'),
                        'video_url': media.get('video_versions', [{}])[0].get('url') if media.get('video_versions') else None,
                        'thumbnail': media.get('image_versions2', {}).get('candidates', [{}])[0].get('url'),
                        'caption': media.get('caption', {}).get('text', '') if media.get('caption') else '',
                        'view_count': media.get('view_count', 0),
                        'like_count': media.get('like_count', 0),
                        'comment_count': media.get('comment_count', 0),
                        'play_count': media.get('play_count', 0),
                        'taken_at': media.get('taken_at'),
                        'permalink': f"https://www.instagram.com/reel/{media.get('code')}/",
                        
                        # User info
                        'user_id': media.get('user', {}).get('pk'),
                        'username': media.get('user', {}).get('username'),
                        
                        # Audio information
                        'is_original_audio': audio_info['is_original'],
                        'audio_type': audio_info['audio_type'],
                        'audio_title': audio_info['audio_title'],
                        'audio_artist': audio_info['audio_artist'],
                        'audio_id': audio_info['audio_id']
                    }
                    reels.append(reel_data)
                    
                    if len(reels) >= limit:
                        break
                
                if len(reels) >= limit:
                    break
            
            return reels[:limit]
            
        except Exception as e:
            print(f"Error searching reels for '{query}': {e}")
            return []
    
    
    def search_reels_by_keyword(self, keyword: str, limit: int = 30) -> List[Dict]:
        """
        Search for reels using the general search API
        
        Args:
            keyword: Search keyword
            limit: Maximum number of results (default: 30)
            
        Returns:
            List of reel dictionaries
        """
        url = 'https://www.instagram.com/api/v1/web/search/topsearch/'
        params = {
            'query': keyword,
            'context': 'blended',
        }
        
        try:
            response = self.session.get(url, params=params, proxies=self.proxies, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            reels = []
            
            # Check for reels in search results
            for item in data.get('clips', [])[:limit]:
                clip = item.get('clip', {})
                media = clip.get('media', {})
                
                # Extract audio information
                audio_info = self._parse_audio_info_clips(media)
                
                reel_data = {
                    'id': media.get('id'),
                    'code': media.get('code'),
                    'video_url': media.get('video_versions', [{}])[0].get('url') if media.get('video_versions') else None,
                    'thumbnail': media.get('image_versions2', {}).get('candidates', [{}])[0].get('url'),
                    'caption': media.get('caption', {}).get('text', '') if media.get('caption') else '',
                    'view_count': media.get('view_count', 0),
                    'like_count': media.get('like_count', 0),
                    'comment_count': media.get('comment_count', 0),
                    'play_count': media.get('play_count', 0),
                    'taken_at': media.get('taken_at'),
                    'permalink': f"https://www.instagram.com/reel/{media.get('code')}/",
                    
                    # User info
                    'user_id': media.get('user', {}).get('pk'),
                    'username': media.get('user', {}).get('username'),
                    
                    # Audio information
                    'is_original_audio': audio_info['is_original'],
                    'audio_type': audio_info['audio_type'],
                    'audio_title': audio_info['audio_title'],
                    'audio_artist': audio_info['audio_artist'],
                    'audio_id': audio_info['audio_id']
                }
                reels.append(reel_data)
            
            return reels[:limit]
            
        except Exception as e:
            print(f"Error searching reels by keyword '{keyword}': {e}")
            return []
        

    def get_user_id(self, username: str) -> Optional[str]:
        """
        Get numeric user ID from username and save/update user data to Firestore
        
        Args:
            username: Instagram username
            
        Returns:
            User ID as string or None if not found
        """
        url = f'https://www.instagram.com/api/v1/users/web_profile_info/'
        params = {'username': username}
        
        try:
            response = self.session.get(url, params=params, proxies=self.proxies, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            user_data = data['data']['user']
            user_id = user_data['id']
            
            # Check if user already exists in Firestore
            user_exists = False
            if self.db:
                user_exists = self.check_user_exists_in_firestore(user_id)
                if user_exists:
                    print(f"✓ User {username} (ID: {user_id}) exists in Firestore, updating...")
                else:
                    print(f"🆕 User {username} (ID: {user_id}) is new, creating...")
            
            # Extract user information
            profile_pic_url = user_data.get('profile_pic_url_hd') or user_data.get('profile_pic_url')
            
            # Upload profile picture to R2 if configured
            profile_pic_cdn_url = None
            if self.r2_client and profile_pic_url:
                # Extract file extension from URL or default to jpg
                match = re.search(r'\.([a-z]{3,4})(?:\?|$)', profile_pic_url)
                ext = match.group(1) if match else 'jpg'
                
                profile_pic_key = f"ig_profiles/{user_id}.{ext}"
                print(f"  ⏳ Uploading profile picture for {username}...")
                
                # Check if already exists
                if self.check_file_exists_in_r2(profile_pic_key):
                    profile_pic_cdn_url = f"{self.cdn_url}/{profile_pic_key}"
                    print(f"  ✓ Profile picture already exists: {profile_pic_cdn_url}")
                else:
                    profile_pic_cdn_url = self.upload_to_r2(
                        profile_pic_url,
                        profile_pic_key,
                        f'image/{ext}',
                        skip_if_exists=False
                    )
                    if profile_pic_cdn_url:
                        print(f"  ✓ Profile picture uploaded: {profile_pic_cdn_url}")
            
            # Prepare user data for Firestore
            user_firestore_data = {
                'user_id': user_id,
                'username': username,
                'full_name': user_data.get('full_name', ''),
                'bio': user_data.get('biography', ''),
                'category': user_data.get('category_name') or user_data.get('category', ''),
                'follower_count': user_data.get('edge_followed_by', {}).get('count', 0),
                'following_count': user_data.get('edge_follow', {}).get('count', 0),
                'media_count': user_data.get('edge_owner_to_timeline_media', {}).get('count', 0),
                'profile_pic_url': profile_pic_cdn_url or profile_pic_url,
                'public_email': user_data.get('business_email') or user_data.get('public_email', ''),
                'updated_at': firestore.SERVER_TIMESTAMP if firestore else None,
            }
            
            # Only set these for new users
            if not user_exists:
                user_firestore_data['status'] = 'initial'
                user_firestore_data['created_at'] = firestore.SERVER_TIMESTAMP if firestore else None
            
            # Remove None values
            user_firestore_data = {k: v for k, v in user_firestore_data.items() if v is not None}
            
            # Save/update to Firestore if configured
            if self.db:
                self.save_user_to_firestore(user_firestore_data, skip_if_exists=False)
            
            return user_id
            
        except Exception as e:
            print(f"Error getting user data for {username}: {e}")
            return None
    
    def save_user_to_firestore(self, user_data: Dict, skip_if_exists: bool = True) -> bool:
        """
        Save user data to Firestore ig_creators collection
        
        Args:
            user_data: User data dictionary
            skip_if_exists: Skip if document already exists (default: True)
            
        Returns:
            True if saved successfully, False otherwise
        """
        if not self.db:
            print("⚠ Firestore client not initialized")
            return False
        
        user_id = user_data.get('user_id')
        if not user_id:
            print("⚠ No user_id found in user data")
            return False
        
        try:
            # Check if document already exists
            if skip_if_exists and self.check_user_exists_in_firestore(user_id):
                print(f"  ✓ User {user_id} already exists in ig_creators, skipping")
                return True
            
            # Save to Firestore using user_id as document ID
            doc_ref = self.db.collection('ig_creators').document(user_id)
            doc_ref.set(user_data)
            
            print(f"  ✓ Saved user {user_data.get('username')} (ID: {user_id}) to ig_creators collection")
            return True
            
        except Exception as e:
            print(f"❌ Error saving user {user_id} to Firestore: {e}")
            return False

    def check_user_exists_in_firestore(self, user_id: str) -> bool:
        """
        Check if a user already exists in Firestore ig_creators collection
        
        Args:
            user_id: Instagram user ID
            
        Returns:
            True if user exists, False otherwise
        """
        if not self.db:
            return False
        
        try:
            doc_ref = self.db.collection('ig_creators').document(user_id)
            doc = doc_ref.get()
            return doc.exists
        except Exception as e:
            print(f"⚠ Error checking Firestore for user {user_id}: {e}")
            return False
    
    def get_reel_info(self, shortcode: str, retries: int = 3, delay: float = 1.0) -> Optional[Dict]:
        """
        Get detailed information for a specific reel using GraphQL API
        
        Args:
            shortcode: Reel shortcode/code
            retries: Number of retry attempts (default: 3)
            delay: Initial delay between retries in seconds (default: 1.0)
            
        Returns:
            Dictionary with detailed reel info including views, video_url, thumbnail_url, or None if error
        """
        import json
        import urllib.parse
        
        BASE_URL = "https://www.instagram.com/graphql/query"
        INSTAGRAM_DOCUMENT_ID = "9510064595728286"
        
        variables = json.dumps({
            'shortcode': shortcode,
            'fetch_tagged_user_count': None,
            'hoisted_comment_id': None,
            'hoisted_reply_id': None
        })
        
        data_body = urllib.parse.urlencode({
            'variables': variables,
            'doc_id': INSTAGRAM_DOCUMENT_ID
        })
        
        headers = {
            'X-CSRFToken': self.csrf_token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        
        current_delay = delay
        
        for attempt in range(retries + 1):
            try:
                response = self.session.post(
                    BASE_URL, 
                    data=data_body, 
                    headers=headers,
                    proxies=self.proxies, 
                    timeout=30
                )
                
                # Handle rate limiting
                if response.status_code in [429, 403]:
                    if attempt < retries:
                        retry_after = response.headers.get('retry-after')
                        wait_time = int(retry_after) if retry_after else current_delay
                        print(f"  ⚠ Rate limited, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        current_delay *= 2
                        continue
                    else:
                        print(f"  ❌ Rate limited after {retries} retries")
                        return None
                
                response.raise_for_status()
                data = response.json()
                
                # Extract media from GraphQL response
                media = data.get('data', {}).get('xdt_shortcode_media')
                if not media:
                    print(f"  ❌ No media found in response for {shortcode}")
                    return None
                
                return self._parse_graphql_reel_media(media, shortcode)
                
            except Exception as e:
                if attempt < retries:
                    print(f"  ⚠ Attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(current_delay)
                    current_delay *= 2
                else:
                    print(f"  ❌ Error fetching reel info for {shortcode}: {e}")
                    return None
        
        return None
    
    def _parse_graphql_reel_media(self, media: Dict, shortcode: str) -> Dict:
        """
        Parse GraphQL media object to extract reel info
        
        Args:
            media: Media object from GraphQL API response
            shortcode: Reel shortcode
            
        Returns:
            Dictionary with reel info
        """
        # Extract video URL from GraphQL response
        video_url = media.get('video_url')
        
        # Extract thumbnail URL
        thumbnail_url = media.get('display_url') or media.get('thumbnail_src')
        
        # Get counts
        view_count = media.get('video_view_count', 0) or media.get('video_play_count', 0) or 0
        like_count = media.get('edge_media_preview_like', {}).get('count', 0)
        comment_count = media.get('edge_media_preview_comment', {}).get('count', 0) or media.get('edge_media_to_parent_comment', {}).get('count', 0)
        
        return {
            'view_count': view_count,
            'like_count': like_count,
            'comment_count': comment_count,
            'video_url': video_url,
            'thumbnail_url': thumbnail_url,
            'code': shortcode
        }
    
    def _parse_reel_media(self, media: Dict, shortcode: str) -> Dict:
        """
        Parse media object to extract reel info
        
        Args:
            media: Media object from API response
            shortcode: Reel shortcode
            
        Returns:
            Dictionary with reel info
        """
        # Extract video URL
        video_url = None
        video_versions = media.get('video_versions', [])
        if video_versions:
            video_url = video_versions[0].get('url')
        
        # Extract thumbnail URL
        thumbnail_url = None
        image_versions = media.get('image_versions2', {}).get('candidates', [])
        if image_versions:
            thumbnail_url = image_versions[0].get('url')
        
        return {
            'view_count': media.get('play_count', 0) or media.get('view_count', 0),
            'like_count': media.get('like_count', 0),
            'comment_count': media.get('comment_count', 0),
            'video_url': video_url,
            'thumbnail_url': thumbnail_url,
            'code': shortcode
        }
        
    def upload_to_r2(self, url: str, key: str, content_type: str, skip_if_exists: bool = True) -> Optional[str]:
        """
        Download file from URL and upload to Cloudflare R2
        
        Args:
            url: URL to download from
            key: R2 object key (path in bucket)
            content_type: MIME type (e.g., 'image/jpeg', 'video/mp4')
            skip_if_exists: Skip upload if file already exists (default: True)
            
        Returns:
            CDN URL of uploaded file or None if failed
        """
        if not self.r2_client:
            print("⚠ R2 client not configured")
            return None
        
        # Check if file already exists
        if skip_if_exists and self.check_file_exists_in_r2(key):
            cdn_url = f"{self.cdn_url}/{key}"
            print(f"  ✓ File already exists: {cdn_url}")
            return cdn_url
        
        try:
            # Download file
            response = self.session.get(url, proxies=self.proxies, timeout=60, stream=True)
            response.raise_for_status()
            
            # Upload to R2
            self.r2_client.put_object(
                Bucket=self.r2_bucket,
                Key=key,
                Body=response.content,
                ContentType=content_type
            )
            
            # Return CDN URL
            cdn_url = f"{self.cdn_url}/{key}"
            return cdn_url
            
        except ClientError as e:
            print(f"❌ R2 upload error for {key}: {e}")
            return None
        except Exception as e:
            print(f"❌ Download/upload error for {key}: {e}")
            return None
    
    def check_file_exists_in_r2(self, key: str) -> bool:
        """
        Check if a file already exists in R2
        
        Args:
            key: R2 object key (path in bucket)
            
        Returns:
            True if file exists, False otherwise
        """
        if not self.r2_client:
            return False
        
        try:
            self.r2_client.head_object(Bucket=self.r2_bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"⚠ Error checking file existence for {key}: {e}")
                return False
        except Exception as e:
            print(f"⚠ Error checking file existence for {key}: {e}")
            return False

    def download_and_upload_reel(self, reel: Dict, upload_video: bool = False,  # Video upload disabled by default
                                  upload_thumbnail: bool = True, skip_if_exists: bool = True) -> Dict:
        """
        Download and upload reel video and thumbnail to R2
        
        Args:
            reel: Reel dictionary with video_url, thumbnail, and code
            upload_video: Whether to upload video (default: False - disabled)
            upload_thumbnail: Whether to upload thumbnail (default: True)
            skip_if_exists: Skip upload if file already exists (default: True)
            
        Returns:
            Dictionary with CDN URLs: {'video_cdn_url': '...', 'thumbnail_cdn_url': '...'}
        """
        result = {
            'video_cdn_url': None,
            'thumbnail_cdn_url': None
        }
        
        if not self.r2_client:
            print("⚠ R2 client not configured. Skipping upload.")
            return result
        
        shortcode = reel.get('code') or reel.get('shortcode')
        if not shortcode:
            print("⚠ No shortcode found in reel data")
            return result
        
        # Upload thumbnail
        if upload_thumbnail and reel.get('thumbnail_url'):
            thumbnail_key = f"ig_thumbnails/{shortcode}.jpg"
            
            # Check if file already exists
            if skip_if_exists and self.check_file_exists_in_r2(thumbnail_key):
                result['thumbnail_cdn_url'] = f"{self.cdn_url}/{thumbnail_key}"
                print(f"  ✓ Thumbnail already exists: {shortcode}.jpg")
            else:
                print(f"  ⏳ Uploading thumbnail: {shortcode}.jpg")
                thumbnail_cdn = self.upload_to_r2(
                    reel['thumbnail_url'], 
                    thumbnail_key, 
                    'image/jpeg',
                    skip_if_exists=False  # Already checked above
                )
                if thumbnail_cdn:
                    result['thumbnail_cdn_url'] = thumbnail_cdn
                    print(f"  ✓ Thumbnail uploaded: {thumbnail_cdn}")
        
        # Upload video
        if upload_video and reel.get('video_url'):
            video_key = f"ig_videos/{shortcode}.mp4"
            
            # Check if file already exists
            if skip_if_exists and self.check_file_exists_in_r2(video_key):
                result['video_cdn_url'] = f"{self.cdn_url}/{video_key}"
                print(f"  ✓ Video already exists: {shortcode}.mp4")
            else:
                print(f"  ⏳ Uploading video: {shortcode}.mp4")
                video_cdn = self.upload_to_r2(
                    reel['video_url'], 
                    video_key, 
                    'video/mp4',
                    skip_if_exists=False  # Already checked above
                )
                if video_cdn:
                    result['video_cdn_url'] = video_cdn
                    print(f"  ✓ Video uploaded: {video_cdn}")
        
        return result
    
    def download_and_upload_all_reels(self, reels: List[Dict], upload_video: bool = False,  # Video upload disabled by default
                                       upload_thumbnail: bool = True, delay: float = 1.0) -> List[Dict]:
        """
        Download and upload all reels to R2 with progress tracking
        
        Args:
            reels: List of reel dictionaries
            upload_video: Whether to upload videos (default: False - disabled)
            upload_thumbnail: Whether to upload thumbnails (default: True)
            delay: Delay between uploads in seconds (default: 1.0)
            
        Returns:
            List of reels with added cdn_urls field
        """
        if not self.r2_client:
            print("⚠ R2 client not configured. Skipping uploads.")
            return reels
        
        total = len(reels)
        print(f"\n📤 Starting upload of {total} reels to R2...")
        
        for i, reel in enumerate(reels, 1):
            print(f"\n[{i}/{total}] Processing {reel.get('code') or reel.get('shortcode')}...")
            
            cdn_urls = self.download_and_upload_reel(reel, upload_video, upload_thumbnail)
            # reel['video_url'] = cdn_urls["video_cdn_url"] or reel.get('video_url')  # Video upload commented out
            reel['thumbnail_url'] = cdn_urls["thumbnail_cdn_url"] or reel.get('thumbnail_url')
            
            # Add delay between uploads to avoid rate limiting
            if i < total:
                time.sleep(delay)
        
        print(f"\n✅ Upload complete!")
        return reels
    
    def enrich_reels_with_views(self, reels: List[Dict], delay: float = 1.0, 
                                 show_progress: bool = True) -> List[Dict]:
        """
        Enrich reels with accurate view counts by fetching individual reel info
        
        Args:
            reels: List of reel dictionaries
            delay: Delay between requests in seconds (default 1.0)
            show_progress: Show progress bar (default True)
            
        Returns:
            Updated list of reels with accurate view counts
        """
        import time
        
        enriched_reels = []
        total = len(reels)
        
        if show_progress:
            print(f"\nEnriching {total} reels with view counts...")
        
        for i, reel in enumerate(reels, 1):
            shortcode = reel.get('code') or reel.get('shortcode')
            
            if show_progress and i % 10 == 0:
                print(f"  Progress: {i}/{total} reels processed...")
            
            # Get detailed info
            info = self.get_reel_info(shortcode)
            
            if info:
                # Update counts with accurate data
                reel['view_count'] = info['view_count']
                reel['like_count'] = info['like_count']
                reel['comment_count'] = info['comment_count']
            
            enriched_reels.append(reel)
            
            # Rate limiting
            if i < total:
                time.sleep(delay)
        
        if show_progress:
            print(f"  ✓ Completed enriching {total} reels")
        
        return enriched_reels
    
    def get_reels_clips_api(self, user_id: str, page_size: int = 12, max_id: Optional[str] = None) -> Optional[Dict]:
        """
        Fetch reels using the /api/v1/clips/user/ endpoint (Recommended)
        
        Args:
            user_id: Numeric Instagram user ID
            page_size: Number of reels to fetch per page (default 12)
            max_id: Pagination cursor for next page (None for first page)
            
        Returns:
            Dictionary with 'items' (reels list), 'paging_info', and 'has_more' or None if error
        """
        url = 'https://www.instagram.com/api/v1/clips/user/'
        
        data = {
            'target_user_id': user_id,
            'page_size': page_size,
            'include_feed_video': 'true'
        }
        
        # Add pagination cursor if provided
        if max_id:
            data['max_id'] = max_id
        
        try:
            response = self.session.post(url, data=data, proxies=self.proxies, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            reels = []
            for item in result.get('items', []):
                
                print(item)
                print("-------------------")
                media = item.get('media', {})
                
                # Extract audio information
                audio_info = self._parse_audio_info_clips(media)

                # Extract collaborators from initial response
                collaborators = self._extract_collaborators(media)


                
                reel_data = {
                    'id': media.get('id'),
                    'user_id': media.get('caption', {}).get('user', '{}').get('pk') if media.get('caption') else None,
                    'code': media.get('code'),
                    'video_url': media.get('video_versions', [{}])[0].get('url') if media.get('video_versions') else None,
                    'thumbnail_url': media.get('image_versions2', {}).get('candidates', [{}])[0].get('url'),
                    'caption': media.get('caption', {}).get('text', '') if media.get('caption') else '',
                    'view_count': media.get('play_count', 0) or media.get('view_count', 0) or 0,
                    'like_count': media.get('like_count', 0),
                    'comment_count': media.get('comment_count', 0),
                    'play_count': media.get('play_count', 0) or media.get('view_count', 0) or 0,
                    'taken_at': media.get('taken_at'),
                    'permalink': f"https://www.instagram.com/reel/{media.get('code')}/",
                    
                    # Audio information
                    'is_original_audio': audio_info['is_original'],
                    'audio_type': audio_info['audio_type'],
                    'audio_title': audio_info['audio_title'],
                    'audio_artist': audio_info['audio_artist'],
                    'audio_id': audio_info['audio_id'],

                    # Collaborators
                    'collaborators': collaborators,
                    'has_collaborators': len(collaborators) > 0,
                    'collaborator_count': len(collaborators)
                }
                reels.append(reel_data)
            
            # Get pagination info
            paging_info = result.get('paging_info', {})
            
            return {
                'items': reels,
                'paging_info': paging_info,
                'has_more': paging_info.get('more_available', False),
                'next_max_id': paging_info.get('max_id')
            }
            
        except Exception as e:
            print(f"Error fetching reels: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            return None
    
    def _parse_audio_info_clips(self, media: Dict) -> Dict:
        """
        Parse audio information from Clips API media object
        
        Args:
            media: Media object from Clips API response
            
        Returns:
            Dictionary with audio information including is_original flag
        """
        audio_info = {
            'is_original': True,  # Default to True (original audio)
            'audio_type': 'original',
            'audio_title': None,
            'audio_artist': None,
            'audio_id': None
        }
        
        # Check for music_metadata (Instagram music library)
        music_metadata = media.get('music_metadata')
        if music_metadata:
            audio_info['is_original'] = False
            audio_info['audio_type'] = 'instagram_music'
            audio_info['audio_title'] = music_metadata.get('music_info', {}).get('song_name')
            audio_info['audio_artist'] = music_metadata.get('music_info', {}).get('artist_name')
            audio_info['audio_id'] = music_metadata.get('music_info', {}).get('audio_cluster_id')
            return audio_info
        
        # Check for clips_metadata
        clips_metadata = media.get('clips_metadata', {})
        
        # Check music_info inside clips_metadata
        music_info = clips_metadata.get('music_info')
        if music_info:
            audio_info['is_original'] = False
            audio_info['audio_type'] = 'instagram_music'
            audio_info['audio_title'] = music_info.get('song_name') or music_info.get('display_artist')
            audio_info['audio_artist'] = music_info.get('artist_name')
            audio_info['audio_id'] = music_info.get('audio_cluster_id') or music_info.get('id')
            return audio_info
        
        # Check original_sound_info
        original_sound = clips_metadata.get('original_sound_info')
        if original_sound:
            audio_info['audio_id'] = original_sound.get('audio_asset_id')
            audio_info['audio_title'] = original_sound.get('original_audio_title')
            
            # Check if it's truly original (created by this user) or reused
            is_reused = original_sound.get('is_reused_audio', False)
            can_remix = original_sound.get('can_remix_be_shared_to_fb', False)
            
            if is_reused or can_remix:
                audio_info['is_original'] = False
                audio_info['audio_type'] = 'reused_original_audio'
            else:
                audio_info['is_original'] = True
                audio_info['audio_type'] = 'original'
            return audio_info
        
        # Check mashup_info (mixed audio)
        mashup_info = clips_metadata.get('mashup_info')
        if mashup_info:
            audio_info['is_original'] = False
            audio_info['audio_type'] = 'mashup'
            return audio_info
        
        # Check if audio is explicitly disabled
        is_audio_muted = clips_metadata.get('is_audio_muted', False)
        if is_audio_muted:
            audio_info['audio_type'] = 'no_audio'
            audio_info['is_original'] = True  # Consider muted as original (no borrowed audio)
            return audio_info
        
        return audio_info
    
    def get_all_reels_clips_api(self, user_id: str, max_reels: Optional[int] = None, delay: float = 2.0, max_age_days: int = 365) -> List[Dict]:
        """
        Fetch all reels with automatic pagination
        
        Args:
            user_id: Numeric Instagram user ID
            max_reels: Maximum number of reels to fetch (None for all)
            delay: Delay between requests in seconds (default 2.0)
            max_age_days: Maximum age of reels in days (default 365 = 1 year)
            
        Returns:
            List of all reel data dictionaries
        """
        import time
        from datetime import datetime, timedelta
        
        all_reels = []
        max_id = None
        page_num = 1
        
        # Calculate cutoff timestamp (reels older than this will be skipped)
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        print(f"📅 Fetching reels from the last {max_age_days} days (since {cutoff_date.strftime('%Y-%m-%d')})")
        
        while True:
            print(f"Fetching page {page_num}...")
            
            result = self.get_reels_clips_api(user_id, page_size=50, max_id=max_id)
            
            if not result or not result['items']:
                print("No more reels to fetch")
                break
            
            # Filter reels by age and check if we've hit the cutoff
            reels_before_cutoff = 0
            for reel in result['items']:
                taken_at = reel.get('taken_at')
                
                # Check if reel is older than cutoff
                if taken_at and taken_at < cutoff_timestamp:
                    reels_before_cutoff += 1
                    continue
                
                all_reels.append(reel)
            
            print(f"  Found {len(result['items'])} reels, {reels_before_cutoff} too old (total kept: {len(all_reels)})")
            
            # If all reels in this batch were too old, stop fetching
            if reels_before_cutoff == len(result['items']):
                print(f"🛑 All reels in this batch are older than {max_age_days} days, stopping")
                break
            
            # Check if we've reached the limit
            if max_reels and len(all_reels) >= max_reels:
                all_reels = all_reels[:max_reels]
                print(f"Reached maximum limit of {max_reels} reels")
                break
            
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
            
            # Add delay to avoid rate limiting
            time.sleep(delay)
        
        print(f"✅ Fetched {len(all_reels)} reels within the last {max_age_days} days")
        return all_reels
    
    def filter_original_audio_reels(self, reels: List[Dict]) -> List[Dict]:
        """
        Filter reels to only include those with original audio (no Instagram music)
        
        Args:
            reels: List of reel dictionaries
            
        Returns:
            Filtered list containing only reels with original audio
        """
        return [reel for reel in reels if reel.get('is_original_audio', False)]
    
    def get_audio_statistics(self, reels: List[Dict]) -> Dict:
        """
        Get statistics about audio usage in reels
        
        Args:
            reels: List of reel dictionaries
            
        Returns:
            Dictionary with audio statistics
        """
        total = len(reels)
        if total == 0:
            return {
                'total_reels': 0,
                'original_audio': 0,
                'instagram_music': 0,
                'other': 0,
                'original_percentage': 0.0
            }
        
        original_count = sum(1 for r in reels if r.get('is_original_audio', False))
        instagram_music_count = sum(1 for r in reels if r.get('audio_type') == 'instagram_music')
        other_count = total - original_count - instagram_music_count
        
        return {
            'total_reels': total,
            'original_audio': original_count,
            'instagram_music': instagram_music_count,
            'other': other_count,
            'original_percentage': (original_count / total) * 100,
            'instagram_music_percentage': (instagram_music_count / total) * 100
        }
    
    def _parse_audio_info(self, node: Dict) -> Dict:
        """
        Parse audio information from reel node to determine if it's original audio
        
        Args:
            node: Reel node data from GraphQL response
            
        Returns:
            Dictionary with audio information including is_original flag
        """
        audio_info = {
            'is_original': True,  # Default to True (original audio)
            'audio_type': 'original',
            'audio_title': None,
            'audio_artist': None,
            'audio_id': None
        }
        
        # Check for clips_music_attribute_info (Instagram audio library)
        clips_music = node.get('clips_music_attribute_info')
        if clips_music:
            audio_info['is_original'] = False
            audio_info['audio_type'] = 'instagram_music'
            audio_info['audio_title'] = clips_music.get('song_name') or clips_music.get('audio_cluster_id')
            audio_info['audio_artist'] = clips_music.get('artist_name')
            audio_info['audio_id'] = clips_music.get('audio_id') or clips_music.get('audio_cluster_id')
            return audio_info
        
        # Check for original_audio_info
        original_audio = node.get('original_audio_info')
        if original_audio:
            audio_info['audio_id'] = original_audio.get('audio_id')
            audio_info['audio_title'] = original_audio.get('audio_title')
            # Original audio created by the user
            audio_info['is_original'] = True
            audio_info['audio_type'] = 'original'
            return audio_info
        
        # Check for coauthor_producers (sometimes indicates music from Instagram library)
        coauthor_producers = node.get('coauthor_producers', [])
        if coauthor_producers:
            # If there are coauthor producers, it might be using their audio
            audio_info['is_original'] = False
            audio_info['audio_type'] = 'shared_audio'
            return audio_info
        
        # Check edge_media_to_tagged_user for music tags
        tagged_users = node.get('edge_media_to_tagged_user', {}).get('edges', [])
        for tagged_edge in tagged_users:
            tagged_node = tagged_edge.get('node', {})
            if tagged_node.get('x') == 0 and tagged_node.get('y') == 0:
                # Music is often tagged at position (0, 0)
                audio_info['is_original'] = False
                audio_info['audio_type'] = 'instagram_music'
                break
        
        return audio_info
    
    def check_reel_exists_in_firestore(self, shortcode: str) -> bool:
        """
        Check if a reel already exists in Firestore
        
        Args:
            shortcode: Instagram reel shortcode
            
        Returns:
            True if reel exists, False otherwise
        """
        if not self.db:
            return False
        
        try:
            doc_ref = self.db.collection('ig_reels').document(shortcode)
            doc = doc_ref.get()
            return doc.exists
        except Exception as e:
            print(f"⚠ Error checking Firestore for {shortcode}: {e}")
            return False
    
    def save_reel_to_firestore(self, reel: Dict, skip_if_exists: bool = True) -> bool:
        """
        Save a single reel to Firestore
        
        Args:
            reel: Reel dictionary
            skip_if_exists: Skip if document already exists (default: True)
            
        Returns:
            True if saved successfully, False otherwise
        """
        if not self.db:
            print("⚠ Firestore client not initialized")
            return False
        
        shortcode = reel.get('code') or reel.get('shortcode')
        if not shortcode:
            print("⚠ No shortcode found in reel data")
            return False
        
        try:
            # Check if document already exists
            if skip_if_exists and self.check_reel_exists_in_firestore(shortcode):
                print(f"  ✓ Reel {shortcode} already exists in Firestore, skipping")
                return True
            
            # Prepare data for Firestore
            firestore_data = {
                **reel,
                'is_transcribed': False,  # Default value
                'is_framewatched': False,  # Default value
                'created_at': firestore.SERVER_TIMESTAMP,
                'updated_at': firestore.SERVER_TIMESTAMP,
            }
            
            # Remove None values
            firestore_data = {k: v for k, v in firestore_data.items() if v is not None}
            
            # Save to Firestore using shortcode as document ID
            doc_ref = self.db.collection('ig_reels').document(shortcode)
            doc_ref.set(firestore_data)
            
            print(f"  ✓ Saved {shortcode} to Firestore")
            return True
            
        except Exception as e:
            print(f"❌ Error saving {shortcode} to Firestore: {e}")
            return False
    
    def save_all_reels_to_firestore(self, reels: List[Dict], user_id: Optional[str] = None, 
                                     skip_if_exists: bool = True) -> Dict:
        """
        Save all reels to Firestore with progress tracking
        
        Args:
            reels: List of reel dictionaries
            user_id: Instagram user ID (optional, for updating user status)
            skip_if_exists: Skip if document already exists (default: True)
            
        Returns:
            Dictionary with statistics: {'saved': int, 'skipped': int, 'failed': int}
        """
        if not self.db:
            print("⚠ Firestore client not initialized. Skipping Firestore save.")
            return {'saved': 0, 'skipped': 0, 'failed': 0}
        
        total = len(reels)
        saved = 0
        skipped = 0
        failed = 0
        
        print(f"\n💾 Saving {total} reels to Firestore...")
        if skip_if_exists:
            print("   (Skipping documents that already exist)")
        
        for i, reel in enumerate(reels, 1):
            shortcode = reel.get('code') or reel.get('shortcode')
            print(f"\n[{i}/{total}] Saving {shortcode} to Firestore...")
            
            # Check if already exists
            if skip_if_exists and self.check_reel_exists_in_firestore(shortcode):
                skipped += 1
                print(f"  ✓ Already exists, skipping")
            else:
                # Save to Firestore
                success = self.save_reel_to_firestore(reel, skip_if_exists=False)
                if success:
                    saved += 1
                else:
                    failed += 1
        
        print(f"\n✅ Firestore save complete!")
        print(f"   📊 Statistics: {saved} saved, {skipped} skipped, {failed} failed")
        
        # Update user status to "extracted" after saving all reels
        if user_id and (saved > 0 or skipped > 0):
            print(f"\n📝 Updating user {user_id} status to 'extracted'...")
            self.update_user_status(user_id, 'extracted')
        
        return {
            'saved': saved,
            'skipped': skipped,
            'failed': failed
        }
    
    def update_user_status(self, user_id: str, status: str) -> bool:
        """
        Update user status and updated_at timestamp in ig_creators collection
        
        Args:
            user_id: Instagram user ID
            status: New status value (e.g., 'extracted', 'processing', 'completed')
            
        Returns:
            True if updated successfully, False otherwise
        """
        if not self.db:
            print("⚠ Firestore client not initialized")
            return False
        
        try:
            doc_ref = self.db.collection('ig_creators').document(user_id)
            
            # Check if document exists
            if not doc_ref.get().exists:
                print(f"⚠ User {user_id} not found in ig_creators collection")
                return False
            
            # Update status and updated_at
            doc_ref.update({
                'status': status,
                'updated_at': firestore.SERVER_TIMESTAMP
            })
            
            print(f"  ✓ Updated user {user_id} status to '{status}'")
            return True
            
        except Exception as e:
            print(f"❌ Error updating user {user_id} status: {e}")
            return False
    
    


# Example usage
if __name__ == "__main__":
    print("=== Instagram Reels Scraper ===\n")
    
    # The .env file is automatically loaded at import time!
    # Just use the from_env() method
    
    print("Loading configuration from .env file...")
    print("Expected .env variables:")
    print("  - SESSION_ID (required)")
    print("  - CSRF_TOKEN (optional, will auto-generate if not provided)")
    print("  - PROXY_USERNAME (optional)")
    print("  - PROXY_PASSWORD (optional)")
    print("  - PROXY_HOST (optional)")
    print("  - PROXY_PORT (optional)\n")
    
    # ===== METHOD 1: Load everything from .env (Recommended) =====
    try:
        scraper = InstagramReelsScraper.from_env()
        print("✓ Scraper initialized from .env file\n")
    except Exception as e:
        print(f"❌ Error initializing scraper: {e}")
        print("\nMake sure your .env file exists with at least SESSION_ID set.")
        print("\nExample .env file:")
        print("SESSION_ID=your_session_id_here")
        print("PROXY_USERNAME=your_proxy_username")
        print("PROXY_PASSWORD=your_proxy_password")
        print("PROXY_HOST=brd.superproxy.io")
        print("PROXY_PORT=22225")
        exit(1)

    # search_reels = scraper.search_users("drissea", limit=20)
    # print(search_reels)
    scraper.get_user_id("keentoeat_")
    
    # ===== Alternative: Manual override with .env fallback =====
    # If you want to override specific values:
    # scraper = InstagramReelsScraper(
    #     session_id=os.getenv('SESSION_ID'),  # From .env
    #     csrf_token=None,  # Auto-generate
    #     proxy_config={
    #         'username': os.getenv('PROXY_USERNAME'),
    #         'password': os.getenv('PROXY_PASSWORD'),
    #         'host': os.getenv('PROXY_HOST'),
    #         'port': int(os.getenv('PROXY_PORT', 22225))
    #     } if os.getenv('PROXY_HOST') else None
    # )
    
    # # ===== Fetch Reels =====
    # username = "thetravel.bite"  # Replace with target username
    
    # # Get user ID
    # print(f"Fetching user ID for @{username}...")
    # user_id = scraper.get_user_id(username)
    
    # if not user_id:
    #     print(f"❌ Could not find user ID for {username}")
    #     exit(1)
    
    # print(f"✓ User ID: {user_id}\n")
    
    # # Fetch all reels with automatic pagination
    # print(f"Fetching all reels for @{username}...\n")
    # all_reels = scraper.get_all_reels_clips_api(user_id, max_reels=100, delay=2.0)
    
    # if all_reels:
    #     print(f"\n✓ Total reels fetched: {len(all_reels)}")
        
    #     # Show audio statistics
    #     audio_stats = scraper.get_audio_statistics(all_reels)
    #     print(f"\n=== Audio Statistics ===")
    #     print(f"Total reels: {audio_stats['total_reels']}")
    #     print(f"Original audio: {audio_stats['original_audio']} ({audio_stats['original_percentage']:.1f}%)")
    #     print(f"Instagram music: {audio_stats['instagram_music']} ({audio_stats['instagram_music_percentage']:.1f}%)")
    #     print(f"Other: {audio_stats['other']}")
        
    #     # Filter for original audio only
    #     original_audio_reels = scraper.filter_original_audio_reels(all_reels)
    #     print(f"\n✓ Reels with original audio: {len(original_audio_reels)}")

    #     # ===== UPLOAD TO R2 =====
    #     # Upload all reels to R2 (if R2 is configured)
    #     if scraper.r2_client:
    #         print("\n" + "="*50)
    #         print("UPLOADING TO CLOUDFLARE R2")
    #         print("="*50)
            
    #         # Option 1: Upload only original audio reels
    #         all_reels = scraper.download_and_upload_all_reels(
    #             all_reels, 
    #             upload_video=True, 
    #             upload_thumbnail=True,
    #             delay=1.5
    #         )
        
    #     # ===== SAVE TO FIRESTORE =====
    #     # Save all reels to Firestore (if Firestore is configured)
    #     if scraper.db:
    #         print("\n" + "="*50)
    #         print("SAVING TO FIRESTORE")
    #         print("="*50)
            
    #         firestore_stats = scraper.save_all_reels_to_firestore(all_reels, user_id=user_id, skip_if_exists=True)
            
    #     # Show statistics
    #     total_views = sum(r['view_count'] for r in all_reels)
    #     total_likes = sum(r['like_count'] for r in all_reels)
        
    #     print(f"\nOverall statistics:")
    #     print(f"  Total views: {total_views:,}")
    #     print(f"  Total likes: {total_likes:,}")
        
    #     # Show top 5 most viewed reels
    #     print("\n=== Top 5 Most Viewed Reels ===")
    #     sorted_reels = sorted(all_reels, key=lambda x: x['view_count'], reverse=True)
        
    #     for i, reel in enumerate(sorted_reels[:5], 1):
    #         audio_emoji = "🎤" if reel['is_original_audio'] else "🎵"
    #         print(f"\n{i}. {reel['code']} {audio_emoji}")
    #         print(f"   Views: {reel['view_count']:,}")
    #         print(f"   Likes: {reel['like_count']:,}")
    #         print(f"   Audio: {reel['audio_type']}")
    #         if reel['audio_title']:
    #             print(f"   Song: {reel['audio_title']}")
    #         if reel['audio_artist']:
    #             print(f"   Artist: {reel['audio_artist']}")
    #         print(f"   URL: {reel['permalink']}")
        
    #     # Show top 5 reels with ORIGINAL audio only
    #     if original_audio_reels:
    #         print("\n=== Top 5 Reels with Original Audio ===")
    #         sorted_original = sorted(original_audio_reels, key=lambda x: x['view_count'], reverse=True)
            
    #         for i, reel in enumerate(sorted_original[:5], 1):
    #             print(f"\n{i}. {reel['code']} 🎤")
    #             print(f"   Views: {reel['view_count']:,}")
    #             print(f"   Likes: {reel['like_count']:,}")
    #             print(f"   Audio Type: {reel['audio_type']}")
    #             print(f"   URL: {reel['permalink']}")
        
    #     # Save all reels to JSON
    #     output_file = f'{username}_reels.json'
    #     with open(output_file, 'w', encoding='utf-8') as f:
    #         json.dump(all_reels, f, indent=2, ensure_ascii=False)
    #     print(f"\n✓ Saved all reels to {output_file}")
        
    #     # Save original audio reels only to separate file
    #     if original_audio_reels:
    #         original_output_file = f'{username}_reels_original_audio.json'
    #         with open(original_output_file, 'w', encoding='utf-8') as f:
    #             json.dump(original_audio_reels, f, indent=2, ensure_ascii=False)
    #         print(f"✓ Saved original audio reels to {original_output_file}")
    # else:
    #     print("❌ No reels found or error occurred")