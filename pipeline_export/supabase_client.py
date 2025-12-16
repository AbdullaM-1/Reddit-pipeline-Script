"""
Simple Supabase Database Client for Reddit Scraper Pipeline
"""

from datetime import datetime
from typing import Optional
from dotenv import dotenv_values
import hashlib

try:
    from supabase import create_client, Client  # type: ignore
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None  # type: ignore
    print("Warning: supabase-py not installed. Install with: pip install supabase")


def get_supabase_client() -> Optional[Client]:
    """Create and return a Supabase client instance."""
    if not SUPABASE_AVAILABLE:
        return None
    
    config = dotenv_values(".env")
    # Check for NEXT_PUBLIC_SUPABASE_URL first, then SUPABASE_URL
    supabase_url = config.get("NEXT_PUBLIC_SUPABASE_URL") or config.get("SUPABASE_URL")
    # Prefer service role key for write operations, fallback to anon key
    supabase_key = (
        config.get("SUPABASE_SERVICE_ROLE_KEY") or 
        config.get("NEXT_PUBLIC_SUPABASE_ANON_KEY") or 
        config.get("SUPABASE_ANON_KEY")
    )
    
    if not supabase_url or not supabase_key:
        print("Warning: NEXT_PUBLIC_SUPABASE_URL/SUPABASE_URL and Supabase key not found in .env")
        return None
    
    try:
        return create_client(supabase_url, supabase_key)
    except Exception as e:
        print(f"Error creating Supabase client: {e}")
        return None


def hash_post_id(post_id: str) -> str:
    """Generate SHA256 hash of post_id."""
    return hashlib.sha256(post_id.encode()).hexdigest()


def post_exists(post_id: str, client: Optional[Client] = None) -> bool:
    """Check if a post exists in the database."""
    if client is None:
        client = get_supabase_client()
    
    if client is None:
        return False
    
    try:
        post_id_hash = hash_post_id(post_id)
        response = client.table('posts').select('id').eq('post_id_hash', post_id_hash).limit(1).execute()
        return len(response.data) > 0
    except Exception as e:
        print(f"Error checking if post exists: {e}")
        return False


def upsert_post(
    post_id: str,
    title: str,
    correct_link: Optional[str],
    post_date: datetime,
    client: Optional[Client] = None
) -> Optional[str]:
    """Insert or update a post in the database."""
    if client is None:
        client = get_supabase_client()
    
    if client is None:
        return None
    
    try:
        post_id_hash = hash_post_id(post_id)
        post_data = {
            'post_id': post_id,
            'post_id_hash': post_id_hash,
            'title': title,
            'correct_link': correct_link,
            'post_date': post_date.isoformat()
        }
        
        # Upsert: insert or update on conflict
        response = client.table('posts').upsert(post_data, on_conflict='post_id').execute()
        
        if response.data and len(response.data) > 0:
            return response.data[0].get('id')
        return None
    except Exception as e:
        print(f"Error upserting post: {e}")
        return None


def load_existing_post_ids(client: Optional[Client] = None) -> set[str]:
    """Load all existing post IDs from the database."""
    if client is None:
        client = get_supabase_client()
    
    if client is None:
        return set()
    
    try:
        response = client.table('posts').select('post_id').execute()
        if response.data:
            return {post['post_id'] for post in response.data}
        return set()
    except Exception as e:
        print(f"Error loading existing post IDs: {e}")
        return set()
