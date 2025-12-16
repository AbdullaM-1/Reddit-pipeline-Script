#!/usr/bin/env python
"""
Incremental Pipeline Updater - Processes only new posts

This pipeline is designed for regular updates:
1. Fetches the last N posts from a subreddit (sorted by "new" by default)
2. Checks each post against the existing database (pipeline_results.json)
3. Skips posts that already exist
4. For new posts only:
   - Identifies the correct outbound link from the author's comment
   - Downloads every image found at that link
   - Runs OCR on the downloaded images
   - Stores result in pipeline_results/pipeline_results.json

This script stitches together the previously built modules:
    posts.py                        -> low-level Reddit fetching helpers
    identify_correct_links.py       -> link selection logic
    extract_images_from_links.py    -> HTML/image scraping + download
    extract_text_from_images.py     -> EasyOCR / pytesseract text extraction

Usage:
    python run_full_pipeline.py --subreddit SextStories --max-posts 100

The script will automatically:
- Load existing post IDs from pipeline_results.json and posts.json
- Fetch recent posts from Reddit
- Process only posts that don't exist in the database
- Skip posts that have already been processed
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, TextIO, cast
from requests.exceptions import HTTPError
from collections import defaultdict, Counter
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

from colorama import Fore, Style
from dotenv import dotenv_values

# Try to import boto3 for S3 upload support (optional)
BOTO3_AVAILABLE = False
boto3 = None
ClientError = None
BotoCoreError = None

try:
    import boto3  # type: ignore
    from botocore.exceptions import ClientError, BotoCoreError  # type: ignore
    BOTO3_AVAILABLE = True
except ImportError:
    pass

# Force UTF-8 output on Windows terminals so we can dump the raw JSON safely.
if sys.platform.startswith("win"):
    stdout = cast(TextIO, sys.stdout)
    stderr = cast(TextIO, sys.stderr)
    stdout_reconfigure = getattr(stdout, "reconfigure", None)
    if callable(stdout_reconfigure):
        stdout_reconfigure(encoding="utf-8")
    stderr_reconfigure = getattr(stderr, "reconfigure", None)
    if callable(stderr_reconfigure):
        stderr_reconfigure(encoding="utf-8")
    if not callable(stdout_reconfigure):
        os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from posts import (
    buildComments,
    buildMedia,
    buildPosts,
    fetchAwards,
    fetchPostArticleByPostID,
    getHeaders,
    getSession,
    getToken,
    getUserAgent,
    Post as RedditPost,
)

from identify_correct_links import (
    extract_links_from_text,
    find_author_comment,
    identify_correct_link_with_llm,
)
from extract_images_from_links import (
    download_image,
    extract_images_from_url,
    sanitize_filename,
)
from extract_text_from_images import (
    extract_text_from_image_easyocr,
    extract_text_from_image_pytesseract,
    should_skip_image,
)

# Optional OCR libraries
EASYOCR_AVAILABLE = False
PYTESSERACT_AVAILABLE = False
PYTESSERACT_READY = False
easyocr = None
pytesseract = None
Image = None

try:
    import easyocr  # type: ignore

    EASYOCR_AVAILABLE = True
except Exception:
    try:
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore

        PYTESSERACT_AVAILABLE = True
    except Exception:
        pass

# ============================================
# CONFIGURATION
# ============================================
DEFAULT_SUBREDDIT = "SextStories"
SORT_FILTER = "new"  # hot/new/top/rising/controversial - always use "new" for incremental updates
FETCH_ALL = False  # Changed to False - only fetch recent posts
MAX_POSTS = 100  # Fetch last 100 posts from "new" sort (adjust as needed)
RECENT_POSTS_LIMIT = 100  # Number of recent posts to check for updates
POSTS_PER_REQUEST_AUTH = 100
POSTS_PER_REQUEST_ANON = 25
MAX_LISTING_RETRIES = 5
BASE_BACKOFF_SECONDS = 5
PER_BATCH_SLEEP = 1.0
DEFAULT_QUERY_SEQUENCE = [chr(code) for code in range(ord("a"), ord("z") + 1)]

OUTPUT_ROOT = Path("pipeline_results")
IMAGES_DIR = Path("downloaded_images")
STATE_PATH = Path("pagination_state.json")
AGGREGATE_OUTPUT_FILE = OUTPUT_ROOT / "pipeline_results.json"
CHUNKS_DIR = OUTPUT_ROOT / "chunks"
IMAGES_DIR.mkdir(exist_ok=True)
OUTPUT_ROOT.mkdir(exist_ok=True)
CHUNKS_DIR.mkdir(exist_ok=True)

AGGREGATE_LOCK = threading.Lock()
AGGREGATE_DATA: Dict[str, Any] = {"metadata": {}, "posts": {}, "order": []}
CHUNK_SIZE = 50
PROCESSED_CHUNKS_DIR = Path("processed_link_chunks")
PROCESSED_CHUNKS_DIR = Path("processed_link_chunks")


def init_aggregate_storage(output_file: Optional[Path] = None) -> None:
    global AGGREGATE_DATA, AGGREGATE_OUTPUT_FILE
    if output_file:
        AGGREGATE_OUTPUT_FILE = output_file
    AGGREGATE_DATA = {"metadata": {}, "posts": {}, "order": []}
    if AGGREGATE_OUTPUT_FILE.exists():
        try:
            data = json.loads(AGGREGATE_OUTPUT_FILE.read_text(encoding="utf-8"))
            AGGREGATE_DATA["metadata"] = data.get("metadata", {})
            AGGREGATE_DATA["posts"] = data.get("posts", {})
            AGGREGATE_DATA["order"] = data.get("order", [])
        except json.JSONDecodeError:
            print(
                f"{Fore.YELLOW}Aggregated pipeline results corrupted; starting fresh.{Style.RESET_ALL}"
            )
    record_restart_chunk(len(AGGREGATE_DATA.get("order", [])))


def check_post_exists_in_supabase(post_id: str) -> bool:
    """
    Check if a single post exists in Supabase database.
    Uses efficient hash-based lookup instead of loading all IDs.
    
    Args:
        post_id: Reddit post ID to check
    
    Returns:
        True if post exists, False otherwise
    """
    try:
        from supabase_client import post_exists, get_supabase_client
        supabase_client = get_supabase_client()
        if supabase_client:
            return post_exists(post_id, supabase_client)
        return False
    except ImportError:
        return False
    except Exception as e:
        print(f"{Fore.YELLOW}Error checking post in Supabase: {e}{Style.RESET_ALL}")
        return False


def save_post_to_supabase(
    post_id: str,
    title: str,
    correct_link: str | None,
    post_date: datetime | None,
    post_data: RedditPost | None = None
) -> bool:
    """
    Save processed post to Supabase database after successful processing.
    
    Args:
        post_id: Reddit post ID
        title: Post title
        correct_link: Identified correct link from author's comment
        post_date: Date of the post (datetime object)
        post_data: Optional Reddit post data to extract date from if post_date is None
    
    Returns:
        True if successful, False otherwise
    """
    try:
        from supabase_client import upsert_post, get_supabase_client
        
        supabase_client = get_supabase_client()
        if not supabase_client:
            print(f"{Fore.YELLOW}Supabase client not available, skipping database save{Style.RESET_ALL}")
            return False
        
        # Extract post date if not provided
        if post_date is None and post_data:
            created_utc = post_data.get("created_utc")
            if created_utc:
                try:
                    if isinstance(created_utc, (int, float)):
                        post_date = datetime.fromtimestamp(created_utc)
                    elif isinstance(created_utc, str) and created_utc.isdigit():
                        post_date = datetime.fromtimestamp(int(created_utc))
                except (ValueError, OSError):
                    pass
        
        # Use current time as fallback if date still not available
        if post_date is None:
            post_date = datetime.now()
            print(f"{Fore.YELLOW}Warning: Could not extract post date, using current time{Style.RESET_ALL}")
        
        # Save to Supabase
        result_id = upsert_post(
            post_id=post_id,
            title=title,
            correct_link=correct_link,
            post_date=post_date,
            client=supabase_client
        )
        
        if result_id:
            print(f"{Fore.GREEN}[SUPABASE] Saved post {post_id} to database{Style.RESET_ALL}")
            return True
        else:
            print(f"{Fore.YELLOW}[SUPABASE] Failed to save post {post_id} to database{Style.RESET_ALL}")
            return False
            
    except ImportError:
        print(f"{Fore.YELLOW}supabase_client not available, skipping database save{Style.RESET_ALL}")
        return False
    except Exception as e:
        print(f"{Fore.YELLOW}Error saving post to Supabase: {e}{Style.RESET_ALL}")
        return False


def load_all_existing_post_ids() -> set[str]:
    """
    Load existing post IDs from local files only (pipeline_results.json, posts.json).
    
    NOTE: We do NOT load all IDs from Supabase here - that would be inefficient.
    Instead, we check Supabase individually for each post using check_post_exists_in_supabase()
    which uses the hash index for fast O(1) lookups.
    
    Returns a set of post IDs from local files only.
    """
    existing_ids: set[str] = set()
    
    # Load from pipeline_results/pipeline_results.json if it exists
    try:
        if AGGREGATE_OUTPUT_FILE.exists():
            with open(AGGREGATE_OUTPUT_FILE, "r", encoding="utf-8") as fp:
                pipeline_data = json.load(fp)
                posts_dict = pipeline_data.get("posts", {})
                for post_id in posts_dict.keys():
                    existing_ids.add(post_id)
            print(f"{Fore.CYAN}Loaded {len(existing_ids)} existing post IDs from pipeline_results.json{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.YELLOW}Error loading pipeline_results.json: {e}{Style.RESET_ALL}")
    
    # Also load from posts.json if it exists (from the main run.py pipeline)
    try:
        posts_json_path = Path("posts.json")
        if posts_json_path.exists():
            posts_from_file = 0
            with open(posts_json_path, "r", encoding="utf-8") as fp:
                posts_list = json.load(fp)
                for post in posts_list:
                    post_id = post.get("id", "")
                    if post_id:
                        existing_ids.add(post_id)
                        posts_from_file += 1
            if posts_from_file > 0:
                print(f"{Fore.CYAN}Also loaded {posts_from_file} post IDs from posts.json{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.YELLOW}Error loading posts.json: {e}{Style.RESET_ALL}")
    
    return existing_ids


def get_existing_result(post_id: str) -> Optional[Dict[str, Any]]:
    with AGGREGATE_LOCK:
        posts = AGGREGATE_DATA.setdefault("posts", {})
        return posts.get(post_id)


def _atomic_replace(tmp_path: Path, target_path: Path, attempts: int = 5) -> bool:
    """Try replacing target with tmp file, retrying on intermittent permission errors."""
    for attempt in range(attempts):
        try:
            tmp_path.replace(target_path)
            return True
        except PermissionError:
            if attempt == attempts - 1:
                return False
            time.sleep(0.1 * (attempt + 1))
    return False


def persist_result(payload: Dict[str, Any]) -> None:
    with AGGREGATE_LOCK:
        posts = AGGREGATE_DATA.setdefault("posts", {})
        order = AGGREGATE_DATA.setdefault("order", [])
        posts[payload["post_id"]] = payload
        if payload["post_id"] not in order:
            order.append(payload["post_id"])
        metadata = AGGREGATE_DATA.setdefault("metadata", {})
        metadata["total_posts"] = len(posts)
        metadata["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        metadata.setdefault("chunk_ids", [])
        maybe_create_chunk(metadata["total_posts"], metadata)
        tmp_path = AGGREGATE_OUTPUT_FILE.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(AGGREGATE_DATA, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        if not _atomic_replace(tmp_path, AGGREGATE_OUTPUT_FILE):
            print(
                f"{Fore.YELLOW}Unable to replace {AGGREGATE_OUTPUT_FILE.name}; "
                "falling back to overwriting directly.{Style.RESET_ALL}"
            )
            try:
                AGGREGATE_OUTPUT_FILE.write_text(
                    tmp_path.read_text(encoding="utf-8"), encoding="utf-8"
                )
            except Exception as exc:
                print(
                    f"{Fore.RED}Failed to persist aggregated results: {exc}{Style.RESET_ALL}"
                )
            finally:
                tmp_path.unlink(missing_ok=True)


def maybe_create_chunk(total_posts: int, metadata: Dict[str, Any]) -> None:
    if CHUNK_SIZE <= 0 or total_posts == 0 or total_posts % CHUNK_SIZE != 0:
        return
    chunk_id = total_posts // CHUNK_SIZE
    chunk_ids = metadata.setdefault("chunk_ids", [])
    if chunk_id in chunk_ids:
        return

    order = AGGREGATE_DATA.get("order", [])
    start_index = (chunk_id - 1) * CHUNK_SIZE
    end_index = chunk_id * CHUNK_SIZE
    chunk_post_ids = order[start_index:end_index]
    if len(chunk_post_ids) < CHUNK_SIZE:
        return

    posts = AGGREGATE_DATA.get("posts", {})
    chunk_posts = {pid: posts[pid] for pid in chunk_post_ids if pid in posts}
    chunk_payload = {
        "chunk_id": chunk_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "range": {
            "start": start_index + 1,
            "end": min(end_index, total_posts),
            "count": len(chunk_post_ids),
        },
        "posts": chunk_posts,
    }
    chunk_path = CHUNKS_DIR / f"pipeline_results_chunk_{chunk_id:04d}.json"
    tmp_path = chunk_path.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(chunk_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    tmp_path.replace(chunk_path)
    chunk_ids.append(chunk_id)


def record_restart_chunk(processed_posts: int) -> None:
    restart_payload = {
        "type": "restart_snapshot",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "processed_posts": processed_posts,
    }
    filename = f"pipeline_restart_{int(time.time())}.json"
    restart_path = CHUNKS_DIR / filename
    restart_path.write_text(
        json.dumps(restart_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def load_pagination_state(subreddit: str, sort_filter: str) -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if data.get("subreddit") != subreddit or data.get("sort") != sort_filter:
        return {}
    return data


def save_pagination_state(
    subreddit: str,
    sort_filter: str,
    after: Optional[str],
    last_post_fullname: Optional[str],
    processed_total: int,
) -> None:
    payload = {
        "subreddit": subreddit,
        "sort": sort_filter,
        "after": after,
        "last_post_fullname": last_post_fullname,
        "processed_posts": processed_total,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    tmp_path = STATE_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(STATE_PATH)

# Reddit credentials (optional but recommended to avoid 429s)
config = dotenv_values(".env")
USERNAME = config.get("username", "")
PASSWORD = config.get("password", "")
CLIENT_ID = config.get("client_id", "")
CLIENT_SECRET = config.get("client_secret", "")
# ============================================


def ensure_token() -> str:
    """Fetch OAuth token if creds exist; otherwise empty string."""
    if not all([CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD]):
        return ""
    params = {
        "grant_type": "password",
        "username": USERNAME,
        "password": PASSWORD,
    }
    return getToken(params, 10)


def fetch_listing(
    session, url: str, headers: Dict[str, str], params: Dict[str, str]
) -> Optional[dict]:
    """Fetch subreddit listing with retries/backoff."""
    for attempt in range(1, MAX_LISTING_RETRIES + 1):
        try:
            response = session.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 429:
                raise ConnectionError("HTTP 429 Too Many Requests")
            response.raise_for_status()
            return response.json()
        except Exception as err:
            if attempt == MAX_LISTING_RETRIES:
                print(
                    f"{Fore.RED}Listing failed after {attempt} attempts: {err}{Style.RESET_ALL}"
                )
                return None
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(
                f"{Fore.YELLOW}Listing attempt {attempt}/{MAX_LISTING_RETRIES} failed: {err}{Style.RESET_ALL}"
            )
            print(f"{Fore.YELLOW}Sleeping {backoff}s before retrying...{Style.RESET_ALL}")
            time.sleep(backoff)
    return None


OCR_LOCK = threading.Lock()


def normalize_fullname(value: str) -> str:
    if value.startswith("t3_"):
        return value
    return f"t3_{value}"


def extract_fullname_from_link(link: str) -> Optional[str]:
    pattern = r"/comments/([a-z0-9]{6,})"
    match = re.search(pattern, link or "", re.IGNORECASE)
    if match:
        return f"t3_{match.group(1)}"
    return None


def load_chunk_links(path: Path) -> List[str]:
    if not path.exists():
        print(f"{Fore.YELLOW}Link chunk {path} not found; skipping.{Style.RESET_ALL}")
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        print(f"{Fore.YELLOW}Failed to decode chunk {path}; skipping.{Style.RESET_ALL}")
        return []
    return [link for link in payload.get("unprocessed_links", []) if isinstance(link, str)]


def fetch_post_article(
    subreddit: str, post_id: str, token: str, log_response: bool = False
) -> Dict[str, Any]:
    attempts = 0
    while attempts < 3:
        try:
            detail = cast(
                Dict[str, Any], fetchPostArticleByPostID(subreddit, post_id, token)
            )
            if log_response:
                print(
                    f"{Fore.BLUE}Endpoint payload for {post_id}:{Style.RESET_ALL}\n"
                    f"{json.dumps(detail, indent=2, ensure_ascii=False)}"
                )
            return detail
        except HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            if status == 429:
                backoff = BASE_BACKOFF_SECONDS * (2 ** attempts)
                print(
                    f"{Fore.YELLOW}Rate limited fetching {post_id}; sleeping {backoff}s...{Style.RESET_ALL}"
                )
                time.sleep(backoff)
                attempts += 1
                continue
            print(
                f"{Fore.RED}Fail to get post by id Status Code:{status or exc} Subreddit name:{subreddit} Error :{exc}{Style.RESET_ALL}"
            )
            return {}
    print(f"{Fore.RED}Giving up on post {post_id} after repeated 429s.{Style.RESET_ALL}")
    return {}


def hydrate_posts_from_links(
    links: Sequence[str],
    subreddit: str,
    token: str,
    *,
    log_endpoint_results: bool = False,
) -> List[RedditPost]:
    if not subreddit.startswith("r/"):
        subreddit = f"r/{subreddit}"
    posts: List[RedditPost] = []
    for link in links:
        fullname = extract_fullname_from_link(link)
        if not fullname:
            continue
        post_id = fullname.split("_", 1)[1]
        detail = fetch_post_article(
            subreddit, post_id, token, log_response=log_endpoint_results
        )
        if log_endpoint_results:
            result_state = detail.get("result_state") or {}
            print(
                f"{Fore.BLUE}Endpoint detail for {post_id}: {result_state}{Style.RESET_ALL}"
            )
        payload = detail.get("post") or []
        post_data: Dict[str, Any] = {
            "id": post_id,
            "name": fullname,
            "subreddit": subreddit,
        }
        if isinstance(payload, list) and payload:
            first = payload[0] if payload else None
            if first:
                first_data = first.get("data", {})
                post_children = first_data.get("children", [])
                if post_children:
                    post_detail_data = post_children[0].get("data", {})
                else:
                    post_detail_data = first_data
                post_data.update(post_detail_data)
                media_children = post_detail_data.get("children", [])
                if media_children:
                    post_data["media_content"] = buildMedia(
                        media_children[0].get("data", {})
                    )
            if len(payload) > 1:
                comments_children = payload[1].get("data", {}).get("children", [])
                subreddit_users = defaultdict(set)
                comments, num_comments = buildComments(
                    comments_children,
                    subreddit_users,
                    post_data.get("subreddit_id", ""),
                    post_data.get("subreddit", subreddit),
                )
                post_data["comments"] = comments
                post_data["num_comments"] = num_comments
        post_data["_detail_payload"] = payload
        posts.append(cast(RedditPost, post_data))
    return posts


def write_processed_chunk(chunk_path: Path, links: Sequence[str]) -> None:
    PROCESSED_CHUNKS_DIR.mkdir(exist_ok=True, parents=True)
    dest = PROCESSED_CHUNKS_DIR / f"{chunk_path.stem}.processed.json"
    dest.write_text(
        json.dumps({"processed_links": list(links)}, indent=2), encoding="utf-8"
    )


def run_chunk_mode(
    chunk_paths: Sequence[Path],
    subreddit: str,
    reader: Any | None,
    workers: int,
    ocr_workers: int,
    max_posts: Optional[int],
) -> None:
    token = ensure_token()
    processed_total = 0
    for chunk in chunk_paths:
        chunk_result_path = OUTPUT_ROOT / f"{chunk.stem}.json"
        init_aggregate_storage(chunk_result_path)
        print(
            f"{Fore.CYAN}Writing chunk results to {chunk_result_path}{Style.RESET_ALL}"
        )
        if max_posts and processed_total >= max_posts:
            break
        links = load_chunk_links(chunk)
        if not links:
            continue
        for offset in range(0, len(links), workers):
            if max_posts and processed_total >= max_posts:
                break
            batch_links = links[offset : offset + workers]
            posts = hydrate_posts_from_links(
                batch_links, subreddit, token, log_endpoint_results=True
            )
            if not posts:
                continue
            if max_posts:
                remaining = max_posts - processed_total
                if remaining <= 0:
                    break
                posts = posts[:remaining]
            process_batch_concurrently(posts, reader, workers, ocr_workers)
            processed_total += len(posts)
        write_processed_chunk(chunk, links)
    print(
        f"\n{Fore.CYAN}Chunk pipeline complete. Processed {processed_total} posts from link chunks.{Style.RESET_ALL}"
    )


def stream_full_post_batches(
    subreddit: str,
    sort_filter: str,
    fetch_all: bool,
    max_posts: Optional[int],
    resume_after: Optional[str] = None,
) -> Generator[List[RedditPost], None, None]:
    """Yield batches of full post objects (with comments, media) by paging through Reddit listings."""
    token = ensure_token()
    session = getSession()
    awards = fetchAwards()

    if not subreddit.startswith("r/"):
        subreddit = f"r/{subreddit}"

    posts_per_request = (
        min(POSTS_PER_REQUEST_AUTH, 100)
        if token
        else min(POSTS_PER_REQUEST_ANON, 25)
    )

    fetched = 0
    # Always start fresh to get the newest posts (ignore saved pagination state)
    after = None
    if resume_after:
        after = normalize_fullname(resume_after)
        print(
            f"{Fore.YELLOW}Resuming listing from user-specified cursor {after}{Style.RESET_ALL}"
        )
    else:
        print(f"{Fore.CYAN}Starting fresh from {subreddit} ({sort_filter}) - fetching newest posts{Style.RESET_ALL}")
    batch = 0

    print(f"{Fore.CYAN}Starting stream from {subreddit} ({sort_filter}){Style.RESET_ALL}")
    if fetch_all:
        print(
            f"{Fore.YELLOW}Full-history mode: will fetch until Reddit returns no 'after' token.{Style.RESET_ALL}"
        )

    while True:
        if max_posts and fetched >= max_posts:
            break
        current_limit = posts_per_request
        if max_posts:
            current_limit = min(current_limit, max_posts - fetched)
            if current_limit <= 0:
                break

        batch += 1
        params = {"limit": current_limit, "show": "all", "sr_detail": True}
        if after:
            params["after"] = after

        url = (
            f"https://oauth.reddit.com/{subreddit}/{sort_filter}.json"
            if token
            else f"https://www.reddit.com/{subreddit}/{sort_filter}.json"
        )
        headers = getHeaders(getUserAgent(), token)

        if after:
            print(
                f"{Fore.CYAN}Fetching batch {batch} (limit={current_limit}, after={after})...{Style.RESET_ALL}"
            )
        else:
            print(
                f"{Fore.CYAN}Fetching batch {batch} (limit={current_limit}) - starting from newest posts...{Style.RESET_ALL}"
            )
        listing = fetch_listing(session, url, headers, params)
        if not listing or "data" not in listing:
            print(f"{Fore.YELLOW}No listing data returned; stopping.{Style.RESET_ALL}")
            break

        raw_children = listing["data"].get("children", [])
        if not raw_children:
            print(f"{Fore.YELLOW}Listing returned zero children; stopping.{Style.RESET_ALL}")
            break

        built_posts = buildPosts(listing, awards)
        if max_posts and fetched + len(built_posts) > max_posts:
            remaining = max_posts - fetched
            built_posts = built_posts[:remaining]
        print(
            f"{Fore.GREEN}Received {len(built_posts)} posts in batch {batch}{Style.RESET_ALL}"
        )

        batch_posts: List[RedditPost] = []
        for post in built_posts:
            post_id = post.get("id")
            detail = fetchPostArticleByPostID(
                post.get("subreddit", subreddit), post_id, token
            )
            payload = detail.get("post") or []
            if isinstance(payload, list) and payload:
                first = payload[0] if len(payload) > 0 else None
                if first:
                    media_children = first.get("data", {}).get("children", [])
                    if media_children:
                        post["media_content"] = buildMedia(
                            media_children[0].get("data", {})
                        )
                if len(payload) > 1:
                    comments_children = payload[1].get("data", {}).get("children", [])
                    subreddit_users = defaultdict(set)
                    comments, num_comments = buildComments(
                        comments_children,
                        subreddit_users,
                        post.get("subreddit_id", ""),
                        post.get("subreddit", subreddit),
                    )
                    post["comments"] = comments
                    post["num_comments"] = num_comments

            fetched += 1
            batch_posts.append(post)

        if batch_posts:
            yield batch_posts

        last_fullname = (
            batch_posts[-1].get("name") if batch_posts else None
        )
        save_pagination_state(
            subreddit=subreddit,
            sort_filter=sort_filter,
            after=after,
            last_post_fullname=last_fullname,
            processed_total=fetched,
        )
        after = listing["data"].get("after")
        if not after or (not fetch_all and max_posts and fetched >= max_posts):
            break

        time.sleep(PER_BATCH_SLEEP)


def stream_search_query_batches(
    subreddit: str,
    sort_filter: str,
    queries: List[str],
    max_posts: Optional[int],
    page_limit: Optional[int],
    cid: Optional[str],
    iid: Optional[str],
    include_over_18: bool,
    safe: Optional[str],
) -> Generator[List[RedditPost], None, None]:
    token = ensure_token()
    session = getSession()
    awards = fetchAwards()

    if not subreddit.startswith("r/"):
        subreddit = f"r/{subreddit}"

    posts_per_request = (
        min(POSTS_PER_REQUEST_AUTH, 100)
        if token
        else min(POSTS_PER_REQUEST_ANON, 25)
    )
    fetched = 0
    page_counter = defaultdict(int)
    print(
        f"{Fore.CYAN}Starting search-based stream from {subreddit} ({sort_filter}){Style.RESET_ALL}"
    )
    for query in queries:
        if max_posts and fetched >= max_posts:
            break
        if not query:
            continue
        after = None
        while True:
            if max_posts and fetched >= max_posts:
                break
            current_limit = posts_per_request if page_limit is None else min(posts_per_request, page_limit)
            page_counter[query] += 1
            params = {
                "limit": current_limit,
                "restrict_sr": "on",
                "syntax": "cloudsearch",
                "q": query,
                "show": "all",
            }
            if sort_filter:
                params["sort"] = sort_filter
            if include_over_18:
                params["include_over_18"] = "on"
            if safe:
                params["safe"] = safe
            if cid:
                params["cId"] = cid
            if iid:
                params["iId"] = iid
            if after:
                params["after"] = after

            url = (
                f"https://oauth.reddit.com/{subreddit}/search.json"
                if token
                else f"https://www.reddit.com/{subreddit}/search.json"
            )
            headers = getHeaders(getUserAgent(), token)
            print(
                f"{Fore.CYAN}Search query '{query}' page {page_counter[query]} (limit={current_limit}, after={after}){Style.RESET_ALL}"
            )
            listing = fetch_listing(session, url, headers, params)
            if not listing or "data" not in listing:
                print(
                    f"{Fore.YELLOW}Search query '{query}' returned no listing data; moving to next query.{Style.RESET_ALL}"
                )
                break

            raw_children = listing["data"].get("children", [])
            if not raw_children:
                print(
                    f"{Fore.YELLOW}Search query '{query}' returned zero children; moving to next query.{Style.RESET_ALL}"
                )
                break

            built_posts = buildPosts(listing, awards)
            print(
                f"{Fore.GREEN}Search query '{query}' page {page_counter[query]} returned {len(built_posts)} posts{Style.RESET_ALL}"
            )

            batch_posts: List[RedditPost] = []
            for post in built_posts:
                post_id = post.get("id")
                detail = fetchPostArticleByPostID(
                    post.get("subreddit", subreddit), post_id, token
                )
                payload = detail.get("post") or []
                if isinstance(payload, list) and payload:
                    first = payload[0] if len(payload) > 0 else None
                    if first:
                        media_children = first.get("data", {}).get("children", [])
                        if media_children:
                            post["media_content"] = buildMedia(
                                media_children[0].get("data", {})
                            )
                    if len(payload) > 1:
                        comments_children = payload[1].get("data", {}).get("children", [])
                        subreddit_users = defaultdict(set)
                        comments, num_comments = buildComments(
                            comments_children,
                            subreddit_users,
                            post.get("subreddit_id", ""),
                            post.get("subreddit", subreddit),
                        )
                        post["comments"] = comments
                        post["num_comments"] = num_comments

                fetched += 1
                batch_posts.append(post)

            if batch_posts:
                yield batch_posts

            after_token = listing["data"].get("after")
            if not after_token:
                print(
                    f"{Fore.YELLOW}Search query '{query}' exhausted after {page_counter[query]} pages.{Style.RESET_ALL}"
                )
                break
            after = after_token

            time.sleep(PER_BATCH_SLEEP)



def step_identify_correct_link(post: RedditPost) -> Optional[str]:
    post_title = post.get("title", "")
    author_comment = find_author_comment(cast(Dict[str, Any], post))
    if not author_comment:
        author_comment = extract_author_comment_from_payload(post)
        if not author_comment:
            print(f"  {Fore.YELLOW}No author comment found.{Style.RESET_ALL}")
            return None
    links = extract_links_from_text(author_comment.get("body", ""))
    if not links:
        print(f"  {Fore.YELLOW}No links in author comment.{Style.RESET_ALL}")
        return None
    if len(links) == 1:
        return links[0]
    return identify_correct_link_with_llm(
        post_title=post_title,
        author_comment=author_comment.get("body", ""),
        links=links,
        use_openai=True,
    )


def extract_author_comment_from_payload(post: RedditPost) -> Optional[Dict[str, Any]]:
    author = post.get("author")
    payload = post.get("_detail_payload") or []
    if not author or not isinstance(payload, list) or len(payload) < 2:
        return None
    comments_children = payload[1].get("data", {}).get("children", [])
    if not comments_children:
        return None
    subreddit_users = defaultdict(set)
    comments, _ = buildComments(
        comments_children,
        subreddit_users,
        post.get("subreddit_id", ""),
        post.get("subreddit", ""),
    )
    temp_post = {"author": author, "comments": comments}
    return find_author_comment(temp_post)


def step_download_images(post_id: str, correct_link: str) -> Dict:
    session = getSession()
    image_urls = extract_images_from_url(correct_link, session)
    print(
        f"  {Fore.CYAN}Image extraction found {len(image_urls)} URLs for post {post_id}{Style.RESET_ALL}"
    )
    post_dir = IMAGES_DIR / sanitize_filename(post_id)
    post_dir.mkdir(parents=True, exist_ok=True)

    local_images = []
    for idx, url in enumerate(image_urls, 1):
        filename = sanitize_filename(Path(url).name) or f"image_{idx}"
        save_path = post_dir / filename
        counter = 1
        while save_path.exists():
            save_path = post_dir / f"{save_path.stem}_{counter}{save_path.suffix}"
            counter += 1
        if download_image(url, str(save_path), session):
            print(f"    {Fore.GREEN}[saved] Image {idx}: {save_path.name}{Style.RESET_ALL}")
            local_images.append(
                {
                    "url": url,
                    "local_path": str(save_path),
                }
            )
        else:
            print(f"    {Fore.YELLOW}[skip] Failed download {idx}: {url[:80]}{Style.RESET_ALL}")
    return {
        "image_urls": image_urls,
        "local_images": local_images,
        "downloaded_count": len(local_images),
    }


def init_ocr_reader():
    if not EASYOCR_AVAILABLE or easyocr is None:
        return None
    try:
        print(
            f"{Fore.CYAN}Initializing EasyOCR ({'GPU' if False else 'CPU'})...{Style.RESET_ALL}"
        )
        return easyocr.Reader(["en"], gpu=False)
    except Exception as err:
        print(f"{Fore.YELLOW}EasyOCR init failed: {err}{Style.RESET_ALL}")
        return None


def ensure_pytesseract_ready() -> bool:
    """Make sure pytesseract can find the native tesseract binary."""
    global PYTESSERACT_READY
    if not PYTESSERACT_AVAILABLE or pytesseract is None:
        return False
    if PYTESSERACT_READY:
        return True

    candidate_paths = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        r"C:\Tesseract-OCR\tesseract.exe",
    ]

    current_cmd = getattr(pytesseract.pytesseract, "tesseract_cmd", "")
    if current_cmd and os.path.exists(current_cmd):
        PYTESSERACT_READY = True
        return True

    for candidate in candidate_paths:
        if os.path.exists(candidate):
            pytesseract.pytesseract.tesseract_cmd = candidate
            PYTESSERACT_READY = True
            print(f"{Fore.GREEN}Using Tesseract at: {candidate}{Style.RESET_ALL}")
            return True

    print(
        f"{Fore.YELLOW}Tesseract executable not found. Install from https://github.com/UB-Mannheim/tesseract/wiki or set pytesseract.pytesseract.tesseract_cmd manually.{Style.RESET_ALL}"
    )
    return False


def step_ocr_streamed(local_images: List[Dict], reader: Any | None, ocr_workers: int) -> Dict:
    ocr_queue: Queue[Optional[str]] = Queue()
    ocr_results: List[Dict[str, Any]] = []
    summary = {
        "images_processed": 0,
        "total_chars": 0,
        "total_lines": 0,
        "image_results": ocr_results,
    }
    result_lock = threading.Lock()

    def ocr_worker() -> None:
        if not reader and PYTESSERACT_AVAILABLE:
            ensure_pytesseract_ready()

        while True:
            image_path = ocr_queue.get()
            if image_path is None:
                ocr_queue.task_done()
                break

            filename = os.path.basename(image_path)
            if should_skip_image(filename):
                ocr_queue.task_done()
                continue

            if reader:
                with OCR_LOCK:
                    extraction = extract_text_from_image_easyocr(image_path, reader)
            elif PYTESSERACT_AVAILABLE:
                with OCR_LOCK:
                    extraction = extract_text_from_image_pytesseract(image_path)
            else:
                extraction = {"success": False, "error": "No OCR backend available."}

            with result_lock:
                if extraction.get("success"):
                    summary["total_chars"] += extraction.get("char_count", 0)
                    summary["total_lines"] += extraction.get("line_count", 0)
                summary["images_processed"] += 1
                ocr_results.append(
                    {
                        "filename": filename,
                        "file_path": image_path,
                        "extraction": extraction,
                    }
                )

            ocr_queue.task_done()

    with ThreadPoolExecutor(max_workers=ocr_workers) as executor:
        for _ in range(ocr_workers):
            executor.submit(ocr_worker)
        for img in local_images:
            path = img.get("local_path")
            if path and os.path.exists(path):
                ocr_queue.put(path)
        for _ in range(ocr_workers):
            ocr_queue.put(None)
        ocr_queue.join()

    print(
        f"  {Fore.GREEN}OCR summary: {summary['images_processed']} images, "
        f"{summary['total_lines']} lines, {summary['total_chars']} chars{Style.RESET_ALL}"
    )
    return summary


def cleanup_images(local_images: List[Dict[str, Any]], post_id: str) -> None:
    """
    Delete downloaded images after processing is complete.
    
    Args:
        local_images: List of image dictionaries with 'local_path' key
        post_id: Post ID for logging purposes
    """
    deleted_count = 0
    failed_count = 0
    
    for img in local_images:
        image_path = img.get("local_path")
        if not image_path:
            continue
            
        try:
            path = Path(image_path)
            if path.exists() and path.is_file():
                path.unlink()  # Delete the file
                deleted_count += 1
        except Exception as e:
            failed_count += 1
            print(f"{Fore.YELLOW}Warning: Failed to delete image {image_path}: {e}{Style.RESET_ALL}")
    
    # Try to delete the post directory if it's empty
    try:
        post_dir = IMAGES_DIR / sanitize_filename(post_id)
        if post_dir.exists() and post_dir.is_dir():
            # Check if directory is empty
            try:
                post_dir.rmdir()  # Will only work if directory is empty
                print(f"{Fore.CYAN}[CLEANUP] Deleted empty directory: {post_dir}{Style.RESET_ALL}")
            except OSError:
                # Directory not empty, that's okay - just leave it
                pass
    except Exception as e:
        # Ignore directory deletion errors
        pass
    
    if deleted_count > 0:
        print(f"{Fore.GREEN}[CLEANUP] Deleted {deleted_count} images for post {post_id}{Style.RESET_ALL}")
    if failed_count > 0:
        print(f"{Fore.YELLOW}[CLEANUP] Failed to delete {failed_count} images for post {post_id}{Style.RESET_ALL}")


def process_post(post: RedditPost, reader: Any | None, ocr_workers: int, existing_post_ids: set[str] | None = None) -> Dict[str, Any]:
    post_id = str(post.get("id") or post.get("name") or "unknown_post")
    title = post.get("title", "")
    print(f"\n{Fore.MAGENTA}===== Processing post {post_id}: {title[:60]} ====={Style.RESET_ALL}")

    # Note: We already filtered this post before calling process_post(), so it's guaranteed to be new
    # Skip redundant checks and proceed directly to processing
    
    print(f"{Fore.CYAN}[PROCESSING] Post {post_id}: {title[:60]} - starting processing...{Style.RESET_ALL}")

    result: Dict[str, Any] = {
        "post_id": post_id,
        "title": title,
        "status": "started",
    }

    link = step_identify_correct_link(post)
    if not link:
        result["status"] = "no_link"
        persist_result(result)
        
        print(f"{Fore.YELLOW}[NO LINK] Post {post_id}: {title[:60]} - no correct link found{Style.RESET_ALL}")
        
        # Save to Supabase even if no link found (still record the post)
        save_post_to_supabase(
            post_id=post_id,
            title=title,
            correct_link=None,
            post_date=None,
            post_data=post
        )
        
        # Mark as processed
        if existing_post_ids is not None:
            existing_post_ids.add(post_id)
        
        return result

    print(f"{Fore.CYAN}[LINK FOUND] Post {post_id}: {title[:60]} - correct link: {link[:50]}...{Style.RESET_ALL}")
    result["correct_link"] = link
    image_result = step_download_images(post_id, link)
    result["images"] = image_result

    if not image_result["local_images"]:
        result["status"] = "no_images"
        persist_result(result)
        
        print(f"{Fore.YELLOW}[NO IMAGES] Post {post_id}: {title[:60]} - no images found at link{Style.RESET_ALL}")
        
        # Save to Supabase even if no images found (still record the post with link)
        save_post_to_supabase(
            post_id=post_id,
            title=title,
            correct_link=link,
            post_date=None,
            post_data=post
        )
        
        # Mark as processed
        if existing_post_ids is not None:
            existing_post_ids.add(post_id)
        
        return result

    print(f"{Fore.CYAN}[DOWNLOADED] Post {post_id}: {title[:60]} - downloaded {len(image_result.get('local_images', []))} images, starting OCR...{Style.RESET_ALL}")
    ocr_result = step_ocr_streamed(image_result["local_images"], reader, ocr_workers)
    result["ocr"] = ocr_result
    result["status"] = "success"

    persist_result(result)
    
    # Save to Supabase database after successful processing
    correct_link = result.get("correct_link")
    save_success = save_post_to_supabase(
        post_id=post_id,
        title=title,
        correct_link=correct_link,
        post_date=None,  # Will be extracted from post_data
        post_data=post
    )
    
    # Delete images after processing is complete (OCR done, data saved)
    cleanup_images(image_result["local_images"], post_id)
    
    print(f"{Fore.GREEN}[COMPLETE] Post {post_id}: {title[:60]} - processing complete (status: success){Style.RESET_ALL}")
    
    # Mark as processed in existing_post_ids set to avoid duplicate processing in same run
    if existing_post_ids is not None:
        existing_post_ids.add(post_id)
    
    return result


def process_batch_concurrently(
    posts: List[RedditPost], reader: Any | None, worker_count: int, ocr_workers: int, existing_post_ids: set[str] | None = None
) -> None:
    if not posts:
        return
    worker_count = max(1, min(worker_count, len(posts)))
    print(
        f"{Fore.CYAN}Processing batch of {len(posts)} posts using {worker_count} workers...{Style.RESET_ALL}"
    )

    failures = 0
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_post = {
            executor.submit(process_post, post, reader, ocr_workers, existing_post_ids): post for post in posts
        }
        for future in as_completed(future_to_post):
            post = future_to_post[future]
            try:
                future.result()
            except Exception as err:
                failures += 1
                pid = post.get("id") or post.get("name") or "unknown"
                print(
                    f"{Fore.RED}[batch] Post {pid} failed with: {err}{Style.RESET_ALL}"
                )

    if failures:
        print(
            f"{Fore.YELLOW}Batch completed with {failures}/{len(posts)} posts failing.{Style.RESET_ALL}"
        )
    else:
        print(
            f"{Fore.GREEN}Batch completed: all {len(posts)} posts processed successfully.{Style.RESET_ALL}"
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Incremental pipeline updater: fetches recent posts and processes only new ones that don't exist in database."
    )
    parser.add_argument("--subreddit", default=DEFAULT_SUBREDDIT)
    parser.add_argument("--sort", default=SORT_FILTER)
    parser.add_argument(
        "--max-posts",
        type=int,
        default=MAX_POSTS if MAX_POSTS is not None else RECENT_POSTS_LIMIT,
        help=f"Number of recent posts to fetch and check for updates (default: {RECENT_POSTS_LIMIT}).",
    )
    parser.add_argument(
        "--fetch-all",
        action="store_true",
        default=FETCH_ALL,
        help="Keep paging until Reddit stops returning listings (not recommended for update mode).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=20,
        help="Number of concurrent worker threads per batch.",
    )
    parser.add_argument(
        "--ocr-workers",
        type=int,
        default=45,
        help="Number of parallel OCR worker threads per post.",
    )
    parser.add_argument(
        "--resume-after",
        type=str,
        default=None,
        help="Reddit fullname (or post id) to resume after; takes precedence over stored cursor.",
    )
    parser.add_argument(
        "--search-queries",
        "-Q",
        nargs="?",
        const=",".join(DEFAULT_QUERY_SEQUENCE),
        help=(
            "Enable search-based pagination using comma-separated q values "
            "(defaults to the alphabet)."
        ),
    )
    parser.add_argument(
        "--search-limit",
        type=int,
        default=None,
        help="Override per-search request limit (defaults to the API max for authenticated/anon).",
    )
    parser.add_argument("--search-cid", help="Optional cId to send to search requests.")
    parser.add_argument("--search-iid", help="Optional iId to send to search requests.")
    parser.add_argument(
        "--search-include-over-18",
        action="store_true",
        help="Add include_over_18=on when using the search endpoint (required for NSFW subs).",
    )
    parser.add_argument(
        "--link-chunks",
        "-L",
        nargs="+",
        type=Path,
        help="Process pre-collected link chunk files instead of scraping the listing.",
    )
    parser.add_argument(
        "--search-safe",
        choices=["true", "false"],
        help="Set the safe parameter (true/false) when using search mode.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    max_posts = args.max_posts if args.max_posts > 0 else None

    reader = init_ocr_reader()
    processed = 0
    skipped = 0
    newly_processed_post_ids: set[str] = set()  # Track which posts were newly processed in this run

    workers = max(1, args.workers)
    ocr_workers = max(1, args.ocr_workers)
    chunk_paths = [path for path in (args.link_chunks or []) if path]
    search_queries = None
    if args.search_queries:
        search_queries = [q.strip() for q in args.search_queries.split(",") if q.strip()]

    if chunk_paths:
        run_chunk_mode(
            chunk_paths=chunk_paths,
            subreddit=args.subreddit,
            reader=reader,
            workers=workers,
            ocr_workers=ocr_workers,
            max_posts=max_posts,
        )
        return

    # Initialize aggregate storage
    init_aggregate_storage()
    
    # Load existing post IDs from local files (for faster initial check)
    # Supabase will be checked individually per post (more efficient)
    existing_post_ids = load_all_existing_post_ids()
    
    print(f"{Fore.CYAN}Starting pipeline update mode: fetching recent posts and processing only new ones{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Found {len(existing_post_ids)} existing posts in local files{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Will check Supabase for each post individually (using hash index for fast lookup){Style.RESET_ALL}")

    # Set default max_posts if not specified (for update mode)
    if max_posts is None:
        max_posts = RECENT_POSTS_LIMIT
        print(f"{Fore.CYAN}Fetching last {max_posts} posts from '{args.sort}' sort to check for updates{Style.RESET_ALL}")

    # Stream posts and process incrementally (fetch batch -> filter -> process -> fetch next batch)
    print(f"{Fore.CYAN}Starting incremental processing from {args.subreddit}...{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Will process posts as they are fetched (batch by batch){Style.RESET_ALL}")
    
    stream_gen = (
            stream_search_query_batches(
                subreddit=args.subreddit,
                sort_filter=args.sort,
                queries=search_queries,
                max_posts=max_posts,
                page_limit=args.search_limit,
                cid=args.search_cid,
                iid=args.search_iid,
                include_over_18=args.search_include_over_18,
                safe=args.search_safe,
            )
            if search_queries
            else stream_full_post_batches(
                subreddit=args.subreddit,
                sort_filter=args.sort,
                fetch_all=args.fetch_all,
                max_posts=max_posts,
                resume_after=args.resume_after or None,
            )
        )
    
    total_fetched = 0
    batch_number = 0
    
    # Process batches incrementally: fetch -> filter -> process -> repeat
    for batch in stream_gen:
        batch_number += 1
        if not batch:
            print(f"{Fore.YELLOW}No more posts available, stopping.{Style.RESET_ALL}")
            break
        
        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Batch {batch_number}: Fetched {len(batch)} posts{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")
        
        total_fetched += len(batch)
        
        # Filter this batch: check which posts already exist
        new_posts_in_batch: List[RedditPost] = []
        skipped_in_batch = 0
        
        print(f"{Fore.CYAN}Checking batch {batch_number} against existing database...{Style.RESET_ALL}")
        
        for post in batch:
            post_id = str(post.get("id") or post.get("name") or "")
            post_title = post.get("title", "No title")[:60]  # Get title for logging
            if not post_id:
                continue
                
            # First check local files (fast, already loaded)
            if post_id in existing_post_ids:
                skipped_in_batch += 1
                skipped += 1
                if skipped_in_batch <= 3 or skipped_in_batch % 50 == 0:
                    print(f"{Fore.YELLOW}[SKIP] Post {post_id}: {post_title} - already exists in local files{Style.RESET_ALL}")
                continue
            
            # Check Supabase database for this specific post (efficient per-post check)
            if check_post_exists_in_supabase(post_id):
                skipped_in_batch += 1
                skipped += 1
                existing_post_ids.add(post_id)  # Cache it to avoid re-checking
                if skipped_in_batch <= 3 or skipped_in_batch % 50 == 0:
                    print(f"{Fore.YELLOW}[SKIP] Post {post_id}: {post_title} - already exists in Supabase{Style.RESET_ALL}")
            else:
                new_posts_in_batch.append(post)
                # Don't add to existing_post_ids here - these posts will be processed and saved
                # We'll add them after successful processing to avoid duplicates within this run
                print(f"{Fore.GREEN}[NEW] Post {post_id}: {post_title} - will be processed{Style.RESET_ALL}")
        
        print(f"{Fore.CYAN}Batch {batch_number} filtered: {len(new_posts_in_batch)} new posts, {skipped_in_batch} skipped{Style.RESET_ALL}")
        
        # Process new posts from this batch immediately
        if new_posts_in_batch:
            print(f"{Fore.GREEN}Processing {len(new_posts_in_batch)} new posts from batch {batch_number}...{Style.RESET_ALL}")
            # Track which post IDs are being processed in this batch
            batch_post_ids = {str(post.get("id") or post.get("name") or "") for post in new_posts_in_batch}
            newly_processed_post_ids.update(batch_post_ids)
            
            process_batch_concurrently(
                new_posts_in_batch, 
                reader, 
                workers, 
                ocr_workers, 
                existing_post_ids
            )
            processed += len(new_posts_in_batch)
            print(f"{Fore.GREEN}Completed batch {batch_number}: {len(new_posts_in_batch)} posts processed{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}No new posts to process in batch {batch_number} (all already exist){Style.RESET_ALL}")
        
        # Check if we've reached the max_posts limit
        if max_posts and total_fetched >= max_posts:
            print(f"{Fore.CYAN}Reached max_posts limit ({max_posts}), stopping fetch.{Style.RESET_ALL}")
            break
        
        # Also check if we've processed enough new posts
        if max_posts and processed >= max_posts:
            print(f"{Fore.CYAN}Processed {processed} new posts, stopping.{Style.RESET_ALL}")
            break
    
    print(f"\n{Fore.GREEN}Total fetched: {total_fetched} posts from Reddit{Style.RESET_ALL}")

    print(
        f"\n{Fore.CYAN}Pipeline update complete. Processed {processed} new posts, skipped {skipped} existing posts from {args.subreddit}.{Style.RESET_ALL}"
    )
    
    # Run cleaning/processing steps on the aggregated results
    # Only process and upload newly processed posts
    if AGGREGATE_OUTPUT_FILE.exists() and newly_processed_post_ids:
        print(f"\n{Fore.CYAN}Starting data cleaning and flattening for {len(newly_processed_post_ids)} newly processed posts...{Style.RESET_ALL}")
        try:
            run_cleaning_steps(newly_processed_post_ids=newly_processed_post_ids)
            print(f"{Fore.GREEN}Data cleaning complete!{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.YELLOW}Warning: Data cleaning failed: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
    elif not newly_processed_post_ids:
        print(f"\n{Fore.CYAN}No new posts were processed, skipping cleaning and upload steps.{Style.RESET_ALL}")


def normalize_path(path: str) -> str:
    """Normalize file path for comparison."""
    if not path:
        return ""
    return str(Path(path).as_posix()).lower()


def text_from_extraction(extraction: Dict[str, Any]) -> str:
    """Extract text from OCR extraction result in various formats."""
    if not extraction:
        return ""
    value = extraction.get("text")
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        lines = [line.strip() for line in value if isinstance(line, str) and line.strip()]
        if lines:
            return "\n".join(lines)
    lines = extraction.get("text_lines") or extraction.get("lines")
    if isinstance(lines, list):
        segments = []
        for line in lines:
            if isinstance(line, str):
                trimmed = line.strip()
                if trimmed:
                    segments.append(trimmed)
            elif isinstance(line, dict):
                line_text = line.get("text")
                if isinstance(line_text, str):
                    trimmed = line_text.strip()
                    if trimmed:
                        segments.append(trimmed)
        if segments:
            return "\n".join(segments)
    return ""


def aggregate_post_text(post: Dict[str, Any]) -> str:
    """Aggregate all OCR text from images in a post, maintaining order."""
    local_images = [
        item.get("local_path", "")
        for item in post.get("images", {}).get("local_images", [])
    ]
    ocr_entries = post.get("ocr", {}).get("image_results", [])
    path_index: defaultdict[str, List[Dict[str, Any]]] = defaultdict(list)
    name_index: defaultdict[str, List[Dict[str, Any]]] = defaultdict(list)
    for entry in ocr_entries:
        file_path = entry.get("file_path", "")
        key = normalize_path(file_path)
        if key:
            path_index[key].append(entry)
        name = Path(file_path).name.lower()
        if name:
            name_index[name].append(entry)

    ordered_texts: List[str] = []

    def extract_next_entry(path_key: str, name_key: str | None) -> Dict[str, Any] | None:
        if path_key and path_index.get(path_key):
            return path_index[path_key].pop(0)
        if name_key and name_index.get(name_key):
            return name_index[name_key].pop(0)
        return None

    for local_path in local_images:
        key = normalize_path(local_path)
        name = Path(local_path).name.lower()
        entry = extract_next_entry(key, name)
        if entry:
            text = text_from_extraction(entry.get("extraction", {}))
            if text:
                ordered_texts.append(text)

    # Append any remaining OCR entries that were not matched via local_images order
    for entry_list in list(path_index.values()):
        while entry_list:
            entry = entry_list.pop(0)
            text = text_from_extraction(entry.get("extraction", {}))
            if text:
                ordered_texts.append(text)
    for entry_list in list(name_index.values()):
        while entry_list:
            entry = entry_list.pop(0)
            text = text_from_extraction(entry.get("extraction", {}))
            if text:
                ordered_texts.append(text)

    return "\n".join(ordered_texts)


def rebuild_order(posts: Dict[str, Any], order: List[str]) -> List[str]:
    """Rebuild order list, removing duplicates and adding missing posts."""
    seen = set()
    cleaned_order = []
    for post_id in order:
        if post_id in posts and post_id not in seen:
            cleaned_order.append(post_id)
            seen.add(post_id)
    extras = sorted(pid for pid in posts if pid not in seen)
    cleaned_order.extend(extras)
    return cleaned_order


def build_clean_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean pipeline results by removing duplicates and fixing order."""
    posts = data.get("posts", {})
    order = data.get("order", [])
    cleaned_posts = {pid: posts[pid] for pid in posts if isinstance(pid, str)}
    cleaned_order = rebuild_order(cleaned_posts, order)
    metadata = dict(data.get("metadata", {}))
    metadata["total_posts"] = len(cleaned_posts)
    metadata["order_cleaned_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    chunk_ids = metadata.get("chunk_ids", [])
    if isinstance(chunk_ids, list):
        metadata["chunk_ids"] = sorted({_ for _ in chunk_ids if isinstance(_, int)})
    else:
        metadata["chunk_ids"] = []
    return {"metadata": metadata, "posts": cleaned_posts, "order": cleaned_order}


def build_flat_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten pipeline results into a simpler structure with just essential fields."""
    posts = data.get("posts", {})
    ordered = data.get("order", [])
    seen = set()
    flat_list: List[Dict[str, Any]] = []

    def append_entry(pid: str, post: Dict[str, Any]) -> None:
        entry = {
            "post_id": pid,
            "title": post.get("title", ""),
            "status": post.get("status", ""),
            "correct_link": post.get("correct_link"),
            "ocr_text": aggregate_post_text(post),
        }
        flat_list.append(entry)

    for post_id in ordered:
        post = posts.get(post_id)
        if not post:
            continue
        seen.add(post_id)
        append_entry(post_id, post)
    for post_id, post in posts.items():
        if post_id in seen:
            continue
        append_entry(post_id, post)
    return flat_list


def sanitize_filename_for_splitter(filename: str) -> str:
    """Sanitize filename by removing/replacing invalid characters."""
    # Remove or replace invalid characters for Windows filesystem
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '_', filename)
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip(' .')
    # Limit length to avoid filesystem issues
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    return sanitized


def split_posts_into_individual_files(flat_path: Path, use_temp_dir: bool = False) -> Path | None:
    """
    Split each post from flattened JSON into individual files with metadata convention.
    
    For each post, creates ONE file: {post_id},{title},{correct_link}.json
    Contains nested metadata and content:
    {
        "metadata": {
            "post_id": "...",
            "title": "...",
            "correct_link": "...",
            "status": "...",
            // ... all other metadata fields
        },
        "content": "OCR text here..."  // Gets chunked and embedded
    }
    
    Args:
        flat_path: Path to flattened JSON file
        use_temp_dir: If True, use a temporary directory that will be cleaned up after upload
    
    Returns:
        Path to output directory if successful, None otherwise
    """
    if not flat_path.exists():
        print(f"{Fore.YELLOW}No flattened file found at {flat_path}, skipping split.{Style.RESET_ALL}")
        return None
    
    print(f"{Fore.CYAN}Loading flattened data from {flat_path}...{Style.RESET_ALL}")
    with flat_path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    
    posts = data.get('posts', [])
    total_posts = len(posts)
    print(f"{Fore.CYAN}Total posts to split: {total_posts}{Style.RESET_ALL}")
    
    if total_posts == 0:
        print(f"{Fore.YELLOW}No posts to split!{Style.RESET_ALL}")
        return None
    
    # Determine output directory - use temp directory if requested, otherwise use timestamped directory
    if use_temp_dir:
        output_dir_path = Path(tempfile.mkdtemp(prefix="reddit_posts_", suffix="_temp"))
        print(f"{Fore.CYAN}Using temporary directory: {output_dir_path}{Style.RESET_ALL}")
    else:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_dir_path = flat_path.parent / f"posts_{timestamp}"
        output_dir_path.mkdir(parents=True, exist_ok=True)
        print(f"{Fore.CYAN}Output directory: {output_dir_path}{Style.RESET_ALL}")
    print()
    
    # Process each post
    saved_count = 0
    skipped_count = 0
    
    for i, post in enumerate(posts, 1):
        post_id = post.get('post_id', f'post_{i}')
        title = post.get('title', 'Untitled')
        correct_link = post.get('correct_link') or ''  # Handle None values
        ocr_text = post.get('ocr_text', '')
        
        # Sanitize title and correct_link for filesystem safety
        title_str = str(title) if title else 'no_title'
        sanitized_title = sanitize_filename_for_splitter(title_str) or 'no_title'
        
        # For correct_link, remove protocol and sanitize
        sanitized_link = 'no_link'  # Default placeholder
        if correct_link:
            link_str = str(correct_link)
            # Remove http:// or https://
            link_str = link_str.replace('https://', '').replace('http://', '')
            # Replace remaining problematic characters
            sanitized_link = sanitize_filename_for_splitter(link_str) or 'no_link'
        
        # Build filename: post_id,title,correct_link.json
        # Ensure all parts are strings (handle None/empty values)
        filename_parts = [
            str(post_id) if post_id else 'no_id',
            str(sanitized_title) if sanitized_title else 'no_title',
            str(sanitized_link) if sanitized_link else 'no_link'
        ]
        # Final safety check: ensure no None values remain (convert any None to string)
        filename_parts = [str(part) if part is not None and part != '' else 'no_value' for part in filename_parts]
        # Double-check: ensure all are actually strings before joining
        filename_parts = [str(p) for p in filename_parts if p is not None]
        filename = f"{','.join(filename_parts)}.json"
        
        output_file = output_dir_path / filename
        
        # Skip if file already exists (to avoid overwriting)
        if output_file.exists():
            if skipped_count < 3:
                print(f"{Fore.YELLOW}[{i}/{total_posts}] Skipping {filename} (already exists){Style.RESET_ALL}")
            skipped_count += 1
            continue
        
        # Create single file with content (OCR text) and nested metadata
        # Structure: metadata object + content field
        metadata_fields = {k: v for k, v in post.items() if k != 'ocr_text'}
        
        file_data = {
            "metadata": metadata_fields,  # All fields except ocr_text nested in metadata
            "content": ocr_text  # Content field for chunking and embedding
        }
        
        # Save post to file
        try:
            with output_file.open('w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
            
            saved_count += 1
            if saved_count % 50 == 0:
                print(f"{Fore.CYAN}[{i}/{total_posts}] Saved {saved_count} posts...{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}[{i}/{total_posts}] Error saving {filename}: {e}{Style.RESET_ALL}")
            skipped_count += 1
    
    print(f"\n{Fore.GREEN}Successfully saved {saved_count} posts to individual files{Style.RESET_ALL}")
    if skipped_count > 0:
        print(f"{Fore.YELLOW}Skipped {skipped_count} posts (already exist or errors){Style.RESET_ALL}")
    print(f"{Fore.GREEN}Output directory: {output_dir_path}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Each post has 1 file: {{post_id}},{{title}},{{correct_link}}.json (contains content + metadata){Style.RESET_ALL}")
    
    return output_dir_path


def upload_to_s3(local_dir: Path, bucket_name: str, s3_prefix: str = "", cleanup_after: bool = False) -> bool:
    """
    Upload all files from a local directory to S3 bucket.
    
    Args:
        local_dir: Local directory containing files to upload
        bucket_name: S3 bucket name
        s3_prefix: S3 key prefix (optional, e.g., "posts/2024/")
        cleanup_after: If True, delete the local directory after successful upload
    
    Returns:
        True if successful, False otherwise
    """
    if not BOTO3_AVAILABLE:
        print(f"{Fore.YELLOW}S3 upload skipped: boto3 not available{Style.RESET_ALL}")
        return False
    
    if not local_dir.exists() or not local_dir.is_dir():
        print(f"{Fore.YELLOW}S3 upload skipped: directory {local_dir} does not exist{Style.RESET_ALL}")
        return False
    
    # Load AWS credentials from environment
    config = dotenv_values(".env")
    aws_access_key_id = config.get("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = config.get("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = config.get("AWS_REGION") or os.getenv("AWS_REGION", "us-east-1")
    
    if not aws_access_key_id or not aws_secret_access_key:
        print(f"{Fore.YELLOW}S3 upload skipped: AWS credentials not found in environment{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file{Style.RESET_ALL}")
        return False
    
    try:
        # Initialize S3 client (boto3 is guaranteed to be available here due to BOTO3_AVAILABLE check)
        if boto3 is None:  # type: ignore
            return False
        
        s3_client = boto3.client(  # type: ignore
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        
        # Get all JSON files from directory
        json_files = list(local_dir.glob("*.json"))
        total_files = len(json_files)
        
        if total_files == 0:
            print(f"{Fore.YELLOW}No JSON files found in {local_dir} to upload{Style.RESET_ALL}")
            return False
        
        print(f"\n{Fore.CYAN}Uploading {total_files} files to S3 bucket: {bucket_name}{Style.RESET_ALL}")
        if s3_prefix:
            print(f"{Fore.CYAN}S3 prefix: {s3_prefix}{Style.RESET_ALL}")
        
        uploaded_count = 0
        failed_count = 0
        
        for i, file_path in enumerate(json_files, 1):
            # Construct S3 key
            filename = file_path.name
            if s3_prefix:
                s3_key = f"{s3_prefix.rstrip('/')}/{filename}"
            else:
                s3_key = filename
            
            try:
                # Upload file to S3
                s3_client.upload_file(
                    str(file_path),
                    bucket_name,
                    s3_key,
                    ExtraArgs={'ContentType': 'application/json'}
                )
                uploaded_count += 1
                
                if uploaded_count % 50 == 0:
                    print(f"{Fore.CYAN}[{i}/{total_files}] Uploaded {uploaded_count} files...{Style.RESET_ALL}")
            except Exception as e:
                failed_count += 1
                error_msg = str(e)
                # Try to extract error code if it's a boto3 ClientError
                if ClientError and isinstance(e, ClientError):
                    try:
                        error_code = e.response.get('Error', {}).get('Code', 'Unknown')  # type: ignore
                        error_msg = f"{error_code}: {error_msg}"
                    except (AttributeError, KeyError):
                        pass  # Fall back to original error message
                print(f"{Fore.RED}[{i}/{total_files}] Failed to upload {filename}: {error_msg}{Style.RESET_ALL}")
                if failed_count <= 3:
                    print(f"{Fore.YELLOW}  Full error: {str(e)}{Style.RESET_ALL}")
        
        print(f"\n{Fore.GREEN}✓ S3 Upload Complete!{Style.RESET_ALL}")
        print(f"{Fore.GREEN}  Uploaded: {uploaded_count}/{total_files} files{Style.RESET_ALL}")
        if failed_count > 0:
            print(f"{Fore.YELLOW}  Failed: {failed_count} files{Style.RESET_ALL}")
        
        # Construct S3 URL
        s3_url = f"s3://{bucket_name}/{s3_prefix.rstrip('/') if s3_prefix else ''}"
        print(f"{Fore.CYAN}S3 Location: {s3_url}{Style.RESET_ALL}")
        
        # Clean up local directory if requested and upload was successful
        if cleanup_after and uploaded_count > 0 and local_dir.exists():
            try:
                print(f"{Fore.CYAN}Cleaning up temporary directory: {local_dir}{Style.RESET_ALL}")
                shutil.rmtree(local_dir)
                print(f"{Fore.GREEN}✓ Temporary directory deleted{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.YELLOW}Warning: Failed to delete temporary directory {local_dir}: {e}{Style.RESET_ALL}")
        
        return uploaded_count > 0
        
    except Exception as e:
        print(f"{Fore.RED}Error during S3 upload: {str(e)}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()
        return False


def sync_knowledge_base(kb_id: str, datasource_id: str) -> bool:
    """
    Start a sync job for Amazon Bedrock Knowledge Base to ingest data from S3.
    
    Args:
        kb_id: Knowledge Base ID
        datasource_id: Data Source ID
    
    Returns:
        True if successful, False otherwise
    """
    if not BOTO3_AVAILABLE:
        print(f"{Fore.YELLOW}Bedrock sync skipped: boto3 not available{Style.RESET_ALL}")
        return False
    
    # Load AWS credentials from environment
    config = dotenv_values(".env")
    aws_access_key_id = config.get("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = config.get("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = config.get("AWS_REGION") or os.getenv("AWS_REGION", "us-east-1")
    
    if not aws_access_key_id or not aws_secret_access_key:
        print(f"{Fore.YELLOW}Bedrock sync skipped: AWS credentials not found in environment{Style.RESET_ALL}")
        return False
    
    if not kb_id or not datasource_id:
        print(f"{Fore.YELLOW}Bedrock sync skipped: KB_ID or DATASOURCE_ID not provided{Style.RESET_ALL}")
        return False
    
    try:
        # Initialize Bedrock Agent client
        if boto3 is None:  # type: ignore
            return False
        
        bedrock_client = boto3.client(  # type: ignore
            'bedrock-agent',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        
        print(f"\n{Fore.CYAN}Starting Bedrock Knowledge Base sync...{Style.RESET_ALL}")
        print(f"{Fore.CYAN}  Knowledge Base ID: {kb_id}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}  Data Source ID: {datasource_id}{Style.RESET_ALL}")
        
        # Start ingestion job
        response = bedrock_client.start_ingestion_job(
            knowledgeBaseId=kb_id,
            dataSourceId=datasource_id
        )
        
        ingestion_job = response.get('ingestionJob', {})
        ingestion_job_id = ingestion_job.get('ingestionJobId', 'Unknown')
        ingestion_job_status = ingestion_job.get('status', 'Unknown')
        
        print(f"\n{Fore.GREEN}✓ Bedrock Knowledge Base sync started!{Style.RESET_ALL}")
        print(f"{Fore.GREEN}  Ingestion Job ID: {ingestion_job_id}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}  Status: {ingestion_job_status}{Style.RESET_ALL}")
        
        return True
        
    except Exception as e:
        print(f"{Fore.RED}Error starting Bedrock Knowledge Base sync: {str(e)}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()
        return False


def run_cleaning_steps(newly_processed_post_ids: set[str] | None = None) -> None:
    """
    Run cleaning and flattening steps on pipeline results.
    
    Args:
        newly_processed_post_ids: Set of post IDs that were newly processed in this run.
                                  If provided, only these posts will be split and uploaded.
                                  If None, all posts will be processed (legacy behavior).
    """
    input_path = AGGREGATE_OUTPUT_FILE
    if not input_path.exists():
        print(f"{Fore.YELLOW}No pipeline results file found at {input_path}, skipping cleaning.{Style.RESET_ALL}")
        return
    
    print(f"{Fore.CYAN}Loading pipeline results from {input_path}...{Style.RESET_ALL}")
    with input_path.open(encoding="utf-8") as fh:
        raw_data = json.load(fh)
    
    # Clean the data
    print(f"{Fore.CYAN}Cleaning pipeline results (removing duplicates, fixing order)...{Style.RESET_ALL}")
    cleaned_payload = build_clean_payload(raw_data)
    
    # Only save cleaned version if we're processing all posts (not just new ones)
    # When processing only new posts, we don't need to write this file
    if not newly_processed_post_ids:
        cleaned_path = OUTPUT_ROOT / "pipeline_results.cleaned.json"
        tmp_path = cleaned_path.with_suffix(cleaned_path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(cleaned_payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        if not _atomic_replace(tmp_path, cleaned_path):
            cleaned_path.write_text(tmp_path.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"{Fore.GREEN}Cleaned payload written to: {cleaned_path}{Style.RESET_ALL}")
    else:
        print(f"{Fore.CYAN}Skipping cleaned.json write (only processing new posts, using temp files){Style.RESET_ALL}")
    
    # Create flattened version
    print(f"{Fore.CYAN}Creating flattened dataset...{Style.RESET_ALL}")
    flat_entries = build_flat_posts(cleaned_payload)
    
    # Filter to only newly processed posts if specified
    if newly_processed_post_ids:
        original_count = len(flat_entries)
        flat_entries = [post for post in flat_entries if post.get("post_id") in newly_processed_post_ids]
        print(f"{Fore.CYAN}Filtered to {len(flat_entries)} newly processed posts (from {original_count} total){Style.RESET_ALL}")
        if len(flat_entries) == 0:
            print(f"{Fore.YELLOW}No newly processed posts found in flattened data, skipping split and upload.{Style.RESET_ALL}")
            return
    
    # Create a temporary flat payload for the filtered posts (for splitting/uploading)
    filtered_flat_payload = {
        "metadata": {
            "source": input_path.name,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_posts": len(flat_entries),
            "filtered": newly_processed_post_ids is not None,
            "newly_processed_count": len(flat_entries) if newly_processed_post_ids else None,
        },
        "posts": flat_entries,
    }
    
    # Only save the full flattened dataset if we're processing all posts (not just new ones)
    # When processing only new posts, we use temp files and don't write to disk
    if not newly_processed_post_ids:
        flat_path = OUTPUT_ROOT / "pipeline_results.cleaned.flat.json"
        full_flat_payload = {
            "metadata": {
                "source": input_path.name,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "total_posts": len(flat_entries),
            },
            "posts": flat_entries,
        }
        tmp_flat_path = flat_path.with_suffix(flat_path.suffix + ".tmp")
        tmp_flat_path.write_text(
            json.dumps(full_flat_payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        if not _atomic_replace(tmp_flat_path, flat_path):
            flat_path.write_text(tmp_flat_path.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"{Fore.GREEN}Flattened dataset written to: {flat_path} ({len(flat_entries)} entries){Style.RESET_ALL}")
    else:
        print(f"{Fore.CYAN}Skipping flattened dataset write (only processing new posts, using temp files){Style.RESET_ALL}")
        print(f"{Fore.CYAN}Will split and upload only {len(flat_entries)} newly processed posts{Style.RESET_ALL}")
    
    # Print summary (only for newly processed posts if filtering)
    if newly_processed_post_ids:
        status_counts = Counter(post.get("status", "unknown") for post in flat_entries)
        print(f"\n{Fore.CYAN}Cleaning Summary (Newly Processed Posts Only):{Style.RESET_ALL}")
        print(f"  Total new posts: {len(flat_entries)}")
    else:
        status_counts = Counter(post.get("status", "unknown") for post in cleaned_payload["posts"].values())
        print(f"\n{Fore.CYAN}Cleaning Summary:{Style.RESET_ALL}")
        print(f"  Total posts: {len(cleaned_payload['posts'])}")
    print(f"  Status breakdown:")
    for status, count in status_counts.most_common():
        print(f"    {status:15} {count}")
    
    # Split posts into individual files with metadata convention
    # This is the final output - individual files with metadata as filename
    print(f"\n{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Splitting posts into individual files (FINAL OUTPUT)...{Style.RESET_ALL}")
    print(f"{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
    try:
        # Use temp files when processing only new posts
        use_temp = newly_processed_post_ids is not None
        
        # If filtering by newly processed posts, create a temporary file with only those posts
        split_input_path: Path | None = None
        temp_filtered_path: Path | None = None
        
        if newly_processed_post_ids and flat_entries:
            # Create temporary filtered file for splitting (will be cleaned up after use)
            temp_filtered_path = Path(tempfile.mktemp(suffix=".json", prefix="pipeline_filtered_"))
            temp_filtered_path.write_text(
                json.dumps(filtered_flat_payload, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            split_input_path = temp_filtered_path
            print(f"{Fore.CYAN}Using temporary filtered file with {len(flat_entries)} newly processed posts{Style.RESET_ALL}")
        elif not newly_processed_post_ids:
            # Use the saved flat file
            flat_path = OUTPUT_ROOT / "pipeline_results.cleaned.flat.json"
            if flat_path.exists():
                split_input_path = flat_path
            else:
                print(f"{Fore.YELLOW}No flattened file found, skipping split!{Style.RESET_ALL}")
                split_input_path = None
        else:
            print(f"{Fore.YELLOW}No posts to split!{Style.RESET_ALL}")
            split_input_path = None
        
        if split_input_path:
            split_output_dir = split_posts_into_individual_files(split_input_path, use_temp_dir=use_temp)
            
            # Clean up temporary filtered file if it was created
            if temp_filtered_path and temp_filtered_path.exists():
                try:
                    temp_filtered_path.unlink()
                    print(f"{Fore.CYAN}Cleaned up temporary filtered file{Style.RESET_ALL}")
                except Exception as e:
                    print(f"{Fore.YELLOW}Warning: Failed to delete temp file {temp_filtered_path}: {e}{Style.RESET_ALL}")
        else:
            split_output_dir = None
        if split_output_dir:
            print(f"\n{Fore.GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
            print(f"{Fore.GREEN}✓✓✓ FILES SPLIT COMPLETE ✓✓✓{Style.RESET_ALL}")
            print(f"{Fore.GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
            print(f"{Fore.GREEN}Individual post files in directory:{Style.RESET_ALL}")
            print(f"{Fore.CYAN}{split_output_dir}{Style.RESET_ALL}")
            print(f"{Fore.GREEN}Each file format: {{post_id}},{{title}},{{correct_link}}.json{Style.RESET_ALL}")
            
            # Upload split files to S3 bucket
            config = dotenv_values(".env")
            s3_bucket = config.get("S3_BUCKET_NAME") or os.getenv("S3_BUCKET_NAME")
            s3_prefix = config.get("S3_PREFIX", "") or os.getenv("S3_PREFIX", "")
            
            if s3_bucket:
                print(f"\n{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                print(f"{Fore.CYAN}Uploading split files to S3 bucket...{Style.RESET_ALL}")
                print(f"{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                # Clean up temp directory after upload if we're processing only new posts
                cleanup_after_upload = newly_processed_post_ids is not None
                if cleanup_after_upload:
                    print(f"{Fore.CYAN}Note: Using temporary directory - will be deleted after upload{Style.RESET_ALL}")
                upload_success = upload_to_s3(split_output_dir, s3_bucket, s3_prefix, cleanup_after=cleanup_after_upload)
                if upload_success:
                    print(f"\n{Fore.GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                    print(f"{Fore.GREEN}✓ Files uploaded to S3 bucket: {s3_bucket}{Style.RESET_ALL}")
                    if cleanup_after_upload:
                        print(f"{Fore.GREEN}✓ Temporary files cleaned up{Style.RESET_ALL}")
                    
                    # Sync Bedrock Knowledge Base after successful S3 upload
                    kb_id = config.get("BEDROCK_KB_ID") or os.getenv("BEDROCK_KB_ID")
                    datasource_id = config.get("BEDROCK_DATASOURCE_ID") or os.getenv("BEDROCK_DATASOURCE_ID")
                    
                    if kb_id and datasource_id:
                        print(f"\n{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                        sync_success = sync_knowledge_base(kb_id, datasource_id)
                        if sync_success:
                            print(f"{Fore.GREEN}✓ Bedrock Knowledge Base sync initiated{Style.RESET_ALL}")
                        else:
                            print(f"{Fore.YELLOW}Bedrock Knowledge Base sync failed or was skipped{Style.RESET_ALL}")
                        print(f"{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                    else:
                        print(f"{Fore.YELLOW}Bedrock sync skipped: BEDROCK_KB_ID or BEDROCK_DATASOURCE_ID not configured in .env{Style.RESET_ALL}")
                    
                    print(f"\n{Fore.GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                    print(f"{Fore.GREEN}✓✓✓ PIPELINE COMPLETE ✓✓✓{Style.RESET_ALL}")
                    print(f"{Fore.GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                else:
                    print(f"\n{Fore.YELLOW}Files split successfully but S3 upload failed or was skipped.{Style.RESET_ALL}")
                    # Still try to clean up temp directory even if upload failed
                    if cleanup_after_upload and split_output_dir.exists():
                        try:
                            print(f"{Fore.CYAN}Cleaning up temporary directory after failed upload...{Style.RESET_ALL}")
                            shutil.rmtree(split_output_dir)
                            print(f"{Fore.GREEN}✓ Temporary directory cleaned up{Style.RESET_ALL}")
                        except Exception as e:
                            print(f"{Fore.YELLOW}Warning: Failed to clean up temp directory: {e}{Style.RESET_ALL}")
            else:
                print(f"\n{Fore.CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
                print(f"{Fore.GREEN}✓✓✓ PIPELINE COMPLETE ✓✓✓{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}S3 upload skipped: S3_BUCKET_NAME not configured in .env{Style.RESET_ALL}")
                print(f"{Fore.GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}Post splitting was skipped.{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.YELLOW}Warning: Post splitting failed: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

