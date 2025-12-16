#!/usr/bin/env python
"""
Script to extract all images from the identified correct links using BeautifulSoup
and download them to local storage.
"""

import json
import re
import time
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from colorama import Fore, Style
from dotenv import dotenv_values
import os
from urllib.parse import urlparse, urljoin
from pathlib import Path
import base64
import html

# ============================================
# CONFIGURATION
# ============================================
INPUT_FILE = "posts_with_correct_links.json"  # Input file with identified links
OUTPUT_FILE = "posts_with_images.json"  # Output file with extracted images
IMAGES_DIR = "downloaded_images"  # Directory to save downloaded images
DELAY_BETWEEN_REQUESTS = 1  # Seconds to wait between requests (to avoid rate limiting)
DELAY_BETWEEN_IMAGE_DOWNLOADS = 0.5  # Seconds to wait between image downloads
# ============================================

# Load environment variables
config = dotenv_values(".env")


def get_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_image_extension(url: str, content_type: Optional[str] = None) -> str:
    """
    Get file extension for an image URL
    
    Args:
        url: Image URL
        content_type: Content-Type header from response
    
    Returns:
        File extension (e.g., '.jpg', '.png')
    """
    # Check content-type first
    if content_type:
        if 'jpeg' in content_type or 'jpg' in content_type:
            return '.jpg'
        elif 'png' in content_type:
            return '.png'
        elif 'gif' in content_type:
            return '.gif'
        elif 'webp' in content_type:
            return '.webp'
        elif 'bmp' in content_type:
            return '.bmp'
    
    # Extract from URL
    parsed = urlparse(url)
    path = parsed.path.lower()
    
    # Check for common image extensions
    for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
        if path.endswith(ext):
            return ext
    
    # Default to jpg if unknown
    return '.jpg'


def clean_url(url: str) -> str:
    """
    Clean URL by removing escaped characters and fixing common issues
    
    Args:
        url: URL that may contain escaped characters
    
    Returns:
        Cleaned URL
    """
    # Remove escaped forward slashes (common in JSON/JavaScript)
    url = url.replace('\\/', '/')
    # Remove other escaped characters
    url = url.replace('\\"', '"')
    url = url.replace("\\'", "'")
    url = url.replace('\\n', '')
    url = url.replace('\\t', '')
    url = url.replace('\\r', '')
    # Decode HTML entities
    url = html.unescape(url)
    # Remove trailing backslashes
    url = url.rstrip('\\')
    
    # Extract first valid URL if additional JSON/text is attached
    match = re.search(r'https?://[^\s"\'<>]+', url)
    if match:
        url = match.group(0)
    return url.strip()


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename to remove invalid characters
    
    Args:
        filename: Original filename
    
    Returns:
        Sanitized filename
    """
    # Remove invalid characters for Windows/Linux
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    return filename


def download_image(image_url: str, save_path: str, session: requests.Session) -> bool:
    """
    Download an image from URL and save it locally
    
    Args:
        image_url: URL of the image to download
        save_path: Local path where to save the image
        session: Requests session object
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Clean URL first (remove escaped characters)
        image_url = clean_url(image_url)
        
        # Handle base64 data URIs
        if image_url.startswith('data:image/'):
            # Extract base64 data
            header, data = image_url.split(',', 1)
            # Get extension from header
            ext_match = re.search(r'data:image/([^;]+)', header)
            ext = f".{ext_match.group(1)}" if ext_match else '.png'
            # Decode and save
            image_data = base64.b64decode(data)
            with open(save_path, 'wb') as f:
                f.write(image_data)
            return True
        
        # Regular HTTP/HTTPS URL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': image_url
        }
        
        response = session.get(image_url, headers=headers, timeout=15, stream=True)
        response.raise_for_status()
        
        # Get content type
        content_type = response.headers.get('Content-Type', '')
        
        # If save_path doesn't have extension, add one based on content-type or URL
        if not any(save_path.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']):
            ext = get_image_extension(image_url, content_type)
            save_path = save_path + ext
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # Save image
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True
        
    except Exception as e:
        print(f"      {Fore.RED}Error downloading {image_url[:50]}...: {e}{Style.RESET_ALL}")
        return False


def extract_images_from_url(url: str, session: requests.Session) -> List[str]:
    """
    Extract all image URLs from a given URL using BeautifulSoup
    
    Args:
        url: URL to fetch and parse
        session: Requests session object
    
    Returns:
        List of image URLs found
    """
    images = []
    
    try:
        print(f"    {Fore.CYAN}Fetching: {url}{Style.RESET_ALL}")
        
        # Set headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        print(f"    {Fore.GREEN}Successfully fetched (Status: {response.status_code}){Style.RESET_ALL}")
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all img tags
        img_tags = soup.find_all('img')
        print(f"    {Fore.CYAN}Found {len(img_tags)} <img> tags{Style.RESET_ALL}")
        
        # Common image attributes to check (for lazy loading, galleries, etc.)
        image_attrs = ['src', 'data-src', 'data-lazy-src', 'data-original', 'data-full', 
                       'data-image', 'data-url', 'data-href', 'data-srcset', 'srcset']
        
        for img in img_tags:
            # Check all possible image attributes
            for attr in image_attrs:
                attr_value = img.get(attr, '')
                if attr_value:
                    # Handle srcset (can contain multiple URLs)
                    if attr == 'srcset' or attr == 'data-srcset':
                        # Parse srcset: "url1 1x, url2 2x" format
                        srcset_urls = re.findall(r'([^\s,]+)', attr_value)
                        for srcset_url in srcset_urls:
                            if any(ext in srcset_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']):
                                if srcset_url.startswith('//'):
                                    srcset_url = 'https:' + srcset_url
                                elif srcset_url.startswith('/'):
                                    srcset_url = urljoin(url, srcset_url)
                                elif not srcset_url.startswith('http'):
                                    srcset_url = urljoin(url, srcset_url)
                                if srcset_url not in images:
                                    images.append(srcset_url)
                    else:
                        # Regular single URL attribute
                        if attr_value.startswith('//'):
                            attr_value = 'https:' + attr_value
                        elif attr_value.startswith('/'):
                            attr_value = urljoin(url, attr_value)
                        elif not attr_value.startswith('http'):
                            attr_value = urljoin(url, attr_value)
                        
                        # Only add if it looks like an image URL
                        if any(ext in attr_value.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']) or 'image' in attr_value.lower():
                            if attr_value not in images:
                                images.append(attr_value)
        
        # Search entire page source for image URLs (works for all sites, not just imgchest)
        page_text = response.text
        print(f"    {Fore.CYAN}Searching page source for image URLs...{Style.RESET_ALL}")
        
        # Pattern 1: Direct image URLs with extensions in quotes (handle escaped slashes)
        src_patterns = re.findall(r'["\']([^"\']+\.(?:jpg|jpeg|png|gif|webp|bmp)(?:\?[^"\']*)?)["\']', page_text, re.IGNORECASE)
        for match in src_patterns:
            match = clean_url(match)  # Clean escaped characters
            if match.startswith('//'):
                match = 'https:' + match
            elif match.startswith('/'):
                match = urljoin(url, match)
            elif not match.startswith('http'):
                match = urljoin(url, match)
            if match not in images:
                images.append(match)
        
        # Pattern 2: Image URLs in data attributes
        data_patterns = re.findall(r'data-[^=]*=["\']([^"\']+\.(?:jpg|jpeg|png|gif|webp|bmp)(?:\?[^"\']*)?)["\']', page_text, re.IGNORECASE)
        for match in data_patterns:
            match = clean_url(match)  # Clean escaped characters
            if match.startswith('//'):
                match = 'https:' + match
            elif match.startswith('/'):
                match = urljoin(url, match)
            elif not match.startswith('http'):
                match = urljoin(url, match)
            if match not in images:
                images.append(match)
        
        # Pattern 3: Full HTTP/HTTPS image URLs (more comprehensive, handle escaped slashes)
        # This pattern needs to handle escaped slashes in JavaScript/JSON
        http_image_patterns = re.findall(r'(https?:?[\\/]*[\\/]+[^\s"\'<>\)]+\.(?:jpg|jpeg|png|gif|webp|bmp)(?:\?[^\s"\'<>\)]*)?)', page_text, re.IGNORECASE)
        for match in http_image_patterns:
            match = clean_url(match)  # Clean escaped characters
            # Clean up URL (remove trailing punctuation that might not be part of URL)
            match = match.rstrip('.,;:!?)')
            if match not in images:
                images.append(match)
        
        # Pattern 4: Look for base64 images (data URIs)
        base64_patterns = re.findall(r'data:image/[^;]+;base64,[A-Za-z0-9+/=]+', page_text)
        for match in base64_patterns:
            if match not in images:
                images.append(match)
        
        # Pattern 5: Look for JSON data structures that might contain image URLs
        # Common patterns: {"url": "...", "image": "...", "src": "...", "thumbnail": "..."}
        json_patterns = re.findall(r'["\'](?:url|image|src|thumbnail|photo|picture|img)["\']\s*:\s*["\']([^"\']+\.(?:jpg|jpeg|png|gif|webp|bmp)(?:\?[^"\']*)?)["\']', page_text, re.IGNORECASE)
        for match in json_patterns:
            match = clean_url(match)  # Clean escaped characters
            if match.startswith('//'):
                match = 'https:' + match
            elif match.startswith('/'):
                match = urljoin(url, match)
            elif not match.startswith('http'):
                match = urljoin(url, match)
            if match not in images:
                images.append(match)
        
        # Pattern 6: Look in script tags for image URLs (common in galleries)
        script_tags = soup.find_all('script')
        for script in script_tags:
            if script.string:
                script_text = script.string
                # Look for image URLs in JavaScript (handle escaped slashes)
                js_image_patterns = re.findall(r'["\']([^"\']+\.(?:jpg|jpeg|png|gif|webp|bmp)(?:\?[^"\']*)?)["\']', script_text, re.IGNORECASE)
                for match in js_image_patterns:
                    match = clean_url(match)  # Clean escaped characters
                    if match.startswith('//'):
                        match = 'https:' + match
                    elif match.startswith('/'):
                        match = urljoin(url, match)
                    elif not match.startswith('http'):
                        match = urljoin(url, match)
                    if match not in images:
                        images.append(match)
        
        # Pattern 7: Special handling for imgchest.com - extract from data-page JSON attribute
        if 'imgchest.com' in url:
            print(f"    {Fore.CYAN}Detected imgchest.com - extracting from data-page JSON...{Style.RESET_ALL}")
            
            # Look for data-page attribute which contains JSON with all file information
            data_page_elements = soup.find_all(attrs={'data-page': True})
            for element in data_page_elements:
                data_page_json = element.get('data-page', '')
                if data_page_json:
                    try:
                        # Parse the JSON (it's HTML-encoded, so we need to decode it)
                        decoded_json = html.unescape(data_page_json)
                        page_data = json.loads(decoded_json)
                        
                        # Navigate to the files array: props.post.files
                        if 'props' in page_data and 'post' in page_data['props']:
                            post_data = page_data['props']['post']
                            if 'files' in post_data:
                                files = post_data['files']
                                print(f"    {Fore.CYAN}Found {len(files)} files in data-page JSON{Style.RESET_ALL}")
                                
                                for file_info in files:
                                    img_url = None
                                    
                                    # Get the direct link to the image
                                    if 'link' in file_info:
                                        img_url = file_info['link']
                                        img_url = clean_url(img_url)
                                        if img_url not in images:
                                            images.append(img_url)
                                    
                                    # Also get thumbnail if it's different
                                    if 'thumbnail' in file_info:
                                        thumb_url = file_info['thumbnail']
                                        thumb_url = clean_url(thumb_url)
                                        if thumb_url not in images and (img_url is None or thumb_url != img_url):
                                            images.append(thumb_url)
                                    
                                    # For videos, get the thumbnail
                                    if file_info.get('mp4', 0) == 1 and 'thumbnail' in file_info:
                                        thumb_url = file_info['thumbnail']
                                        thumb_url = clean_url(thumb_url)
                                        if thumb_url not in images:
                                            images.append(thumb_url)
                    except json.JSONDecodeError as e:
                        print(f"    {Fore.YELLOW}Error parsing data-page JSON: {e}{Style.RESET_ALL}")
                    except Exception as e:
                        print(f"    {Fore.YELLOW}Error extracting from data-page: {e}{Style.RESET_ALL}")
            
            # Also look for cdn.imgchest.com URLs in the page source as fallback
            imgchest_cdn_patterns = re.findall(r'(https?://[\\/]*cdn\.imgchest\.com[\\/]+[^\s"\'<>\)]+\.(?:jpg|jpeg|png|gif|webp|bmp|PNG|JPG|JPEG|mp4)(?:\?[^\s"\'<>\)]*)?)', page_text, re.IGNORECASE)
            for match in imgchest_cdn_patterns:
                match = clean_url(match)  # Clean escaped characters
                # Skip video files, only keep images
                if not match.lower().endswith('.mp4'):
                    if match not in images:
                        images.append(match)
        
        print(f"    {Fore.CYAN}Found {len(images)} total image references in page source{Style.RESET_ALL}")
        
        # Also look for images in background-image CSS
        style_tags = soup.find_all(style=True)
        for tag in style_tags:
            style = tag.get('style', '')
            # Extract background-image URLs
            bg_images = re.findall(r'background-image:\s*url\(["\']?([^"\']+)["\']?\)', style)
            for bg_img in bg_images:
                if bg_img.startswith('//'):
                    bg_img = 'https:' + bg_img
                elif bg_img.startswith('/'):
                    bg_img = urljoin(url, bg_img)
                elif not bg_img.startswith('http'):
                    bg_img = urljoin(url, bg_img)
                if bg_img not in images:
                    images.append(bg_img)
        
        # Look for images in data attributes (common in image galleries)
        for tag in soup.find_all(attrs={'data-image': True}):
            data_img = tag.get('data-image', '')
            if data_img and data_img not in images:
                if data_img.startswith('//'):
                    data_img = 'https:' + data_img
                elif data_img.startswith('/'):
                    data_img = urljoin(url, data_img)
                elif not data_img.startswith('http'):
                    data_img = urljoin(url, data_img)
                images.append(data_img)
        
        # Look for images in link tags (e.g., <link rel="image_src">)
        link_tags = soup.find_all('link')
        for link in link_tags:
            rel = link.get('rel', [])
            if isinstance(rel, list):
                rel_str = ' '.join(rel).lower()
            else:
                rel_str = str(rel).lower()
            if 'image' in rel_str:
                href = link.get('href', '')
                if href:
                    if href.startswith('//'):
                        href = 'https:' + href
                    elif href.startswith('/'):
                        href = urljoin(url, href)
                    elif not href.startswith('http'):
                        href = urljoin(url, href)
                    if href not in images:
                        images.append(href)
        
        # Look for images in picture/source tags
        picture_tags = soup.find_all('picture')
        for picture in picture_tags:
            source_tags = picture.find_all('source')
            for source in source_tags:
                srcset = source.get('srcset', '')
                if srcset:
                    srcset_urls = re.findall(r'([^\s,]+)', srcset)
                    for srcset_url in srcset_urls:
                        if any(ext in srcset_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']):
                            if srcset_url.startswith('//'):
                                srcset_url = 'https:' + srcset_url
                            elif srcset_url.startswith('/'):
                                srcset_url = urljoin(url, srcset_url)
                            elif not srcset_url.startswith('http'):
                                srcset_url = urljoin(url, srcset_url)
                            if srcset_url not in images:
                                images.append(srcset_url)
        
        # Look for images in anchor tags with image extensions
        anchor_tags = soup.find_all('a', href=True)
        for anchor in anchor_tags:
            href = anchor.get('href', '')
            if href and any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']):
                if href.startswith('//'):
                    href = 'https:' + href
                elif href.startswith('/'):
                    href = urljoin(url, href)
                elif not href.startswith('http'):
                    href = urljoin(url, href)
                if href not in images:
                    images.append(href)
        
        # Remove duplicates and filter out non-image URLs
        seen = set()
        unique_images = []
        for img in images:
            if not img:
                continue
            
            # Clean URL first
            img = clean_url(img)
            
            # Skip data URIs that are too small (likely icons)
            if img.startswith('data:image/'):
                # Keep base64 images
                if img not in seen:
                    seen.add(img)
                    unique_images.append(img)
                continue
            
            # Filter out non-image URLs (must have image extension or be from known image CDNs)
            img_lower = img.lower()
            has_image_ext = any(ext in img_lower for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
            is_image_cdn = any(cdn in img_lower for cdn in ['imgchest', 'i.imgur', 'i.redd.it', 'cdn', 'images', 'static'])
            
            # Skip if it doesn't look like an image URL
            if not has_image_ext and not is_image_cdn:
                continue
            
            # Filter out favicons and small icons (but keep actual content images)
            img_lower_path = img.lower()
            if any(icon in img_lower_path for icon in ['favicon', 'apple-touch-icon', 'icon-', 'logo-']):
                # Only skip if it's clearly a small icon (has size in name like 16x16, 32x32)
                if re.search(r'(16x16|32x32|48x48|64x64|128x128)', img_lower_path):
                    continue
            
            # Clean up URL (remove fragments and common tracking params, but keep image params)
            try:
                parsed = urlparse(img)
                if not parsed.netloc:
                    # Invalid URL, skip
                    continue
                clean_url_str = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                if parsed.query:
                    # Keep query params that might be needed for image (like size, quality)
                    clean_url_str += f"?{parsed.query}"
                
                if clean_url_str not in seen:
                    seen.add(clean_url_str)
                    unique_images.append(clean_url_str)
            except Exception as e:
                # Skip invalid URLs
                print(f"    {Fore.YELLOW}Warning: Skipping invalid URL: {img[:50]}...{Style.RESET_ALL}")
                continue
        
        print(f"    {Fore.GREEN}Extracted {len(unique_images)} unique image URLs{Style.RESET_ALL}")
        return unique_images
        
    except requests.exceptions.RequestException as e:
        print(f"    {Fore.RED}Error fetching URL: {e}{Style.RESET_ALL}")
        return []
    except Exception as e:
        print(f"    {Fore.RED}Error parsing HTML: {e}{Style.RESET_ALL}")
        return []


def process_posts(input_file: str, output_file: str):
    """
    Process all posts, extract images from their correct links, and download them locally
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file
    """
    print(f"{Fore.CYAN}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Extracting and Downloading Images from Identified Links{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*70}{Style.RESET_ALL}\n")
    
    # Create images directory
    os.makedirs(IMAGES_DIR, exist_ok=True)
    print(f"{Fore.CYAN}Images will be saved to: {IMAGES_DIR}/{Style.RESET_ALL}\n")
    
    # Load posts with correct links
    print(f"{Fore.CYAN}Loading posts from {input_file}...{Style.RESET_ALL}")
    if not os.path.exists(input_file):
        print(f"{Fore.RED}Error: Input file '{input_file}' not found{Style.RESET_ALL}")
        return
    
    with open(input_file, "r", encoding="utf-8") as f:
        posts = json.load(f)
    
    print(f"{Fore.GREEN}Loaded {len(posts)} posts{Style.RESET_ALL}\n")
    
    session = get_session()
    results = []
    stats = {
        "total": len(posts),
        "with_links": 0,
        "images_extracted": 0,
        "images_downloaded": 0,
        "no_links": 0,
        "failed": 0
    }
    
    # Process each post
    for i, post in enumerate(posts, 1):
        post_id = post.get("post_id", f"post_{i}")
        post_title = post.get("title", "N/A")
        correct_link = post.get("correct_link")
        
        print(f"[{i}/{len(posts)}] Processing: {post_title[:60]}...")
        
        if not correct_link:
            print(f"  {Fore.YELLOW}No correct link found, skipping{Style.RESET_ALL}")
            stats["no_links"] += 1
            results.append({
                **post,
                "images": [],
                "local_images": [],
                "image_count": 0,
                "images_downloaded": 0,
                "extraction_status": "no_link"
            })
            continue
        
        stats["with_links"] += 1
        
        # Extract images from the link
        image_urls = extract_images_from_url(correct_link, session)
        
        if not image_urls:
            stats["failed"] += 1
            print(f"  {Fore.YELLOW}No images found or extraction failed{Style.RESET_ALL}")
            results.append({
                **post,
                "images": [],
                "local_images": [],
                "image_count": 0,
                "images_downloaded": 0,
                "extraction_status": "no_images"
            })
            # Delay between requests to avoid rate limiting
            if i < len(posts):
                time.sleep(DELAY_BETWEEN_REQUESTS)
            continue
        
        stats["images_extracted"] += 1
        print(f"  {Fore.GREEN}Successfully extracted {len(image_urls)} image URLs{Style.RESET_ALL}")
        
        # Download images
        print(f"  {Fore.CYAN}Downloading images...{Style.RESET_ALL}")
        local_images = []
        post_dir = os.path.join(IMAGES_DIR, sanitize_filename(post_id))
        os.makedirs(post_dir, exist_ok=True)
        
        downloaded_count = 0
        for img_idx, img_url in enumerate(image_urls, 1):
            # Generate filename
            if img_url.startswith('data:image/'):
                # For base64 images, use a generic name
                filename = f"image_{img_idx}"
            else:
                # Extract filename from URL
                parsed = urlparse(img_url)
                filename = os.path.basename(parsed.path)
                if not filename or '.' not in filename:
                    filename = f"image_{img_idx}"
                else:
                    # Sanitize filename
                    name, ext = os.path.splitext(filename)
                    filename = sanitize_filename(name) + ext
            
            # Ensure unique filename
            save_path = os.path.join(post_dir, filename)
            counter = 1
            while os.path.exists(save_path):
                name, ext = os.path.splitext(filename)
                save_path = os.path.join(post_dir, f"{name}_{counter}{ext}")
                counter += 1
            
            # Download image
            print(f"    [{img_idx}/{len(image_urls)}] Downloading: {img_url[:60]}...")
            if download_image(img_url, save_path, session):
                # Store relative path
                relative_path = os.path.join(IMAGES_DIR, sanitize_filename(post_id), os.path.basename(save_path))
                local_images.append({
                    "url": img_url,
                    "local_path": relative_path,
                    "absolute_path": os.path.abspath(save_path)
                })
                downloaded_count += 1
                print(f"      {Fore.GREEN}Saved: {os.path.basename(save_path)}{Style.RESET_ALL}")
            else:
                print(f"      {Fore.YELLOW}Failed to download{Style.RESET_ALL}")
            
            # Small delay between image downloads
            if img_idx < len(image_urls):
                time.sleep(DELAY_BETWEEN_IMAGE_DOWNLOADS)
        
        stats["images_downloaded"] += downloaded_count
        print(f"  {Fore.GREEN}Downloaded {downloaded_count}/{len(image_urls)} images{Style.RESET_ALL}")
        
        results.append({
            **post,
            "images": image_urls,
            "local_images": local_images,
            "image_count": len(image_urls),
            "images_downloaded": downloaded_count,
            "extraction_status": "success" if downloaded_count > 0 else "download_failed"
        })
        
        # Delay between requests to avoid rate limiting
        if i < len(posts):
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # Save results
    print(f"\n{Fore.CYAN}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Summary:{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*70}{Style.RESET_ALL}")
    print(f"Total posts: {stats['total']}")
    print(f"Posts with links: {stats['with_links']}")
    print(f"Posts with images extracted: {stats['images_extracted']}")
    print(f"Posts with no links: {stats['no_links']}")
    print(f"Posts with failed extraction: {stats['failed']}")
    
    total_images = sum(post.get('image_count', 0) for post in results)
    total_downloaded = sum(post.get('images_downloaded', 0) for post in results)
    print(f"\n{Fore.GREEN}Total image URLs extracted: {total_images}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Total images downloaded: {total_downloaded}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Images saved to: {os.path.abspath(IMAGES_DIR)}{Style.RESET_ALL}")
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n{Fore.GREEN}Results saved to: {output_file}{Style.RESET_ALL}")
    
    # Show sample of posts with images
    posts_with_images = [p for p in results if p.get('image_count', 0) > 0]
    if posts_with_images:
        print(f"\n{Fore.CYAN}Sample posts with downloaded images:{Style.RESET_ALL}")
        for post in posts_with_images[:5]:
            print(f"  - {post.get('title', 'N/A')[:50]}: {post.get('images_downloaded', 0)}/{post.get('image_count', 0)} images downloaded")


if __name__ == "__main__":
    process_posts(INPUT_FILE, OUTPUT_FILE)

