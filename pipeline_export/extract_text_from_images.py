#!/usr/bin/env python
"""
Script to extract text from all downloaded images using EasyOCR.
Processes all images in the downloaded_images directory and saves extracted text.

NOTE: If EasyOCR is installed in a virtual environment, activate it first:
    .\\venv\\Scripts\\Activate.ps1  (Windows PowerShell)
    or
    venv\\Scripts\\activate.bat     (Windows CMD)
"""

import json
import os
import time
import sys
from typing import List, Dict, Optional
from pathlib import Path
from colorama import Fore, Style

# Try to import EasyOCR, fallback to pytesseract if it fails
EASYOCR_AVAILABLE = False
PYTESSERACT_AVAILABLE = False
easyocr = None
pytesseract = None
Image = None

try:
    import easyocr
    EASYOCR_AVAILABLE = True
except Exception:
    EASYOCR_AVAILABLE = False
    try:
        import pytesseract
        from PIL import Image
        PYTESSERACT_AVAILABLE = True
    except ImportError:
        PYTESSERACT_AVAILABLE = False

# ============================================
# CONFIGURATION
# ============================================
IMAGES_DIR = "Downloaded_images"  # Directory containing downloaded images
OUTPUT_FILE = "extracted_text_from_images.json"  # Output file with extracted text
PROGRESS_FILE = "ocr_progress.json"  # File to save progress (for resuming)
LANGUAGES = ['en']  # Languages for OCR (English by default, can add more like ['en', 'es'])
GPU = False  # Set to True if you have GPU support (faster processing)
SAVE_PROGRESS_INTERVAL = 10  # Save progress after every N images
FORCE_REPROCESS = False  # Set to True to process all images even if already processed
# ============================================

# Supported image extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.PNG', '.JPG', '.JPEG'}

# Files to skip (favicons, icons, etc.)
SKIP_PATTERNS = ['favicon', 'apple-touch-icon', 'icon-', 'logo-']


def should_skip_image(filename: str) -> bool:
    """
    Check if an image should be skipped (favicons, icons, etc.)
    
    Args:
        filename: Image filename
    
    Returns:
        True if image should be skipped, False otherwise
    """
    filename_lower = filename.lower()
    for pattern in SKIP_PATTERNS:
        if pattern in filename_lower:
            # Only skip if it's clearly a small icon (has size in name like 16x16, 32x32)
            if any(size in filename_lower for size in ['16x16', '32x32', '48x48', '64x64', '128x128', '180x180']):
                return True
    return False


def extract_text_from_image_easyocr(image_path: str, reader) -> Dict:
    """
    Extract text from a single image using EasyOCR
    
    Args:
        image_path: Path to the image file
        reader: EasyOCR reader instance
    
    Returns:
        Dictionary with extracted text and metadata
    """
    try:
        print(f"      Processing: {os.path.basename(image_path)}...")
        
        # Read text from image
        results = reader.readtext(image_path)
        
        # Extract text and confidence scores
        extracted_texts = []
        full_text = []
        
        for (bbox, text, confidence) in results:
            extracted_texts.append({
                "text": text,
                "confidence": float(confidence),
                "bbox": bbox
            })
            full_text.append(text)
        
        # Combine all text
        combined_text = " ".join(full_text)
        
        return {
            "success": True,
            "text": combined_text,
            "text_lines": extracted_texts,
            "line_count": len(extracted_texts),
            "char_count": len(combined_text)
        }
        
    except Exception as e:
        print(f"      {Fore.RED}Error processing image: {e}{Style.RESET_ALL}")
        return {
            "success": False,
            "error": str(e),
            "text": "",
            "text_lines": [],
            "line_count": 0,
            "char_count": 0
        }


def extract_text_from_image_pytesseract(image_path: str) -> Dict:
    """
    Extract text from a single image using pytesseract
    
    Args:
        image_path: Path to the image file
    
    Returns:
        Dictionary with extracted text and metadata
    """
    try:
        print(f"      Processing: {os.path.basename(image_path)}...")
        
        # Open image
        img = Image.open(image_path)
        
        # Extract text using pytesseract
        text = pytesseract.image_to_string(img, lang='eng')
        
        # Get detailed data with bounding boxes
        data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
        
        # Extract text lines with confidence
        extracted_texts = []
        full_text = []
        current_line = []
        current_line_text = []
        
        for i in range(len(data['text'])):
            if int(data['conf'][i]) > 0:  # Valid text
                word_text = data['text'][i].strip()
                if word_text:
                    current_line.append({
                        "text": word_text,
                        "confidence": float(data['conf'][i]) / 100.0,  # Convert to 0-1 scale
                        "bbox": [
                            [data['left'][i], data['top'][i]],
                            [data['left'][i] + data['width'][i], data['top'][i]],
                            [data['left'][i] + data['width'][i], data['top'][i] + data['height'][i]],
                            [data['left'][i], data['top'][i] + data['height'][i]]
                        ]
                    })
                    current_line_text.append(word_text)
            
            # Check if this is end of line
            if data['line_num'][i] != data['line_num'][i+1] if i+1 < len(data['text']) else True:
                if current_line:
                    line_text = " ".join(current_line_text)
                    extracted_texts.append({
                        "text": line_text,
                        "confidence": sum(w['confidence'] for w in current_line) / len(current_line) if current_line else 0,
                        "bbox": current_line[0]['bbox'] if current_line else None
                    })
                    full_text.append(line_text)
                    current_line = []
                    current_line_text = []
        
        # Combine all text
        combined_text = text.strip()
        
        return {
            "success": True,
            "text": combined_text,
            "text_lines": extracted_texts,
            "line_count": len(extracted_texts),
            "char_count": len(combined_text)
        }
        
    except Exception as e:
        print(f"      {Fore.RED}Error processing image: {e}{Style.RESET_ALL}")
        return {
            "success": False,
            "error": str(e),
            "text": "",
            "text_lines": [],
            "line_count": 0,
            "char_count": 0
        }


def process_all_images(images_dir: str, output_file: str):
    """
    Process all images in the downloaded_images directory and extract text
    
    Args:
        images_dir: Directory containing downloaded images
        output_file: Path to output JSON file
    """
    print(f"{Fore.CYAN}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Extracting Text from Images using EasyOCR{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*70}{Style.RESET_ALL}\n")
    
    # Check if images directory exists
    if not os.path.exists(images_dir):
        print(f"{Fore.RED}Error: Images directory '{images_dir}' not found{Style.RESET_ALL}")
        return
    
    # Initialize OCR reader
    reader = None
    
    if EASYOCR_AVAILABLE:
        print(f"{Fore.CYAN}Initializing EasyOCR reader (this may take a moment on first run)...{Style.RESET_ALL}")
        try:
            reader = easyocr.Reader(LANGUAGES, gpu=GPU)
            print(f"{Fore.GREEN}EasyOCR reader initialized successfully{Style.RESET_ALL}\n")
        except Exception as e:
            print(f"{Fore.RED}Error initializing EasyOCR: {e}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}This might be a DLL loading issue. Try:{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}1. Install Visual C++ Redistributables: https://aka.ms/vs/17/release/vc_redist.x64.exe{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}2. Restart your computer{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}3. Or try using pytesseract as an alternative{Style.RESET_ALL}")
            reader = None
    
    if not reader and not PYTESSERACT_AVAILABLE:
        print(f"{Fore.RED}No OCR library available!{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Please install EasyOCR or pytesseract:{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  pip install easyocr{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  or{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  pip install pytesseract{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  (Also need Tesseract OCR: https://github.com/UB-Mannheim/tesseract/wiki){Style.RESET_ALL}")
        return
    
    if not reader:
        print(f"{Fore.CYAN}Using pytesseract for OCR...{Style.RESET_ALL}")
        # Try to set Tesseract path if it's in a common location
        tesseract_paths = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            r"C:\Tesseract-OCR\tesseract.exe"
        ]
        for path in tesseract_paths:
            if os.path.exists(path):
                pytesseract.pytesseract.tesseract_cmd = path
                print(f"{Fore.GREEN}Found Tesseract at: {path}{Style.RESET_ALL}\n")
                break
        else:
            print(f"{Fore.YELLOW}Warning: Tesseract not found in common locations.{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Please install Tesseract OCR from: https://github.com/UB-Mannheim/tesseract/wiki{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Or set the path manually in the script.{Style.RESET_ALL}\n")
    
    # Collect all images
    print(f"{Fore.CYAN}Scanning for images in {images_dir}...{Style.RESET_ALL}")
    all_images = []
    
    for post_dir in os.listdir(images_dir):
        post_path = os.path.join(images_dir, post_dir)
        if not os.path.isdir(post_path):
            continue
        
        post_id = post_dir
        images_in_post = []
        
        for filename in os.listdir(post_path):
            file_path = os.path.join(post_path, filename)
            if not os.path.isfile(file_path):
                continue
            
            # Check if it's an image file
            file_ext = os.path.splitext(filename)[1].lower()
            if file_ext not in IMAGE_EXTENSIONS:
                continue
            
            # Skip favicons and icons
            if should_skip_image(filename):
                continue
            
            images_in_post.append({
                "post_id": post_id,
                "filename": filename,
                "file_path": file_path,
                "relative_path": os.path.join(IMAGES_DIR, post_id, filename)
            })
        
        if images_in_post:
            all_images.extend(images_in_post)
    
    total_images = len(all_images)
    print(f"{Fore.GREEN}Found {total_images} images to process{Style.RESET_ALL}\n")
    
    if total_images == 0:
        print(f"{Fore.YELLOW}No images found to process{Style.RESET_ALL}")
        return
    
    # Process images grouped by post_id
    results = []
    stats = {
        "total_images": total_images,
        "processed": 0,
        "successful": 0,
        "failed": 0,
        "total_text_extracted": 0,
        "posts_processed": 0
    }
    
    # Group images by post_id
    images_by_post = {}
    for img_info in all_images:
        post_id = img_info["post_id"]
        if post_id not in images_by_post:
            images_by_post[post_id] = []
        images_by_post[post_id].append(img_info)
    
    # Check for force reprocess flag
    FORCE_REPROCESS = "--force" in sys.argv or "-f" in sys.argv
    if FORCE_REPROCESS:
        print(f"{Fore.CYAN}Force reprocess mode: Will process all images even if already processed{Style.RESET_ALL}\n")
    
    # Load previous progress if exists (unless force reprocess)
    processed_files = set()
    existing_results = {}
    if not FORCE_REPROCESS and os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                progress_data = json.load(f)
                # Get list of already processed files and existing results
                for post_result in progress_data.get("results", []):
                    post_id = post_result.get("post_id", "")
                    if post_id:
                        existing_results[post_id] = post_result
                        for img_result in post_result.get("images", []):
                            file_path = img_result.get("file_path", "")
                            if file_path:
                                processed_files.add(file_path)
            if processed_files:
                print(f"{Fore.YELLOW}Found previous progress: {len(processed_files)} images already processed{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}Resuming from where we left off...{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}Use --force or -f flag to reprocess all images{Style.RESET_ALL}\n")
        except Exception as e:
            print(f"{Fore.YELLOW}Could not load previous progress: {e}{Style.RESET_ALL}\n")
    
    # Process each post
    for post_idx, (post_id, images) in enumerate(images_by_post.items(), 1):
        print(f"[Post {post_idx}/{len(images_by_post)}] Processing post: {post_id}")
        print(f"  {Fore.CYAN}Found {len(images)} images{Style.RESET_ALL}")
        
        post_results = {
            "post_id": post_id,
            "images": [],
            "total_text": "",
            "total_lines": 0,
            "total_chars": 0
        }
        
        # Process each image in the post
        for img_idx, img_info in enumerate(images, 1):
            # Skip if already processed (unless force reprocess)
            if not FORCE_REPROCESS and img_info["relative_path"] in processed_files:
                print(f"    [{img_idx}/{len(images)}] {img_info['filename']} (already processed, skipping)")
                continue
            
            print(f"    [{img_idx}/{len(images)}] {img_info['filename']}")
            
            # Use appropriate OCR method
            if reader:
                result = extract_text_from_image_easyocr(img_info["file_path"], reader)
            elif PYTESSERACT_AVAILABLE:
                result = extract_text_from_image_pytesseract(img_info["file_path"])
            else:
                result = {
                    "success": False,
                    "error": "No OCR library available",
                    "text": "",
                    "text_lines": [],
                    "line_count": 0,
                    "char_count": 0
                }
            
            image_result = {
                "filename": img_info["filename"],
                "file_path": img_info["relative_path"],
                "extraction": result
            }
            
            post_results["images"].append(image_result)
            stats["processed"] += 1
            processed_files.add(img_info["relative_path"])
            
            if result["success"]:
                stats["successful"] += 1
                post_results["total_lines"] += result["line_count"]
                post_results["total_chars"] += result["char_count"]
                if result["text"]:
                    if post_results["total_text"]:
                        post_results["total_text"] += "\n\n"
                    post_results["total_text"] += f"[{img_info['filename']}]\n{result['text']}"
            else:
                stats["failed"] += 1
            
            # Save progress periodically
            if stats["processed"] % SAVE_PROGRESS_INTERVAL == 0:
                # Merge existing results with new results
                all_results = list(existing_results.values()) + results
                if post_results["images"]:
                    # Update or add current post
                    all_results = [r for r in all_results if r.get("post_id") != post_id]
                    all_results.append(post_results)
                
                temp_output = {
                    "metadata": {
                        "total_posts": len(images_by_post),
                        "total_images": total_images,
                        "languages": LANGUAGES,
                        "extraction_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "progress": f"{stats['processed']}/{total_images}"
                    },
                    "statistics": stats,
                    "results": all_results
                }
                with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                    json.dump(temp_output, f, indent=2, ensure_ascii=False)
        
        stats["total_text_extracted"] += post_results["total_chars"]
        stats["posts_processed"] += 1
        results.append(post_results)
        
        print(f"  {Fore.GREEN}Post processed: {post_results['total_lines']} text lines, {post_results['total_chars']} characters{Style.RESET_ALL}\n")
    
    # Merge existing results with new results for final output
    all_final_results = list(existing_results.values()) + results
    # Remove duplicates (keep the latest version of each post)
    seen_posts = set()
    final_results = []
    for result in reversed(all_final_results):
        post_id = result.get("post_id", "")
        if post_id and post_id not in seen_posts:
            seen_posts.add(post_id)
            final_results.append(result)
    final_results.reverse()  # Restore original order
    
    # Save results
    output_data = {
        "metadata": {
            "total_posts": len(images_by_post),
            "total_images": total_images,
            "languages": LANGUAGES,
            "extraction_date": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "statistics": stats,
        "results": final_results
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print(f"\n{Fore.CYAN}{'='*70}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Summary:{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*70}{Style.RESET_ALL}")
    print(f"Total posts processed: {stats['posts_processed']}")
    print(f"Total images processed: {stats['processed']}")
    print(f"Successful extractions: {stats['successful']}")
    print(f"Failed extractions: {stats['failed']}")
    print(f"Total characters extracted: {stats['total_text_extracted']:,}")
    print(f"\n{Fore.GREEN}Results saved to: {output_file}{Style.RESET_ALL}")
    
    # Show sample of posts with text
    posts_with_text = [p for p in results if p.get('total_chars', 0) > 0]
    if posts_with_text:
        print(f"\n{Fore.CYAN}Sample posts with extracted text:{Style.RESET_ALL}")
        for post in posts_with_text[:5]:
            print(f"  - {post['post_id']}: {post['total_chars']} characters, {post['total_lines']} lines")


if __name__ == "__main__":
    process_all_images(IMAGES_DIR, OUTPUT_FILE)

