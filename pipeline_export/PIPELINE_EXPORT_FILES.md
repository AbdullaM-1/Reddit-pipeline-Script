# Files Needed to Run the Full Pipeline

This document lists all files required to run `run_full_pipeline.py` - the main full pipeline script.

## Core Python Files (Required)

These are the essential Python modules that the pipeline depends on:

1. **`run_full_pipeline.py`** - Main entry point and orchestrator
2. **`posts.py`** - Reddit API fetching helpers (buildPosts, buildComments, buildMedia, fetchPostArticleByPostID, etc.)
3. **`identify_correct_links.py`** - Link identification logic (extract_links_from_text, find_author_comment, identify_correct_link_with_llm)
4. **`extract_images_from_links.py`** - Image extraction and download (extract_images_from_url, download_image, sanitize_filename)
5. **`extract_text_from_images.py`** - OCR text extraction (extract_text_from_image_easyocr, extract_text_from_image_pytesseract, should_skip_image)
6. **`subreddits.py`** - Subreddit data structures (imported by posts.py)
7. **`users.py`** - User data structures and helpers (imported by posts.py)
8. **`supabase_client.py`** - Supabase database client (optional, for database operations)

## Configuration Files (Required)

1. **`.env`** - Environment variables file containing:
   - Reddit credentials (optional but recommended):
     - `username`
     - `password`
     - `client_id`
     - `client_secret`
   - OpenAI API key (for LLM link identification):
     - `OPENAI_API_KEY`
   - Supabase credentials (optional, for database):
     - `NEXT_PUBLIC_SUPABASE_URL` or `SUPABASE_URL`
     - `SUPABASE_SERVICE_ROLE_KEY` or `NEXT_PUBLIC_SUPABASE_ANON_KEY` or `SUPABASE_ANON_KEY`
   - AWS credentials (optional, for S3 upload):
     - `AWS_ACCESS_KEY_ID`
     - `AWS_SECRET_ACCESS_KEY`
     - `AWS_REGION`
     - `S3_BUCKET_NAME`
     - `S3_PREFIX`
   - Bedrock Knowledge Base (optional):
     - `BEDROCK_KB_ID`
     - `BEDROCK_DATASOURCE_ID`

2. **`requirements.txt`** - Python package dependencies

## Optional State Files (Created Automatically)

These files are created automatically during pipeline execution and can be used for resuming:

1. **`pagination_state.json`** - Stores pagination state for resuming Reddit API requests
2. **`pipeline_results/pipeline_results.json`** - Main output file with all processed posts
3. **`pipeline_results/pipeline_results.cleaned.json`** - Cleaned version of results
4. **`pipeline_results/pipeline_results.cleaned.flat.json`** - Flattened version for easier processing
5. **`pipeline_results/chunks/`** - Directory containing chunked results
6. **`downloaded_images/`** - Directory for temporary image storage (images are deleted after OCR)

## Directory Structure

The pipeline will automatically create these directories if they don't exist:

- `pipeline_results/` - Main output directory
- `pipeline_results/chunks/` - Chunked output files
- `downloaded_images/` - Temporary image storage (per post ID)
- `processed_link_chunks/` - Processed link chunk files (if using chunk mode)

## Minimum Export Package

To export and run the pipeline on another machine, you need at minimum:

### Required Files:
```
run_full_pipeline.py
posts.py
identify_correct_links.py
extract_images_from_links.py
extract_text_from_images.py
subreddits.py
users.py
supabase_client.py (optional if not using Supabase)
requirements.txt
.env (create from template with your credentials)
```

### Optional but Recommended:
- `README.md` - Documentation
- Any existing `pipeline_results.json` - For incremental processing (skips already processed posts)

## Installation Steps

1. Copy all required files to the target machine
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create `.env` file with your credentials (see Configuration Files section)
5. Run the pipeline:
   ```bash
   python run_full_pipeline.py --subreddit SextStories --max-posts 100
   ```

## Notes

- The pipeline can run without Reddit authentication, but rate limits will apply
- Supabase integration is optional - the pipeline will work without it (just won't save to database)
- S3 upload is optional - pipeline will work without it
- OCR requires either EasyOCR or pytesseract (with Tesseract binary installed)
- Images are automatically cleaned up after OCR processing to save disk space

