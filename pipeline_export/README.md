# Full Pipeline Export Package

This folder contains all the files needed to run the full Reddit scraping pipeline.

## Files Included

### Core Python Files
- `run_full_pipeline.py` - Main entry point
- `posts.py` - Reddit API helpers
- `identify_correct_links.py` - Link identification logic
- `extract_images_from_links.py` - Image extraction/download
- `extract_text_from_images.py` - OCR text extraction
- `subreddits.py` - Subreddit data structures
- `users.py` - User data structures
- `supabase_client.py` - Database client (optional)

### Configuration
- `requirements.txt` - Python dependencies

## Setup Instructions

1. **Create a virtual environment:**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   # or
   source venv/bin/activate  # Linux/Mac
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create `.env` file** with your credentials:
   ```
   # Reddit API (optional but recommended)
   username=your_reddit_username
   password=your_reddit_password
   client_id=your_reddit_client_id
   client_secret=your_reddit_client_secret
   
   # OpenAI API (for LLM link identification)
   OPENAI_API_KEY=your_openai_api_key
   
   # Supabase (optional - for database)
   NEXT_PUBLIC_SUPABASE_URL=your_supabase_url
   SUPABASE_SERVICE_ROLE_KEY=your_supabase_key
   
   # AWS (optional - for S3 upload)
   AWS_ACCESS_KEY_ID=your_aws_access_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   AWS_REGION=us-east-1
   S3_BUCKET_NAME=your_bucket_name
   S3_PREFIX=posts/
   
   # Bedrock Knowledge Base (optional)
   BEDROCK_KB_ID=your_kb_id
   BEDROCK_DATASOURCE_ID=your_datasource_id
   ```

4. **Run the pipeline:**
   ```bash
   python run_full_pipeline.py --subreddit SextStories --max-posts 100
   ```

## Usage Examples

```bash
# Basic run with default settings
python run_full_pipeline.py

# Specify subreddit and max posts
python run_full_pipeline.py --subreddit SextStories --max-posts 50

# Use more workers for faster processing
python run_full_pipeline.py --subreddit SextStories --max-posts 100 --workers 30 --ocr-workers 50

# Process from link chunks
python run_full_pipeline.py --link-chunks chunk1.json chunk2.json
```

## Notes

- The pipeline will automatically create output directories (`pipeline_results/`, `downloaded_images/`)
- Images are automatically deleted after OCR processing
- If you have existing `pipeline_results.json`, the pipeline will skip already processed posts
- See `PIPELINE_EXPORT_FILES.md` for detailed documentation

