# How to Run the Full Pipeline

Complete step-by-step guide to run the Reddit scraping pipeline.

## Prerequisites

- Python 3.9 or higher
- Internet connection
- (Optional) Reddit API credentials
- (Optional) OpenAI API key for LLM link identification
- (Optional) Supabase credentials for database storage
- (Optional) AWS credentials for S3 upload

## Step 1: Navigate to the Pipeline Directory

```bash
cd pipeline_export
```

## Step 2: Create Virtual Environment (Recommended)

**Windows:**
```powershell
python -m venv venv
venv\Scripts\activate
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

## Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

**Note:** This may take a few minutes as it installs:
- EasyOCR (for OCR text extraction)
- OpenAI (for LLM link identification)
- BeautifulSoup (for HTML parsing)
- Requests (for HTTP requests)
- And other dependencies

## Step 4: Create .env File

Create a `.env` file in the `pipeline_export` directory with your credentials:

```env
# Reddit API (optional but recommended to avoid rate limits)
username=your_reddit_username
password=your_reddit_password
client_id=your_reddit_client_id
client_secret=your_reddit_client_secret

# OpenAI API (required for LLM link identification)
OPENAI_API_KEY=sk-your-openai-api-key-here

# Supabase (optional - for database storage)
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# AWS (optional - for S3 upload)
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
S3_PREFIX=posts/

# Bedrock Knowledge Base (optional)
BEDROCK_KB_ID=your-kb-id
BEDROCK_DATASOURCE_ID=your-datasource-id
```

**Minimum required:** At least `OPENAI_API_KEY` for link identification to work properly.

## Step 5: Run the Pipeline

### Basic Usage

```bash
python run_full_pipeline.py
```

This will use default settings:
- Subreddit: `SextStories`
- Max posts: `100`
- Sort: `new`

### Common Usage Examples

**1. Specify subreddit and number of posts:**
```bash
python run_full_pipeline.py --subreddit SextStories --max-posts 50
```

**2. Use different sort order:**
```bash
python run_full_pipeline.py --subreddit SextStories --sort hot --max-posts 100
```

**3. Increase processing speed (more workers):**
```bash
python run_full_pipeline.py --subreddit SextStories --max-posts 100 --workers 30 --ocr-workers 50
```

**4. Fetch all posts (not recommended for first run):**
```bash
python run_full_pipeline.py --subreddit SextStories --fetch-all
```

**5. Resume from a specific post:**
```bash
python run_full_pipeline.py --subreddit SextStories --resume-after t3_1abc123
```

**6. Use search queries (for comprehensive scraping):**
```bash
python run_full_pipeline.py --subreddit SextStories --search-queries "a,b,c,d,e" --max-posts 200
```

**7. Process from pre-collected link chunks:**
```bash
python run_full_pipeline.py --link-chunks chunk1.json chunk2.json
```

## Command Line Arguments

| Argument | Description | Default | Example |
|----------|-------------|---------|---------|
| `--subreddit` | Subreddit name to scrape | `SextStories` | `--subreddit SextStories` |
| `--sort` | Sort order: `new`, `hot`, `top`, `rising`, `controversial` | `new` | `--sort hot` |
| `--max-posts` | Maximum number of posts to process | `100` | `--max-posts 50` |
| `--fetch-all` | Fetch all posts until Reddit stops returning | `False` | `--fetch-all` |
| `--workers` | Number of concurrent worker threads | `20` | `--workers 30` |
| `--ocr-workers` | Number of parallel OCR workers per post | `45` | `--ocr-workers 50` |
| `--resume-after` | Resume from specific post ID | `None` | `--resume-after t3_1abc123` |
| `--search-queries` | Use search-based pagination | `None` | `--search-queries "a,b,c"` |
| `--link-chunks` | Process pre-collected link chunks | `None` | `--link-chunks file.json` |

## What the Pipeline Does

1. **Fetches Posts** - Gets posts from Reddit subreddit
2. **Checks Existing** - Skips posts already in database/results
3. **Identifies Links** - Finds correct link from author's comment using LLM
4. **Downloads Images** - Extracts and downloads all images from the link
5. **Runs OCR** - Extracts text from downloaded images using EasyOCR
6. **Saves Results** - Stores results in `pipeline_results/pipeline_results.json`
7. **Cleans Data** - Creates cleaned and flattened versions
8. **Splits Files** - Splits into individual post files
9. **Uploads to S3** - (Optional) Uploads to S3 bucket
10. **Syncs Bedrock** - (Optional) Syncs with Bedrock Knowledge Base

## Output Files

The pipeline creates these files/directories:

- `pipeline_results/pipeline_results.json` - Main results file
- `pipeline_results/pipeline_results.cleaned.json` - Cleaned version
- `pipeline_results/pipeline_results.cleaned.flat.json` - Flattened version
- `pipeline_results/chunks/` - Chunked output files
- `pipeline_results/posts_YYYYMMDD_HHMMSS/` - Individual post files
- `downloaded_images/` - Temporary image storage (auto-deleted after OCR)
- `pagination_state.json` - State for resuming

## Troubleshooting

### Error: "No module named 'easyocr'"
```bash
pip install easyocr
```

### Error: "OpenAI API key not found"
Make sure your `.env` file contains `OPENAI_API_KEY=your-key-here`

### Error: "Rate limited"
- Add Reddit credentials to `.env` file
- Reduce `--workers` and `--ocr-workers` values
- Add delays between requests

### OCR is slow
- Install GPU version of EasyOCR (if you have GPU)
- Reduce `--ocr-workers` to avoid memory issues
- Use `--max-posts` to limit processing

### Images not downloading
- Check internet connection
- Verify the link extraction is working
- Some sites may block automated requests

## Monitoring Progress

The pipeline provides colored output:
- **Green** - Success messages
- **Yellow** - Warnings/skipped items
- **Red** - Errors
- **Cyan** - Information/status updates
- **Magenta** - Post processing headers

## Stopping the Pipeline

Press `Ctrl+C` to stop the pipeline. It will:
- Save current progress
- Complete current batch processing
- Gracefully exit

## Next Steps

After running the pipeline:
1. Check `pipeline_results/pipeline_results.json` for results
2. Review individual post files in `pipeline_results/posts_*/`
3. If configured, check S3 bucket for uploaded files
4. If configured, check Supabase database for stored posts

## Quick Start (Minimal Setup)

If you just want to test quickly:

```bash
# 1. Create venv and activate
python -m venv venv
venv\Scripts\activate  # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create minimal .env (just OpenAI key)
echo OPENAI_API_KEY=your-key-here > .env

# 4. Run with small test
python run_full_pipeline.py --subreddit SextStories --max-posts 5
```

This will process just 5 posts to test the pipeline.

