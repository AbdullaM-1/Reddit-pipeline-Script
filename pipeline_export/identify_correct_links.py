#!/usr/bin/env python
"""
Script to identify the correct link for each post using LLM.
Analyzes the author's comment and post title to determine which link is correct.
"""

import json
import re
from typing import Optional, List, Dict
from dotenv import dotenv_values
from colorama import Fore, Style
import os

# Try to import OpenAI, if not available, we'll use a fallback
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print(f"{Fore.YELLOW}OpenAI library not found. Install with: pip install openai{Style.RESET_ALL}")

# ============================================
# CONFIGURATION
# ============================================
INPUT_FILE = "subreddit_SextStories_hot_posts.json"  # Input JSON file with posts
OUTPUT_FILE = "posts_with_correct_links.json"  # Output file with identified links
# ============================================

# Load environment variables
config = dotenv_values(".env")
OPENAI_API_KEY = config.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")


def extract_links_from_text(text: str) -> List[str]:
    """
    Extract all URLs from text (both markdown links and plain URLs)
    
    Args:
        text: Text containing links
    
    Returns:
        List of extracted URLs
    """
    links = []
    
    # Extract markdown links: [text](url)
    markdown_links = re.findall(r'\[([^\]]+)\]\(([^\)]+)\)', text)
    for link_text, url in markdown_links:
        links.append(url.strip())
    
    # Extract plain URLs (http/https)
    plain_urls = re.findall(r'https?://[^\s\)]+', text)
    for url in plain_urls:
        # Clean up URL (remove trailing punctuation)
        url = url.rstrip('.,;:!?)')
        if url not in links:
            links.append(url)
    
    return links


def find_author_comment(post: Dict) -> Optional[Dict]:
    """
    Find the comment made by the post author
    
    Args:
        post: Post dictionary
    
    Returns:
        Author's comment or None
    """
    post_author = post.get("author", "")
    comments = post.get("comments", [])
    
    # Search in top-level comments
    for comment in comments:
        if comment.get("author") == post_author:
            return comment
        
        # Search in replies
        replies = comment.get("replies", [])
        for reply in find_author_in_replies(replies, post_author):
            return reply
    
    return None


def find_author_in_replies(replies: List[Dict], author: str) -> List[Dict]:
    """Recursively find comments by author in replies"""
    found = []
    for reply in replies:
        if reply.get("author") == author:
            found.append(reply)
        # Check nested replies
        nested_replies = reply.get("replies", [])
        if nested_replies:
            found.extend(find_author_in_replies(nested_replies, author))
    return found


def identify_correct_link_with_llm(
    post_title: str,
    author_comment: str,
    links: List[str],
    use_openai: bool = True
) -> Optional[str]:
    """
    Use LLM to identify the correct link for a post
    
    Args:
        post_title: Title of the post
        author_comment: Author's comment text
        links: List of links found in the comment
        use_openai: Whether to use OpenAI API
    
    Returns:
        The correct link or None
    """
    if not links:
        return None
    
    if len(links) == 1:
        return links[0]
    
    if use_openai and OPENAI_AVAILABLE and OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            
            # Create prompt for LLM
            prompt = f"""You are analyzing a Reddit post to identify the correct link.

Post Title: "{post_title}"

Author's Comment:
{author_comment}

Links found in the comment:
{chr(10).join([f"{i+1}. {link}" for i, link in enumerate(links)])}

Task: Identify which link is the CORRECT one for this specific post/chapter based on the title.

Rules:
- The post title often contains a chapter number or part number
- The correct link should match the chapter/part mentioned in the title
- Look for patterns like "Chapter 6", "Part 5", etc. in both title and links
- If the title says "Chapter 6", the link should be for Chapter 6, not other chapters
- Return ONLY the link URL, nothing else

Which link is correct? Return only the URL:"""

            # Log what we're sending to LLM
            print(f"  {Fore.CYAN}[OpenAI] Sending request to API...{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}{'='*70}{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}[OpenAI] PROMPT SENT TO LLM:{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}{'='*70}{Style.RESET_ALL}")
            print(f"  {Fore.WHITE}Post Title: {post_title}{Style.RESET_ALL}")
            print(f"  {Fore.WHITE}Author's Comment (first 500 chars): {author_comment[:500]}...{Style.RESET_ALL}" if len(author_comment) > 500 else f"  {Fore.WHITE}Author's Comment: {author_comment}{Style.RESET_ALL}")
            print(f"  {Fore.WHITE}Number of links: {len(links)}{Style.RESET_ALL}")
            print(f"  {Fore.WHITE}Links:{Style.RESET_ALL}")
            for i, link in enumerate(links, 1):
                print(f"    {i}. {link}")
            print(f"  {Fore.CYAN}{'='*70}{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}[OpenAI] FULL PROMPT TEXT:{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}{'='*70}{Style.RESET_ALL}")
            print(f"  {Fore.WHITE}{prompt}{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}{'='*70}{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}Full prompt length: {len(prompt)} characters{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}{'='*70}{Style.RESET_ALL}")
            
            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Using cheaper model, can change to gpt-4 if needed
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that identifies correct links from Reddit posts."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # Low temperature for more deterministic results
                max_tokens=200
            )
            
            # Log the full response
            print(f"  {Fore.CYAN}[OpenAI] Response received:{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}  Model: {response.model}{Style.RESET_ALL}")
            print(f"  {Fore.CYAN}  Usage: {response.usage.prompt_tokens} prompt tokens, {response.usage.completion_tokens} completion tokens{Style.RESET_ALL}")
            
            result = response.choices[0].message.content.strip()
            print(f"  {Fore.CYAN}[OpenAI] Raw response: {result}{Style.RESET_ALL}")
            
            # Extract URL from response (in case LLM adds extra text)
            url_match = re.search(r'https?://[^\s\)]+', result)
            if url_match:
                extracted_url = url_match.group(0).rstrip('.,;:!?)')
                print(f"  {Fore.GREEN}[OpenAI] Extracted URL: {extracted_url}{Style.RESET_ALL}")
                return extracted_url
            
            # Check if result is one of our links
            for link in links:
                if link in result or result in link:
                    print(f"  {Fore.GREEN}[OpenAI] Matched link: {link}{Style.RESET_ALL}")
                    return link
            
            print(f"  {Fore.YELLOW}[OpenAI] Using raw result as link{Style.RESET_ALL}")
            return result
            
        except Exception as e:
            print(f"  {Fore.RED}[OpenAI] Error calling API: {e}{Style.RESET_ALL}")
            print(f"  {Fore.YELLOW}[OpenAI] Falling back to simple pattern matching{Style.RESET_ALL}")
            # Fallback to simple matching
            return identify_correct_link_simple(post_title, links)
    else:
        # Fallback to simple pattern matching
        return identify_correct_link_simple(post_title, links)


def identify_correct_link_simple(post_title: str, links: List[str]) -> Optional[str]:
    """
    Simple pattern matching to identify correct link (fallback method)
    
    Args:
        post_title: Title of the post
        links: List of links
    
    Returns:
        The most likely correct link
    """
    # Extract chapter/part number from title
    chapter_match = re.search(r'[Cc]hapter\s+(\d+)', post_title)
    part_match = re.search(r'[Pp]art\s+(\d+)', post_title)
    
    target_number = None
    if chapter_match:
        target_number = chapter_match.group(1)
    elif part_match:
        target_number = part_match.group(1)
    
    if target_number:
        # Look for link containing the chapter/part number
        for link in links:
            if f"chapter-{target_number}" in link.lower() or \
               f"chapter_{target_number}" in link.lower() or \
               f"ch-{target_number}" in link.lower() or \
               f"part-{target_number}" in link.lower() or \
               f"part_{target_number}" in link.lower() or \
               f"pt-{target_number}" in link.lower():
                return link
    
    # If no match, return first link (usually the most relevant)
    return links[0] if links else None


def process_posts(input_file: str, output_file: str):
    """
    Process all posts and identify correct links
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file
    """
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Identifying Correct Links for Posts{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")
    
    # Load posts
    print(f"{Fore.CYAN}Loading posts from {input_file}...{Style.RESET_ALL}")
    with open(input_file, "r", encoding="utf-8") as f:
        posts = json.load(f)
    
    print(f"{Fore.GREEN}Loaded {len(posts)} posts{Style.RESET_ALL}\n")
    
    # Check if OpenAI is available
    use_openai = OPENAI_AVAILABLE and OPENAI_API_KEY is not None
    if use_openai:
        print(f"{Fore.GREEN}Using OpenAI API for link identification{Style.RESET_ALL}\n")
    else:
        print(f"{Fore.YELLOW}Using simple pattern matching (OpenAI not configured){Style.RESET_ALL}")
        print(f"{Fore.YELLOW}To use OpenAI, set OPENAI_API_KEY in .env file{Style.RESET_ALL}\n")
    
    results = []
    stats = {
        "total": len(posts),
        "with_author_comment": 0,
        "with_links": 0,
        "identified": 0,
        "no_comment": 0,
        "no_links": 0
    }
    
    # Process each post
    for i, post in enumerate(posts, 1):
        post_title = post.get("title", "N/A")
        post_author = post.get("author", "N/A")
        
        print(f"[{i}/{len(posts)}] Processing: {post_title[:50]}...")
        
        # Find author's comment
        author_comment_obj = find_author_comment(post)
        
        if not author_comment_obj:
            print(f"  {Fore.YELLOW}No author comment found{Style.RESET_ALL}")
            stats["no_comment"] += 1
            results.append({
                "post_id": post.get("id"),
                "title": post_title,
                "author": post_author,
                "correct_link": None,
                "all_links": [],
                "author_comment": None,
                "status": "no_author_comment"
            })
            continue
        
        stats["with_author_comment"] += 1
        author_comment_text = author_comment_obj.get("body", "")
        
        # Extract links from author's comment
        links = extract_links_from_text(author_comment_text)
        
        if not links:
            print(f"  {Fore.YELLOW}No links found in author comment{Style.RESET_ALL}")
            stats["no_links"] += 1
            results.append({
                "post_id": post.get("id"),
                "title": post_title,
                "author": post_author,
                "correct_link": None,
                "all_links": [],
                "author_comment": author_comment_text[:200] + "..." if len(author_comment_text) > 200 else author_comment_text,
                "status": "no_links"
            })
            continue
        
        stats["with_links"] += 1
        print(f"  {Fore.CYAN}Found {len(links)} link(s){Style.RESET_ALL}")
        if len(links) > 1:
            print(f"  {Fore.CYAN}Links: {', '.join([link[:50] + '...' if len(link) > 50 else link for link in links[:3]])}{Style.RESET_ALL}")
        
        # Identify correct link using LLM
        correct_link = identify_correct_link_with_llm(
            post_title,
            author_comment_text,
            links,
            use_openai=use_openai
        )
        
        if correct_link:
            stats["identified"] += 1
            print(f"  {Fore.GREEN}Identified correct link: {correct_link[:60]}...{Style.RESET_ALL}")
        else:
            print(f"  {Fore.YELLOW}Could not identify correct link{Style.RESET_ALL}")
        
        results.append({
            "post_id": post.get("id"),
            "title": post_title,
            "author": post_author,
            "correct_link": correct_link,
            "all_links": links,
            "author_comment": author_comment_text[:500] + "..." if len(author_comment_text) > 500 else author_comment_text,
            "status": "success" if correct_link else "failed"
        })
    
    # Save results
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Summary:{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"Total posts: {stats['total']}")
    print(f"Posts with author comment: {stats['with_author_comment']}")
    print(f"Posts with links: {stats['with_links']}")
    print(f"Links identified: {stats['identified']}")
    print(f"No author comment: {stats['no_comment']}")
    print(f"No links found: {stats['no_links']}")
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n{Fore.GREEN}Results saved to: {output_file}{Style.RESET_ALL}")


if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"{Fore.RED}Error: Input file '{INPUT_FILE}' not found{Style.RESET_ALL}")
        exit(1)
    
    process_posts(INPUT_FILE, OUTPUT_FILE)

