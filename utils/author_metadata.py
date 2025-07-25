"""
Author Metadata Module

This module provides functionality to enrich book data with author metadata including
gender and country information. It uses multiple data sources in the following order:
1. Manual mapping (from gender_manual.csv)
2. Goodreads author bio analysis (pronoun detection)
3. Genderize.io API (as a fallback)

Key Features:
- Asynchronous HTTP requests for better performance
- Rate limiting to prevent being blocked by Goodreads
- Automatic retries with exponential backoff
- Caching of Genderize.io API responses
- Comprehensive error handling

Usage:
    from author_metadata import run_enrichment
    
    # Assuming df has 'link' and 'author' columns
    enriched_df = run_enrichment(df)
"""

import nest_asyncio
from pathlib import Path
import pandas as pd
import os
from aiohttp import ClientSession, ClientTimeout, TCPConnector
import asyncio
from bs4 import BeautifulSoup, NavigableString, Tag
import re
from dotenv import load_dotenv
import requests
import json
from tqdm import tqdm
from typing import List, Dict, Tuple, Set, Optional
import time

# Apply nest_asyncio to allow nested event loops (useful in Jupyter notebooks)
nest_asyncio.apply()

# Load environment variables from .env file
load_dotenv()

# Constants
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
MAX_CONCURRENT_REQUESTS = 20  # Maximum number of concurrent HTTP requests
BATCH_SIZE = 50  # Number of authors to process in each batch
TIMEOUT = ClientTimeout(total=30)  # 30 seconds timeout for HTTP requests

# Rate limiting configuration
GOODREADS_REQUEST_DELAY = 15  # Seconds to wait between Goodreads requests
MAX_RETRIES = 3  # Number of retry attempts for failed requests
last_request_time = 0  # Tracks the last request time for rate limiting

# Cache for genderize.io API responses to avoid redundant API calls
genderize_cache = {}

# Load manual gender mappings from CSV file
MANUAL_MAP = {}
manual_map_path = Path(__file__).resolve().parents[1] / "data/manual_overrides/gender_manual.csv"
if manual_map_path.exists():
    try:
        manual_df = pd.read_csv(manual_map_path)
        MANUAL_MAP = dict(zip(manual_df["author"], manual_df["author_gender"]))
    except Exception as e:
        print(f"Error loading manual gender map: {e}")

async def rate_limit():
    """
    Ensure minimum delay between Goodreads requests to prevent rate limiting.
    
    This function checks the time since the last request and waits if necessary.
    """
    global last_request_time
    now = time.time()
    time_since_last = now - last_request_time
    
    if time_since_last < GOODREADS_REQUEST_DELAY:
        sleep_time = GOODREADS_REQUEST_DELAY - time_since_last
        #print(f"[RATE LIMIT] Waiting {sleep_time:.1f}s before next request...")
        await asyncio.sleep(sleep_time)
    
    last_request_time = time.time()

async def fetch_with_retry(session: ClientSession, url: str, max_retries: int = MAX_RETRIES) -> Optional[str]:
    """
    Fetch URL with automatic retry and exponential backoff.
    
    Args:
        session: aiohttp ClientSession for making HTTP requests
        url: URL to fetch
        max_retries: Maximum number of retry attempts (default: 3)
    
    Returns:
        Fetched HTML content or None if all retries fail
    """
    for attempt in range(max_retries):
        try:
            await rate_limit()  # Apply rate limiting before each request
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', 30))
                    print(f"[RATE LIMIT] Rate limited. Waiting {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    continue
                response.raise_for_status()
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                print(f"[WARNING] Failed to fetch {url} after {max_retries} attempts: {e}")
                return None
            # Exponential backoff: 2^attempt * 1 second
            wait_time = (2 ** attempt) * 1
            print(f"[RETRY] Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
    return None

def get_gender_from_name(name: str) -> Optional[str]:
    """
    Get gender from manual mapping only.
    
    Args:
        name: Full name of the author
    
    Returns:
        Gender if found in manual mapping, None otherwise
    """
    if not name:
        return None
    return MANUAL_MAP.get(name)

async def fetch_author_meta(author_url: str, session: ClientSession, author_name: str) -> Tuple[str, str, str]:
    """
    Fetch author metadata from Goodreads with multiple fallback methods.
    
    Priority order:
    1. Manual mapping
    2. Goodreads bio analysis
    3. Genderize.io API
    
    Args:
        author_url: URL of the author's Goodreads page
        session: aiohttp ClientSession for making HTTP requests
        author_name: Full name of the author
    
    Returns:
        Tuple of (country, gender, gender_source)
    """
    try:
        # 1. Check manual mapping first
        try:
            gender = get_gender_from_name(author_name)
            if gender:
                return "unknown", gender, "manual"
        except Exception as e:
            print(f"[WARNING] Error in manual mapping for {author_name}: {str(e)}")
        
        country = ""
        
        # 2. Try to get from Goodreads bio
        try:
            html = await fetch_with_retry(session, author_url)
            if not html:
                raise RuntimeError("Failed to fetch author page")
                
            soup = BeautifulSoup(html, "html.parser")
            
            # Extract country
            try:
                born_div = soup.find("div", class_="dataTitle", string=re.compile(r"^\s*Born\s*$"))
                if born_div:
                    for sib in born_div.next_siblings:
                        if isinstance(sib, NavigableString):
                            txt = sib.strip()
                            if txt:
                                country = txt
                                break
                        elif isinstance(sib, Tag):
                            txt = sib.get_text(strip=True)
                            if txt.lower() != "clear" and txt:
                                country = txt
                                break
            except Exception as e:
                print(f"[WARNING] Error extracting country for {author_name}: {str(e)}")
            
            # Extract bio and check for pronouns
            try:
                bio_container = soup.select_one("div.aboutAuthorInfo") or \
                              soup.find(id=re.compile(r"freeTextContainerauthor"))
                bio_text = bio_container.get_text(" ", strip=True) if bio_container else ""
                
                gender = guess_gender(bio_text) or "unknown"
                if gender != "unknown":
                    return country or "unknown", gender, "goodreads"
            except Exception as e:
                print(f"[WARNING] Error analyzing bio for {author_name}: {str(e)}")
                
        except Exception as e:
            print(f"[WARNING] Error processing Goodreads data for {author_name}: {str(e)}")
        
        # 3. Try Genderize.io as last resort
        try:
            genderize_result = query_genderize(author_name)
            if genderize_result and genderize_result.get("gender"):
                return country or "unknown", genderize_result["gender"], "genderize.io"
        except Exception as e:
            print(f"[WARNING] Error querying Genderize.io for {author_name}: {str(e)}")
        
        return country or "unknown", "unknown", "none"
        
    except Exception as e:
        print(f"[ERROR] Unexpected error processing {author_name}: {str(e)}")
        return "unknown", "unknown", "error"

async def process_author_batch(batch: List[Tuple[str, str]], session: ClientSession) -> List[Tuple[str, str, str, str]]:
    """
    Process a batch of authors in parallel.
    
    Args:
        batch: List of tuples containing (book_url, author_name)
        session: aiohttp ClientSession for making HTTP requests
    
    Returns:
        List of tuples with (url, country, gender, gender_source)
    """
    tasks = []
    for url, author_name in batch:
        task = asyncio.create_task(process_one(url, author_name, session))
        tasks.append(task)
    return await asyncio.gather(*tasks, return_exceptions=True)

async def process_one(url: str, author_name: str, session: ClientSession) -> Tuple[str, str, str, str]:
    """
    Process a single author's metadata.
    
    Args:
        url: URL of the book's Goodreads page
        author_name: Full name of the author
        session: aiohttp ClientSession for making HTTP requests
    
    Returns:
        Tuple of (url, country, gender, gender_source)
    """
    try:
        # Extract author URL from book page if needed
        if not url.startswith("https://www.goodreads.com/author/show"):
            try:
                author_url = await extract_author_url(url, session)
            except Exception as e:
                print(f"[WARNING] Could not extract author URL from {url}: {str(e)}")
                return (url, "unknown", "unknown", "error")
        else:
            author_url = url
            
        country, gender, gender_source = await fetch_author_meta(author_url, session, author_name)
        return (url, country or "unknown", gender or "unknown", gender_source or "none")
        
    except Exception as e:
        print(f"[ERROR] Error processing {url}: {e}")
        return (url, "unknown", "unknown", "error")

async def enrich_books_with_authors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich book data with author metadata using parallel processing.
    
    Args:
        df: DataFrame containing book data with 'link' and 'author' columns
    
    Returns:
        DataFrame with additional columns: author_country, author_gender, gender_source
    """
    out = df.copy()
    
    # Prepare batches
    urls = df["link"].tolist()
    authors = df["author"].tolist()
    
    batches = [
        list(zip(
            urls[i:i + BATCH_SIZE],
            authors[i:i + BATCH_SIZE]
        ))
        for i in range(0, len(urls), BATCH_SIZE)
    ]
    
    # Initialize results
    all_results = []
    
    # Create connector with connection pooling
    connector = TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    
    # Process batches with progress bar
    with tqdm(total=len(urls), desc="Processing authors") as pbar:
        async with ClientSession(connector=connector, timeout=TIMEOUT) as session:
            for batch in batches:
                batch_results = await process_author_batch(batch, session)
                all_results.extend(batch_results)
                pbar.update(len(batch))
    
    # Add results to DataFrame
    out["author_country"] = [r[1] for r in all_results]
    out["author_gender"] = [r[2] for r in all_results]
    out["gender_source"] = [r[3] for r in all_results]
    
    return out

async def extract_author_url(book_url: str, session: ClientSession) -> str:
    """
    Extract author URL from a book's Goodreads page.
    
    Args:
        book_url: URL of the book's Goodreads page
        session: aiohttp ClientSession for making HTTP requests
    
    Returns:
        URL of the author's Goodreads page
    
    Raises:
        RuntimeError: If no author link is found on the page
    """
    html = await fetch_with_retry(session, book_url)
    if not html:
        raise RuntimeError("Failed to fetch book page")
    
    soup = BeautifulSoup(html, "html.parser")

    # old layout
    a = soup.select_one("a.authorName")
    if a and a.get("href"):
        return a["href"]

    # new React layout
    a = soup.select_one(".FeaturedPerson__infoPrimary a.ContributorLink")
    if a and a.get("href"):
        return a["href"]

    raise RuntimeError("no author link")

def query_genderize(name: str) -> dict:
    """
    Query the Genderize.io API to predict gender from a name.
    
    This function caches results to avoid redundant API calls for the same name.
    Only returns results with high confidence (probability > 0.9).
    
    Args:
        name: Full name to query (only the first name is used)
    
    Returns:
        Dictionary containing:
        - gender: 'male', 'female', or None if not found
        - probability: Confidence score (0.0 to 1.0)
    """
    first_name = name.split()[0].lower() if name else ""
    
    if first_name in genderize_cache:
        return genderize_cache[first_name]
    
    time.sleep(0.2)  # ~5 requests/second
    
    url = f"https://api.genderize.io/?name={first_name}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        result = response.json()
        
        if result.get("probability", 0) > 0.9 and result.get("gender"):
            gender = result["gender"]
            genderize_cache[first_name] = {
                "gender": gender,
                "probability": result["probability"]
            }
            return genderize_cache[first_name]
            
    except Exception as e:
        print(f"Error querying Genderize.io API: {e}")
    
    return {"gender": None, "probability": 0.0}

def guess_gender(text: str) -> Optional[str]:
    """
    Guess author's gender based on pronouns in their biography text.
    
    This function looks for gender-indicative pronouns in the text and returns
    the most frequently occurring gender. If no clear pattern is found or if
    the counts are equal, returns 'unknown'.
    
    Args:
        text: Biography text to analyze
    
    Returns:
        'male', 'female', or 'unknown' if no clear indicators found
    """
    if not text:
        return "unknown"
    
    text = text.lower()
    
    male_terms = sum(text.count(term) for term in [" he ", " his ", " him "])
    female_terms = sum(text.count(term) for term in [" she ", " her ", " hers "])
    
    if male_terms > female_terms:
        return "male"
    elif female_terms > male_terms:
        return "female"
    return "unknown"

def run_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Synchronous wrapper for the async enrichment process.
    
    This function allows the async code to be called from synchronous contexts
    like Jupyter notebooks or scripts. It sets up the asyncio event loop
    and runs the enrichment process.
    
    Args:
        df: Input DataFrame containing 'link' and 'author' columns
    
    Returns:
        DataFrame with additional columns:
        - author_country: Extracted country from author's profile
        - author_gender: Determined gender ('male', 'female', or 'unknown')
        - gender_source: Source of the gender data ('manual', 'goodreads', 'genderize.io', or 'none')
    
    Example:
        >>> df = pd.DataFrame({
        ...     'link': ['https://www.goodreads.com/book/show/...', ...],
        ...     'author': ['J.K. Rowling', ...]
        ... })
        >>> enriched_df = run_enrichment(df)
    """
    return asyncio.run(enrich_books_with_authors(df))

# This allows the script to be imported without immediately running the event loop
if __name__ == "__main__":
    # Example usage when run directly
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_df = pd.DataFrame({
            'link': ['https://www.goodreads.com/book/show/3.Harry_Potter_and_the_Sorcerer_s_Stone'],
            'author': ['J.K. Rowling']
        })
        result = run_enrichment(test_df)
        print("Test Results:")
        print(result[["author", "author_gender", "gender_source"]])
