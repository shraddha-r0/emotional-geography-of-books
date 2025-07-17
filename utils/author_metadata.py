import nest_asyncio
from pathlib import Path
import pandas as pd
from time import sleep
import os
from aiohttp import ClientSession, ClientTimeout
import asyncio
from bs4 import BeautifulSoup, NavigableString, Tag
import re
from dotenv import load_dotenv
import requests
import json

load_dotenv()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

MAX_CONCURRENCY = 5  # Adjust based on your needs
TIMEOUT = ClientTimeout(total=30)

# Function to query Genderize.io API
def query_genderize(name: str) -> dict:
    # Extract first name from full name
    first_name = name.split()[0] if name else ""
    url = f"https://api.genderize.io/?name={first_name}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        result = response.json()
        # Only return gender if probability is high enough
        if result.get("probability") and float(result["probability"]) > 0.9:
            return {"gender": result["gender"], "probability": result["probability"]}
        return {"gender": None, "probability": None}
    except Exception as e:
        print(f"Error querying Genderize.io for {name}: {e}")
        return {"gender": None, "probability": None}

# Function to guess gender based on the pronouns used in the author's bio
def guess_gender(text: str) -> str:
    text = text.lower()
    she = len(re.findall(r'\b(she|her|hers)\b', text, re.IGNORECASE))
    he  = len(re.findall(r'\b(he|him|his)\b', text, re.IGNORECASE))
    if she > he:   return "female"
    if he  > she:  return "male"
    return "unknown"

# Function to fetch author URL
async def extract_author_url(book_url: str, session: ClientSession) -> str:
    async with session.get(book_url, headers=HEADERS) as resp:
        resp.raise_for_status()
        html = await resp.text()
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

# Function to fetch author meta from Goodreads
async def fetch_author_meta(author_url: str, session: ClientSession) -> tuple[str, str, str]:
    async with session.get(author_url, headers=HEADERS) as resp:
        resp.raise_for_status()
        html = await resp.text()
    soup = BeautifulSoup(html, "html.parser")

    # country
    country = ""
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

    # bio
    bio_container = soup.select_one("div.aboutAuthorInfo") or \
                    soup.find(id=re.compile(r"freeTextContainerauthor"))
    bio_text = bio_container.get_text(" ", strip=True) if bio_container else ""
    
    # Step 1: Try to get gender from Goodreads bio
    gender = guess_gender(bio_text)
    gender_source = "goodreads" if gender != "unknown" else "unknown"
    
    # Step 2: If not found in bio, try Genderize.io
    if gender == "unknown":
        name_elem = soup.select_one("span.authorName__container") or soup.select_one("h1.authorName")
        if name_elem:
            name = name_elem.get_text(strip=True)
            genderize_result = query_genderize(name)
            if genderize_result["gender"]:
                gender = genderize_result["gender"]
                gender_source = "genderize.io"
    
    # Step 3: If still unknown, try manual mapping
    if gender == "unknown":
        manual_map = load_manual_gender_map()
        print("Manual map:" + str(manual_map))
        name_elem = soup.select_one("span.authorName__container") or soup.select_one("h1.authorName")
        if name_elem:
            name = name_elem.get_text(strip=True)
            print(name)
            if name in manual_map:
                gender = manual_map[name]
                gender_source = "manual_map"
    
    #print(author_url, "-", country, "-", gender, "-", gender_source)
    return country, gender, gender_source

# Function to load manual gender mapping
def load_manual_gender_map() -> dict:
    manual_path = Path(__file__).resolve().parents[1] / "data/manual_overrides/gender_manual.csv"
    if manual_path.exists():
        df = pd.read_csv(manual_path)
        return dict(zip(df["author"], df["author_gender"]))
    return {}

# Main function to enrich books with author metadata
async def enrich_books_with_authors(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    countries, genders, gender_sources = [], [], []
    
    async with ClientSession(timeout=TIMEOUT) as session:
        # Process each book URL
        for url in df["link"]:
            try:
                # Extract author URL
                author_url = await extract_author_url(url, session)
                
                # Fetch author metadata
                country, gender, gender_source = await fetch_author_meta(author_url, session)
                
                countries.append(country)
                genders.append(gender)
                gender_sources.append(gender_source)
                
            except Exception as e:
                print(f"Error processing {url}: {e}")
                countries.append("unknown")
                genders.append("unknown")
                gender_sources.append("unknown")
    
    # Add new columns to the DataFrame
    out["author_country"] = countries
    out["author_gender"] = genders
    out["gender_source"] = gender_sources
    return out

# Function to run the async enrichment process
def run_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """Wrapper function to run the async enrichment process"""
    nest_asyncio.apply()
    return asyncio.run(enrich_books_with_authors(df))
