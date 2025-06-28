import nest_asyncio
from pathlib import Path
import pandas as pd
from time import sleep
import os
from aiohttp import ClientSession, ClientTimeout
import asyncio
from bs4 import BeautifulSoup, NavigableString
import re
from dotenv import load_dotenv
import requests

load_dotenv()
NAMSOR_API_KEY = os.getenv("NAMSOR_API_KEY")
NAMSOR_URL = "https://v2.namsor.com/NamSorAPIv2/api2/json/genderFull/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

MAX_CONCURRENCY = 5  # Adjust based on your needs
TIMEOUT = ClientTimeout(total=30)

# Function to guess gender based on the pronouns used in the author's bio
def guess_gender(text: str) -> str:
    text = text.lower()
    she = len(re.findall(r'\bshe\b', text))
    he  = len(re.findall(r'\bhe\b', text))
    if she > he:   return "female"
    if he  > she:  return "male"
    return "unknown"

#  Async author‐URL extractor
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


# Async author‐meta fetcher
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
    gender = guess_gender(bio_text)
    gender_source = "goodreads" if gender != "unknown" else "unknown"
    return country, gender, gender_source


# Orchestrator: for each book URL find its author and meta, caching per author
async def enrich_books_with_authors_async(df: pd.DataFrame) -> pd.DataFrame:
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    author_cache: dict[str, tuple[str,str]] = {}

    async with ClientSession(timeout=TIMEOUT) as session:

        async def handle_book(book_url: str):
            async with sem:
                try:
                    author_url = await extract_author_url(book_url, session)
                    if author_url not in author_cache:
                        country, gender, gender_source = await fetch_author_meta(author_url, session)
                        author_cache[author_url] = (country, gender, gender_source)
                    return author_cache[author_url]
                except Exception:
                    return ("", "unknown", "")

        # launch one task per book (author fetches will be de-duplicated by cache)
        tasks = [asyncio.create_task(handle_book(url)) for url in df["link"]]
        results = await asyncio.gather(*tasks)

    # unpack into new columns
    countries, genders, gender_sources = zip(*results)
    out = df.copy() 
    out["author_country"] = countries
    out["author_gender"] = genders
    out["gender_source"] = gender_sources
    return out


def query_namsor(name: str = "") -> dict:
    headers = {
        "X-API-KEY": NAMSOR_API_KEY,
        "Accept": "application/json"    
    }
    url = NAMSOR_URL + name
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def fill_from_namsor(row):
    if row["author_gender"] == "unknown":
        meta = namsor_cache.get(row["author"], {"gender": "unknown/non-binary", "confidence": 0})
        gender = meta["gender"]
        if gender in ("male", "female"):
            row["author_gender"] = gender
            row["gender_source"] = "namsor"
    else:
        row["author_gender"] = "unknown/non-binary"
        row["gender_source"] = "namsor"

    # Final fallback to manual map
    manual_map = load_manual_gender_map()
    if row["author_gender"] == "unknown/non-binary":
        manual_gender = manual_map.get(row["author"])
        if manual_gender in ("male", "female"):
            row["author_gender"] = manual_gender
            row["gender_source"] = "manual"

    return row

def enrich_books_with_authors(df: pd.DataFrame) -> pd.DataFrame:

    nest_asyncio.apply()

    # Read the bio of the authors from goodreads and predict gender
    #enriched_df = asyncio.run(enrich_books_with_authors_async(df))

    #Test block
    enriched_df = df.copy()
    enriched_df["author_country"] = ""
    enriched_df["author_gender"] = "unknown"
    enriched_df["gender_source"] = "unknown"

    # NamSor fallback for unknowns
    mask_unknown = enriched_df["author_gender"] == "unknown"
    unknown_authors = (
        enriched_df[mask_unknown]
        .loc[:, ["author"]]
        .drop_duplicates("author")
        .reset_index(drop=True)
    )

    namsor_cache = {}
    for _, row in unknown_authors.iterrows():
        name = row["author"].strip()
        try:
            result = query_namsor(name=name)
            gender = result.get("likelyGender", "unknown")
            confidence = result.get("probabilityCalibrated", 0.0)
        except Exception as e:
            print(f"⚠️ NamSor failed for {name!r}: {e}")
            gender, confidence = "unknown", 0.0

        if confidence < 0.85:
            gender = "unknown/non-binary"

        namsor_cache[name] = {"gender": gender, "confidence": confidence}
        sleep(0.5)

    df_final = enriched_df.apply(fill_from_namsor, axis=1)
    return df_final

def load_manual_gender_map() -> dict:
    manual_path = Path(__file__).resolve().parents[1] / "data/manual_overrides/gender_manual.csv"
    if manual_path.exists():
        df = pd.read_csv(manual_path)
        return dict(zip(df["author"], df["author_gender"]))
    return {}