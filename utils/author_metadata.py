# Function to guess gender based on the pronouns used in the author's bio
def guess_gender(text: str) -> str:
    text = text.lower()
    she = len(re.findall(r'\bshe\b', text))
    he  = len(re.findall(r'\bhe\b', text))
    if she > he:   return "female"
    if he  > she:  return "male"
    return "unknown"

#  Async author‐URL extractor
async def extract_author_url(book_url: str, session: aiohttp.ClientSession) -> str:
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
async def fetch_author_meta(author_url: str, session: aiohttp.ClientSession) -> tuple[str, str]:
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

    return country, gender


# Orchestrator: for each book URL find its author and meta, caching per author
async def enrich_books_with_authors_async(df: pd.DataFrame) -> pd.DataFrame:
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    author_cache: dict[str, tuple[str,str]] = {}

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:

        async def handle_book(book_url: str):
            async with sem:
                try:
                    author_url = await extract_author_url(book_url, session)
                    if author_url not in author_cache:
                        author_cache[author_url] = await fetch_author_meta(author_url, session)
                    return author_cache[author_url]
                except Exception:
                    return ("", "unknown")

        # launch one task per book (author fetches will be de-duplicated by cache)
        tasks = [asyncio.create_task(handle_book(url)) for url in df["link"]]
        results = await asyncio.gather(*tasks)

    # unpack into new columns
    countries, genders = zip(*results)
    out = df.copy()
    out["author_country"] = countries
    out["author_gender"]  = genders
    return out


# Usage in notebook
# Assuming `df` is your DataFrame with a "link" column of book URLs:
enriched = await enrich_books_with_authors_async(df)
