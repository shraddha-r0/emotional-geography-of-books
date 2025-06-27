from bs4 import BeautifulSoup
import pandas as pd
import time
from pathlib import Path

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}
MAX_CONCURRENCY = 30
TIMEOUT = aiohttp.ClientTimeout(total=15)
nest_asyncio.apply()


# 🛠 Setup visible Chrome browser
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")  # for stability
    return webdriver.Chrome(options=chrome_options)

# 🧠 Scrape DOM using BeautifulSoup
def parse_books_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    books = []
    items = soup.find_all("article", class_="BookListItem")
    print(f"✅ Found {len(items)} books")

    for item in items:
        title_tag = item.find("a", attrs={"data-testid": "bookTitle"})
        author_tag = item.find("span", attrs={"data-testid": "name"})
        rating_tag = item.find("span", attrs={"data-testid": "ratingValue"})
        ratings_count_tag = item.find("div", class_="AverageRating__ratingsCount")
        desc_tag = item.find("div", attrs={"data-testid": "contentContainer"})

        books.append({
            "title": title_tag.text.strip() if title_tag else "",
            "author": author_tag.text.strip() if author_tag else "",
            "link": title_tag["href"] if title_tag else "",
            "rating": rating_tag.text.strip() if rating_tag else "",
            "ratings_count": ratings_count_tag.text.strip() if ratings_count_tag else "",
            "description": desc_tag.text.strip() if desc_tag else ""
        })

    return pd.DataFrame(books)

# 🚀 Main execution
if __name__ == "__main__":
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    years = [2020, 2021, 2022, 2023, 2024]

    for year in years:
        url = f"https://www.goodreads.com/book/popular_by_date/{year}"
        print(f"📚 Now scraping year: {year}")
        driver = setup_driver()
        driver.get(url)

        print(f"🔍 Loaded page for {year}. Manually click 'Show more books' a few times...")
        input("⏸ Press ENTER when done and ready to scrape...")

        html = driver.page_source
        driver.quit()

        df = parse_books_from_html(html)
        print(f"✅ Scraped {len(df)} books for {year}")
        df.to_csv(f"data/raw/goodreads_books_{year}.csv", index=False)
        print(f"💾 Saved to data/raw/goodreads_books_{year}.csv\n")
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    print("🎉 Done!")