from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd

# 🛠 Setup visible Chrome browser
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")  # for stability
    return webdriver.Chrome(options=chrome_options)

# 🧠 Parse book data from HTML
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