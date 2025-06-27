from pathlib import Path
import pandas as pd
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.config import RAW_DATA_PATH
from utils.scraper_utils import setup_driver, parse_books_from_html

# 🚀 Main execution
if __name__ == "__main__":
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
        df.to_csv(RAW_DATA_PATH / f"goodreads_books_{year}.csv", index=False)
        print(f"💾 Saved to {RAW_DATA_PATH / f'goodreads_books_{year}.csv'}\n")

    print("🎉 Done!")