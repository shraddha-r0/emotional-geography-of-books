import pandas as pd
from pathlib import Path
from tqdm import tqdm

from utils.config import RAW_DATA_PATH

def load_all_books(raw_path=RAW_DATA_PATH) -> pd.DataFrame:
    files = list(raw_path.glob("goodreads_books_*.csv"))
    if not files:
        raise FileNotFoundError("🚫 No Goodreads files found!")

    print(f"📂 Found {len(files)} Goodreads files.")
    all_dfs = []

    for file in tqdm(files, desc="📂 Loading Goodreads files"):
        try:
            year = int(file.stem.split("_")[-1])
            df = pd.read_csv(file)
            df["published_year"] = year
            all_dfs.append(df)
        except Exception as e:
            print(f"⚠️ Error loading {file.name}: {e}")

    if not all_dfs:
        raise ValueError("🚫 No valid book data loaded!")

    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all.sort_values(by="published_year", inplace=True)
    print(f"📚 Total books loaded: {len(df_all)}")

    return df_all

def clean_ratings_count(value):
    if pd.isna(value):
        return 0
    value = value.lower().replace("ratings", "").strip()
    multipliers = {"k": 1_000, "m": 1_000_000}
    for suffix, multiplier in multipliers.items():
        if value.endswith(suffix):
            return int(float(value[:-1]) * multiplier)
    try:
        return int(value.replace(",", ""))
    except:
        return 0

def clean_books(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ratings_count"] = df["ratings_count"].apply(clean_ratings_count)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    df["author"] = df["author"].fillna("").str.strip()
    df["author_first"] = df["author"].apply(lambda x: x.split()[0].lower() if x else "")
    df["source"] = "Goodreads"

    df.drop_duplicates(inplace=True)
    return df