import pandas as pd
from pathlib import Path
from tqdm import tqdm
from utils.config import RAW_DATA_PATH

def load_all_books(raw_path=RAW_DATA_PATH) -> pd.DataFrame:
    files = list(raw_path).glob("goodreads_books_*.csv")
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