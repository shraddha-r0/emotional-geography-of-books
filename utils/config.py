from pathlib import Path
# Project root = 2 levels up from config.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
CLEAN_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "clean_books.csv"