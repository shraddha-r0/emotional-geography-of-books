"""
Country Extraction Module

This module provides functionality to extract and standardize country information
from text using spaCy's Named Entity Recognition (NER) and additional heuristics.
"""

import spacy
import pandas as pd
import re
from typing import Dict, Optional, Pattern, Set, Any
import pycountry
from tqdm import tqdm
from functools import lru_cache

# Cache for country name lookups
country_name_cache: Dict[str, str] = {}

# Pre-compile regex patterns for common country name variations
COUNTRY_PATTERNS = {
    r'\busa\b|\bus\b|\bu\.s\.\b|\bu\.s\.a\.\b|\bamerica\b': 'United States',
    r'\buk\b|\bu\.k\.\b|\bbritain\b|\bgreat britain\b|\bengland\b|\bscotland\b|\bwales\b|\bnorthern ireland\b': 'United Kingdom',
    r'\bsouth korea\b': 'South Korea',
    r'\bnorth korea\b': 'North Korea',
    r'\brussia\b': 'Russian Federation',
    r'\bvietnam\b': 'Viet Nam',
    r'\bczech republic\b': 'Czechia',
    r'\bburma\b': 'Myanmar',
    r'\bdrc\b|\bdr congo\b': 'Democratic Republic of the Congo',
    r'\btanzania\b': 'United Republic of Tanzania',
    r'\bivory coast\b': "Côte d'Ivoire",
    r'\bholland\b': 'Netherlands',
}

# Compile all patterns once
COMPILED_PATTERNS = [(re.compile(pattern, re.IGNORECASE), country) 
                     for pattern, country in COUNTRY_PATTERNS.items()]

# Lazy loading of spaCy model
_nlp = None

def get_nlp():
    """Lazily load the spaCy model only when needed."""
    global _nlp
    if _nlp is None:
        try:
            _nlp = spacy.load("en_core_web_sm")  # Using small model for speed
        except OSError:
            import subprocess
            import sys
            print("Downloading spaCy model (this will only happen once)...")
            subprocess.run([sys.executable, "-m", "spacy", "download", "en_core_web_sm"], 
                         check=True)
            _nlp = spacy.load("en_core_web_sm")
    return _nlp

def load_country_mappings() -> Dict[str, str]:
    """
    Load a comprehensive mapping of country names and variations to standardized names.
    """
    mappings = {}
    
    # Add country patterns
    for pattern, country in COUNTRY_PATTERNS.items():
        mappings[country.lower()] = country
    
    # Add official country names and common variations
    for country in pycountry.countries:
        name = country.name.lower()
        mappings[name] = country.name
        
        if hasattr(country, 'common_name'):
            mappings[country.common_name.lower()] = country.name
            
        if hasattr(country, 'official_name'):
            mappings[country.official_name.lower()] = country.name
    
    return mappings

# Load country mappings
COUNTRY_MAPPINGS = load_country_mappings()

def standardize_country_name(country_name: str) -> Optional[str]:
    """Standardize country name to a canonical form."""
    if not country_name:
        return None
        
    normalized = country_name.lower().strip()
    
    # Check against our mapping first (fast path)
    if normalized in COUNTRY_MAPPINGS:
        return COUNTRY_MAPPINGS[normalized]
    
    # Try patterns for common variations
    for pattern, country in COMPILED_PATTERNS:
        if pattern.search(normalized):
            return country
    
    # Check for substrings in country names (slower path)
    for name, std_name in COUNTRY_MAPPINGS.items():
        if normalized in name or name in normalized:
            return std_name
    
    return None

def extract_country_from_text(text: str) -> Optional[str]:
    """Extract country from text using optimized matching and spaCy NER as fallback."""
    if not text or pd.isna(text) or str(text).lower() in {'unknown', 'nan', 'none', ''}:
        return None
    
    text_str = str(text).lower()
    
    # First try direct lookup (fastest)
    if text_str in COUNTRY_MAPPINGS:
        return COUNTRY_MAPPINGS[text_str]
    
    # Try patterns (fast)
    for pattern, country in COMPILED_PATTERNS:
        if pattern.search(text_str):
            return country
    
    # Only use spaCy if no match found (slow)
    try:
        doc = get_nlp()(text_str)
        
        # Look for GPE (Geopolitical Entity) entities
        for ent in doc.ents:
            if ent.label_ in {'GPE', 'LOC', 'NORP'} and len(ent.text) > 2:
                standardized = standardize_country_name(ent.text)
                if standardized:
                    return standardized
    except Exception as e:
        print(f"Error processing text with spaCy: {e}")
    
    return None

def extract_countries_from_dataframe(
    df: pd.DataFrame, 
    text_column: str = 'author_country',
    output_column: str = 'extracted_country'
) -> pd.DataFrame:
    """Extract countries from a DataFrame column containing location text."""
    # Make a copy to avoid modifying the original
    result_df = df.copy()
    
    # Initialize progress bar
    tqdm.pandas(desc="Extracting countries")
    
    # Apply extraction to each row
    result_df[output_column] = result_df[text_column].progress_apply(extract_country_from_text)
    
    # Print summary
    unknown_count = result_df[output_column].isna().sum()
    total = len(result_df)
    print(f"\nExtraction complete. Found countries for {total - unknown_count}/{total} "
          f"({((total - unknown_count) / total * 100):.1f}%) authors.")
    
    if unknown_count > 0:
        print("\nTop 10 most common unknown values:")
        print(result_df[result_df[output_column].isna()][text_column].value_counts().head(10))
    
    return result_df

def analyze_country_distribution(df: pd.DataFrame, country_column: str = 'extracted_country') -> None:
    """Print a summary of country distribution in the DataFrame."""
    if country_column not in df.columns:
        print(f"Error: Column '{country_column}' not found in DataFrame")
        return
    
    print("\nCountry Distribution:")
    country_counts = df[country_column].value_counts()
    print(country_counts.head(20))
    
    single_occurrence = country_counts[country_counts == 1].index.tolist()
    if single_occurrence:
        print(f"\nCountries with only one occurrence: {', '.join(map(str, single_occurrence))}")

# Example usage
if __name__ == "__main__":
    # Example DataFrame
    data = {
        'author': ['John Smith', 'Jane Doe', 'Carlos Ruiz Zafón', 'Haruki Murakami'],
        'author_country': [
            'New York, United States',
            'London, UK',
            'Barcelona, Spain',
            'Kyoto, Japan'
        ]
    }
    
    df = pd.DataFrame(data)
    
    # Extract countries
    result_df = extract_countries_from_dataframe(df)
    
    # Print results
    print("\nExtraction Results:")
    print(result_df[['author', 'author_country', 'extracted_country']])
    
    # Analyze distribution
    analyze_country_distribution(result_df)