"""
Country Extraction Module

This module provides functionality to extract and standardize country information
from text using spaCy's Named Entity Recognition (NER) and additional heuristics.
"""

import spacy
import pandas as pd
from typing import List, Dict, Optional, Tuple
import pycountry
from tqdm import tqdm

# Load spaCy model (medium model for better accuracy with locations)
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    # If the model is not found, download it
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_md"])
    nlp = spacy.load("en_core_web_md")

# Cache for country name lookups
country_name_cache = {}


def load_country_mappings() -> Dict[str, str]:
    """
    Load a comprehensive mapping of country names and variations to standardized names.
    
    Returns:
        Dictionary mapping country names/variations to standardized names
    """
    mappings = {
        'usa': 'United States',
        'us': 'United States',
        'u.s.': 'United States',
        'u.s.a.': 'United States',
        'america': 'United States',
        'united states of america': 'United States',
        'uk': 'United Kingdom',
        'u.k.': 'United Kingdom',
        'britain': 'United Kingdom',
        'great britain': 'United Kingdom',
        'england': 'United Kingdom',
        'scotland': 'United Kingdom',
        'wales': 'United Kingdom',
        'northern ireland': 'United Kingdom',
        'republic of ireland': 'Ireland',
        'south korea': 'South Korea',
        'north korea': 'North Korea',
        'russia': 'Russian Federation',
        'vietnam': 'Viet Nam',
        'czech republic': 'Czechia',
        'burma': 'Myanmar',
        'congo': 'Congo',
        'drc': 'Democratic Republic of the Congo',
        'dr congo': 'Democratic Republic of the Congo',
        'tanzania': 'United Republic of Tanzania',
        'ivory coast': "Côte d'Ivoire",
        'holland': 'Netherlands',
    }
    
    # Add official country names and common variations
    for country in pycountry.countries:
        name = country.name.lower()
        mappings[name] = country.name
        
        # Add common name if different from official name
        if hasattr(country, 'common_name'):
            mappings[country.common_name.lower()] = country.name
            
        # Add official name variations
        if hasattr(country, 'official_name'):
            mappings[country.official_name.lower()] = country.name
    
    return mappings

# Load country mappings
COUNTRY_MAPPINGS = load_country_mappings()


def standardize_country_name(country_name: str) -> Optional[str]:
    """
    Standardize country name to a canonical form.
    
    Args:
        country_name: Input country name to standardize
        
    Returns:
        Standardized country name or None if not a valid country
    """
    if not country_name:
        return None
        
    # Check cache first
    normalized = country_name.lower().strip()
    if normalized in country_name_cache:
        return country_name_cache[normalized]
    
    # Check against our mapping
    if normalized in COUNTRY_MAPPINGS:
        country_name_cache[normalized] = COUNTRY_MAPPINGS[normalized]
        return COUNTRY_MAPPINGS[normalized]
    
    # Try to find a close match
    for key, value in COUNTRY_MAPPINGS.items():
        if country_name.lower() in key or key in country_name.lower():
            country_name_cache[normalized] = value
            return value
    
    return None


def extract_country_from_text(text: str) -> Optional[str]:
    """
    Extract country from text using spaCy NER and additional heuristics.
    
    Args:
        text: Input text to extract country from
        
    Returns:
        Standardized country name or None if no country found
    """
    if not text or pd.isna(text) or str(text).lower() in ['unknown', 'nan', 'none', '']:
        return None
    
    # First try to find exact matches in our mappings
    text_lower = str(text).lower()
    for country_variant, standard_name in COUNTRY_MAPPINGS.items():
        if country_variant in text_lower:
            return standard_name
    
    # If no exact match, use spaCy NER
    doc = nlp(str(text))
    
    # Look for GPE (Geopolitical Entity) entities
    for ent in doc.ents:
        if ent.label_ in ['GPE', 'LOC', 'NORP']:
            standardized = standardize_country_name(ent.text)
            if standardized:
                return standardized
    
    # If no GPE found, check for country names in the text
    for token in doc:
        standardized = standardize_country_name(token.text)
        if standardized:
            return standardized
    
    return None


def extract_countries_from_dataframe(
    df: pd.DataFrame, 
    text_column: str = 'author_country',
    output_column: str = 'extracted_country'
) -> pd.DataFrame:
    """
    Extract countries from a DataFrame column containing location text.
    
    Args:
        df: Input DataFrame
        text_column: Name of the column containing location text
        output_column: Name of the column to store extracted countries
        
    Returns:
        DataFrame with an additional column containing extracted countries
    """
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
    """
    Print a summary of country distribution in the DataFrame.
    
    Args:
        df: Input DataFrame
        country_column: Name of the column containing country information
    """
    if country_column not in df.columns:
        print(f"Error: Column '{country_column}' not found in DataFrame")
        return
    
    print("\nCountry Distribution:")
    print("=" * 50)
    
    # Count countries
    country_counts = df[country_column].value_counts(dropna=False)
    
    # Print top countries
    print("\nTop 20 Countries:")
    print(country_counts.head(20))
    
    # Print summary stats
    print(f"\nTotal unique countries: {len(country_counts) - (1 if 'Unknown' in country_counts else 0)}")
    unknown_count = country_counts.get('Unknown', 0) + country_counts.get(None, 0)
    print(f"Unknown countries: {unknown_count} ({unknown_count/len(df)*100:.1f}%)")
    
    # Print countries with only one occurrence
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