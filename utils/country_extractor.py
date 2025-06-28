import pycountry
import re
from fuzzywuzzy import process
from bs4 import BeautifulSoup
import random
from typing import Optional
import requests
import pandas as pd

# List of common country name variations to standardize
country_variations = {
    'usa': 'United States',
    'america': 'United States',
    'uk': 'United Kingdom',
    'britain': 'United Kingdom',
    'england': 'United Kingdom',
    'scotland': 'United Kingdom',
    'wales': 'United Kingdom',
    'ireland': 'Ireland',
    'australia': 'Australia',
    'canada': 'Canada',
    'new zealand': 'New Zealand',
    'india': 'India',
    'china': 'China',
    'japan': 'Japan',
    'germany': 'Germany',
    'france': 'France',
    'italy': 'Italy',
    'spain': 'Spain',
    'russia': 'Russia',
    'brazil': 'Brazil',
    'mexico': 'Mexico',
    'south africa': 'South Africa',
    'netherlands': 'Netherlands',
    'belgium': 'Belgium',
    'sweden': 'Sweden',
    'norway': 'Norway',
    'denmark': 'Denmark',
    'switzerland': 'Switzerland',
    'austria': 'Austria',
    'greece': 'Greece',
    'turkey': 'Turkey',
    'egypt': 'Egypt',
    'south korea': 'South Korea',
    'north korea': 'North Korea',
    'vietnam': 'Vietnam',
    'thailand': 'Thailand',
    'malaysia': 'Malaysia',
    'singapore': 'Singapore',
    'indonesia': 'Indonesia',
    'philippines': 'Philippines',
    'argentina': 'Argentina',
    'chile': 'Chile',
    'peru': 'Peru',
    'colombia': 'Colombia'
}

def extract_country(text):
    """
    Extract and standardize country names from text using fuzzy matching
    """
    # First try to find exact matches
    text_lower = text.lower()
    for country in country_variations:
        if country in text_lower:
            return country_variations[country]
    
    # If no exact match, try fuzzy matching
    countries = list(country_variations.values())
    if countries:
        match = process.extractOne(text, countries)
        if match and match[1] > 80:  # Confidence threshold
            return match[0]
    
    return None

def extract_countries_from_df(df, text_column='author_country'):
    """
    Extract countries from a DataFrame column and add standardized country names
    """
    # Create a new column for extracted countries
    df['country'] = df[text_column].apply(extract_country)
    
    # Get unique countries and their counts
    country_counts = df['country'].value_counts(dropna=True)
    
    # Add standardized country information
    df['country_info'] = df['country'].apply(get_country_info)
    
    return country_counts, df

def get_country_info(country_name):
    """
    Get standardized country information using pycountry
    """
    try:
        country = pycountry.countries.get(name=country_name)
        if country:
            return {
                'name': country.name,
                'alpha_2': country.alpha_2,
                'alpha_3': country.alpha_3,
                'numeric': country.numeric
            }
        return None
    except:
        return None


def search_author_country(author_name: str) -> Optional[str]:
    """
    Search for author's country using web search
    """
    try:
        # Create search URL
        search_url = f"https://www.google.com/search?q={author_name}+author+biography+country"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(1, 3))
        
        # Make request
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for country mentions in the first few search results
            text = soup.get_text()
            
            # Extract potential country mentions
            potential_countries = []
            for country in country_variations.values():
                if country.lower() in text.lower():
                    potential_countries.append(country)
            
            # If we found any potential matches, use extract_country to standardize
            if potential_countries:
                return extract_country(potential_countries[0])
            
            # If no direct matches, try to extract from text
            country = extract_country_from_text(text)
            if country:
                return country
                
    except Exception as e:
        print(f"Error searching for {author_name}: {str(e)}")
    
    return None

def extract_country_from_text(text: str) -> Optional[str]:
    """
    Extract country information from text using existing extract_country function
    """
    # Look for common patterns
    patterns = [
        r"(born|from|nationality|origin|lives in|based in)\s*(?:the\s+)?([A-Za-z\s]+(?:\s+Republic)?(?:\s+of)?(?:\s+the)?(?:\s+United)?(?:\s+States)?)",
        r"(?:the\s+)?([A-Za-z\s]+(?:\s+Republic)?(?:\s+of)?(?:\s+the)?(?:\s+United)?(?:\s+States)?)\s+(?:author|writer|novelist)",
        r"(?:the\s+)?([A-Za-z\s]+(?:\s+Republic)?(?:\s+of)?(?:\s+the)?(?:\s+United)?(?:\s+States)?)\s+(?:literature|fiction)"
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            # Get the first match and standardize it
            country = matches[0][1].strip()
            return extract_country(country)
    
    return None

def get_countries_for_authors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get countries for authors with missing country information
    """
    # Get authors with missing country info
    unknown_authors = df[df['author_country'] == 'Unknown']['author'].unique()
    
    # Create a dictionary to store results
    author_countries = {}
    
    # Process each author
    for author in unknown_authors:
        country = search_author_country(author)
        author_countries[author] = country
    
    # Update the DataFrame
    df['author_country'] = df['author'].map(author_countries)
    
    return df