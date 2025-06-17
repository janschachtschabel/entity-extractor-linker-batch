"""
HTML Scraping Utilities for the Entity Extractor.

Functions for scraping and extracting content from HTML pages,
especially for fallbacks with Wikipedia pages.
"""

import requests
from loguru import logger
from bs4 import BeautifulSoup
from entityextractor.utils.text_utils import is_valid_wikipedia_url

def scrape_wikipedia_extract(url, timeout=10):
    """
    Extracts the main content of a Wikipedia page using BeautifulSoup when the API fails.
    
    Args:
        url: Wikipedia URL
        timeout: Timeout for the request in seconds
        
    Returns:
        Dictionary with extract, title and categories (if available)
    """
    if not is_valid_wikipedia_url(url):
        logger.warning(f"Invalid Wikipedia URL for scraping: {url}")
        return None
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title
        title_tag = soup.find('h1', {'id': 'firstHeading'})
        title = title_tag.text.strip() if title_tag else ""
        
        # Extract the first paragraph (Extract)
        extract = ""
        content_div = soup.find('div', {'id': 'mw-content-text'})
        if content_div:
            paragraphs = content_div.find_all('p', recursive=False)
            for p in paragraphs:
                if p.text.strip():  # Skip empty paragraphs
                    extract = p.text.strip()
                    break
        
        # Extract categories
        categories = []
        catlinks = soup.find('div', {'id': 'catlinks'})
        if catlinks:
            cat_items = catlinks.find_all('li')
            for item in cat_items:
                cat_text = item.text.strip()
                if cat_text:
                    categories.append(cat_text)
        
        logger.info(f"BeautifulSoup extraction for '{url}' successful: Title={title}, Extract length={len(extract)}")
        
        return {
            "extract": extract,
            "title": title,
            "categories": categories,
            "scraped": True
        }
    
    except Exception as e:
        logger.error(f"Error during BeautifulSoup scraping of '{url}': {str(e)}")
        return None
