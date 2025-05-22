"""
HTML Scraping Utilities für den Entity Extractor.

Funktionen zum Scrapen und Extrahieren von Inhalten aus HTML-Seiten,
insbesondere für Fallbacks bei Wikipedia-Seiten.
"""

import requests
import logging
from bs4 import BeautifulSoup
from entityextractor.utils.text_utils import is_valid_wikipedia_url

def scrape_wikipedia_extract(url, timeout=10):
    """
    Extrahiert den Hauptinhalt einer Wikipedia-Seite per BeautifulSoup wenn die API fehlschlägt.
    
    Args:
        url: Wikipedia-URL
        timeout: Timeout für die Anfrage in Sekunden
        
    Returns:
        Dictionary mit extract, title und categories (wenn verfügbar)
    """
    if not is_valid_wikipedia_url(url):
        logging.warning(f"Ungültige Wikipedia-URL für Scraping: {url}")
        return None
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        # Parse HTML mit BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Titel extrahieren
        title_tag = soup.find('h1', {'id': 'firstHeading'})
        title = title_tag.text.strip() if title_tag else ""
        
        # Extrahiere den ersten Absatz (Extract)
        extract = ""
        content_div = soup.find('div', {'id': 'mw-content-text'})
        if content_div:
            paragraphs = content_div.find_all('p', recursive=False)
            for p in paragraphs:
                if p.text.strip():  # Überspringe leere Absätze
                    extract = p.text.strip()
                    break
        
        # Kategorien extrahieren
        categories = []
        catlinks = soup.find('div', {'id': 'catlinks'})
        if catlinks:
            cat_items = catlinks.find_all('li')
            for item in cat_items:
                cat_text = item.text.strip()
                if cat_text:
                    categories.append(cat_text)
        
        logging.info(f"BeautifulSoup-Extraktion für '{url}' erfolgreich: Titel={title}, Extract-Länge={len(extract)}")
        
        return {
            "extract": extract,
            "title": title,
            "categories": categories,
            "scraped": True
        }
    
    except Exception as e:
        logging.error(f"Fehler beim BeautifulSoup-Scraping von '{url}': {str(e)}")
        return None
