#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Übersetzungs-Hilfsfunktionen für den Entity Extractor.

Dieses Modul stellt Funktionen zur Übersetzung und Transformation von Titeln
zwischen verschiedenen Sprachen bereit, hauptsächlich durch Nutzung der
Wikipedia-Sprachlinks.
"""

import logging
import hashlib
import json
import re
import os
import time
import requests

from entityextractor.utils.cache_utils import get_cache_path, load_cache, save_cache
from entityextractor.utils.rate_limiter import RateLimiter
from entityextractor.utils.language_utils import detect_language, clean_title
from entityextractor.config.settings import get_config, DEFAULT_CONFIG

_config = get_config()
_rate_limiter = RateLimiter(
    _config["RATE_LIMIT_MAX_CALLS"], 
    _config["RATE_LIMIT_PERIOD"], 
    _config["RATE_LIMIT_BACKOFF_BASE"], 
    _config["RATE_LIMIT_BACKOFF_MAX"]
)

@_rate_limiter
def _limited_get(url, **kwargs):
    """
    Führt einen GET-Request mit Rate-Limiting durch.
    
    Args:
        url: URL für den Request
        **kwargs: Zusätzliche Parameter für requests.get
        
    Returns:
        Response-Objekt
    """
    return requests.get(url, **kwargs)


def get_wikipedia_title_in_language(title, from_lang="de", to_lang="en", config=None):
    """
    Konvertiert einen Wikipedia-Titel von einer Sprache in eine andere über Sprachlinks.
    
    Args:
        title: Der Wikipedia-Artikel-Titel
        from_lang: Ausgangssprache des Titels
        to_lang: Zielsprache für den Titel
        config: Konfigurationswörterbuch mit Timeout-Einstellungen
        
    Returns:
        Der entsprechende Titel in der Zielsprache oder None wenn keine Übersetzung gefunden wurde
    """
    if from_lang == to_lang:
        return title
        
    if config is None:
        config = DEFAULT_CONFIG
        
    api_url = f"https://{from_lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "langlinks",
        "titles": title,
        "lllang": to_lang,
        "format": "json",
        "maxlag": config.get("WIKIPEDIA_MAXLAG")
    }
    
    headers = {"User-Agent": config.get("USER_AGENT")}
    
    try:
        logging.info(f"Suche Übersetzung von {from_lang}:{title} nach {to_lang}")
        r = _limited_get(api_url, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
        r.raise_for_status()
        data = r.json()
        
        pages = data.get("query", {}).get("pages", {})
        target_title = None
        
        for page_id, page in pages.items():
            langlinks = page.get("langlinks", [])
            if langlinks:
                # Nehme den ersten Eintrag - dies sollte die Version in der Zielsprache sein
                target_title = langlinks[0].get("*")
                break
                
        if target_title:
            logging.info(f"Übersetzung gefunden: {from_lang}:{title} -> {to_lang}:{target_title}")
            return target_title
        else:
            logging.info(f"Keine Übersetzung gefunden von {from_lang}:{title} nach {to_lang}")
            return None
            
    except Exception as e:
        logging.error(f"Fehler beim Abrufen der Übersetzung für {title}: {e}")
        return None


def translate_to_english(title, lang="auto", cache_ttl=86400*30, config=None):
    """
    Übersetzt einen Titel ins Englische unter Verwendung der Wikipedia-Sprachlinks.
    
    Args:
        title: Der zu übersetzende Titel
        lang: Die Quellsprache oder 'auto' für automatische Erkennung
        cache_ttl: Cache-Zeit in Sekunden
        config: Konfigurationswörterbuch
        
    Returns:
        Der übersetzte Titel oder der Originaltitel wenn keine Übersetzung gefunden wurde
    """
    if not title:
        return None
    
    # Entferne Klammerzusätze für besseres Matching
    clean_title_str = clean_title(title)
    
    # Wenn Sprache nicht angegeben, versuche automatische Erkennung
    if lang == "auto":
        lang = detect_language(clean_title_str)
        logging.info(f"Automatisch erkannte Sprache für '{clean_title_str}': {lang}")
    
    # Falls es bereits Englisch ist oder keine Sprache erkannt wurde, Original zurückgeben
    if lang == "en" or not lang:
        return title
    
    if config is None:
        config = DEFAULT_CONFIG
    
    # Cache-Schlüssel generieren
    cache_key = f"translate_{lang}_{hashlib.sha256(clean_title_str.encode()).hexdigest()}"
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
    cache_file = get_cache_path(cache_dir, "wikipedia", cache_key)
    
    # Cache überprüfen
    cached = load_cache(cache_file)
    cache_valid = cached and (("timestamp" not in cached) or (time.time() - cached.get("timestamp", 0) < cache_ttl))
    if cache_valid:
        return cached.get("translated_title", clean_title_str)
    
    # Zuerst versuchen, über Wikipedia-Sprachlinks zu übersetzen
    english_title = get_wikipedia_title_in_language(clean_title_str, from_lang=lang, to_lang="en", config=config)
    
    # Ergebnis cachen und zurückgeben
    if english_title:
        save_cache(cache_file, {"translated_title": english_title, "timestamp": time.time()})
        return english_title
    
    # Kein Ergebnis gefunden, Originaltitel zurückgeben
    save_cache(cache_file, {"translated_title": clean_title_str, "timestamp": time.time()}) 
    return clean_title_str
