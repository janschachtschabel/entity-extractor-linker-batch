#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Translation utilities for the Entity Extractor.

This module provides functions for translating and transforming titles
between different languages, primarily using Wikipedia language links.
"""

import hashlib
import json
import re
import os
import time
import requests
from loguru import logger

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
    Performs a GET request with rate limiting.
    
    Args:
        url: URL for the request
        **kwargs: Additional parameters for requests.get
        
    Returns:
        Response object
    """
    return requests.get(url, **kwargs)


def get_wikipedia_title_in_language(title, from_lang="de", to_lang="en", config=None):
    """
    Converts a Wikipedia title from one language to another using language links.
    
    Args:
        title: The Wikipedia article title
        from_lang: Source language of the title
        to_lang: Target language for the title
        config: Configuration dictionary with timeout settings
        
    Returns:
        The corresponding title in the target language or None if no translation was found
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
        logger.info(f"Looking for translation from {from_lang}:{title} to {to_lang}")
        r = _limited_get(api_url, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
        r.raise_for_status()
        data = r.json()
        
        pages = data.get("query", {}).get("pages", {})
        target_title = None
        
        for page_id, page in pages.items():
            langlinks = page.get("langlinks", [])
            if langlinks:
                # Take the first entry - this should be the version in the target language
                target_title = langlinks[0].get("*")
                break
                
        if target_title:
            logger.info(f"Translation found: {from_lang}:{title} -> {to_lang}:{target_title}")
            return target_title
        else:
            logger.info(f"No translation found from {from_lang}:{title} to {to_lang}")
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving translation for {title}: {e}")
        return None


def translate_to_english(title, lang="auto", cache_ttl=86400*30, config=None):
    """
    Translates a title to English using Wikipedia language links.
    
    Args:
        title: The title to translate
        lang: The source language or 'auto' for automatic detection
        cache_ttl: Cache time in seconds
        config: Configuration dictionary
        
    Returns:
        The translated title or the original title if no translation was found
    """
    if not title:
        return None
    
    # Remove parenthetical additions for better matching
    clean_title_str = clean_title(title)
    
    # If language is not specified, try automatic detection
    if lang == "auto":
        lang = detect_language(clean_title_str)
        logger.info(f"Automatically detected language for '{clean_title_str}': {lang}")
    
    # If it's already English or no language was detected, return the original
    if lang == "en" or not lang:
        return title
    
    if config is None:
        config = DEFAULT_CONFIG
    
    # Generate cache key
    cache_key = f"translate_{lang}_{hashlib.sha256(clean_title_str.encode()).hexdigest()}"
    if config is None or "CACHE_DIR" not in config:
        raise ValueError("translate_to_english requires a config with 'CACHE_DIR'.")
    cache_dir = config["CACHE_DIR"]
    cache_file = get_cache_path(cache_dir, "wikipedia", cache_key)
    
    # Check cache
    cached = load_cache(cache_file)
    cache_valid = cached and (("timestamp" not in cached) or (time.time() - cached.get("timestamp", 0) < cache_ttl))
    if cache_valid:
        return cached.get("translated_title", clean_title_str)
    
    # First try to translate via Wikipedia language links
    english_title = get_wikipedia_title_in_language(clean_title_str, from_lang=lang, to_lang="en", config=config)
    
    # Cache result and return
    if english_title:
        save_cache(cache_file, {"translated_title": english_title, "timestamp": time.time()})
        return english_title
    
    # No result found, return original title
    save_cache(cache_file, {"translated_title": clean_title_str, "timestamp": time.time()}) 
    return clean_title_str
