#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Centralized API requests and rate-limiting functionality for the Entity Extractor.

This module provides unified functions for HTTP requests with rate limiting,
error handling, and batch processing which can be used by all service modules.
"""

import time
import requests
import aiohttp
import xml.etree.ElementTree as ET
import json
import urllib.parse
from typing import Dict, Any, Optional, Union, List
from functools import wraps
from loguru import logger

from entityextractor.utils.rate_limiter import RateLimiter
from entityextractor.utils.cache_utils import get_cache_path, load_cache, save_cache
from entityextractor.config.settings import get_config, DEFAULT_CONFIG

# Load configuration
_config = get_config()

# Rate-Limiter for all HTTP requests
_rate_limiter = RateLimiter(
    _config.get("RATE_LIMIT_MAX_CALLS", 10), 
    _config.get("RATE_LIMIT_PERIOD", 1.0), 
    _config.get("RATE_LIMIT_BACKOFF_BASE", 1.0), 
    _config.get("RATE_LIMIT_BACKOFF_MAX", 60.0)
)

# Asynchronous Rate-Limiter for all HTTP requests
_async_rate_limiter = RateLimiter(
    _config.get("RATE_LIMIT_MAX_CALLS", 10), 
    _config.get("RATE_LIMIT_PERIOD", 1.0), 
    _config.get("RATE_LIMIT_BACKOFF_BASE", 1.0), 
    _config.get("RATE_LIMIT_BACKOFF_MAX", 60.0)
)

def create_standard_headers(user_agent=None, config=None):
    """
    Creates standard headers for API requests.
    
    Args:
        user_agent: Optional custom User-Agent
        config: Optional configuration object
        
    Returns:
        Dict with standard headers
    """
    if config is None:
        config = get_config()
        
    headers = {
        "User-Agent": user_agent or config.get("USER_AGENT", "EntityExtractor/1.0"),
        "Accept": "application/json, text/html, application/xml;q=0.9, */*;q=0.8"
    }
    
    # Add additional headers depending on the API
    if config.get("API_KEY"):
        headers["Authorization"] = f"Bearer {config.get('API_KEY')}"
        
    # Add Accept-Language if specified in config
    if config.get("LANGUAGE"):
        headers["Accept-Language"] = config.get("LANGUAGE")
        
    return headers

@_rate_limiter
def limited_get(url, headers=None, params=None, timeout=None, config=None):
    """
    Performs a GET request with rate limiting.
    
    Args:
        url: URL for the request
        headers: Optional, HTTP headers
        params: Optional, URL parameters
        timeout: Optional, timeout in seconds
        config: Optional, configuration
        
    Returns:
        Response object
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    if headers is None:
        headers = create_standard_headers()
        
    if timeout is None:
        timeout = config.get("TIMEOUT_THIRD_PARTY", 15)
        
    return requests.get(url, headers=headers, params=params, timeout=timeout)


@_rate_limiter
async def limited_get_async(session, url, headers=None, params=None, timeout=None, config=None, format=None):
    """
    Performs an asynchronous GET request with rate limiting.
    
    Args:
        session: aiohttp.ClientSession for HTTP requests
        url: URL for the request
        headers: Optional, HTTP headers
        params: Optional, URL parameters
        timeout: Optional, timeout in seconds
        config: Optional, configuration
        format: Optional, format of the response ('json' or None)
        
    Returns:
        Response object or JSON data, depending on format
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    if headers is None:
        headers = create_standard_headers()
        
    if timeout is None:
        timeout = config.get("TIMEOUT_THIRD_PARTY", 15)
    
    try:
        async with session.get(url, headers=headers, params=params, timeout=timeout) as response:
            if response.status != 200:
                logger.warning(f"HTTP error {response.status} for URL: {url}")
                return None
            
            if format == 'json':
                return await response.json()
            else:
                return response
    except Exception as e:
        logger.error(f"Error in asynchronous GET request for URL {url}: {e}")
        return None

@_rate_limiter
def limited_post(url, data=None, json=None, headers=None, timeout=None, config=None):
    """
    Performs a POST request with rate limiting.
    
    Args:
        url: URL for the request
        data: Optional, form data
        json: Optional, JSON data
        headers: Optional, HTTP headers
        timeout: Optional, timeout in seconds
        config: Optional, configuration
        
    Returns:
        Response object
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    if headers is None:
        headers = create_standard_headers()
        
    if timeout is None:
        timeout = config.get("TIMEOUT_THIRD_PARTY", 15)
        
    return requests.post(url, data=data, json=json, headers=headers, timeout=timeout)

@_async_rate_limiter
async def async_limited_get(url, headers=None, params=None, timeout=None, config=None):
    """
    Performs an asynchronous GET request with rate limiting.
    
    Args:
        url: URL for the request
        headers: Optional, HTTP headers
        params: Optional, URL parameters
        timeout: Optional, timeout in seconds
        config: Optional, configuration
        
    Returns:
        JSON response or None on error
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    if headers is None:
        headers = create_standard_headers()
        
    if timeout is None:
        timeout = config.get("TIMEOUT_THIRD_PARTY", 15)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=timeout) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"HTTP-Fehler {response.status} bei {url}")
                    return None
    except Exception as e:
        logger.error(f"Fehler bei API-Anfrage an {url}: {str(e)}")
        return None

def batch_request(urls, method="GET", headers=None, params=None, cache_dir=None, 
                  cache_prefix="api_batch", cache_ttl=86400, config=None):
    """
    Performs multiple HTTP requests in a batch with caching and rate limiting.
    
    Args:
        urls: List of URLs or Dict with keys and URLs
        method: HTTP method (GET or POST)
        headers: Optional, HTTP headers
        params: Optional, URL parameters
        cache_dir: Optional, directory for cache files
        cache_prefix: Optional, prefix for cache files
        cache_ttl: Optional, cache validity in seconds
        config: Optional, configuration
        
    Returns:
        Dict with URLs as keys and response data as values
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    if cache_dir is None:
        if config is None or "CACHE_DIR" not in config:
            raise ValueError("batch_request erfordert eine config mit 'CACHE_DIR'.")
        cache_dir = config["CACHE_DIR"]
    
    is_dict_input = isinstance(urls, dict)
    url_dict = urls if is_dict_input else {url: url for url in urls}
    
    # Vorbereiten der Parameter-Liste
    if params is None:
        params_list = [None] * len(url_dict)
    elif isinstance(params, dict):
        params_list = [params] * len(url_dict)
    else:
        params_list = params
        
    results = {}
    
    for i, (key, url) in enumerate(url_dict.items()):
        # Cache-Schlüssel erstellen
        current_params = params_list[i] if i < len(params_list) else None
        cache_key = f"{cache_prefix}:{url}"
        if current_params:
            cache_key += f":{hash(frozenset(current_params.items()))}"
        
        cache_path = get_cache_path(cache_dir, "api", cache_key)
        cached = load_cache(cache_path)
        
        # Cache-Prüfung
        if cached and (time.time() - cached.get("timestamp", 0) <= cache_ttl):
            logger.info(f"Cache-Treffer für URL: {url}")
            results[key] = cached
            continue
            
        try:
            if method.upper() == "GET":
                response = limited_get(url, headers=headers, params=current_params, config=config)
            else:
                response = limited_post(url, json=current_params, headers=headers, config=config)
                
            response.raise_for_status()
            
            # Versuchen, JSON zu parsen
            try:
                data = response.json()
                content_type = "json"
            except json.JSONDecodeError:
                # Wenn es kein JSON ist, Text zurückgeben
                data = response.text
                content_type = "text"
                
                # Versuchen, als XML zu parsen, wenn es wie XML aussieht
                if data.strip().startswith("<") and data.strip().endswith(">"):
                    try:
                        xml_root = ET.fromstring(data)
                        content_type = "xml"
                    except ET.ParseError:
                        pass
            
            result = {
                "status": "success",
                "data": data,
                "content_type": content_type,
                "timestamp": time.time()
            }
            
            # In Cache speichern
            save_cache(cache_path, result)
            results[key] = result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Fehler bei der API-Abfrage für {url}: {e}")
            results[key] = {
                "status": "error",
                "error": str(e),
                "timestamp": time.time()
            }
    
    return results

def convert_wikipedia_to_dbpedia_uri(wikipedia_url, use_dbpedia_de=True):
    """
    Converts a Wikipedia URL to a DBpedia resource URI.
    
    Args:
        wikipedia_url: Wikipedia URL, e.g., https://de.wikipedia.org/wiki/Berlin
        use_dbpedia_de: Optional, whether to use the German DBpedia
        
    Returns:
        DBpedia resource URI, e.g., http://de.dbpedia.org/resource/Berlin
    """
    import re
    import urllib.parse
    
    # Sprache und Titel extrahieren
    match = re.match(r'https?://([a-z]+)\.wikipedia\.org/wiki/(.+)', wikipedia_url)
    if not match:
        logger.warning(f"Ungültige Wikipedia-URL: {wikipedia_url}")
        return None
        
    lang = match.group(1)
    title = match.group(2)
    
    # URL decoding and then correct encoding for DBpedia
    title = urllib.parse.unquote(title)
    
    # Create DBpedia URI based on use_dbpedia_de parameter
    # With use_dbpedia_de=True we use de.dbpedia.org, otherwise dbpedia.org
    target_domain = "de.dbpedia.org" if use_dbpedia_de else "dbpedia.org"
    
    # If the target domain doesn't match the language in the URL,
    # we log the change
    original_domain = f"{lang}.dbpedia.org"
    if original_domain != target_domain:
        logger.info(f"Changing DBpedia domain from {original_domain} to {target_domain} based on configuration")
    
    dbpedia_uri = f"http://{target_domain}/resource/{urllib.parse.quote(title)}"
    
    logger.info(f"Converting Wikipedia URL to DBpedia URI: {wikipedia_url} -> {dbpedia_uri}")
    return dbpedia_uri

def limited_sparql_query(endpoint, query, config=None):
    """
    Performs a SPARQL query with rate limiting.
    
    Args:
        endpoint: The SPARQL endpoint (URL)
        query: The SPARQL query as a string
        config: Configuration dictionary
        
    Returns:
        The result of the SPARQL query as a Python object
    """
    if config is None:
        from entityextractor.config.settings import DEFAULT_CONFIG
        config = DEFAULT_CONFIG
        
    import SPARQLWrapper
    from entityextractor.utils.rate_limiter import RateLimiter
    
    # Use Rate-Limiter for SPARQL queries
    limiter = RateLimiter(
        max_calls=config.get("SPARQL_RATE_LIMIT_CALLS", 5),
        period=config.get("SPARQL_RATE_LIMIT_PERIOD", 1.0)
    )
    
    @limiter
    def execute_sparql_query(endpoint, query, timeout):
        sparql = SPARQLWrapper.SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(SPARQLWrapper.JSON)
        sparql.setTimeout(timeout)
        return sparql.query().convert()
    
    # Execute SPARQL query
    timeout = config.get("DBPEDIA_TIMEOUT", 10)
    return execute_sparql_query(endpoint, query, timeout)

def create_limited_api_call(api_call_func, rate_limiter=None):
    """
    Creates a rate-limited version of an API call function.
    
    Args:
        api_call_func: The function to be rate-limited
        rate_limiter: Optional, a RateLimiter object (default: global standard)
        
    Returns:
        The rate-limited function
    """
    limiter = rate_limiter or _rate_limiter
    
    @limiter
    @wraps(api_call_func)
    def limited_func(*args, **kwargs):
        return api_call_func(*args, **kwargs)
    
    return limited_func


def handle_api_error(response, max_retries=3, retry_delay=2):
    """
    Handles API errors with automatic retry attempts.
    
    Args:
        response: Response object from requests
        max_retries: Maximum number of retry attempts
        retry_delay: Wait time in seconds between attempts
        
    Returns:
        True if retry should be attempted, False if a permanent error occurred
    
    Raises:
        Exception if the error cannot be handled
    """
    if response.status_code >= 500:  # Server error
        if max_retries > 0:
            logger.warning(f"Server error {response.status_code}, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            return True
        else:
            logger.error(f"Server error {response.status_code} after all retry attempts")
            raise Exception(f"Server error {response.status_code}")
    
    elif response.status_code == 429:  # Rate Limit
        retry_after = int(response.headers.get("Retry-After", retry_delay * 2))
        logger.warning(f"Rate limit reached, waiting time: {retry_after} seconds")
        time.sleep(retry_after)
        return True
        
    elif response.status_code >= 400:  # Client error
        logger.error(f"Client error: {response.status_code} - {response.text}")
        return False
        
    return False  # Unknown error


def safe_json_loads(json_str, default=None):
    """
    Safely loads a JSON string.
    
    Args:
        json_str: The JSON string to load
        default: Default value to return in case of error
        
    Returns:
        The parsed JSON object or the default value in case of error
    """
    try:
        return json.loads(json_str)
    except Exception as e:
        logger.error(f"JSON parsing error: {e}")
        return default


def batch_processor(items, batch_size=10, processing_function=None, **kwargs):
    """
    Generic batch processing function for API calls.
    
    Args:
        items: List or Dict of elements to process
        batch_size: Size of each batch
        processing_function: Function to be called per batch
        **kwargs: Additional parameters for the processing function
        
    Returns:
        Dict with combined results of all batches
    """
    if not items:
        return {}
        
    if not processing_function:
        raise ValueError("No processing function specified")
    
    # Split list or dict into batches
    batches = []
    if isinstance(items, dict):
        item_list = list(items.items())
        for i in range(0, len(item_list), batch_size):
            batches.append(dict(item_list[i:i+batch_size]))
    else:
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i+batch_size])
    
    # Process batches and combine results
    results = {}
    for batch in batches:
        try:
            batch_results = processing_function(batch, **kwargs)
            if isinstance(batch_results, dict):
                results.update(batch_results)
        except Exception as e:
            logger.error(f"Error during batch processing: {e}")
    
    return results


def extract_wikidata_id_from_wikipedia(wikipedia_url, config=None):
    """
    Extracts the Wikidata ID from a Wikipedia URL using the Wikipedia API.
    
    Args:
        wikipedia_url: Wikipedia URL
        config: Optional, configuration
        
    Returns:
        Wikidata ID or None
    """
    import re
    import urllib.parse
    
    if config is None:
        config = DEFAULT_CONFIG
    
    # Sprache und Titel extrahieren
    match = re.match(r'https?://([a-z]+)\.wikipedia\.org/wiki/(.+)', wikipedia_url)
    if not match:
        logger.warning(f"Ungültige Wikipedia-URL: {wikipedia_url}")
        return None
        
    lang = match.group(1)
    title = match.group(2)
    
    # URL decoding
    title = urllib.parse.unquote(title)
    
    # Cache-Schlüssel erstellen
    cache_key = f"wikidata_id_from_wikipedia:{lang}:{title}"
    if config is None or "CACHE_DIR" not in config:
        raise ValueError("extract_wikidata_id_from_wikipedia requires a config with 'CACHE_DIR'.")
    cache_path = get_cache_path(config["CACHE_DIR"], "wikidata", cache_key)
    
    # Check cache
    cached = load_cache(cache_path)
    if cached is not None:
        logger.info(f"Using cached Wikidata ID for {title} ({lang}): {cached}")
        return cached
    
    # Create URL for the Wikipedia API
    url = f"https://{lang}.wikipedia.org/w/api.php?action=query&format=json&prop=pageprops&titles={urllib.parse.quote(title)}"
    
    try:
        # Execute query
        headers = create_standard_headers()
        response = limited_get(url, headers=headers, config=config)
        data = response.json()
        
        # Extract Wikidata ID
        if "query" in data and "pages" in data["query"]:
            for page_id, page_info in data["query"]["pages"].items():
                if "pageprops" in page_info and "wikibase_item" in page_info["pageprops"]:
                    wikidata_id = page_info["pageprops"]["wikibase_item"]
                    
                    # Save to cache
                    save_cache(cache_path, wikidata_id)
                    
                    logger.info(f"Wikidata ID for {title} ({lang}) found: {wikidata_id}")
                    return wikidata_id
        
        logger.warning(f"No Wikidata ID found for {title} ({lang})")
        return None
    
    except Exception as e:
        logger.error(f"Error retrieving Wikidata ID for {title} ({lang}): {e}")
        return None
