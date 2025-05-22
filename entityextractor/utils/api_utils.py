#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
API-Hilfsfunktionen für den Entity Extractor.

Dieses Modul stellt gemeinsame Funktionen für API-Aufrufe, Fehlerbehandlung,
und Ratelimiting zur Verfügung, die von verschiedenen Services verwendet werden.
"""

import logging
import requests
import time
import json
import urllib.parse
from functools import wraps

from entityextractor.utils.rate_limiter import RateLimiter
from entityextractor.config.settings import get_config, DEFAULT_CONFIG

_config = get_config()
_default_rate_limiter = RateLimiter(
    _config["RATE_LIMIT_MAX_CALLS"], 
    _config["RATE_LIMIT_PERIOD"], 
    _config["RATE_LIMIT_BACKOFF_BASE"], 
    _config["RATE_LIMIT_BACKOFF_MAX"]
)


@_default_rate_limiter
def limited_get(url, **kwargs):
    """
    Führt einen GET-Request mit Rate-Limiting durch.
    
    Args:
        url: URL für den Request
        **kwargs: Zusätzliche Parameter für requests.get
        
    Returns:
        Response-Objekt
    """
    # Stelle sicher, dass der Timeout aus der Konfiguration verwendet wird
    if 'timeout' not in kwargs:
        kwargs['timeout'] = _config.get("TIMEOUT_THIRD_PARTY", 15)
    
    return requests.get(url, **kwargs)


def create_limited_api_call(api_call_func, rate_limiter=None):
    """
    Erzeugt eine ratelimitierte Version einer API-Aufruffunktion.
    
    Args:
        api_call_func: Die Funktion, die ratelimitiert werden soll
        rate_limiter: Optional, ein RateLimiter-Objekt (default: globaler Standard)
        
    Returns:
        Die ratelimitierte Funktion
    """
    limiter = rate_limiter or _default_rate_limiter
    
    @limiter
    @wraps(api_call_func)
    def limited_func(*args, **kwargs):
        return api_call_func(*args, **kwargs)
    
    return limited_func


def create_standard_headers(config=None):
    """
    Erzeugt standardisierte Header für API-Anfragen.
    
    Args:
        config: Konfigurationsobjekt (optional)
        
    Returns:
        Dict mit standardisierten HTTP-Headern
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    headers = {
        "User-Agent": config.get("USER_AGENT", "EntityExtractor/1.0"),
        "Accept": "application/json",
        "Accept-Language": config.get("LANGUAGE", "de")
    }
    
    # Weitere Header je nach API hinzufügen
    if config.get("API_KEY"):
        headers["Authorization"] = f"Bearer {config.get('API_KEY')}"
        
    return headers


def handle_api_error(response, max_retries=3, retry_delay=2):
    """
    Behandelt API-Fehler mit automatischen Wiederholungsversuchen.
    
    Args:
        response: Response-Objekt von requests
        max_retries: Maximale Anzahl Wiederholungsversuche
        retry_delay: Wartezeit in Sekunden zwischen Versuchen
        
    Returns:
        True wenn weitererversucht werden soll, False wenn ein permanenter Fehler vorliegt
    
    Raises:
        Exception wenn der Fehler nicht behandelt werden kann
    """
    if response.status_code >= 500:  # Server-Fehler
        if max_retries > 0:
            logging.warning(f"Server-Fehler {response.status_code}, Wiederholung in {retry_delay} Sekunden...")
            time.sleep(retry_delay)
            return True
        else:
            logging.error(f"Server-Fehler {response.status_code} nach allen Wiederholungsversuchen")
            raise Exception(f"Server error {response.status_code}")
    
    elif response.status_code == 429:  # Rate Limit
        retry_after = int(response.headers.get("Retry-After", retry_delay * 2))
        logging.warning(f"Rate Limit erreicht, Wartezeit: {retry_after} Sekunden")
        time.sleep(retry_after)
        return True
        
    elif response.status_code >= 400:  # Client-Fehler
        logging.error(f"Client-Fehler: {response.status_code} - {response.text}")
        return False
        
    return False  # Unbekannter Fehler


def safe_json_loads(json_str, default=None):
    """
    Sicheres Laden eines JSON-Strings.
    
    Args:
        json_str: Der zu ladende JSON-String
        default: Standardwert, der bei Fehler zurückgegeben wird
        
    Returns:
        Das geparste JSON-Objekt oder den Standardwert bei Fehler
    """
    try:
        return json.loads(json_str)
    except Exception as e:
        logging.error(f"JSON-Parsing-Fehler: {e}")
        return default


def batch_processor(items, batch_size=10, processing_function=None, **kwargs):
    """
    Generische Batch-Verarbeitungsfunktion für API-Aufrufe.
    
    Args:
        items: Liste oder Dict der zu verarbeitenden Elemente
        batch_size: Größe jedes Batches
        processing_function: Funktion, die pro Batch aufgerufen wird
        **kwargs: Weitere Parameter für die Verarbeitungsfunktion
        
    Returns:
        Dict mit kombinierten Ergebnissen aller Batches
    """
    if not items:
        return {}
        
    if not processing_function:
        raise ValueError("Keine Verarbeitungsfunktion angegeben")
    
    # Liste oder Dict in Batches aufteilen
    batches = []
    if isinstance(items, dict):
        item_list = list(items.items())
        for i in range(0, len(item_list), batch_size):
            batches.append(dict(item_list[i:i+batch_size]))
    else:
        for i in range(0, len(items), batch_size):
            batches.append(items[i:i+batch_size])
    
    # Batches verarbeiten und Ergebnisse kombinieren
    results = {}
    for batch in batches:
        try:
            batch_results = processing_function(batch, **kwargs)
            if isinstance(batch_results, dict):
                results.update(batch_results)
        except Exception as e:
            logging.error(f"Fehler bei Batch-Verarbeitung: {e}")
    
    return results
