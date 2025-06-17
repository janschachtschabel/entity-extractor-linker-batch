#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fallback mechanisms for the DBpedia service.

This module provides fallback strategies for when the primary DBpedia
SPARQL queries fail to return the required data.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple

import json
import ssl
import aiohttp # Added import
from entityextractor.utils.logging_utils import get_service_logger

# Configure logger
logger = get_service_logger(__name__, 'dbpedia')

# Module-level constants and singletons
DEFAULT_LOOKUP_ENDPOINT = 'https://lookup.dbpedia.org/api/search/KeywordSearch'
DEFAULT_RATE_LIMIT_FALLBACKS = 5 # Default rate limit for fallback operations

# RateLimiter class
class RateLimiter:
    """Simple rate limiter for controlling request rates."""
    def __init__(self, rate_limit: float = DEFAULT_RATE_LIMIT_FALLBACKS):
        self.rate_limit = rate_limit
        # Ensure semaphore count is an integer and at least 1 if rate_limit > 0
        semaphore_count = int(rate_limit) if rate_limit > 0 else 1
        self.semaphore = asyncio.Semaphore(semaphore_count)
        self.last_call = 0.0
        
    async def __aenter__(self):
        await self.semaphore.acquire()
        now = asyncio.get_event_loop().time()
        time_since_last = now - self.last_call
        
        if self.rate_limit > 0 and time_since_last < 1.0 / self.rate_limit:
            await asyncio.sleep((1.0 / self.rate_limit) - time_since_last)
            
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.last_call = asyncio.get_event_loop().time()
        self.semaphore.release()

fallback_rate_limiter = RateLimiter(rate_limit=DEFAULT_RATE_LIMIT_FALLBACKS)

# Placeholder for create_standard_headers - replace with actual import if available
def create_standard_headers() -> Dict[str, str]:
    """Placeholder for utility function to create standard HTTP headers."""
    return {
        'Accept-Charset': 'utf-8',
        'Connection': 'keep-alive',
    }

async def _execute_lookup_api_call(
    query: str,
    config: Dict[str, Any],
    max_hits: int = 5,
    timeout: Optional[float] = None
) -> List[Dict[str, Any]]:
    """Fetch data from DBpedia Lookup API. (Actual API call)"""
    if not query:
        return []
    
    lookup_endpoint = config.get('DBPEDIA_LOOKUP_ENDPOINT', DEFAULT_LOOKUP_ENDPOINT)
    timeout = timeout or config.get('TIMEOUT_THIRD_PARTY', 10.0)
    user_agent = config.get('USER_AGENT', 'EntityExtractor/1.0')
    
    params = {
        'QueryString': query,
        'QueryClass': 'http://dbpedia.org/ontology/Thing',
        'MaxHits': max_hits,
        'format': 'json'
    }
    
    headers = create_standard_headers()
    headers['User-Agent'] = user_agent
    headers['Accept'] = 'application/json'
    
    try:
        async with fallback_rate_limiter:
            async with aiohttp.ClientSession() as session:
                logger.debug(f"Querying DBpedia Lookup API: {query} at {lookup_endpoint} with params {params}")
                
                ssl_context = None
                if config.get('DBPEDIA_SSL_VERIFY', True) is False:
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                
                # DBpedia Lookup API typically uses GET. The original async_fetchers used POST.
                # Reverting to GET for Lookup standard, params handle QueryString.
                async with session.get(
                    lookup_endpoint,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    params=params,
                    ssl=ssl_context
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return _process_lookup_results(data.get('docs', []))
                    
    except Exception as e:
        logger.warning(f"DBpedia Lookup API request failed for query '{query}': {str(e)}", exc_info=logger.isEnabledFor(logging.DEBUG))
        return []

def _process_lookup_results(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process raw results from DBpedia Lookup API."""
    results = []
    for doc in docs:
        if not doc.get('resource') or not doc.get('label'):
            continue
            
        entity = {
            'uri': doc.get('resource', [''])[0],
            'label': doc.get('label', [''])[0],
            'abstract': doc.get('comment', [''])[0],
            'types': doc.get('type', []),
            'categories': doc.get('category', []),
            'score': float(doc.get('score', [0])[0]) if 'score' in doc else 0.0,
            'source': 'lookup_api_fallback',
            'raw': doc
        }
        
        if 'redirect' in doc and doc['redirect']:
            entity['redirects_to'] = doc['redirect'][0]
            
        if 'refCount' in doc and doc['refCount']:
            entity['reference_count'] = int(doc['refCount'][0])
            
        if 'data' in doc and doc['data']:
            for data_item in doc['data']:
                if 'http://dbpedia.org/ontology/thumbnail' in data_item:
                    entity['image_url'] = data_item['http://dbpedia.org/ontology/thumbnail'][0]
                elif 'http://www.w3.org/2002/07/owl#sameAs' in data_item:
                    if 'same_as' not in entity:
                        entity['same_as'] = []
                    entity['same_as'].extend(data_item['http://www.w3.org/2002/07/owl#sameAs'])
        
        results.append(entity)
    
    results.sort(key=lambda x: x.get('score', 0), reverse=True)
    return results


async def apply_dbpedia_fallbacks(
    entity_name: str,
    dbpedia_uri: str,
    current_data: Optional[Dict[str, Any]],
    config: Dict[str, Any],
    max_attempts: int = 3
) -> Optional[Dict[str, Any]]:
    """
    Apply fallback strategies to get DBpedia data when the primary method fails.
    
    Args:
        entity_name: Name of the entity being processed
        dbpedia_uri: DBpedia URI of the entity
        current_data: Any data already retrieved (may be None or incomplete)
        config: Configuration dictionary
        max_attempts: Maximum number of fallback attempts
        
    Returns:
        Dictionary with DBpedia data if successful, None otherwise
    """
    if not entity_name or not dbpedia_uri:
        return None
    
    # If we already have valid data, return it
    if current_data and _has_required_data(current_data):
        return current_data
    
    logger.info(f"Applying fallbacks for entity: {entity_name}")
    
    # List of fallback strategies to try
    fallback_strategies = [
        _try_alternative_endpoints,
        _try_dbpedia_lookup,
        _try_language_fallback
    ]
    
    # Try each fallback strategy in order
    for strategy in fallback_strategies:
        try:
            result = await strategy(entity_name, dbpedia_uri, current_data, config, max_attempts)
            if result and _has_required_data(result):
                logger.info(f"Fallback successful for {entity_name} using {strategy.__name__}")
                return result
        except Exception as e:
            logger.warning(f"Fallback {strategy.__name__} failed for {entity_name}: {str(e)}")
            continue
    
    logger.warning(f"All fallbacks exhausted for {entity_name}")
    return None

async def _try_alternative_endpoints(
    entity_name: str,
    dbpedia_uri: str,
    current_data: Optional[Dict[str, Any]],
    config: Dict[str, Any],
    max_attempts: int
) -> Optional[Dict[str, Any]]:
    """
    Try alternative DBpedia SPARQL endpoints.
    """
    # Get alternative endpoints from config
    endpoints = config.get('DBPEDIA_ALTERNATE_ENDPOINTS', [
        'https://dbpedia.org/sparql',
        'http://dbpedia.org/sparql',
        'http://live.dbpedia.org/sparql'
    ])
    use_de = config.get('DBPEDIA_USE_DE', True)
    if not use_de:
        endpoints = [ep for ep in endpoints if 'de.dbpedia.org' not in ep]
        logger.info("DBPEDIA_USE_DE is False: Filtering out German DBpedia endpoints from fallbacks.")
    # Remove the primary endpoint if it's in the list
    primary_endpoint = config.get('DBPEDIA_ENDPOINT')
    if primary_endpoint and primary_endpoint in endpoints:
        endpoints.remove(primary_endpoint)
    
    if not endpoints:
        return None
    
    logger.debug(f"Trying alternative endpoints for {entity_name}")
    
    # Try each endpoint
    for endpoint in endpoints[:max_attempts]:
        try:
            # Try to fetch data from this endpoint
            # This requires async_fetchers to be importable or moved to a shared location
            # For now, assuming async_fetchers can be imported if this file is in the same package depth
            from . import async_fetchers # Relative import
            results = await async_fetchers.async_fetch_dbpedia_data(
                dbpedia_uris=[dbpedia_uri],
                endpoints=[endpoint],
                batch_size=config.get('DBPEDIA_MAX_BATCH_SIZE', 50),
                user_agent=config.get('USER_AGENT', 'EntityExtractor/1.0'),
                timeout=config.get('DBPEDIA_TIMEOUT', 30),
                max_retries=config.get('DBPEDIA_MAX_RETRIES', 3),
                retry_delay=config.get('DBPEDIA_RETRY_DELAY', 1.0),
                ssl_verify=config.get('DBPEDIA_SSL_VERIFY', False),
                debug_mode=config.get('DEBUG_MODE', False)
            )
            
            if results and dbpedia_uri in results:
                return results[dbpedia_uri]
                
        except Exception as e:
            logger.debug(f"Endpoint {endpoint} failed: {str(e)}")
            continue
    
    return None

async def _try_dbpedia_lookup(
    entity_name: str,
    dbpedia_uri: str,
    current_data: Optional[Dict[str, Any]],
    config: Dict[str, Any],
    max_attempts: int
) -> Optional[Dict[str, Any]]:
    """
    Try the DBpedia Lookup service as a fallback.
    """
    if not config.get('DBPEDIA_USE_LOOKUP', True):
        return None
    
    logger.debug(f"Trying DBpedia Lookup for {entity_name}")
    
    try:
        # Use the entity name for lookup
        lookup_results = await _execute_lookup_api_call(
            query=entity_name,
            config=config,
            max_hits=5
        )
        
        if not lookup_results:
            return None
        
        # Find the best matching result
        for result in lookup_results:
            # Check if this result matches our URI or has a high confidence
            if result.get('uri') == dbpedia_uri or result.get('score', 0) > 0.7:
                return _format_lookup_result(result)
        
        # If no exact match, return the first result
        return _format_lookup_result(lookup_results[0])
        
    except Exception as e:
        logger.warning(f"DBpedia Lookup failed for {entity_name}: {str(e)}")
        return None

async def _try_language_fallback(
    entity_name: str,
    dbpedia_uri: str,
    current_data: Optional[Dict[str, Any]],
    config: Dict[str, Any],
    max_attempts: int
) -> Optional[Dict[str, Any]]:
    """
    Try fetching data in a different language.
    """
    # Get the current language and fallback languages
    current_lang = config.get('LANGUAGE', 'en')
    fallback_langs = config.get('DBPEDIA_FALLBACK_LANGUAGES', ['en', 'de', 'fr', 'es'])
    use_de = config.get('DBPEDIA_USE_DE', True)
    # Remove current language from fallbacks
    fallback_langs = [lang for lang in fallback_langs if lang != current_lang]
    if not use_de and 'de' in fallback_langs:
        logger.info("DBPEDIA_USE_DE is False: Skipping German as a fallback language.")
        fallback_langs = [lang for lang in fallback_langs if lang != 'de']
    
    if not fallback_langs:
        return None
    
    logger.debug(f"Trying language fallbacks for {entity_name}")
    
    # Try each fallback language
    for lang in fallback_langs[:max_attempts]:
        try:
            # Modify the config to use the fallback language
            lang_config = config.copy()
            lang_config['LANGUAGE'] = lang
            
            # Try to fetch data with this language
            from . import async_fetchers # Relative import
            results = await async_fetchers.async_fetch_dbpedia_data(
                dbpedia_uris=[dbpedia_uri],
                endpoints=config.get('DBPEDIA_ENDPOINTS', []),
                batch_size=config.get('DBPEDIA_MAX_BATCH_SIZE', 50),
                user_agent=config.get('USER_AGENT', 'EntityExtractor/1.0'),
                timeout=config.get('DBPEDIA_TIMEOUT', 30),
                max_retries=config.get('DBPEDIA_MAX_RETRIES', 3),
                retry_delay=config.get('DBPEDIA_RETRY_DELAY', 1.0),
                ssl_verify=config.get('DBPEDIA_SSL_VERIFY', False),
                debug_mode=config.get('DEBUG_MODE', False),
                languages=[lang]
            )
            
            if results and dbpedia_uri in results:
                return results[dbpedia_uri]
                
        except Exception as e:
            logger.debug(f"Language fallback to {lang} failed: {str(e)}")
            continue
    
    return None

def _has_required_data(data: Dict[str, Any]) -> bool:
    """
    Check if the data contains the minimum required fields.
    
    Args:
        data: DBpedia data to check
        
    Returns:
        True if all required fields are present, False otherwise
    """
    if not data:
        return False
    
    # Required fields: URI, label, and abstract
    required = ['uri', 'label', 'abstract']
    return all(field in data and data[field] for field in required)

def _format_lookup_result(lookup_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a DBpedia Lookup API result into our standard format.
    
    Args:
        lookup_result: Raw result from the Lookup API
        
    Returns:
        Formatted data
    """
    if not lookup_result:
        return {}
    
    return {
        'uri': lookup_result.get('uri', ''),
        'label': lookup_result.get('label', ''),
        'abstract': lookup_result.get('abstract', ''),
        'types': lookup_result.get('types', []),
        'categories': lookup_result.get('categories', []),
        'source': 'lookup',
        'raw': lookup_result
    }
