#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Asynchronous data fetchers for the DBpedia service.

This module provides async functions to fetch data from DBpedia's SPARQL endpoints
and Lookup API, with support for batch processing, rate limiting, and error handling.
"""

import json
import logging
import asyncio
import aiohttp
import urllib.parse
from typing import Dict, Any, List, Optional, Tuple

# Custom exceptions for SPARQL query execution
class SparqlQueryError(Exception):
    """Base class for SPARQL query errors."""
    def __init__(self, message, endpoint=None, query_snippet=None, status_code=None, response_snippet=None):
        super().__init__(message)
        self.endpoint = endpoint
        self.query_snippet = query_snippet
        self.status_code = status_code
        self.response_snippet = response_snippet

    def __str__(self):
        details = super().__str__()
        if self.endpoint: details += f" | Endpoint: {self.endpoint}"
        if self.status_code: details += f" | Status: {self.status_code}"
        if self.query_snippet: details += f" | Query: {self.query_snippet}..."
        if self.response_snippet: details += f" | Response: {self.response_snippet}..."
        return details

class SparqlQueryHttpError(SparqlQueryError):
    """Represents an HTTP error during a SPARQL query that should not be retried by the current method."""
    def __init__(self, status_code, response_text, endpoint, query):
        self.status_code = status_code
        self.response_text = response_text
        # Store only a snippet of query and response to avoid large exception objects
        query_snip = query[:200] if query else "N/A"
        response_snip = response_text[:200] if response_text else "N/A"
        super().__init__(
            f"HTTP client error {status_code} from {endpoint}. This method will not retry.",
            endpoint=endpoint,
            query_snippet=query_snip,
            status_code=status_code,
            response_snippet=response_snip
        )

from entityextractor.utils.api_request_utils import create_standard_headers
from entityextractor.utils.logging_utils import get_service_logger
from .formatters import build_sparql_query, process_sparql_results

# Configure logger
logger = get_service_logger(__name__, 'dbpedia')

# Default configuration values
DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0
MAX_BATCH_SIZE = 50
DEFAULT_RATE_LIMIT = 5  # requests per second

# Default DBpedia endpoints (HTTP first for better compatibility)
DEFAULT_ENDPOINTS = [
    'http://dbpedia.org/sparql',  # Primary endpoint (HTTP)
    'http://live.dbpedia.org/sparql',  # Live endpoint (HTTP)
    'https://dbpedia.org/sparql'  # Fallback to HTTPS if needed
]


class RateLimiter:
    """Simple rate limiter for controlling request rates."""
    
    def __init__(self, rate_limit: float = DEFAULT_RATE_LIMIT):
        self.rate_limit = rate_limit
        self.semaphore = asyncio.Semaphore(rate_limit)
        self.last_call = 0.0
        
    async def __aenter__(self):
        await self.semaphore.acquire()
        now = asyncio.get_event_loop().time()
        time_since_last = now - self.last_call
        
        if time_since_last < 1.0 / self.rate_limit:
            await asyncio.sleep((1.0 / self.rate_limit) - time_since_last)
            
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.last_call = asyncio.get_event_loop().time()
        self.semaphore.release()

# Global rate limiter instance
rate_limiter = RateLimiter()

async def async_sparql_fetch_dbpedia_data(
    dbpedia_uris: List[str],
    endpoints: Optional[List[str]] = None,
    batch_size: int = MAX_BATCH_SIZE,
    user_agent: str = 'EntityExtractor/1.0',
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    ssl_verify: bool = False,
    config: Optional[Dict[str, Any]] = None,
    languages: List[str] = ['en', 'de']
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch data for multiple DBpedia URIs using SPARQLWrapper for direct SPARQL execution.
    
    Args:
        dbpedia_uris: List of DBpedia URIs to fetch data for
        endpoints: List of SPARQL endpoints to try (in order)
        batch_size: Maximum number of URIs to include in a single batch query
        user_agent: User-Agent header value
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts per batch
        retry_delay: Base delay between retries in seconds
        ssl_verify: Whether to verify SSL certificates
        config: Additional configuration parameters
        languages: List of languages to try in order of preference
        
    Returns:
        Dictionary mapping URIs to their data
    """
    from SPARQLWrapper import SPARQLWrapper, JSON
    import time
    import ssl
    
    if not dbpedia_uris:
        return {}
        
    # Use provided endpoints or defaults
    if not endpoints:
        endpoints = DEFAULT_ENDPOINTS
        
    # Initialize results dictionary
    results: Dict[str, Dict[str, Any]] = {}
    remaining_uris = set(dbpedia_uris)
    
    # Process URIs in batches
    for i in range(0, len(dbpedia_uris), batch_size):
        batch_uris = dbpedia_uris[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        # Format URIs for SPARQL query
        formatted_uris = ' '.join([f'<{uri}>' for uri in batch_uris])
        
        logger.info(f"Processing batch {batch_num} with {len(batch_uris)} URIs")
        logger.info(f"URIs in batch {batch_num}: {batch_uris}")
        
        # Try each endpoint until successful
        batch_success = False
        for endpoint in endpoints:
            if batch_success:
                break
                
            logger.info(f"Trying endpoint {endpoint} for batch {batch_num}")
            
            # Try each language in order
            for lang in languages:
                if batch_success:
                    break
                    
                try:
                    # Create query with current language
                    query = build_sparql_query(uris=batch_uris, language=lang)
                    
                    # Create SPARQLWrapper instance without auth
                    sparql = SPARQLWrapper(endpoint)
                    sparql.setQuery(query)
                    sparql.setReturnFormat(JSON)
                    sparql.setTimeout(int(timeout))
                    
                    # Set user agent if provided
                    if user_agent:
                        sparql.addCustomHttpHeader('User-Agent', user_agent)
                    
                    # Create custom SSL context if needed
                    if not ssl_verify:
                        ssl_context = ssl.create_default_context()
                        ssl_context.check_hostname = False
                        ssl_context.verify_mode = ssl.CERT_NONE
                        # Note: SPARQLWrapper doesn't directly support passing ssl_context
                        # So we'll use our direct HTTP method if SSL issues persist
                    
                    # Execute query with retries
                    for attempt in range(max_retries + 1):
                        try:
                            # Apply rate limiting between attempts
                            if attempt > 0:
                                delay = retry_delay * (2 ** attempt)
                                logger.info(f"Retry attempt {attempt}/{max_retries} for batch {batch_num}, waiting {delay}s")
                                time.sleep(delay)
                            
                            logger.info(f"Executing SPARQL query for batch {batch_num} with language {lang}")
                            query_results = sparql.query().convert()
                            
                            # Process results using the formatter function
                            processed_batch_lang_results = process_sparql_results(sparql_results=query_results, expected_uris=batch_uris)

                            # Merge with main results, prioritizing more complete data or preferred language
                            for uri, new_data in processed_batch_lang_results.items():
                                if uri in remaining_uris: # Only process if still needed for this URI
                                    current_data = results.get(uri)
                                    # Prioritize if: no current data, current data lacks label, new data is 'en' and has label, or new data has label and current doesn't
                                    should_replace = False
                                    if not current_data:
                                        should_replace = True
                                    elif not current_data.get('label') and new_data.get('label'):
                                        should_replace = True
                                    elif new_data.get('label') and lang == 'en': # Prioritize English if it has a label
                                        should_replace = True
                                    # If both have labels, but current is not English and new one is, prefer new if it's English
                                    elif current_data.get('label') and new_data.get('label') and lang == 'en' and results.get(uri, {}).get('_lang') != 'en':
                                        should_replace = True
                                    # If new data has an abstract and current doesn't (but has a label)
                                    elif new_data.get('abstract') and current_data and current_data.get('label') and not current_data.get('abstract'):
                                        results[uri]['abstract'] = new_data.get('abstract') # Just update abstract
                                        # Add other fields from new_data if they are missing in current_data
                                        for key, value in new_data.items():
                                            if key not in ['uri', 'label', 'abstract'] and value and not results[uri].get(key):
                                                results[uri][key] = value
                                        results[uri]['_lang'] = lang # Mark language of abstract

                                    if should_replace:
                                        results[uri] = new_data
                                        results[uri]['_lang'] = lang # Store the language this data came from

                            logger.info(f"Successfully processed batch {batch_num} with language {lang} from {endpoint}")
                            # Check if all URIs in the batch have at least label and abstract from any language attempt so far
                            all_uris_in_batch_processed = True
                            for u in batch_uris:
                                if u in remaining_uris and not (results.get(u, {}).get('label') and results.get(u, {}).get('abstract')):
                                    all_uris_in_batch_processed = False
                                    break
                            if all_uris_in_batch_processed:
                                batch_success = True # Mark success for this batch if all URIs got minimal data
                            
                            # If this language provided data for all remaining items in batch, or it's English, it's a success for this lang/batch
                            # This logic is tricky. The original was simpler: if any data, break. 
                            # Let's stick to: if we got any results from this query, consider this language attempt successful for the batch.
                            if processed_batch_lang_results and any(processed_batch_lang_results.values()):
                                batch_success = True

                            break # Break from retry loop for this language
                        except SPARQLWrapper.SPARQLExceptions.EndPointNotFound as e_nf:
                            logger.error(f"SPARQL Endpoint not found {endpoint}: {str(e_nf)}. Trying next endpoint if available.")
                            break # Break from retry loop, to try next endpoint
                        except (SPARQLWrapper.SPARQLExceptions.QueryBadFormed, TypeError) as e_query:
                            logger.error(f"SPARQL query bad formed or TypeError for {endpoint} with lang {lang}: {str(e_query)}. Query: {query[:200]}...", exc_info=logger.isEnabledFor(logging.DEBUG))
                            break # Break from retry loop for this language, as query is likely bad for this setup
                        except Exception as e_exec:
                            logger.error(f"Error executing SPARQL query for batch {batch_num}, lang {lang}, attempt {attempt + 1}/{max_retries + 1} on {endpoint}: {str(e_exec)}", exc_info=logger.isEnabledFor(logging.DEBUG))
                            if attempt == max_retries:
                                logger.warning(f"All {max_retries + 1} retries failed for batch {batch_num}, lang {lang} on {endpoint}.")
                    # This is the end of the retry loop for a specific language and endpoint
                except Exception as e_lang_processing:
                    logger.error(f"Error processing language {lang} for batch {batch_num} on endpoint {endpoint}: {str(e_lang_processing)}", exc_info=logger.isEnabledFor(logging.DEBUG))
                    # Continue to the next language or endpoint
                    continue
        
        if not batch_success:
            logger.warning(f"Failed to get data for batch {batch_num} from any endpoint")

    if remaining_uris:
        logger.warning(f"No data found for {len(remaining_uris)} URIs")
    
    return results

async def async_fetch_dbpedia_data(
    dbpedia_uris: List[str],
    endpoints: Optional[List[str]] = None,
    batch_size: int = 10,
    languages: Optional[List[str]] = None,
    query_type: str = "full",
    formatter: Optional[Callable] = None,
    session: Optional[aiohttp.ClientSession] = None,
    retry_count: int = 3,
    rate_limit_per_sec: int = 10,
    timeout: int = 60,  
    user_agent: Optional[str] = None,
    ssl_verify: bool = True,
    debug_mode: bool = False
) -> Dict[str, Dict[str, Any]]:
    """
    Asynchronously fetch DBpedia data for multiple URIs in batches with retry and error handling.
    
    Args:
        dbpedia_uris: List of DBpedia URIs to fetch data for.
        endpoints: List of SPARQL endpoints to query. If None, uses default endpoints.
        batch_size: Number of URIs to process in each batch.
        languages: List of language codes to filter results. If None, defaults to ['en'].
        query_type: Type of query to build ('full' or 'minimal').
        formatter: Formatter function to build SPARQL queries and process results.
        session: Optional aiohttp ClientSession for making requests.
        retry_count: Number of retries for failed requests.
        rate_limit_per_sec: Rate limit for requests per second.
        timeout: Timeout for each request in seconds (increased to 60).
        user_agent: Custom User-Agent string for requests.
        ssl_verify: Whether to verify SSL certificates.
        debug_mode: Enable detailed debug logging.
    
    Returns:
        Dictionary mapping URIs to their fetched data.
    """
    if not dbpedia_uris:
        logger.warning("Keine DBpedia-URIs zum Abrufen übergeben.")
        return {}
    
    # Use provided endpoints or defaults
    endpoints = endpoints or DEFAULT_ENDPOINTS
    
    # Use passed parameters directly
    logger.debug(f"Using batch size: {batch_size}")
    logger.debug(f"Using timeout: {timeout} seconds")
    logger.debug(f"Using max retries: {retry_count}")
    logger.debug(f"SSL verification: {'enabled' if ssl_verify else 'disabled'}")
    logger.debug(f"Debug mode: {'enabled' if debug_mode else 'disabled'}")
    logger.info(f"Endpoints to be used for SPARQL queries: {endpoints}")
    
    # Prepare results dictionary
    results = {}
    logger.info(f"Preparing to fetch data for {len(dbpedia_uris)} DBpedia URIs in batches of {batch_size}")
    
    # Process URIs in batches
    total_batches = (len(dbpedia_uris) + batch_size - 1) // batch_size
    for i in range(0, len(dbpedia_uris), batch_size):
        batch = dbpedia_uris[i:i + batch_size]
        batch_num = i // batch_size + 1
        logger.debug(f"Verarbeite Batch {batch_num} mit URIs: {batch}")
        
        # Versuche, Daten für den aktuellen Batch abzurufen
        query = build_sparql_query(uris=batch)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Vollständige SPARQL-Abfrage für Batch {batch_num}:\n{query}")
        
        batch_results_json = await _execute_sparql_query(
            query=query, endpoints=endpoints, user_agent=user_agent, timeout=timeout, max_retries=retry_count, retry_delay=1, ssl_verify=ssl_verify, debug_mode=debug_mode
        )
        
        if not batch_results_json:
            logger.warning(f"Keine Ergebnisse für Batch {batch_num} erhalten. Versuche einzelne URIs.")
            # Fallback: Versuche jede URI einzeln abzufragen
            for uri in batch:
                logger.debug(f"Einzelabfrage für URI: {uri}")
                single_query = build_sparql_query(uris=[uri])
                single_result_json = await _execute_sparql_query(
                    query=single_query, endpoints=endpoints, user_agent=user_agent, timeout=timeout, max_retries=retry_count, retry_delay=1, ssl_verify=ssl_verify, debug_mode=debug_mode
                )
                if single_result_json:
                    single_processed_data = process_sparql_results(
                        results_json=single_result_json, uris=[uri]
                    )
                    results.update(single_processed_data)
                    logger.debug(f"Erfolgreich Daten für einzelne URI {uri} erhalten.")
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Daten für {uri}: {single_processed_data.get(uri, {})}")
                else:
                    logger.error(f"Fehler bei der Abfrage der einzelnen URI {uri}. Keine Daten erhalten.")
            continue
        
        # Verarbeite die Ergebnisse des Batches
        processed_batch_data = process_sparql_results(
            results_json=batch_results_json, uris=batch
        )
        
        # Logge, welche URIs in diesem Batch erfolgreich verarbeitet wurden
        successful_uris = [uri for uri, data in processed_batch_data.items() if data]
        if successful_uris:
            logger.debug(f"Erfolgreich verarbeitete URIs in Batch {batch_num}: {successful_uris}")
            if logger.isEnabledFor(logging.DEBUG):
                for uri in successful_uris:
                    logger.debug(f"Daten für {uri}: {processed_batch_data.get(uri, {})}")
        else:
            logger.warning(f"Keine erfolgreich verarbeiteten URIs in Batch {batch_num}. Versuche einzelne URIs.")
            # Fallback: Versuche jede URI einzeln abzufragen, wenn keine Daten im Batch vorhanden sind
            for uri in batch:
                logger.debug(f"Einzelabfrage für URI: {uri}")
                single_query = build_sparql_query(uris=[uri])
                single_result_json = await _execute_sparql_query(
                    query=single_query, endpoints=endpoints, user_agent=user_agent, timeout=timeout, max_retries=retry_count, retry_delay=1, ssl_verify=ssl_verify, debug_mode=debug_mode
                )
                if single_result_json:
                    single_processed_data = process_sparql_results(
                        results_json=single_result_json, uris=[uri]
                    )
                    results.update(single_processed_data)
                    logger.debug(f"Erfolgreich Daten für einzelne URI {uri} erhalten.")
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Daten für {uri}: {single_processed_data.get(uri, {})}")
                else:
                    logger.error(f"Fehler bei der Abfrage der einzelnen URI {uri}. Keine Daten erhalten.")
        
        results.update(processed_batch_data)
    
    # Ensure all originally requested URIs are in the results, even if empty
    for uri in dbpedia_uris:
        if uri not in results:
            results[uri] = {}
    
    logger.info(f"Completed fetching data for {len(dbpedia_uris)} DBpedia URIs")
    return results

async def async_fetch_dbpedia_sparql(query: str, endpoint: str, session: aiohttp.ClientSession, timeout: int = 30) -> Dict[str, Any]:
    """
    Führt eine SPARQL-Abfrage asynchron über einen gegebenen Endpunkt aus und gibt die Ergebnisse zurück.
    """
    try:
        logger.debug(f"Führe asynchrone SPARQL-Abfrage an {endpoint} aus")
        headers = {
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "DBpedia-Extractor/1.0"
        }
        data = {"query": query}
        logger.debug(f"SPARQL-Abfrage Header: {headers}, Daten: {data}")

        async with session.post(endpoint, headers=headers, data=data, timeout=timeout) as response:
            if response.status != 200:
                logger.error(f"SPARQL-Anfrage fehlgeschlagen mit Status {response.status}: {await response.text()}")
                return {}
            json_response = await response.json()
            logger.debug(f"SPARQL-Antwort erhalten: {json_response}")
            if json_response is None:
                logger.error("SPARQL-Antwort ist None")
                return {}
            return json_response
    except aiohttp.ClientError as ce:
        logger.error(f"Client-Fehler bei SPARQL-Abfrage an {endpoint}: {str(ce)}")
        return {}
    except Exception as e:
        logger.error(f"Fehler bei SPARQL-Abfrage an {endpoint}: {str(e)}")
        return {}

async def async_fetch_dbpedia_lookup(query: str, session: aiohttp.ClientSession, max_results: int = 5, lang: str = "en") -> List[Dict[str, Any]]:
    """
    Führt eine asynchrone Anfrage an die DBpedia Lookup API aus und gibt die Ergebnisse zurück.
    """
    try:
        lookup_url = "https://lookup.dbpedia.org/api/search"
        params = {
            "query": query,
            "format": "json",
            "maxResults": max_results,
            "lang": lang
        }
        logger.debug(f"DBpedia Lookup API Anfrage: {lookup_url} mit Parametern {params}")
        headers = {"User-Agent": "DBpedia-Extractor/1.0"}

        async with session.get(lookup_url, params=params, headers=headers) as response:
            if response.status != 200:
                logger.error(f"DBpedia Lookup API Fehler: Status {response.status}")
                return []
            result = await response.json()
            if result is None:
                logger.error("DBpedia Lookup API Antwort ist None")
                return []
            logger.debug(f"DBpedia Lookup API Ergebnisse: {result}")
            docs = result.get("docs", [])
            return docs
    except aiohttp.ClientError as ce:
        logger.error(f"Client-Fehler bei DBpedia Lookup API: {str(ce)}")
        return []
    except Exception as e:
        logger.error(f"Fehler bei DBpedia Lookup API: {str(e)}")
        return []

async def _execute_sparql_query(
    endpoint: str,
    query: str,
    user_agent: str,
    timeout: float,
    max_retries: int,
    retry_delay: float,
    ssl_verify: bool = False,
    debug_mode: bool = False # Added debug_mode parameter
) -> Dict[str, Any]: # Changed return type to Dict, as SPARQL JSON result is a dict
    """
    Execute a SPARQL query against the specified endpoint with retries.
    
    Args:
        endpoint: SPARQL endpoint URL
        query: SPARQL query string
        user_agent: User-Agent header value
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        retry_delay: Base delay between retries in seconds (will use exponential backoff)
        ssl_verify: Whether to verify SSL certificates
        
    Returns:
        List of query results
        
    Raises:
        Exception: If all retry attempts fail
    """
    headers = create_standard_headers()
    headers['User-Agent'] = user_agent
    params = {'query': query, 'format': 'json'}
    headers['Accept'] = 'application/sparql-results+json, application/json'

    # The max_retries and retry_delay parameters are kept in the signature for interface consistency
    # but are not used for a general retry loop within this function itself.
    # This function will make one primary attempt, and one conditional retry for HTTP 429.
    # Higher-level retries (e.g., trying a different endpoint) are handled by the caller.

    import ssl
    ssl_context = None
    if not ssl_verify:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    endpoint_for_request = endpoint

    if debug_mode:
        logger.debug(f"Sending SPARQL GET query to {endpoint_for_request}:\nParams: {params}\nHeaders: {headers}")
    else:
        logger.info(f"Attempting SPARQL GET query to {endpoint_for_request}.")

    try:
        async with aiohttp.ClientSession() as session:
            # Primary attempt
            async with session.get(
                endpoint_for_request,
                headers=headers,
                params=params,
                timeout=timeout,
                ssl=ssl_context
            ) as response:
                logger.info(f"Received response status: {response.status} from {endpoint_for_request}")

                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'application/sparql-results+json' in content_type or 'application/json' in content_type:
                        try:
                            json_data = await response.json()
                            logger.debug(f"Successfully received JSON data from {endpoint_for_request}")
                            return json_data
                        except aiohttp.ContentTypeError as e_ct:
                            response_text_ct = await response.text()
                            err_msg = f"ContentTypeError parsing JSON from {endpoint_for_request}: {e_ct}. Response: {response_text_ct[:500]}"
                            logger.error(err_msg)
                            raise SparqlQueryError(err_msg, endpoint=endpoint_for_request, query_snippet=query[:200], status_code=response.status, response_snippet=response_text_ct[:200]) from e_ct
                        except json.JSONDecodeError as e_json:
                            response_text_json = await response.text()
                            err_msg = f"JSONDecodeError parsing JSON from {endpoint_for_request}: {e_json}. Response: {response_text_json[:500]}"
                            logger.error(err_msg)
                            raise SparqlQueryError(err_msg, endpoint=endpoint_for_request, query_snippet=query[:200], status_code=response.status, response_snippet=response_text_json[:200]) from e_json
                    else:
                        response_text_snippet = await response.text()
                        err_msg = f"Unexpected Content-Type '{content_type}' from {endpoint_for_request}. Expected JSON. Response: {response_text_snippet[:500]}"
                        logger.error(err_msg)
                        raise SparqlQueryError(err_msg, endpoint=endpoint_for_request, query_snippet=query[:200], status_code=response.status, response_snippet=response_text_snippet[:200])

                elif response.status == 429: # Rate limit
                    error_text_429 = await response.text()
                    retry_after_header = response.headers.get('Retry-After')
                    if retry_after_header:
                        try:
                            actual_delay = float(retry_after_header)
                            actual_delay = min(max(0.1, actual_delay), 60.0) # Cap delay, ensure positive
                            logger.warning(f"Rate limit (429) from {endpoint_for_request}. Retrying ONCE after {actual_delay:.2f}s. Details: {error_text_429[:200]}")
                            await asyncio.sleep(actual_delay)
                            
                            # Single retry attempt for 429
                            async with session.get(
                                endpoint_for_request,
                                headers=headers,
                                params=params,
                                timeout=timeout,
                                ssl=ssl_context
                            ) as retry_response:
                                logger.info(f"Received response status from 429-retry: {retry_response.status} for {endpoint_for_request}")
                                if retry_response.status == 200:
                                    content_type_retry = retry_response.headers.get('Content-Type', '').lower()
                                    if 'application/sparql-results+json' in content_type_retry or 'application/json' in content_type_retry:
                                        try:
                                            json_data_retry = await retry_response.json()
                                            logger.debug(f"Successfully received JSON data from {endpoint_for_request} after 429 retry")
                                            return json_data_retry
                                        except aiohttp.ContentTypeError as e_retry_ct:
                                            retry_response_text_ct = await retry_response.text()
                                            err_msg_retry = f"ContentTypeError on 429-retry from {endpoint_for_request}: {e_retry_ct}. Response: {retry_response_text_ct[:500]}"
                                            logger.error(err_msg_retry)
                                            raise SparqlQueryError(err_msg_retry, endpoint=endpoint_for_request, query_snippet=query[:200], status_code=retry_response.status, response_snippet=retry_response_text_ct[:200]) from e_retry_ct
                                        except json.JSONDecodeError as e_retry_json:
                                            retry_response_text_json = await retry_response.text()
                                            err_msg_retry = f"JSONDecodeError on 429-retry from {endpoint_for_request}: {e_retry_json}. Response: {retry_response_text_json[:500]}"
                                            logger.error(err_msg_retry)
                                            raise SparqlQueryError(err_msg_retry, endpoint=endpoint_for_request, query_snippet=query[:200], status_code=retry_response.status, response_snippet=retry_response_text_json[:200]) from e_retry_json
                                    else:
                                        retry_response_text = await retry_response.text()
                                        err_msg_retry = f"Unexpected Content-Type '{content_type_retry}' on 429-retry from {endpoint_for_request}. Expected JSON. Response: {retry_response_text[:500]}"
                                        logger.error(err_msg_retry)
                                        raise SparqlQueryError(err_msg_retry, endpoint=endpoint_for_request, query_snippet=query[:200], status_code=retry_response.status, response_snippet=retry_response_text[:200])
                                else:
                                    # Retry for 429 also failed
                                    error_text_retry_fail = await retry_response.text()
                                    logger.error(f"Retry after 429 also failed for {endpoint_for_request} with status {retry_response.status}. Details: {error_text_retry_fail[:500]}")
                                    raise SparqlQueryHttpError(retry_response.status, error_text_retry_fail, endpoint_for_request, query)
                        except ValueError:
                            logger.warning(f"Invalid Retry-After header for 429 from {endpoint_for_request}: '{retry_after_header}'. Not retrying. Details: {error_text_429[:200]}")
                            raise SparqlQueryHttpError(response.status, error_text_429, endpoint_for_request, query)
                    else:
                        logger.warning(f"Rate limit (429) from {endpoint_for_request} with no Retry-After header. Not retrying. Details: {error_text_429[:200]}")
                        raise SparqlQueryHttpError(response.status, error_text_429, endpoint_for_request, query)
                
                elif 400 <= response.status < 500: # Other client errors (e.g., 400, 401, 403, 404, 406)
                    error_text_client = await response.text()
                    logger.warning(f"Client error {response.status} from {endpoint_for_request}. Details: {error_text_client[:500]}")
                    raise SparqlQueryHttpError(response.status, error_text_client, endpoint_for_request, query)
                
                else: # Includes 5xx server errors or other unexpected non-200, non-4xx statuses
                    error_text_server = await response.text()
                    logger.error(f"SPARQL query to {endpoint_for_request} failed with status {response.status}. Details: {error_text_server[:500]}")
                    # Use SparqlQueryError for server-side issues or unexpected responses not covered by SparqlQueryHttpError
                    raise SparqlQueryError(f"HTTP error {response.status}", endpoint=endpoint_for_request, query_snippet=query[:200], status_code=response.status, response_snippet=error_text_server[:200])

    except aiohttp.ClientError as ce:
        logger.error(f"Client-Fehler bei SPARQL-Abfrage an {endpoint_for_request}: {str(ce)}")
        raise SparqlQueryError(f"aiohttp.ClientError: {str(ce)}", endpoint=endpoint_for_request, query_snippet=query[:200]) from ce
    except asyncio.TimeoutError as e_timeout: # More specific timeout catch, though ClientError often wraps this
        logger.error(f"asyncio.TimeoutError during SPARQL query to {endpoint_for_request}: {e_timeout}")
        raise SparqlQueryError(f"asyncio.TimeoutError: {str(e_timeout)}", endpoint=endpoint_for_request, query_snippet=query[:200]) from e_timeout
    except Exception as e_unexpected: # Catch any other unexpected errors during the process
        logger.error(f"Unexpected error in _execute_sparql_query for {endpoint_for_request}: {str(e_unexpected)}", exc_info=debug_mode)
        raise SparqlQueryError(f"Unexpected error: {str(e_unexpected)}", endpoint=endpoint_for_request, query_snippet=query[:200]) from e_unexpected
    
    # This should never be reached due to the raise in the except block
    raise Exception(
        f"Unexpected error in SPARQL query execution. Last error: {last_error}"
    )
