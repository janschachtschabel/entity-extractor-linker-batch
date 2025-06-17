#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utility functions for the DBpedia service.
"""

import urllib.parse
from typing import Optional, Any
import logging

from entityextractor.core.context import EntityProcessingContext

def extract_wikipedia_url(context: EntityProcessingContext, logger: logging.Logger) -> Optional[str]:
    """
    Extract Wikipedia URL from various locations in the entity context.
    Prioritizes English Wikipedia URL if available.

    Args:
        context: Entity processing context
        logger: Logger instance

    Returns:
        Wikipedia URL if found, None otherwise
    """
    wikipedia_url = None
    preferred_english_url_found = False

    # Priority 1: Check for a specifically provided English Wikipedia URL
    # A. In context.processing_data['wikipedia']['english_url']
    if 'wikipedia' in context.processing_data:
        wikipedia_data = context.processing_data['wikipedia']
        if isinstance(wikipedia_data, dict) and 'english_url' in wikipedia_data and wikipedia_data['english_url']:
            wikipedia_url = wikipedia_data['english_url']
            logger.debug(f"Found preferred English Wikipedia URL in processing_data['wikipedia']['english_url']: {wikipedia_url}")
            preferred_english_url_found = True

    # B. In context.entity.english_wikipedia_url (assuming context.entity exists and has this attribute)
    if not preferred_english_url_found and hasattr(context, 'entity') and hasattr(context.entity, 'english_wikipedia_url') and context.entity.english_wikipedia_url:
        wikipedia_url = context.entity.english_wikipedia_url
        logger.debug(f"Found preferred English Wikipedia URL in context.entity.english_wikipedia_url: {wikipedia_url}")
        preferred_english_url_found = True
    
    if preferred_english_url_found and wikipedia_url:
        return wikipedia_url

    # Fallback to existing logic if no preferred English URL is found
    # 1. Check processing_data (where services store their data for general 'url')
    if 'wikipedia' in context.processing_data:
        wikipedia_data = context.processing_data['wikipedia']
        if isinstance(wikipedia_data, dict) and 'url' in wikipedia_data and wikipedia_data['url']:
            wikipedia_url = wikipedia_data['url']
            logger.debug(f"Found general Wikipedia URL in processing_data: {wikipedia_url}")

    # 2. Check output_data sources
    if not wikipedia_url and 'sources' in context.output_data:
        sources = context.output_data['sources']
        if 'wikipedia' in sources and isinstance(sources['wikipedia'], dict):
            if 'url' in sources['wikipedia'] and sources['wikipedia']['url']:
                wikipedia_url = sources['wikipedia']['url']
                logger.debug(f"Found general Wikipedia URL in output_data sources: {wikipedia_url}")
            elif 'wikipedia_url' in sources['wikipedia'] and sources['wikipedia']['wikipedia_url']:
                wikipedia_url = sources['wikipedia']['wikipedia_url']
                logger.debug(f"Found general wikipedia_url in output_data sources: {wikipedia_url}")

    # 3. Check if we have a direct Wikipedia URL in the context
    if not wikipedia_url and hasattr(context, 'wikipedia_url') and context.wikipedia_url:
        wikipedia_url = context.wikipedia_url
        logger.debug(f"Found general Wikipedia URL as direct attribute: {wikipedia_url}")

    # 4. Check if we have a wikipedia object with url in the context
    if not wikipedia_url and hasattr(context, 'wikipedia') and context.wikipedia and 'url' in context.wikipedia and context.wikipedia['url']:
        wikipedia_url = context.wikipedia['url']
        logger.debug(f"Found general Wikipedia URL in wikipedia object: {wikipedia_url}")

    if wikipedia_url:
        logger.info(f"Using Wikipedia URL: {wikipedia_url} (English preferred: {preferred_english_url_found})")
    else:
        logger.warning(f"Could not extract any Wikipedia URL for entity: {context.entity_name if hasattr(context, 'entity_name') else 'Unknown'}")
        
    return wikipedia_url

def wikipedia_to_dbpedia_uri(wikipedia_url: str, logger: logging.Logger, debug_mode: bool = False) -> Optional[str]:
    """
    Convert a Wikipedia URL to a DBpedia resource URI.
    Handles URL-encoded characters and language-specific domains.

    Args:
        wikipedia_url: URL of the Wikipedia article (can be URL-encoded)
        logger: Logger instance
        debug_mode: Boolean indicating if debug logging is enabled

    Returns:
        DBpedia resource URI or None if conversion fails
    """
    logger.info(f"Attempting to convert Wikipedia URL: {wikipedia_url}")
    if not wikipedia_url or not wikipedia_url.startswith('http'):
        logger.warning(f"Invalid Wikipedia URL provided: {wikipedia_url}")
        return None

    try:
        parsed_url = urllib.parse.urlparse(wikipedia_url)
        path_parts = parsed_url.path.split('/')

        if len(path_parts) < 3 or path_parts[1] != 'wiki':
            logger.warning(f"Wikipedia URL does not match expected format: {wikipedia_url}")
            return None

        article_title_encoded = path_parts[-1]
        article_title = urllib.parse.unquote_plus(article_title_encoded)
        resource_name = article_title.replace(' ', '_')

        # The DBpedia URI should always point to the global dbpedia.org/resource/
        # Language-specific DBpedia endpoints (like de.dbpedia.org) are for SPARQL queries, not resource URIs.
        dbpedia_uri = f"http://dbpedia.org/resource/{resource_name}"

        domain_parts = parsed_url.netloc.split('.')
        language_code = 'en' # Default language, can be extracted for logging/debugging if needed
        if len(domain_parts) > 2 and domain_parts[0] != 'www':
            language_code = domain_parts[0]

        logger.info(f"Successfully converted Wikipedia URL '{wikipedia_url}' (source lang: {language_code}) to DBpedia URI: {dbpedia_uri}")
        if debug_mode:
            logger.debug(
                f"Detailed conversion: Input='{wikipedia_url}', SourceLang='{language_code}', Output='{dbpedia_uri}'"
            )
        return dbpedia_uri

    except Exception as e:
        logger.error(f"Error converting Wikipedia URL '{wikipedia_url}': {str(e)}", exc_info=debug_mode)
        return None
