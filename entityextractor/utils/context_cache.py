#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
context_cache.py

Advanced caching system for the context-based architecture.
Enables efficient caching of EntityProcessingContext objects,
relationship networks, and individual service data.
"""

import os
import json
import time
import hashlib
import pickle
from typing import Dict, Any, List, Optional, Union, Set, Tuple
from loguru import logger

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.utils.cache_utils import get_cache_path, load_cache, save_cache

# Cache validity duration in seconds for different data types
CACHE_TTL = {
    "wikipedia": 604800,  # 7 days
    "wikidata": 1209600,  # 14 days
    "dbpedia": 1209600,   # 14 days
    "context": 86400,     # 1 day
    "relationships": 86400, # 1 day
    "default": 86400      # 1 day (fallback)
}


def get_context_cache_key(context: EntityProcessingContext) -> str:
    """
    Generates a unique cache key for an EntityProcessingContext.
    
    Args:
        context: The EntityProcessingContext
        
    Returns:
        Cache key as string
    """
    components = [
        context.entity_name,
        context.entity_id,
        context.entity_type or ""
    ]
    # Stable representation through sorting
    components = sorted(filter(None, components))
    
    # Generate unique key
    key_string = "_".join(components).lower()
    return key_string


def cache_context(context: EntityProcessingContext, cache_dir: Optional[str] = None) -> bool:
    """
    Stores an EntityProcessingContext in the cache.
    
    Args:
        context: The EntityProcessingContext to cache
        cache_dir: Cache directory (optional, read from configuration if not specified)
        
    Returns:
        True on success, False on error
    """
    if not cache_dir:
        config = get_config()
        cache_dir = config.get("CACHE_DIR", "entityextractor_cache")
    
    # Generate cache key
    cache_key = get_context_cache_key(context)
    
    # JSON serialization for the primary cache
    try:
        # Convert to serializable dictionary
        context_dict = context.to_dict()
        
        # Save as JSON
        json_cache_path = get_cache_path(cache_dir, "contexts", cache_key)
        save_cache(json_cache_path, context_dict)
        
        # Also save as Pickle for faster access
        pickle_cache_path = json_cache_path.replace(".json", ".pickle")
        try:
            with open(pickle_cache_path, "wb") as f:
                pickle.dump(context, f)
            logger.debug(f"Context saved as Pickle: {pickle_cache_path}")
        except Exception as e:
            logger.warning(f"Could not save context as Pickle: {e}")
        
        logger.info(f"Context for '{context.entity_name}' saved in cache")
        return True
    except Exception as e:
        logger.error(f"Error caching the context: {e}")
        return False


def load_context_from_cache(entity_name: str, entity_id: Optional[str] = None, 
                            entity_type: Optional[str] = None, 
                            cache_dir: Optional[str] = None,
                            max_age_seconds: Optional[int] = None) -> Optional[EntityProcessingContext]:
    """
    Loads an EntityProcessingContext from the cache.
    
    Args:
        entity_name: Name of the entity
        entity_id: Entity ID (optional)
        entity_type: Entity type (optional)
        cache_dir: Cache directory (optional)
        max_age_seconds: Maximum age of the cache entry in seconds (optional)
        
    Returns:
        The loaded EntityProcessingContext or None if not in cache or too old
    """
    if not cache_dir:
        config = get_config()
        cache_dir = config.get("CACHE_DIR", "entityextractor_cache")
    
    # Default value for max_age_seconds if not specified
    if max_age_seconds is None:
        max_age_seconds = CACHE_TTL["context"]
    
    # Create a temporary context for key generation
    temp_context = EntityProcessingContext(entity_name, entity_id or "", entity_type)
    cache_key = get_context_cache_key(temp_context)
    
    # First try to load the Pickle (faster)
    pickle_cache_path = get_cache_path(cache_dir, "contexts", cache_key).replace(".json", ".pickle")
    if os.path.exists(pickle_cache_path):
        # Check the age
        file_age = time.time() - os.path.getmtime(pickle_cache_path)
        if file_age > max_age_seconds:
            logger.debug(f"Cache entry for '{entity_name}' is too old ({file_age:.2f} s), ignoring")
            return None
        
        try:
            with open(pickle_cache_path, "rb") as f:
                context = pickle.load(f)
            logger.debug(f"Context for '{entity_name}' loaded from Pickle cache")
            return context
        except Exception as e:
            logger.warning(f"Error loading from Pickle cache: {e}, trying JSON cache")
    
    # Fallback to JSON cache
    json_cache_path = get_cache_path(cache_dir, "contexts", cache_key)
    if os.path.exists(json_cache_path):
        # Check the age
        file_age = time.time() - os.path.getmtime(json_cache_path)
        if file_age > max_age_seconds:
            logger.debug(f"Cache entry for '{entity_name}' is too old ({file_age:.2f} s), ignoring")
            return None
        
        try:
            context_dict = load_cache(json_cache_path)
            
            # Create a new context from the dictionary
            context = EntityProcessingContext.from_dict(context_dict)
            logger.debug(f"Context for '{entity_name}' loaded from JSON cache")
            
            # Also save as Pickle for future requests
            try:
                with open(pickle_cache_path, "wb") as f:
                    pickle.dump(context, f)
            except Exception as e:
                logger.debug(f"Could not update Pickle cache: {e}")
                
            return context
        except Exception as e:
            logger.warning(f"Error loading from JSON cache: {e}")
    
    logger.debug(f"No cache entry found for '{entity_name}'")
    return None


def cache_service_data(entity_name: str, service_name: str, data: Dict[str, Any], 
                       cache_dir: Optional[str] = None) -> bool:
    """
    Stores service data for an entity in the cache.
    
    Args:
        entity_name: Name of the entity
        service_name: Name of the service (wikipedia, wikidata, dbpedia)
        data: The service data to cache
        cache_dir: Cache directory (optional)
        
    Returns:
        True on success, False on error
    """
    if not cache_dir:
        config = get_config()
        cache_dir = config.get("CACHE_DIR", "entityextractor_cache")
    
    # Normalize entity_name for the cache key
    cache_key = entity_name.lower().replace(" ", "_")
    
    # Service-specific caching directory
    service_cache_dir = os.path.join(cache_dir, service_name)
    os.makedirs(service_cache_dir, exist_ok=True)
    
    # Generate cache path
    cache_path = os.path.join(service_cache_dir, f"{cache_key}.json")
    
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug(f"{service_name.capitalize()} data for '{entity_name}' saved in cache")
        return True
    except Exception as e:
        logger.warning(f"Error caching {service_name} data: {e}")
        return False


def load_service_data_from_cache(entity_name: str, service_name: str, 
                                 cache_dir: Optional[str] = None,
                                 max_age_seconds: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Loads service data for an entity from the cache.
    
    Args:
        entity_name: Name of the entity
        service_name: Name of the service (wikipedia, wikidata, dbpedia)
        cache_dir: Cache directory (optional)
        max_age_seconds: Maximum age of the cache entry in seconds (optional)
        
    Returns:
        The loaded service data or None if not in cache or too old
    """
    if not cache_dir:
        config = get_config()
        cache_dir = config.get("CACHE_DIR", "entityextractor_cache")
    
    # Default value for max_age_seconds if not specified
    if max_age_seconds is None:
        max_age_seconds = CACHE_TTL.get(service_name, CACHE_TTL["default"])
    
    # Normalize entity_name for the cache key
    cache_key = entity_name.lower().replace(" ", "_")
    
    # Service-specific caching directory
    service_cache_dir = os.path.join(cache_dir, service_name)
    
    # Cache path
    cache_path = os.path.join(service_cache_dir, f"{cache_key}.json")
    
    if os.path.exists(cache_path):
        # Check the age
        file_age = time.time() - os.path.getmtime(cache_path)
        if file_age > max_age_seconds:
            logger.debug(f"Cache for '{entity_name}' ({service_name}) is too old ({file_age:.2f} s), ignoring")
            return None
        
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.debug(f"{service_name.capitalize()} data for '{entity_name}' loaded from cache")
            return data
        except Exception as e:
            logger.warning(f"Error loading {service_name} data from cache: {e}")
    
    logger.debug(f"No cache entry found for '{entity_name}' ({service_name})")
    return None


def cache_batch_request(service_name: str, query_key: str, results: List[Dict[str, Any]], 
                        cache_dir: Optional[str] = None) -> bool:
    """
    Stores the results of a batch request in the cache.
    
    Args:
        service_name: Name of the service (wikipedia, wikidata, dbpedia)
        query_key: Unique key for the request
        results: The results to cache
        cache_dir: Cache directory (optional)
        
    Returns:
        True on success, False on error
    """
    if not cache_dir:
        config = get_config()
        cache_dir = config.get("CACHE_DIR", "entityextractor_cache")
    
    # Normalize query_key for the cache key
    hash_key = hashlib.md5(query_key.encode("utf-8")).hexdigest()
    
    # Service-specific batch caching directory
    batch_cache_dir = os.path.join(cache_dir, f"{service_name}_batch")
    os.makedirs(batch_cache_dir, exist_ok=True)
    
    # Generate cache path
    cache_path = os.path.join(batch_cache_dir, f"{hash_key}.json")
    
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False)
        logger.debug(f"Batch results for {service_name} saved in cache (key: {hash_key})")
        return True
    except Exception as e:
        logger.warning(f"Error caching batch results: {e}")
        return False


def load_batch_request_from_cache(service_name: str, query_key: str, 
                                  cache_dir: Optional[str] = None,
                                  max_age_seconds: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
    """
    Loads the results of a batch request from the cache.
    
    Args:
        service_name: Name of the service (wikipedia, wikidata, dbpedia)
        query_key: Unique key for the request
        cache_dir: Cache directory (optional)
        max_age_seconds: Maximum age of the cache entry in seconds (optional)
        
    Returns:
        The loaded batch results or None if not in cache or too old
    """
    if not cache_dir:
        config = get_config()
        cache_dir = config.get("CACHE_DIR", "entityextractor_cache")
    
    # Default value for max_age_seconds if not specified
    if max_age_seconds is None:
        max_age_seconds = CACHE_TTL.get(service_name, CACHE_TTL["default"])
    
    # Normalize query_key for the cache key
    hash_key = hashlib.md5(query_key.encode("utf-8")).hexdigest()
    
    # Service-specific batch caching directory
    batch_cache_dir = os.path.join(cache_dir, f"{service_name}_batch")
    
    # Cache path
    cache_path = os.path.join(batch_cache_dir, f"{hash_key}.json")
    
    if os.path.exists(cache_path):
        # Check the age
        file_age = time.time() - os.path.getmtime(cache_path)
        if file_age > max_age_seconds:
            logger.debug(f"Batch cache for {service_name} is too old ({file_age:.2f} s), ignoring")
            return None
        
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.debug(f"Batch results for {service_name} loaded from cache (key: {hash_key})")
            return data
        except Exception as e:
            logger.warning(f"Error loading batch results from cache: {e}")
    
    logger.debug(f"No batch cache entry found for {service_name} (key: {hash_key})")
    return None
