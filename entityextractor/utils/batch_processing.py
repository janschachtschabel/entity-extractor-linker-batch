#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
batch_processing.py

Optimized batch processing for the context-based architecture.
Provides functions for efficient processing of entity groups,
especially for API requests and relationship extraction.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional, Callable, TypeVar, Awaitable, Tuple, Set
from functools import partial
from loguru import logger

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.utils.context_cache import cache_context, load_context_from_cache

# Type definitions for better type hints
T = TypeVar('T')
EntityContextList = List[EntityProcessingContext]
ProcessorFunc = Callable[[EntityProcessingContext], Awaitable[None]]

# Optimal batch sizes for different services
OPTIMAL_BATCH_SIZES = {
    "wikipedia": 50,     # Wikipedia allows up to 50 titles per request
    "wikidata": 50,      # Wikidata API allows up to 50 elements per request
    "dbpedia": 25,       # SPARQL queries get slower with more elements
    "default": 20        # Default value for other services
}

# Rate limits for different services (requests per second)
RATE_LIMITS = {
    "wikipedia": 10,     # Wikipedia API recommends max. 10 requests per second
    "wikidata": 5,       # Wikidata API recommends max. 5 requests per second
    "dbpedia": 2,        # DBpedia SPARQL endpoints are often slower
    "default": 5         # Default value for other services
}


async def process_contexts_in_batches(
    contexts: EntityContextList,
    processor_func: ProcessorFunc,
    service_name: str,
    config: Optional[Dict[str, Any]] = None,
    use_cache: bool = True
) -> EntityContextList:
    """
    Processes a list of EntityProcessingContext objects in optimized batches.
    
    Args:
        contexts: List of EntityProcessingContext objects
        processor_func: Asynchronous function that processes a context
        service_name: Name of the service (for batch size and rate limit)
        config: Configuration (optional)
        use_cache: Whether to use the cache
        
    Returns:
        List of processed contexts
    """
    if not contexts:
        return []
    
    config = config or get_config()
    
    # Determine optimal batch size and rate limit
    batch_size = config.get(f"{service_name.upper()}_BATCH_SIZE", 
                         OPTIMAL_BATCH_SIZES.get(service_name, OPTIMAL_BATCH_SIZES["default"]))
    
    rate_limit = config.get(f"{service_name.upper()}_RATE_LIMIT", 
                         RATE_LIMITS.get(service_name, RATE_LIMITS["default"]))
    
    # Minimum delay between batches in seconds
    min_delay = 1.0 / rate_limit if rate_limit > 0 else 0
    
    # Filter out already processed contexts
    unprocessed_contexts = [
        ctx for ctx in contexts 
        if not ctx.is_processed_by(service_name)
    ]
    
    if len(unprocessed_contexts) < len(contexts):
        logger.info(f"{len(contexts) - len(unprocessed_contexts)} contexts already processed by {service_name}")
    
    if not unprocessed_contexts:
        logger.info(f"All contexts already processed by {service_name}, skipping")
        return contexts
    
    # Try to load contexts from cache if use_cache is enabled
    if use_cache and config.get("CACHE_ENABLED", True):
        # Identify and process contexts that are in the cache
        cached_contexts_count = 0
        for ctx in unprocessed_contexts[:]:
            cached_context = load_context_from_cache(ctx.entity_name, ctx.entity_id, ctx.entity_type)
            if cached_context and cached_context.is_processed_by(service_name):
                # Copy the relevant service data from the cache
                service_data = cached_context.get_service_data(service_name)
                if service_data:
                    # Important: Copy the entire processing_data, not just the service data
                    # This ensures that fields like 'wikipedia_multilang' are preserved
                    for key, value in cached_context.processing_data.items():
                        if key != service_name and key not in ctx.processing_data:
                            ctx.processing_data[key] = value
                            logger.debug(f"Additional processing_data field '{key}' copied from cache for {ctx.entity_name}")
                    
                    # Now add the service data
                    ctx.add_service_data(service_name, service_data)
                    ctx.log_processing_info({
                        "service": service_name,
                        "status": "loaded_from_cache",
                        "timestamp": time.time()
                    })
                    ctx.set_service_data(service_name, service_data)
                    ctx.mark_processed_by(service_name)
                    unprocessed_contexts.remove(ctx)
                    cached_contexts_count += 1
        
        if cached_contexts_count > 0:
            logger.info(f"{cached_contexts_count} contexts loaded from cache for {service_name}")
    
    if not unprocessed_contexts:
        logger.info(f"All contexts for {service_name} loaded from cache, skipping API requests")
        return contexts
    
    # Group similar contexts for more efficient processing
    start_time = time.time()
    grouped_contexts = group_contexts_by_similarity(unprocessed_contexts)
    
    # Process the groups in batches
    total_groups = len(grouped_contexts)
    total_contexts = len(unprocessed_contexts)
    
    logger.info(f"Starting processing of {total_contexts} contexts in {total_groups} groups with {service_name}")
    
    start_time = time.time()
    
    # Process each group
    for group_idx, group in enumerate(grouped_contexts):
        group_start = time.time()
        
        # Split the group into batches
        for batch_idx in range(0, len(group), batch_size):
            batch = group[batch_idx:batch_idx + batch_size]
            batch_start = time.time()
            
            # Process the batch in parallel
            tasks = [processor_func(ctx) for ctx in batch]
            await asyncio.gather(*tasks)
            
            # Save processed contexts in cache if enabled
            if use_cache and config.get("CACHE_ENABLED", True):
                for ctx in batch:
                    if ctx.is_processed_by(service_name):
                        cache_context(ctx)
            
            batch_duration = time.time() - batch_start
            logger.debug(f"Batch {batch_idx//batch_size + 1} of group {group_idx + 1}/{total_groups} processed in {batch_duration:.2f}s")
            
            # Respect the rate limit
            if min_delay > 0 and batch_idx + batch_size < len(group):
                await asyncio.sleep(min_delay)
        
        group_duration = time.time() - group_start
        logger.info(f"Group {group_idx + 1}/{total_groups} with {len(group)} contexts processed in {group_duration:.2f}s")
    
    # Total duration and summary
    total_duration = time.time() - start_time
    logger.info(f"All {total_contexts} contexts processed with {service_name} in {total_duration:.2f}s")
    
    return contexts


async def process_relationships_in_batches(
    contexts: EntityContextList,
    extractor_func: Callable[[List[Dict[str, Any]]], Awaitable[List[Dict[str, Any]]]],
    config: Optional[Dict[str, Any]] = None,
    batch_size: int = 20
) -> List[Dict[str, Any]]:
    """
    Extracts relationships between entities in batches for better performance.
    
    Args:
        contexts: List of EntityProcessingContext objects
        extractor_func: Asynchronous function for extracting relationships
        config: Configuration (optional)
        batch_size: Size of batches to process
        
    Returns:
        List of all extracted relationships
    """
    if not contexts:
        return []
    
    config = config or get_config()
    
    # Convert contexts to the format needed for relationship extraction
    entity_data = []
    for ctx in contexts:
        entity_info = {
            "id": ctx.entity_id,
            "name": ctx.entity_name,
            "type": ctx.entity_type or "",
        }
        
        # Add Wikipedia data if available
        wikipedia_data = ctx.get_service_data("wikipedia").get("wikipedia", {})
        if wikipedia_data:
            entity_info["abstract"] = wikipedia_data.get("abstract", "")
            entity_info["url"] = wikipedia_data.get("url", "")
        
        # Add Wikidata types if available
        wikidata_data = ctx.get_service_data("wikidata").get("wikidata", {})
        if wikidata_data:
            entity_info["wikidata_types"] = wikidata_data.get("types", [])
        
        entity_data.append(entity_info)
    
    # Split entities into batches for relationship extraction
    all_relationships = []
    total_batches = (len(entity_data) + batch_size - 1) // batch_size
    
    logger.info(f"Extracting relationships for {len(entity_data)} entities in {total_batches} batches")
    start_time = time.time()
    
    for batch_idx in range(0, len(entity_data), batch_size):
        batch_start_time = time.time()
        
        # Extract the current batch
        current_batch = entity_data[batch_idx:batch_idx + batch_size]
        
        # Extract relationships for this batch
        batch_relationships = await extractor_func(current_batch)
        all_relationships.extend(batch_relationships)
        
        # Batch progress
        batch_num = batch_idx // batch_size + 1
        batch_duration = time.time() - batch_start_time
        logger.info(f"Relationship batch {batch_num}/{total_batches} processed in {batch_duration:.2f}s, "
                  f"{len(batch_relationships)} relationships extracted")
    
    total_duration = time.time() - start_time
    logger.info(f"All relationships extracted in {total_duration:.2f}s, "
              f"total of {len(all_relationships)} relationships")
    
    # Update the contexts with the extracted relationships
    for ctx in contexts:
        entity_id = ctx.entity_id
        # Find relationships where this entity appears
        ctx_relationships = [
            rel for rel in all_relationships
            if rel.get("subject") == entity_id or rel.get("object") == entity_id
        ]
        
        # Add relationships to the context
        for rel in ctx_relationships:
            subject_id = rel.get("subject")
            predicate = rel.get("predicate")
            object_id = rel.get("object")
            
            # Extract metadata like types if available
            subject_type = rel.get("subject_type")
            object_type = rel.get("object_type")
            metadata = {k: v for k, v in rel.items() 
                       if k not in ["subject", "predicate", "object", "subject_type", "object_type"]}
            
            ctx.add_relationship(subject_id, predicate, object_id, 
                                subject_type=subject_type, 
                                object_type=object_type,
                                metadata=metadata)
    
    return all_relationships


def group_contexts_by_similarity(
    contexts: EntityContextList,
    similarity_threshold: float = 0.7
) -> List[List[EntityProcessingContext]]:
    """
    Groups contexts by similarity of their names and types.
    This can be used to optimize batch processing for similar entities.
    
    Args:
        contexts: List of EntityProcessingContext objects
        similarity_threshold: Threshold for similarity (0.0-1.0)
        
    Returns:
        List of groups of similar contexts
    """
    if not contexts:
        return []
    
    # Helper function to calculate similarity between two strings
    def string_similarity(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        
        a_words = set(a.lower().split())
        b_words = set(b.lower().split())
        
        if not a_words or not b_words:
            return 0.0
        
        intersection = len(a_words.intersection(b_words))
        union = len(a_words.union(b_words))
        
        return intersection / union if union > 0 else 0.0
    
    # Helper function to calculate overall similarity between two contexts
    def context_similarity(ctx1: EntityProcessingContext, ctx2: EntityProcessingContext) -> float:
        name_sim = string_similarity(ctx1.entity_name, ctx2.entity_name)
        
        # Type similarity (if type is available)
        type_sim = 0.0
        if ctx1.entity_type and ctx2.entity_type:
            type_sim = 1.0 if ctx1.entity_type == ctx2.entity_type else 0.0
        
        # Weighted combination: name is more important than type
        return 0.7 * name_sim + 0.3 * type_sim
    
    # Group similar contexts
    groups = []
    remaining = list(contexts)
    
    while remaining:
        current = remaining.pop(0)
        current_group = [current]
        
        i = 0
        while i < len(remaining):
            candidate = remaining[i]
            if context_similarity(current, candidate) >= similarity_threshold:
                current_group.append(candidate)
                remaining.pop(i)
            else:
                i += 1
        
        groups.append(current_group)
    
    # Log the grouping
    group_sizes = [len(group) for group in groups]
    avg_size = sum(group_sizes) / len(groups) if groups else 0
    logger.info(f"{len(groups)} context groups created, average size: {avg_size:.1f}")
    
    return groups
