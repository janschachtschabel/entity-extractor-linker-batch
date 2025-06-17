"""
generate.py

Consolidated API module for entity generation.

This module combines the basic generation functionality from entityextractor.core.generator
with the linking functionality from entityextractor.core.api.link to provide a complete
generation and linking process.

This is the recommended public API for entity generation with linking to knowledge sources.
Supports the new context-based architecture.
"""

import asyncio
from typing import List, Dict, Any, Optional, Union

from loguru import logger

from entityextractor.core.generator import generate_entities as generate_entities_internal
from entityextractor.core.api.link import link_entities, link_contexts
from entityextractor.core.context import EntityProcessingContext
from entityextractor.config.settings import get_config
from entityextractor.utils.id_utils import generate_entity_id
from entityextractor.core.process.result_formatter import format_contexts_to_result

async def generate_entities(topic, config=None, use_context_architecture=True):
    """
    Generates and links entities for a specific topic.
    
    Args:
        topic: The topic for which entities should be generated
        config: Configuration dictionary (optional)
        use_context_architecture: Whether to use the new context-based architecture
        
    Returns:
        With use_context_architecture=False: List of generated and linked entities
        With use_context_architecture=True: Formatted result object with entities and metadata
    """
    config = get_config(config)
    logger.info(f"[generate_api] Starting generation for topic: {topic}")
    
    # Generate entities for the specified topic
    entities = generate_entities_internal(topic, config)
    logger.info(f"[generate_api] Generated {len(entities)} entities")
    
    # Assign UUID4 for each entity if not already present
    for entity in entities:
        if 'id' not in entity:
            entity['id'] = generate_entity_id()
    
    if not use_context_architecture:
        # Legacy mode: Link all entities with knowledge sources and return the list
        linked_entities = await link_entities(entities, config)
        logger.info(f"[generate_api] Linked {len(linked_entities)} entities with legacy architecture")
        return linked_entities
    else:
        # New context-based architecture
        logger.info("[generate_api] Using context-based architecture")
        
        # Convert dictionary entities to EntityProcessingContext objects
        contexts = []
        for entity in entities:
            context = EntityProcessingContext(
                entity_name=entity.get("name", ""),
                entity_id=entity.get("id", ""),
                entity_type=entity.get("type", ""),
                original_text=topic  # Use the topic as original text
            )
            contexts.append(context)
        
        # Link the contexts with knowledge sources
        linked_contexts = await link_contexts(contexts, config)
        logger.info(f"[generate_api] Linked {len(linked_contexts)} entity contexts")
        
        # Format the contexts to a standardized result object
        result = format_contexts_to_result(linked_contexts)
        
        return result
