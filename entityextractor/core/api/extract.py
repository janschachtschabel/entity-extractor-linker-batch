"""
extract.py

Consolidated API module for entity extraction.

This module combines the basic extraction functionality from entityextractor.core.extractor
with the linking functionality from entityextractor.core.api.link to provide a complete
extraction and linking process.

This is the recommended public API for entity extraction with linking.
Supports the new context-based architecture.
"""

import time
from typing import List, Dict, Any, Optional, Union

from loguru import logger

from entityextractor.services.openai_service import extract_entities_with_openai
from entityextractor.core.entity_inference import infer_entities
from entityextractor.core.api.link import link_entities, link_contexts
from entityextractor.core.context import EntityProcessingContext
from entityextractor.config.settings import get_config
from entityextractor.utils.id_utils import generate_entity_id
from entityextractor.core.process.circular_import_fix import format_contexts_to_result

async def extract_entities(text, config=None, use_context_architecture=True):
    """
    Extracts and links entities from a text.
    
    Args:
        text: The text from which entities should be extracted
        config: Configuration dictionary (optional)
        use_context_architecture: Whether to use the new context-based architecture
        
    Returns:
        With use_context_architecture=False: List of extracted and linked entities
        With use_context_architecture=True: Formatted result object with entities and metadata
    """
    config = get_config(config)
    logger.info("[extract_api] Starting extraction and linking...")
    start_extraction = time.time()
    
    # Extract explicit entities with OpenAI
    logger.info("Starting entity extraction...")
    entities = extract_entities_with_openai(text, config)

    # Assign UUID4 for each entity
    for entity in entities:
        if 'id' not in entity:
            entity['id'] = generate_entity_id()
    
    # Perform entity inference if enabled
    if config.get("ENABLE_ENTITY_INFERENCE", False):
        logger.info("Entity Inference activated: Creating implicit entities via LLM...")
        entities = infer_entities(text, entities, config)
        logger.info(f"Entities after inference: {len(entities)}")
        # Assign IDs to new entities after inference
        for entity in entities:
            if 'id' not in entity:
                entity['id'] = generate_entity_id()
    
    elapsed_time = time.time() - start_extraction
    logger.info(f"Entity extraction completed in {elapsed_time:.2f} seconds")
    logger.info(f"[extract_api] Extracted {len(entities)} entities")
    
    if not use_context_architecture:
        # Legacy mode: Link all entities with knowledge sources and return the list
        linked_entities = await link_entities(entities, config)
        logger.info(f"[extract_api] Linked {len(linked_entities)} entities with legacy architecture")
        # --- Save training data *after* linking so that corrected data from fallbacks is persisted
        if config.get("COLLECT_TRAINING_DATA", False):
            from entityextractor.services.openai_service import save_training_data
            save_training_data(text, linked_entities, config)
        return linked_entities
    else:
        # New context-based architecture
        logger.info("[extract_api] Using context-based architecture")
        
        # Convert dictionary entities to EntityProcessingContext objects
        contexts = []
        for entity in entities:
            # Support both 'name' and 'entity' keys for entity name
            entity_name = entity.get("entity", entity.get("name", ""))
            context = EntityProcessingContext(
                entity_name=entity_name,
                entity_id=entity.get("id", ""),
                entity_type=entity.get("type", ""),
                original_text=text
            )
            # Propagate inference flag so statistics distinguish implicit entities
            if entity.get("inferred", "explicit") != "explicit":
                context.set_as_inferred(entity["inferred"])
            contexts.append(context)
        
        # Link the contexts with knowledge sources
        linked_contexts = await link_contexts(contexts, config)
        logger.info(f"[extract_api] Linked {len(linked_contexts)} entity contexts")
        
        # Format the contexts to a standardized result object
        # Ensure that the original text is present in each context
        for context in linked_contexts:
            if not context.original_text:
                context.original_text = text
                
        result = format_contexts_to_result(linked_contexts)
        
        # --- Save training data from fully linked contexts (post-fallback)
        if config.get("COLLECT_TRAINING_DATA", False):
            from entityextractor.services.openai_service import save_training_data
            training_entities = []
            for ctx in linked_contexts:
                wikipedia_source = ctx.output_data.get("sources", {}).get("wikipedia", {}) if isinstance(ctx.output_data.get("sources", {}), dict) else {}
                wiki_url_de = wikipedia_source.get("url_de", "") or wikipedia_source.get("url", "")  # fallback
                wiki_url_en = wikipedia_source.get("url_en", "") or wikipedia_source.get("url", "")

                details = ctx.output_data.get("details", {})
                label_de = details.get("label_de", "") or ctx.entity_name  # fallback
                label_en = details.get("label_en", "") or ctx.entity_name

                training_entities.append({
                    "name": ctx.entity_name,
                    "type": ctx.entity_type or details.get("typ", ""),
                    "citation": ctx.citation or "",
                    "label_de": label_de,
                    "label_en": label_en,
                    "wikipedia_url_de": wiki_url_de,
                    "wikipedia_url_en": wiki_url_en,
                })
            save_training_data(text, training_entities, config)
        return result
