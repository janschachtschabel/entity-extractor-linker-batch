#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
New Orchestrator for the Entity Extractor

Responsible for coordinating services and processing entities.
Uses EntityProcessingContext for structured data transfer and
schema validation.
"""

import time
import asyncio
from loguru import logger
from typing import List, Dict, Any, Optional, Callable, Awaitable

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.schemas.service_schemas import validate_entity_output
from entityextractor.core.relationship_extraction import extract_relationships_from_contexts
from entityextractor.core.process.context_statistics import generate_context_statistics, format_statistics
from entityextractor.services.dbpedia.service import DBpediaService
from entityextractor.services.wikipedia.service import WikipediaService
from entityextractor.services.wikidata.service import WikidataService

# Use the singleton pattern for DBpediaService
_dbpedia_service_instance = DBpediaService.get_instance(get_config())
# Get service instances
wikipedia_service: Optional[WikipediaService] = None
wikidata_service = WikidataService(get_config())

from entityextractor.utils.id_utils import generate_entity_id
from entityextractor.utils.batch_processing import process_contexts_in_batches, process_relationships_in_batches, group_contexts_by_similarity

# Define a wrapper for extract_relationships_from_contexts to match the expected function signature
async def extract_relationships(processed_entities: List[Dict[str, Any]], 
                               input_text: str, 
                               config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Extract relationships between entities.
    
    This is a wrapper around extract_relationships_from_contexts that converts entity dictionaries
    to EntityProcessingContext objects and then extracts relationships.
    
    Args:
        processed_entities: List of processed entity dictionaries
        input_text: Original text from which the entities were extracted
        config: Optional configuration
        
    Returns:
        List of relationship dictionaries
    """
    # Create contexts from processed entities
    contexts = []
    for entity in processed_entities:
        entity_id = entity.get('id') or generate_entity_id()
        context = EntityProcessingContext(
            entity_name=entity.get('name', ''),
            entity_id=entity_id,
            entity_type=entity.get('type', ''),
            original_text=input_text
        )
        # Add service data to context
        for service_name, service_data in entity.items():
            if service_name not in ['id', 'name', 'type', 'original_text']:
                context.add_service_data(service_name, service_data)
        contexts.append(context)
    
    # Decide which relationship extraction function to use based on config
    from entityextractor.core.api.relationships import infer_entity_relationships

    if config and config.get("ENABLE_RELATIONS_INFERENCE", False):
        # Use high-level inference function that handles explicit and implicit relations
        logger.info("[orchestrator] ENABLE_RELATIONS_INFERENCE=True – using infer_entity_relationships")
        relationships = infer_entity_relationships(processed_entities, text=input_text, config=config)
        return relationships

    # Fallback to legacy extraction without implicit inference
    logger.info("[orchestrator] ENABLE_RELATIONS_INFERENCE=False – using legacy extract_relationships_from_contexts")
    return await extract_relationships_from_contexts(contexts, config)

async def process_entity(entity_name: str, entity_type: Optional[str] = None, 
                         entity_id: Optional[str] = None,
                         original_text: Optional[str] = None,
                         config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Processes a single entity with all configured services.
    
    Args:
        entity_name: Name of the entity to process
        entity_type: Optional entity type
        entity_id: Optional entity ID (will be generated if not provided)
        original_text: Optional original text from which the entity was extracted
        config: Optional configuration
        
    Returns:
        Processed entity data
    """
    if config is None:
        config = get_config()

    global wikipedia_service
    if wikipedia_service is None or wikipedia_service.config.get("LANGUAGE") != config.get("LANGUAGE"):
        wikipedia_service = WikipediaService(config)
        
    start_time = time.time()
    logger.info(f"Processing entity: {entity_name}")
    
    # Generate ID if not provided
    if entity_id is None:
        entity_id = generate_entity_id(entity_name)
        
    # Create context
    context = EntityProcessingContext(entity_name, entity_id, entity_type, original_text)
    
    # Process with Wikipedia (if enabled)
    if config.get("USE_WIKIPEDIA", True):
        await wikipedia_service.process_entity(context)
        
    # Process with Wikidata (if enabled)
    if config.get("USE_WIKIDATA", True):
        await wikidata_service.process_entity(context)
        
    # Process with DBpedia (if enabled)
    if config.get("USE_DBPEDIA", True):
        await _dbpedia_service_instance.process_entity(context)
    
    # Get and validate output
    output = context.get_output()
    is_valid = validate_entity_output(output)
    if not is_valid:
        logger.warning(f"Output for entity '{entity_name}' is not valid")
    
    # Output statistics
    elapsed = time.time() - start_time
    logger.debug(f"Entity '{entity_name}' processed in {elapsed:.2f}s")
    
    # Log context summary
    context.log_summary("INFO")
    
    return output

async def process_entities(entities: List[Dict[str, Any]], 
                           original_text: Optional[str] = None,
                           config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Processes multiple entities with optimized batch processing.
    
    Args:
        entities: List of entities (dictionaries with 'name' and optionally 'type', 'id')
        original_text: Optional original text from which the entities were extracted
        config: Optional configuration
        
    Returns:
        Dictionary with processed entities, relationships, statistics, and visualization info
    """
    if config is None:
        config = get_config()

    global wikipedia_service
    if wikipedia_service is None or wikipedia_service.config.get("LANGUAGE") != config.get("LANGUAGE"):
        wikipedia_service = WikipediaService(config)
        
    start_time = time.time()
    logger.info(f"[orchestrator] Processing {len(entities)} entities")
    
    # Create EntityProcessingContext objects for each entity
    contexts = []
    for entity in entities:
        name = entity.get("name", "")
        entity_type = entity.get("type", None)
        entity_id = entity.get("id", generate_entity_id())
        
        # Create context
        context = EntityProcessingContext(name, entity_id, entity_type, original_text)
        
        # Übertrage Zitationsinformationen, falls vorhanden
        if "citation" in entity:
            context.set_citation(entity["citation"])
            
            # Speichere auch Start- und Endposition, falls vorhanden
            if "citation_start" in entity:
                context.set_processing_info("citation_start", entity["citation_start"])
            if "citation_end" in entity:
                context.set_processing_info("citation_end", entity["citation_end"])
        
        # Übertrage inferred-Status, falls vorhanden
        if entity.get("inferred", "explicit") != "explicit":
            # Setze das Flag sowohl im Context (für Statistiken) als auch als Processing-Info (für Debugging)
            context.set_as_inferred(entity["inferred"])
            context.set_processing_info("inferred", entity["inferred"])
            
        contexts.append(context)
    
    # Group contexts by similarity for batch processing (for logging purposes only)
    if config.get("GROUP_SIMILAR_ENTITIES", True):
        logger.info("[orchestrator] Grouping similar entities for batch processing")
        context_groups = group_contexts_by_similarity(contexts)
        logger.info(f"[orchestrator] {len(context_groups)} entity groups created")
    
    
    # 1. Wikipedia service (if enabled)
    if config.get("USE_WIKIPEDIA", True):
        logger.info("[orchestrator] Processing with Wikipedia service")
        await process_contexts_in_batches(
            contexts, 
            wikipedia_service.process_entity,
            "wikipedia", 
            config,
            use_cache=config.get("CACHE_ENABLED", True)
        )
    
    # 2. Wikidata service (if enabled)
    if config.get("USE_WIKIDATA", True):
        logger.info("[orchestrator] Processing with Wikidata service")
        await process_contexts_in_batches(
            contexts, 
            wikidata_service.process_entity,
            "wikidata", 
            config,
            use_cache=config.get("CACHE_ENABLED", True)
        )
    
    # 3. DBpedia service (if enabled)
    if config.get("USE_DBPEDIA", True):
        await process_contexts_in_batches(
            contexts, 
            _dbpedia_service_instance.process_entity,
            "dbpedia", 
            config,
            use_cache=config.get("CACHE_ENABLED", True)
        )
    
    # Validate all contexts
    valid_count = 0
    for context in contexts:
        output = context.get_output()
        is_valid = validate_entity_output(output)
        if is_valid:
            valid_count += 1
        else:
            logger.warning(f"Output for entity '{context.entity_name}' is not valid")
        
        # Log summary for each entity
        context.log_summary(20)  # 20 is the numeric value for INFO level
    
    # Extract output data from all contexts
    processed_entities = [context.get_output() for context in contexts]
    
    # Create relationships if enabled
    relationships = []
    if config.get("RELATION_EXTRACTION", True):
        logger.info("[orchestrator] Extracting relationships between entities")
        # Use wrapper that decides between explicit-only and explicit+implicit
        relationships = await extract_relationships(processed_entities, original_text, config)
        logger.info(f"[orchestrator] {len(relationships)} relationships extracted")

        # Attach extracted relationships back to contexts so statistics can count them
        if relationships:
            id_to_ctx = {ctx.entity_id: ctx for ctx in contexts if ctx.entity_id}
            # Prepare lower-case name mapping as fallback
            name_to_ctx_lower = {ctx.entity_name.lower(): ctx for ctx in contexts if ctx.entity_name}
            for rel in relationships:
                sid = rel.get("subject_id")
                oid = rel.get("object_id")
                if sid in id_to_ctx:
                    id_to_ctx[sid].add_relationship(rel)
                elif rel.get("subject", "").lower() in name_to_ctx_lower:
                    name_to_ctx_lower[rel["subject"].lower()].add_relationship(rel)

                if oid in id_to_ctx:
                    id_to_ctx[oid].add_relationship(rel)
                elif rel.get("object", "").lower() in name_to_ctx_lower:
                    name_to_ctx_lower[rel["object"].lower()].add_relationship(rel)

        # Persist relationship training data if enabled
        if config.get("COLLECT_TRAINING_DATA", False) and relationships:
            try:
                from entityextractor.services.openai_service import save_relationship_training_data
                system_prompt_rel = "You are a helpful AI system that identifies relationships between entities."
                user_prompt_rel = "Provide relationships in the format: Subject; Predicate; Object."
                save_relationship_training_data(system_prompt_rel, user_prompt_rel, relationships, config)
            except Exception as exc:
                logger.error(f"[orchestrator] Failed to save relationship training data: {exc}")
    
    # Create result structure
    result = {
        "entities": processed_entities,
        "relationships": relationships,
        "original_text": original_text
    }

    # ------------------------------------------------------------------
    # Optional compendium & bibliography using OpenAI
    # ------------------------------------------------------------------
    if config.get("ENABLE_COMPENDIUM", False):
        try:
            from entityextractor.services.compendium_service import generate_compendium
            comp_text, refs = generate_compendium(
                original_text or "",  # topic or text
                processed_entities,
                relationships,
                user_config=config,
            )
            result["compendium"] = comp_text
            result["references"] = refs

        except Exception as e:
            logger.error(f"[orchestrator] Compendium generation failed: {e}")
    
    # Call QA generation after compendium and add to result
    if config.get("QA_PAIR_COUNT", 0) > 0:
        try:
            from entityextractor.services.qa_service import generate_qa_pairs
            qa_pairs, _ = generate_qa_pairs(
                original_text or "",  # topic or text
                result.get("compendium"),
                result.get("references"),
                config,
            )
            result["qa_pairs"] = qa_pairs

        except Exception as e:
            logger.error(f"[orchestrator] QA generation failed: {e}")
    
    # Add statistics (if enabled)
    if config.get("GENERATE_STATISTICS", True):
        logger.info("[orchestrator] Generating statistics")
        result["statistics"] = generate_context_statistics(contexts)
        
        # Log statistics summary
        if config.get("LOG_STATISTICS_SUMMARY", False):
            stats_summary = format_statistics(result["statistics"])
            logger.info(f"[orchestrator] Statistics summary:\n{stats_summary}")
    
    # Optionally create a Knowledge Graph visualization
    if config.get("ENABLE_GRAPH_VISUALIZATION", False):
        from entityextractor.core.visualization.visualizer import visualize_graph
        viz_result = visualize_graph(result, config)
        if viz_result:
            logger.info(f"[orchestrator] Graph visualization created successfully: PNG={viz_result.get('png')}, HTML={viz_result.get('html')}")
        else:
            logger.warning("[orchestrator] Graph visualization was not created successfully")
    
    # Ensure proper cleanup of service sessions
    try:
        elapsed = time.time() - start_time
        logger.info(f"[orchestrator] {len(processed_entities)} entities processed in {elapsed:.2f}s ({valid_count} valid)")
        return result
    finally:
        # Cleanup sessions in a non-blocking way
        asyncio.create_task(DBpediaService.close_all_sessions())
        asyncio.create_task(wikipedia_service.close_session())
        asyncio.create_task(wikidata_service.close_session())
        logger.debug("[orchestrator] Service session cleanup tasks scheduled")

async def process_single_pass(input_text: str, entities: List[Dict[str, Any]], 
                        config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Processes a text with pre-extracted entities in a single pass.
    Uses optimized batch processing for entities and relationships.
    
    Args:
        input_text: The source text
        entities: Pre-extracted entities
        config: Optional configuration
        
    Returns:
        A result object with processed entities and optionally relationships
    """
    logger.info("[orchestrator] Starting optimized single-pass processing")
    config = config or get_config()
    start_time = time.time()
    
    # Process entities with knowledge sources
    processed_entities = await process_entities(entities, input_text, config)
    
    # If process_entities now returns a complete result structure, use it directly
    if isinstance(processed_entities, dict) and "entities" in processed_entities:
        result = processed_entities
    else:
        # For backward compatibility, handle the case where process_entities returns just the entities list
        # Extract relationships if enabled
        relationships = []
        if config.get("RELATION_EXTRACTION", True):
            logger.info("[orchestrator] Extracting relationships between entities")
            relationships = await extract_relationships(processed_entities, input_text, config)
            logger.info(f"[orchestrator] {len(relationships)} relationships extracted")

            # Persist relationship training data if enabled
            if config.get("COLLECT_TRAINING_DATA", False) and relationships:
                try:
                    from entityextractor.services.openai_service import save_relationship_training_data
                    system_prompt_rel = "You are a helpful AI system that identifies relationships between entities."
                    user_prompt_rel = "Provide relationships in the format: Subject; Predicate; Object."
                    save_relationship_training_data(system_prompt_rel, user_prompt_rel, relationships, config)
                except Exception as exc:
                    logger.error(f"[orchestrator] Failed to save relationship training data: {exc}")
            
            # Process relationships in batches (if enabled)
            if config.get("PROCESS_RELATIONSHIPS", False):
                logger.info("[orchestrator] Processing relationships with services")
                # Implementation for batch relationship processing would go here
                batch_size = config.get("RELATIONSHIP_BATCH_SIZE", 10)
                
        # Create result structure
        result = {
            "entities": processed_entities,
            "relationships": relationships,
            "original_text": input_text
        }
        
        # Add statistics (if enabled)
        if config.get("GENERATE_STATISTICS", True):
            logger.info("[orchestrator] Generating statistics")
            # This would need contexts which we don't have here
            # result["statistics"] = generate_context_statistics(contexts)
            
    # -------------------------------------------------------------------
    # Generate QA pairs if requested
    # -------------------------------------------------------------------
    if config.get("ENABLE_QA_PAIRS", True) and int(config.get("QA_PAIR_COUNT", 0)) > 0:
        from entityextractor.services.qa_service import generate_qa_pairs
        try:
            qa_pairs, refs = generate_qa_pairs(
                topic_or_text=input_text,
                compendium_text=result.get("compendium"),
                references=result.get("references"),
                user_config=config,
            )
            result["qa_pairs"] = qa_pairs
            if refs:
                result["references"] = refs
            logger.info(f"[orchestrator] Added {len(qa_pairs)} QA pairs to result")
        except Exception as exc:
            logger.error(f"[orchestrator] QA generation failed: {exc}")

    # Persist relationship training data if enabled and relationships exist (generic place)
    if config.get("COLLECT_TRAINING_DATA", False) and result.get("relationships"):
        try:
            from entityextractor.services.openai_service import save_relationship_training_data
            system_prompt_rel = "You are a helpful AI system that identifies relationships between entities."
            user_prompt_rel = "Provide relationships in the format: Subject; Predicate; Object."
            save_relationship_training_data(system_prompt_rel, user_prompt_rel, result["relationships"], config)
        except Exception as exc:
            logger.error(f"[orchestrator] Failed to save relationship training data: {exc}")

    # Debug output for relationships after formatting
    if "relationships" in result and result["relationships"]:
        logger.info(f"[orchestrator] Relationships after formatting: {len(result['relationships'])}")
    else:
        logger.warning("[orchestrator] No relationships in the formatted result!")
    
    # Optionally create a Knowledge Graph visualization
    if config.get("ENABLE_GRAPH_VISUALIZATION", False):
        from entityextractor.core.visualization.visualizer import visualize_graph
        viz_result = visualize_graph(result, config)
        if viz_result:
            logger.info(f"[orchestrator] Graph visualization created successfully: PNG={viz_result.get('png')}, HTML={viz_result.get('html')}")
        else:
            logger.warning("[orchestrator] Graph visualization was not created successfully")
    
    # Log processing time
    elapsed = time.time() - start_time
    logger.info(f"[orchestrator] Single-pass done in {elapsed:.2f} sec")
    
    return result
