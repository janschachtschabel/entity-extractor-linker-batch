"""
entity_linker/main.py

Main function for linking entities with knowledge sources.
Supports both the traditional dictionary-based API and
the new context-based architecture.
"""

import uuid
from typing import List, Dict, Any, Optional, Union, Tuple

from loguru import logger

from entityextractor.core.context import EntityProcessingContext
from entityextractor.models.entity import Entity
from entityextractor.core.api.entity_linker.converters import dicts_to_entities, entities_to_dicts
from entityextractor.core.api.entity_linker.wikipedia import link_with_wikipedia
from entityextractor.core.api.entity_linker.wikidata import link_with_wikidata

from entityextractor.services.wikidata.service import wikidata_service as global_wikidata_service
from entityextractor.services.dbpedia.service import DBpediaService
import os  # Added for path operations
import logging

async def link_entities(entities: Union[List[Dict[str, Any]], List[EntityProcessingContext], List[Entity]], 
                   config: Optional[Dict[str, Any]] = None) -> Union[List[Dict[str, Any]], List[EntityProcessingContext]]:
    """
    Links entities with knowledge sources (Wikipedia, Wikidata, DBpedia).
    
    This function coordinates the entire linking process and calls
    the specialized modules for the different knowledge sources in sequence.
    
    This function supports both architecture variants:
    1. The traditional dictionary-based API (list of dict entities)
    2. The new context-based architecture (list of EntityProcessingContext objects)
    
    Args:
        entities: List of entities (Dict, EntityProcessingContext or Entity)
        config: Configuration dictionary
        
    Returns:
        List of linked entities in the same format as the input
    """
    if not entities:
        return []
        
    # Determine input type and convert if necessary
    input_type = "dict" if isinstance(entities[0], dict) else "context" if isinstance(entities[0], EntityProcessingContext) else "entity"
    logger.debug(f"Input type for linking: {input_type}")
    
    if isinstance(entities[0], EntityProcessingContext):
        return await _link_contexts(entities, config)
    elif isinstance(entities[0], Entity):
        return await _link_entity_objects(entities, config)
    else:
        # Legacy mode with dictionary entities
        return await _link_dictionaries(entities, config)


async def _link_dictionaries(entities: List[Dict[str, Any]], config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Legacy method for linking dictionary entities with knowledge sources.
    
    Args:
        entities: List of dictionary entities
        config: Configuration dictionary
        
    Returns:
        List of linked dictionary entities
    """
    if not entities:
        return []
    
    # Prepare configuration
    if config is None:
        from entityextractor.config.settings import get_config
        config = get_config()
        
    # Get language from configuration
    language = config.get("LANGUAGE", "de")
    logger.debug(f"Using language: {language}")
    
    # Convert dictionary entities to Entity objects
    entity_objects = dicts_to_entities(entities, language)
    
    # Perform linking
    linked_entity_objects = await _link_entity_objects(entity_objects, config)
    
    # Convert Entity objects back to dictionaries
    result_entities = entities_to_dicts(linked_entity_objects)
    
    return result_entities
    

async def _link_entity_objects(entity_objects: List[Entity], config: Optional[Dict[str, Any]] = None) -> List[Entity]:
    """
    Links Entity objects with knowledge sources.
    
    Args:
        entity_objects: List of Entity objects
        config: Configuration dictionary
        
    Returns:
        List of linked Entity objects
    """
    if not entity_objects:
        return []
        
    # Prepare configuration
    config = config or {}
    
    # 1. Link with Wikipedia
    if config.get("USE_WIKIPEDIA", True):
        entity_objects = await link_with_wikipedia(entity_objects, config)
    
    # 2. Link with Wikidata
    if config.get("USE_WIKIDATA", True):
        # Wikidata linking
        entity_objects = await link_with_wikidata(entity_objects, config)
    
    # 3. Link with DBpedia (new service)
    if config.get("USE_DBPEDIA", True):
        logger.debug(f"[_link_entity_objects] DBpedia linking enabled for {len(entity_objects)} Entity objects.")
        dbpedia_service = DBpediaService(config) # Instantiate new service
        
        contexts_to_process: List[EntityProcessingContext] = []
        # Ensure EntityProcessingContext is imported/available
        # from entityextractor.core.context import EntityProcessingContext
        # from entityextractor.models.base import LanguageCode # If needed for language

        for entity_obj in entity_objects:
            lang_val = entity_obj.language.value if entity_obj.language else config.get("DEFAULT_LANGUAGE", "de")
            
            epc = EntityProcessingContext(
                entity_name=entity_obj.name,
                language=lang_val
            )
            setattr(epc, '_original_entity_ref', entity_obj) # Store ref to original Entity

            wiki_source = entity_obj.get_source('wikipedia')
            if wiki_source and 'url' in wiki_source:
                epc.wikipedia_url = wiki_source['url']
                # Assuming wiki_source['language'] or entity_obj.language provides the correct Wikipedia language
                epc.wikipedia_language = wiki_source.get('language', lang_val) 
            
            if entity_obj.wikidata_id:
                epc.wikidata_id = entity_obj.wikidata_id
                
            contexts_to_process.append(epc)

        if contexts_to_process:
            try:
                await dbpedia_service.process_entities(contexts_to_process)
                # Results are set back on the original_entity_ref via the context
                for context in contexts_to_process:
                    original_entity = getattr(context, '_original_entity_ref', None)
                    if original_entity:
                        dbpedia_service_output = context.get_service_data('dbpedia')
                        if dbpedia_service_output and 'data' in dbpedia_service_output:
                            original_entity.add_source(
                                source_name='dbpedia',
                                data=dbpedia_service_output['data'],
                                status=dbpedia_service_output.get('status', 'unknown')
                            )
                        elif dbpedia_service_output: # Handle error or partial status from context
                             original_entity.add_source('dbpedia', dbpedia_service_output.get('data', {}), status=dbpedia_service_output.get('status', 'error'))

            except Exception as e:
                logger.error(f"[_link_entity_objects] Error during DBpedia processing: {e}")
                for context in contexts_to_process: # Mark entities as failed
                    original_entity = getattr(context, '_original_entity_ref', None)
                    if original_entity:
                        original_entity.add_source('dbpedia', {"error": str(e), "message": "Batch processing error"}, status="error")
        else:
            logger.debug("[_link_entity_objects] No entities prepared for DBpedia processing.")
    
    return entity_objects


async def _link_contexts(contexts: List[EntityProcessingContext], config: Optional[Dict[str, Any]] = None) -> List[EntityProcessingContext]:
    # Context-based linking
    """
    Links EntityProcessingContext objects with knowledge sources.
    
    This method supports the new context-based architecture.
    
    Args:
        contexts: List of EntityProcessingContext objects
        config: Configuration dictionary
        
    Returns:
        List of linked EntityProcessingContext objects
    """
    if not contexts:
        return []
        
    # Prepare configuration
    config = config or {}
    
    # Ensure we have proper contexts
    if not all(isinstance(ctx, EntityProcessingContext) for ctx in contexts):
        logger.warning("All items in contexts must be EntityProcessingContext objects")
        return []
        
    # 1. Wikipedia processing directly with contexts
    wikipedia_contexts = [c for c in contexts if config.get("USE_WIKIPEDIA", True) and not c.is_processed_by("wikipedia")]
    if wikipedia_contexts:
        # Use the WikipediaService directly with contexts
        from entityextractor.services.wikipedia.service import WikipediaService
        wikipedia_service = WikipediaService(config)
        
        # Debug-Logging for context IDs before processing
        for context in wikipedia_contexts:
            logger.info(f"[DEBUG] Context ID before WikipediaService: {id(context)} for '{context.entity_name}'")
        
        # Process all contexts in a batch
        try:
            await wikipedia_service.process_contexts(wikipedia_contexts)
            logger.info(f"Wikipedia processing completed for {len(wikipedia_contexts)} contexts")
        except Exception as e:
            logger.error(f"Error during Wikipedia processing: {e}")
            # Mark all contexts as failed
            for ctx in wikipedia_contexts:
                ctx.processed_by_services.add("wikipedia")
                ctx.add_service_data('wikipedia', {
                    'error': str(e),
                    'status': 'error'
                })
        
        # Debug-Logging for context IDs after processing
        for context in wikipedia_contexts:
            logger.info(f"[DEBUG] Context ID after WikipediaService: {id(context)} for '{context.entity_name}'")
            
            # Check if wikipedia_multilang was set
            wiki_ml = context.processing_data.get('wikipedia_multilang')
            if wiki_ml:
                en_data = wiki_ml.get('en')
                if en_data and en_data.get('label'):
                    logger.info(f"[Wikipedia-Multilang] Entity '{context.entity_name}': English label = '{en_data.get('label')}'")
                else:
                    logger.warning(f"[Wikipedia-Multilang] Entity '{context.entity_name}': No English label found in wikipedia_multilang!")
                    
                    # Try to extract English data from langlinks if available
                    wiki_data = context.get_service_data("wikipedia") or {}
                    wiki_data = wiki_data.get("wikipedia", {})
                    if wiki_data.get("langlinks"):
                        for ll in wiki_data.get("langlinks", []):
                            if ll.get("lang") == "en" and ll.get("url"):
                                import urllib.parse
                                en_title = ll.get("url").split("/wiki/")[-1]
                                en_title = urllib.parse.unquote(en_title)
                                
                                # Create wikipedia_multilang if it does not exist
                                if not wiki_ml:
                                    wiki_ml = {}
                                    context.processing_data['wikipedia_multilang'] = wiki_ml
                                
                                # Set English data
                                wiki_ml["en"] = {
                                    'label': en_title.replace('_', ' '),
                                    'url': ll.get("url")
                                }
                                
                                logger.info(f"[Wikipedia-Multilang] Entity '{context.entity_name}': English label extracted from langlinks = '{en_title.replace('_', ' ')}'")
                                break
            else:
                logger.warning(f"[Wikipedia-Multilang] Entity '{context.entity_name}': No wikipedia_multilang data in context!")
                
                # Try to extract data from the entity
                wiki_data = context.get_service_data("wikipedia") or {}
                wiki_data = wiki_data.get("wikipedia", {})
                if wiki_data:
                    # Create multilang_entry
                    multilang_entry = {}
                    
                    # Extract URL and language
                    url = wiki_data.get("url")
                    if url and "wikipedia.org" in url:
                        lang_code = "de"  # Standard
                        if "/en.wikipedia.org/" in url:
                            lang_code = "en"
                        
                        # Extract title from URL
                        import urllib.parse
                        title = url.split("/wiki/")[-1]
                        title = urllib.parse.unquote(title)
                        
                        # Store data in multilang_entry
                        multilang_entry[lang_code] = {
                            'label': title.replace('_', ' '),
                            'url': url
                        }
                        
                        # If we have German data, try to get English data as well
                        if lang_code == "de" and wiki_data.get("langlinks"):
                            for ll in wiki_data.get("langlinks", []):
                                if ll.get("lang") == "en" and ll.get("url"):
                                    en_title = ll.get("url").split("/wiki/")[-1]
                                    en_title = urllib.parse.unquote(en_title)
                                    multilang_entry["en"] = {
                                        'label': en_title.replace('_', ' '),
                                        'url': ll.get("url")
                                    }
                                    break
                    
                    # Set multilingual data in context
                    if multilang_entry:
                        context.processing_data['wikipedia_multilang'] = multilang_entry
                        logger.info(f"[DEBUG] Manual creation of wikipedia_multilang for '{context.entity_name}': {multilang_entry}")
                        
                        en_label = multilang_entry.get('en', {}).get('label')
                        if en_label:
                            logger.info(f"[Wikipedia-Multilang] Entity '{context.entity_name}': English label manually set = '{en_label}'")
                        else:
                            logger.warning(f"[Wikipedia-Multilang] Entity '{context.entity_name}': No English label manually found!")
                    else:
                        logger.warning(f"[Wikipedia-Multilang] Entity '{context.entity_name}': No multilingual data manually available!")
                else:
                    logger.warning(f"[Wikipedia-Multilang] Entity '{context.entity_name}': No Wikipedia data found in context!")

    
    # 2. Wikidata: Use the dedicated WikidataService for context-based processing
    wikidata_needed_contexts = [c for c in contexts if config.get("USE_WIKIDATA", True) and not c.is_processed_by("wikidata")]
    if wikidata_needed_contexts:
        logger.info(f"Processing {len(wikidata_needed_contexts)} contexts with WikidataService for Wikidata linking.")
        # The global_wikidata_service is pre-configured and its process_entities method updates the contexts in-place.
        await global_wikidata_service.process_entities(wikidata_needed_contexts)
        
        # Optional: Log status after processing by WikidataService
        for context in wikidata_needed_contexts:
            if context.is_processed_by("wikidata") and context.output_data.get('sources', {}).get("wikidata"):
                logger.info(f"Context for '{context.entity_name}' successfully processed by WikidataService. Wikidata data found. Sources: {list(context.output_data.get('sources', {}).keys())}, Processed by: {context.processed_by_services}")
            elif context.is_processed_by("wikidata"):
                 logger.warning(f"Context for '{context.entity_name}' marked as processed by WikidataService, but no Wikidata data found in sources. Processed by: {context.processed_by_services}")
            # else: # Entity might not have been processed by WikidataService if e.g. no ID was found
            #    logger.debug(f"Context for '{context.entity_name}' was not processed for Wikidata by WikidataService (e.g., no ID found or service disabled for entity). Processed by: {context.processed_by_services}")
    
    # 3. DBpedia: Use the dedicated DBpediaService for context-based batch processing
    dbpedia_needed_contexts = [
        c for c in contexts 
        if config.get("USE_DBPEDIA", True) and not c.is_processed_by("dbpedia")
    ]
    if dbpedia_needed_contexts:
        logger.info(f"Processing {len(dbpedia_needed_contexts)} contexts with DBpediaService for DBpedia linking.")
        
        # Convert contexts to EntityData objects for the new DBpediaService
        from entityextractor.models.data_models import EntityData
        
        entity_data_list = []
        for ctx in dbpedia_needed_contexts:
            entity_data = EntityData(
                entity_id=str(id(ctx)),
                entity_name=ctx.entity_name,
                language=config.get("LANGUAGE", config.get("DEFAULT_LANGUAGE", "de"))
            )
            
            # Add Wikidata ID if available
            if hasattr(ctx, 'wikidata_id') and ctx.wikidata_id:
                entity_data.wikidata_id = ctx.wikidata_id
                
            # Store a reference to the original context
            setattr(entity_data, '_original_context_ref', ctx)
            entity_data_list.append(entity_data)
        
        # Create an instance of DBpediaService
        dbpedia_service = DBpediaService(config)
        
        try:
            # Process all entities with DBpediaService
            async with dbpedia_service:
                processed_entities = await dbpedia_service.process_batch(entity_data_list)
            
            # Transfer results back to the contexts
            for processed_entity in processed_entities:
                original_context = getattr(processed_entity, '_original_context_ref', None)
                if not original_context:
                    continue
                    
                # Mark the context as processed by DBpedia
                original_context.processed_by_services.add("dbpedia")
                    
                # If DBpedia data is available and successfully linked
                if processed_entity.dbpedia_data and processed_entity.dbpedia_data.status == "linked":
                    original_context.dbpedia_uri = processed_entity.dbpedia_data.uri
                    original_context.dbpedia_label = processed_entity.dbpedia_data.label
                    original_context.dbpedia_abstract = processed_entity.dbpedia_data.abstract
                    original_context.dbpedia_types = processed_entity.dbpedia_data.types
                    original_context.add_service_data('dbpedia', {
                        'data': {
                            'uri': processed_entity.dbpedia_data.uri,
                            'label': processed_entity.dbpedia_data.label,
                            'abstract': processed_entity.dbpedia_data.abstract,
                            'types': processed_entity.dbpedia_data.types or []
                        },
                        'status': 'linked'
                    })
                    logger.info(f"Context for '{original_context.entity_name}' successfully processed by DBpediaService. DBpedia data found.")
                else:
                    error_msg = processed_entity.dbpedia_data.error if processed_entity.dbpedia_data and processed_entity.dbpedia_data.error else "No DBpedia data found"
                    original_context.add_service_data('dbpedia', {
                        'error': error_msg,
                        'status': 'not_found'
                    })
                    logger.warning(f"Context for '{original_context.entity_name}' processed by DBpediaService, but no DBpedia data found. Error: {error_msg}")
        except Exception as e:
            logger.error(f"Error processing entities with DBpediaService: {str(e)}")
            # Mark all contexts as failed
            for ctx in dbpedia_needed_contexts:
                ctx.processed_by_services.add("dbpedia")
                ctx.add_service_data('dbpedia', {
                    'error': str(e),
                    'status': 'error'
                })

    # Log summary for all contexts after all services have run
    for context in contexts:
        context.log_summary(level=logging.INFO)
        
    return contexts


async def link_contexts(contexts: List[EntityProcessingContext], config: Optional[Dict[str, Any]] = None) -> List[EntityProcessingContext]:
    """
    Direct function for linking EntityProcessingContext objects.
    
    This is a helper function that calls _link_contexts directly, without type checks.
    
    Args:
        contexts: List of EntityProcessingContext objects
        config: Configuration dictionary
        
    Returns:
        List of linked EntityProcessingContext objects
    """
    return await _link_contexts(contexts, config)
