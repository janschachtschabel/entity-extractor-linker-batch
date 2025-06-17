#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Service API

Provides a unified API for the Entity Extractor Services.
This file serves as the central interface for the application and uses
the new context-based architecture.
"""

from loguru import logger
import asyncio
import time
from typing import Dict, List, Any, Optional, Tuple, Union

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.core.process.orchestrator import process_entity, process_entities, process_single_pass
from entityextractor.core.process.result_formatter import (
    format_context_to_result,
    format_contexts_to_result,
    format_results
)
from entityextractor.services.wikipedia.service import wikipedia_service
from entityextractor.services.wikidata.new_service import wikidata_service
from entityextractor.services.dbpedia.service import DBpediaService
# Removed the old import for process_with_dbpedia, as we now work directly with the new service

# Logger is already configured via loguru import

class EntityProcessingException(Exception):
    """Exception for errors during entity processing."""
    pass

async def link_entity(entity_name: str, entity_type: Optional[str] = None, 
                      config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Links a single entity with external knowledge systems (Wikipedia, Wikidata, DBpedia).
    
    Args:
        entity_name: Name of the entity to be linked
        entity_type: Optional type of the entity
        config: Optional configuration (overrides global settings)
        
    Returns:
        The linked entity in standardized format
    """
    logger.info(f"Linking entity: {entity_name}")
    start_time = time.time()
    
    try:
        # Konfiguration initialisieren
        if config is None:
            config = get_config()
        
        # Kontext erstellen
        context = EntityProcessingContext(entity_name, config)
        if entity_type:
            context.entity_type = entity_type
        
        # Execute services sequentially
        # 1. Wikipedia service
        await wikipedia_service.process_entity(context)
        
        # 2. Wikidata service (uses Wikipedia data if available)
        await wikidata_service.process_entity(context)
        
        # 3. DBpedia service (uses the new asynchronous service)
        from entityextractor.models.data_models import EntityData
        
        dbpedia_service = DBpediaService(config)
        try:
            async with dbpedia_service:
                # Create an EntityData object from the context
                entity_data = EntityData(
                    entity_id=str(id(context)),
                    entity_name=context.entity_name,
                    language=context.language or config.get("DEFAULT_LANGUAGE", "de")
                )
                
                # Add Wikidata ID if available
                if hasattr(context, 'wikidata_id') and context.wikidata_id:
                    entity_data.wikidata_id = context.wikidata_id
                
                # Process the entity with the DBpediaService
                processed_entity = await dbpedia_service.process_entity(entity_data)
                
                # Transfer the results back to the context
                if processed_entity.dbpedia_data and processed_entity.dbpedia_data.status == "linked":
                    context.dbpedia_uri = processed_entity.dbpedia_data.uri
                    context.dbpedia_label = processed_entity.dbpedia_data.label
                    context.dbpedia_abstract = processed_entity.dbpedia_data.abstract
                    context.dbpedia_types = processed_entity.dbpedia_data.types
                    context.add_service_data('dbpedia', {
                        'data': {
                            'uri': processed_entity.dbpedia_data.uri,
                            'label': processed_entity.dbpedia_data.label,
                            'abstract': processed_entity.dbpedia_data.abstract,
                            'types': processed_entity.dbpedia_data.types or []
                        },
                        'status': 'linked'
                    })
                else:
                    error_msg = processed_entity.dbpedia.error if processed_entity.dbpedia and processed_entity.dbpedia.error else "No DBpedia data found"
                    context.add_service_data('dbpedia', {
                        'error': error_msg,
                        'status': 'not_found'
                    })
        finally:
            await dbpedia_service.close()
        
        # Verarbeitungszeit speichern
        context.processing_time = time.time() - start_time
        
        # Ergebnis formatieren
        result = format_entity_from_context(context)
        return result
    except Exception as e:
        logger.error(f"Error linking entity '{entity_name}': {str(e)}")
        raise EntityProcessingException(f"Linking error: {str(e)}")

async def link_entities(entities: List[Dict[str, Any]], 
                       config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Links multiple entities in parallel with external knowledge systems.
    
    Args:
        entities: List of entities to link (Format: [{"name": "...", "type": "..."}, ...])
        config: Optional configuration (overrides global settings)
        
    Returns:
        List of linked entities in standardized format
    """
    entity_count = len(entities)
    logger.info(f"Linking {entity_count} entities")
    
    try:
        # Konfiguration initialisieren
        if config is None:
            config = get_config()
        
        # Create contexts for all entities
        contexts = []
        for entity in entities:
            name = entity.get("name") or entity.get("entity")
            if not name:
                logger.warning(f"Skipping entity without name: {entity}")
                continue
                
            context = EntityProcessingContext(name, config)
            entity_type = entity.get("type") or entity.get("typ")
            if entity_type:
                context.entity_type = entity_type
                
            contexts.append(context)
        
        # Verarbeite alle Kontexte parallel
        tasks = []
        for ctx in contexts:
            # 1. Wikipedia-Service
            tasks.append(wikipedia_service.process_entity(ctx))
            
        # Warte auf Abschluss aller Wikipedia-Aufgaben
        await asyncio.gather(*tasks)
        
        # Jetzt Wikidata und DBpedia sequentiell
        for ctx in contexts:
            # 2. Wikidata-Service
            await wikidata_service.process_entity(ctx)
            # 3. DBpedia-Service
            await process_with_dbpedia(ctx)
        
        # Format results
        result = format_contexts_to_result(contexts)
        return result["entities"]
    except Exception as e:
        logger.error(f"Error linking {entity_count} entities: {str(e)}")
        raise EntityProcessingException(f"Linking error: {str(e)}")

def link_entities_sync(entities: List[Dict[str, Any]], 
                     config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Synchronous version of entity linking for easier integration.
    
    Args:
        entities: List of entities to link (Format: [{"name": "...", "type": "..."}, ...])
        config: Optional configuration (overrides global settings)
        
    Returns:
        List of linked entities in standardized format
    """
    return asyncio.run(link_entities(entities, config=config))

def process_text_with_entities(text: str, entities: List[Dict[str, Any]], 
                              config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Processes a text with pre-extracted entities.
    
    Args:
        text: The source text
        entities: Pre-extracted entities (Format: [{"name": "...", "type": "..."}, ...])
        config: Optional configuration (overrides global settings)
        
    Returns:
        A result object with processed entities and optional relationships
    """
    logger.info(f"Processing text ({len(text)} characters) with {len(entities)} entities")
    
    try:
        # Link the entities asynchronously
        linked_entities = link_entities_sync(entities, config)
        
        # Create the result object
        result = {
            "entities": linked_entities,
            "relationships": [],  # Relationships could be added here
            "original_text": text,
            "meta": {
                "entity_count": len(linked_entities),
                "text_length": len(text),
                "processing_time": 0.0  # Wird später aktualisiert
            }
        }
        
        return result
    except Exception as e:
        logger.error(f"Error processing the text: {str(e)}")
        raise EntityProcessingException(f"Text processing error: {str(e)}")

class UnifiedService:
    """
    Unified service for entity processing.
    Provides methods for linking and processing entities.
    
    Supports both the traditional dictionary-based API and
    the new context-based architecture.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initializes the UnifiedService.
        
        Args:
            config: Optional configuration (uses default configuration if not specified)
        """
        self.config = config or get_config()
        self.logger = logger
        
        # Initialize services
        self.wikipedia_service = wikipedia_service
        self.wikidata_service = wikidata_service
        self.dbpedia_service = DBpediaService(self.config)
        
        logger.info("UnifiedService initialized with context-based architecture")
    
    async def link_entity(self, entity_name: str, entity_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Links a single entity with external knowledge systems.
        
        Args:
            entity_name: Name of the entity to link
            entity_type: Optional type of the entity
            
        Returns:
            The linked entity in standardized format
        """
        return await link_entity(entity_name, entity_type, self.config)
    
    async def link_entities(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Links multiple entities with external knowledge systems.
        
        Args:
            entities: List of entities to link
            
        Returns:
            List of linked entities in standardized format
        """
        return await link_entities(entities, self.config)
        
    async def link_entity_context(self, context: EntityProcessingContext) -> EntityProcessingContext:
        """
        Links an EntityProcessingContext with external knowledge systems.
        
        This method supports the new context-based architecture.
        
        Args:
            context: The context with the entity to link
            
        Returns:
            The updated context with data from external knowledge systems
        """
        logger.info(f"Linking entity (context): {context.entity_name}")
        start_time = time.time()
        
        try:
            # 1. Wikipedia-Service
            if self.config.get("USE_WIKIPEDIA", True) and not context.is_processed_by("wikipedia"):
                await self.wikipedia_service.process_entity(context)
            
            # 2. Wikidata-Service
            if self.config.get("USE_WIKIDATA", True) and not context.is_processed_by("wikidata"):
                await self.wikidata_service.process_entity(context)
            
            # 3. DBpedia-Service
            if self.config.get("USE_DBPEDIA", True) and not context.is_processed_by("dbpedia"):
                await process_with_dbpedia(context)
            
            # Save processing time
            processing_time = time.time() - start_time
            context.set_processing_info("processing_time", processing_time)
            
            # Log summary
            context.log_summary()
            
            return context
        except Exception as e:
            logger.error(f"Error linking context '{context.entity_name}': {str(e)}")
            raise EntityProcessingException(f"Context linking error: {str(e)}")
    
    async def link_entity_contexts(self, contexts: List[EntityProcessingContext]) -> List[EntityProcessingContext]:
        """
        Links multiple EntityProcessingContext objects with external knowledge systems.
        
        This method supports the new context-based architecture and
        processes all contexts in parallel.
        
        Args:
            contexts: List of contexts with entities to link
            
        Returns:
            List of updated contexts with data from external knowledge systems
        """
        from entityextractor.core.api.entity_linker.main import link_contexts
        
        context_count = len(contexts)
        logger.info(f"Linking {context_count} entities (context-based)")
        
        try:
            # Use the specialized function from the entity_linker
            updated_contexts = await link_contexts(contexts, self.config)
            
            return updated_contexts
        except Exception as e:
            logger.error(f"Error linking {context_count} contexts: {str(e)}")
            raise EntityProcessingException(f"Batch context linking error: {str(e)}")
    
    def link_entities_sync(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Synchronous version of entity linking.
        
        Args:
            entities: List of entities to link
            
        Returns:
            List of linked entities in standardized format
        """
        return link_entities_sync(entities, self.config)
    
    def process_text_with_entities(self, text: str, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Processes a text with pre-extracted entities.
        
        Args:
            text: The source text
            entities: Pre-extracted entities
            
        Returns:
            A result object with processed entities and optional relationships
        """
        return process_text_with_entities(text, entities, self.config)
        
    async def process_with_contexts(self, text: str, contexts: List[EntityProcessingContext]) -> Dict[str, Any]:
        """
        Processes a text with existing EntityProcessingContext objects.
        
        This method supports the new context-based architecture.
        
        Args:
            text: The source text
            contexts: List of contexts with entities
            
        Returns:
            A result object with processed entities and optional relationships
        """
        logger.info(f"Processing text with {len(contexts)} contexts")
        
        try:
            # Add original text to contexts
            for context in contexts:
                if not context.original_text:
                    context.original_text = text
            
            # Link entities
            linked_contexts = await self.link_entity_contexts(contexts)
            
            # Format result
            result = format_contexts_to_result(linked_contexts, text)
            
            return result
        except Exception as e:
            logger.error(f"Error in context-based processing: {str(e)}")
            raise EntityProcessingException(f"Context processing error: {str(e)}")
    
    def get_service_statistics(self) -> Dict[str, Any]:
        """
        Returns statistics about entity processing.
        
        Returns:
            Statistics dictionary with information from all services
        """
        return {
            "wikipedia": self.wikipedia_service.get_statistics(),
            "wikidata": self.wikidata_service.get_statistics(),
            "dbpedia": self.dbpedia_service.get_statistics()
        }
        
    def create_context(self, entity_name: str, entity_type: Optional[str] = None) -> EntityProcessingContext:
        """
        Creates a new EntityProcessingContext with the settings of this service.
        
        Args:
            entity_name: Name of the entity
            entity_type: Optional type of the entity
            
        Returns:
            A new EntityProcessingContext
        """
        context = EntityProcessingContext(entity_name, entity_id=None, entity_type=entity_type)
        return context
        
    def create_contexts_from_entities(self, entities: List[Dict[str, Any]], original_text: Optional[str] = None) -> List[EntityProcessingContext]:
        """
        Creates EntityProcessingContext objects from a list of dictionary entities.
        
        This method facilitates migration from the dictionary API to the context-based API.
        
        Args:
            entities: List of dictionary entities
            original_text: Optional original text
            
        Returns:
            List of EntityProcessingContext objects
        """
        contexts = []
        
        for entity in entities:
            name = entity.get("name") or entity.get("entity")
            if not name:
                logger.warning(f"Skipping entity without name: {entity}")
                continue
                
            entity_type = entity.get("type") or entity.get("typ")
            
            # Create context
            context = self.create_context(name, entity_type)
            
            # Set original text if available
            if original_text:
                context.original_text = original_text
                
            # Add citation information if available
            if "citation" in entity:
                context.set_citation(entity["citation"])
                
            # Add type and other details
            if "details" in entity and isinstance(entity["details"], dict):
                for key, value in entity["details"].items():
                    if key not in ["typ", "type"]:
                        context.add_additional_data(key, value)
            
            contexts.append(context)
            
        return contexts


# Helper function to convert Context to Entity
def format_entity_from_context(context: EntityProcessingContext) -> Dict[str, Any]:
    """
    Formats an EntityProcessingContext into an entity in standardized format.
    
    Args:
        context: The context with all service data
        
    Returns:
        The formatted entity
    """
    result = format_context_to_result(context)
    if result["entities"]:
        return result["entities"][0]
    
    # Fallback: Create minimal entity
    return {
        "entity": context.entity_name,
        "details": {"typ": context.entity_type or ""},
        "sources": {}
    }


# Global instance for easy access
unified_service = UnifiedService()
