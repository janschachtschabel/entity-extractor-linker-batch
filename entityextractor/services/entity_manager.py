#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Entity Manager with Pydantic models and Loguru.

This manager coordinates the various entity services and manages the data flow between them.
It uses Pydantic for data validation and Loguru for improved logging.
"""

from typing import Dict, List, Any, Optional, Tuple
import asyncio
from loguru import logger
import aiohttp

from entityextractor.config.settings import get_config
from entityextractor.models.data_models import EntityData
from entityextractor.services.wikipedia.new_service import WikipediaService
from entityextractor.services.dbpedia.new_service import DBpediaService
from entityextractor.services.wikidata.new_service import WikidataService


class EntityManager:
    """Manager for processing and managing entities with Pydantic models."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initializes the EntityManager.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.debug_mode = self.config.get("DEBUG", False)
        
        # Initialize Wikipedia service immediately (used in most pipelines)
        self.wikipedia_service = WikipediaService(self.config)
        # Lazily instantiated services, created only on first use
        self.dbpedia_service = None
        self.wikidata_service = None
        
        logger.info("EntityManager initialized")
    
    async def process_entities(self, entities: List[EntityData]) -> List[EntityData]:
        """
        Processes a list of entities through all services.
        
        Args:
            entities: List of EntityData objects
            
        Returns:
            List of processed EntityData objects
        """
        if not entities:
            return []
        
        logger.info(f"Starting processing for {len(entities)} entities")
        
        # 1. Enrich with Wikipedia
        if self.config.get("USE_WIKIPEDIA", True):
            logger.info("Enrichment with Wikipedia data")
            entities = await self.wikipedia_service.process_batch(entities)
        
        # 2. Enrich with DBpedia
        if self.config.get("USE_DBPEDIA", True):
            logger.info("Enrichment with DBpedia data")
            if self.dbpedia_service is None:
                self.dbpedia_service = DBpediaService(self.config)
            entities = await self.dbpedia_service.process_batch(entities)
        
        # 3. Enrich with Wikidata
        if self.config.get("USE_WIKIDATA", True):
            logger.info("Enrichment with Wikidata data")
            if self.wikidata_service is None:
                self.wikidata_service = WikidataService(self.config)
            entities = await self.wikidata_service.process_batch(entities)
        
        # Output statistics
        with_wikipedia = sum(1 for e in entities if e.wikipedia_url)
        with_multilang = sum(1 for e in entities if e.wikipedia_multilang)
        with_en_label = sum(1 for e in entities if e.wikipedia_multilang and e.wikipedia_multilang.en)
        with_dbpedia = sum(1 for e in entities if e.dbpedia_data and e.dbpedia_data.status == "linked")
        with_wikidata = sum(1 for e in entities if e.wikidata_id)
        with_wikidata_data = sum(1 for e in entities if e.wikidata_data)
        
        logger.info(f"Processing completed:")
        logger.info(f"  - Mit Wikipedia-URL: {with_wikipedia}/{len(entities)}")
        logger.info(f"  - Mit mehrsprachigen Daten: {with_multilang}/{len(entities)}")
        logger.info(f"  - Mit englischem Label: {with_en_label}/{len(entities)}")
        logger.info(f"  - Mit DBpedia-Daten: {with_dbpedia}/{len(entities)}")
        logger.info(f"  - Mit Wikidata-ID: {with_wikidata}/{len(entities)}")
        logger.info(f"  - Mit vollständigen Wikidata-Daten: {with_wikidata_data}/{len(entities)}")
        
        return entities
    
    async def close(self):
        """Closes all services."""
        await self.wikipedia_service.close_session()
        if self.dbpedia_service:
            await self.dbpedia_service.close_session()
        if self.wikidata_service:
            await self.wikidata_service.close_session()
        logger.info("All services closed")


# Convenience function for easier access
async def process_entities(entities: List[EntityData], config: Optional[Dict[str, Any]] = None) -> List[EntityData]:
    """
    Processes a list of entities with the EntityManager.
    
    Args:
        entities: List of EntityData objects
        config: Optional configuration dictionary
        
    Returns:
        List of processed EntityData objects
    """
    manager = EntityManager(config)
    try:
        result = await manager.process_entities(entities)
        return result
    finally:
        await manager.close()
        logger.info("EntityManager closed")


# Test Pipeline
async def _test_pipeline():
    """
    Test pipeline for the EntityManager.
    
    This function tests the EntityManager with some example entities.
    """
    from entityextractor.config.settings import get_config
    import sys
    
    # Konfiguration laden
    config = get_config()
    config['DEBUG'] = True
    config['USE_WIKIDATA'] = True  # WikidataService explizit aktivieren
    
    # Loguru konfigurieren
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    
    # Beispiel-Entitäten erstellen
    entities = [
        EntityData(
            entity_id="1",
            entity_name="Albert Einstein",
            entity_type="PERSON"
        ),
        EntityData(
            entity_id="2",
            entity_name="Relativitätstheorie",
            entity_type="CONCEPT"
        ),
        EntityData(
            entity_id="3",
            entity_name="Raum",
            entity_type="CONCEPT"
        ),
        EntityData(
            entity_id="4",
            entity_name="Zeit",
            entity_type="CONCEPT"
        ),
        EntityData(
            entity_id="5",
            entity_name="Gravitation",
            entity_type="CONCEPT"
        )
    ]
    
    # Entitäten verarbeiten
    processed_entities = await process_entities(entities, config)
    
    # Ergebnisse ausgeben
    for entity in processed_entities:
        print(f"\n--- {entity.entity_name} ---")
        
        # Wikipedia-Daten
        if entity.wikipedia_url:
            print(f"Wikipedia-URL: {entity.wikipedia_url}")
        
        # Mehrsprachige Daten
        if entity.wikipedia_multilang:
            langs = []
            for lang in ['de', 'en', 'fr', 'es', 'it']:
                lang_data = getattr(entity.wikipedia_multilang, lang)
                if lang_data:
                    langs.append(f"{lang}: {lang_data.label}")
            if langs:
                print(f"Mehrsprachige Labels: {', '.join(langs)}")
        
        # DBpedia-Daten
        if entity.dbpedia_data:
            print(f"DBpedia-Status: {entity.dbpedia_data.status}")
            if entity.dbpedia_data.uri:
                print(f"DBpedia-URI: {entity.dbpedia_data.uri}")
            if entity.dbpedia_data.abstract and "en" in entity.dbpedia_data.abstract:
                abstract = entity.dbpedia_data.abstract["en"]
                if len(abstract) > 100:
                    abstract = abstract[:100] + "..."
                print(f"DBpedia-Abstract: {abstract}")
        
        # Wikidata-Daten
        if entity.wikidata_id:
            print(f"Wikidata-ID: {entity.wikidata_id}")
            if entity.wikidata_data:
                if entity.wikidata_data.label:
                    labels = []
                    for lang, label in entity.wikidata_data.label.items():
                        labels.append(f"{lang}: {label}")
                    print(f"Wikidata-Labels: {', '.join(labels)}")
                
                if entity.wikidata_data.description:
                    desc = entity.wikidata_data.description.get("de", "") or entity.wikidata_data.description.get("en", "")
                    if desc and len(desc) > 100:
                        desc = desc[:100] + "..."
                    if desc:
                        print(f"Wikidata-Beschreibung: {desc}")


if __name__ == "__main__":
    asyncio.run(_test_pipeline())
