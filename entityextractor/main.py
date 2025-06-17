#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main script for Entity Extraction with Pydantic models and Loguru.

This script demonstrates the use of new services with Pydantic models
and Loguru logging for Entity Extraction.
"""

import asyncio
import sys
from typing import List, Dict, Any, Optional
from loguru import logger

from entityextractor.config.settings import get_config
from entityextractor.utils.logging_config import configure_logging
from entityextractor.models.data_models import EntityData
from entityextractor.services.entity_manager import process_entities
from entityextractor.services.wikipedia.new_service import WikipediaService
from entityextractor.services.dbpedia.new_service import DBpediaService
from entityextractor.services.wikidata.new_service import WikidataService


async def main():
    """
    Main function for the Entity Extractor.
    
    This function demonstrates the use of new services with example entities.
    """
    # Load configuration
    config = get_config()
    config['DEBUG'] = True
    
    # Configure logging
    configure_logging(config)
    
    # Create example entities
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
    
    logger.info(f"Starting processing for {len(entities)} entities")
    
    # Process entities
    processed_entities = await process_entities(entities, config)
    
    # Output results
    for entity in processed_entities:
        logger.info(f"\n--- {entity.entity_name} ---")
        
        # Wikipedia data
        if entity.wikipedia_url:
            logger.info(f"Wikipedia URL: {entity.wikipedia_url}")
        
        # Multilingual data
        if entity.wikipedia_multilang:
            langs = []
            for lang in ['de', 'en', 'fr', 'es', 'it']:
                lang_data = getattr(entity.wikipedia_multilang, lang)
                if lang_data:
                    langs.append(f"{lang}: {lang_data.label}")
            if langs:
                logger.info(f"Multilingual labels: {', '.join(langs)}")
        
        # DBpedia data
        if entity.dbpedia_data:
            logger.info(f"DBpedia status: {entity.dbpedia_data.status}")
            if entity.dbpedia_data.uri:
                logger.info(f"DBpedia URI: {entity.dbpedia_data.uri}")
            if entity.dbpedia_data.abstract and "en" in entity.dbpedia_data.abstract:
                abstract = entity.dbpedia_data.abstract["en"]
                if len(abstract) > 100:
                    abstract = abstract[:100] + "..."
                logger.info(f"DBpedia abstract: {abstract}")
        
        # Wikidata data
        if entity.wikidata_id:
            logger.info(f"Wikidata ID: {entity.wikidata_id}")
            if entity.wikidata_data:
                if entity.wikidata_data.label:
                    labels = []
                    for lang, label in entity.wikidata_data.label.items():
                        labels.append(f"{lang}: {label}")
                    logger.info(f"Wikidata labels: {', '.join(labels)}")
                
                if entity.wikidata_data.description:
                    desc = entity.wikidata_data.description.get("de", "") or entity.wikidata_data.description.get("en", "")
                    if desc and len(desc) > 100:
                        desc = desc[:100] + "..."
                    if desc:
                        logger.info(f"Wikidata description: {desc}")
    
    # Output statistics
    with_wikipedia = sum(1 for e in processed_entities if e.wikipedia_url)
    with_multilang = sum(1 for e in processed_entities if e.wikipedia_multilang)
    with_en_label = sum(1 for e in processed_entities if e.wikipedia_multilang and e.wikipedia_multilang.en)
    with_dbpedia = sum(1 for e in processed_entities if e.dbpedia_data and e.dbpedia_data.status == "linked")
    with_wikidata = sum(1 for e in processed_entities if e.wikidata_id)
    with_wikidata_data = sum(1 for e in processed_entities if e.wikidata_data)
    
    logger.info(f"Processing completed:")
    logger.info(f"  - With Wikipedia URL: {with_wikipedia}/{len(processed_entities)}")
    logger.info(f"  - With multilingual data: {with_multilang}/{len(processed_entities)}")
    logger.info(f"  - With English label: {with_en_label}/{len(processed_entities)}")
    logger.info(f"  - With DBpedia data: {with_dbpedia}/{len(processed_entities)}")
    logger.info(f"  - With Wikidata ID: {with_wikidata}/{len(processed_entities)}")
    logger.info(f"  - With complete Wikidata data: {with_wikidata_data}/{len(processed_entities)}")


if __name__ == "__main__":
    asyncio.run(main())
