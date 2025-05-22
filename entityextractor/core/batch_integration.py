#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Integrationsmodul für Batch-Services

Dieses Modul stellt Verbindungsfunktionen bereit, die die neuen Batch-Services
mit der bestehenden API-Struktur verbinden und die Umstellung erleichtern.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple

from entityextractor.services.batch_wikipedia_service import batch_get_wikipedia_info
from entityextractor.services.batch_dbpedia_service import batch_get_dbpedia_info
from entityextractor.services.batch_wikidata_service import batch_get_wikidata_ids, batch_get_wikidata_entities
from entityextractor.utils.text_utils import is_valid_wikipedia_url
from entityextractor.config.settings import get_config, DEFAULT_CONFIG

def batch_link_entities(entities: List[Dict[str, Any]], config=None) -> List[Dict[str, Any]]:
    """
    Führt die Verlinkung von Entitäten in Batches durch, um die Anzahl der API-Anfragen zu minimieren.
    
    Args:
        entities: Liste von Entitäts-Dicts, die mindestens den Schlüssel "name" enthalten sollten
        config: Konfigurationswörterbuch
        
    Returns:
        Aktualisierte Liste von Entitäts-Dicts mit Verknüpfungsinformationen
    """
    if config is None:
        config = get_config()
    
    if not entities:
        return []
    
    # 1. Wikipedia-Informationen in einem Batch abrufen
    entity_names = [entity.get("name") for entity in entities if entity.get("name")]
    entity_name_map = {entity.get("name"): entity for entity in entities if entity.get("name")}
    
    wiki_results = batch_get_wikipedia_info(entity_names, lang=config.get("LANGUAGE", "de"), config=config)
    
    # 2. Entitäten mit Wikipedia-Informationen aktualisieren
    for entity_name, wiki_data in wiki_results.items():
        if entity_name not in entity_name_map:
            continue
            
        entity = entity_name_map[entity_name]
        
        if wiki_data.get("status") == "found":
            entity["wikipedia_url"] = wiki_data.get("url")
            entity["wikipedia_extract"] = wiki_data.get("extract", "")
            entity["wikipedia_categories"] = wiki_data.get("categories", [])
            
            # Wikidata-ID falls vorhanden
            if wiki_data.get("wikidata_id"):
                entity["wikidata_id"] = wiki_data.get("wikidata_id")
    
    # 3. Wikidata-IDs für Entitäten ohne ID in einem Batch abrufen
    entities_for_wikidata = {
        entity.get("name"): {"name": entity.get("name"), "wikipedia_url": entity.get("wikipedia_url")}
        for entity in entities
        if entity.get("name") and not entity.get("wikidata_id") and entity.get("wikipedia_url")
    }
    
    if entities_for_wikidata:
        wikidata_results = batch_get_wikidata_ids(entities_for_wikidata, config=config)
        
        # Entitäten mit Wikidata-IDs aktualisieren
        for entity_name, wikidata_data in wikidata_results.items():
            if entity_name not in entity_name_map:
                continue
                
            entity = entity_name_map[entity_name]
            
            if wikidata_data.get("status") == "found":
                entity["wikidata_id"] = wikidata_data.get("wikidata_id")
    
    # 4. Wikidata-Details in einem Batch abrufen
    entities_with_wikidata_ids = {
        entity.get("name"): entity.get("wikidata_id")
        for entity in entities
        if entity.get("name") and entity.get("wikidata_id")
    }
    
    if entities_with_wikidata_ids:
        wikidata_details = batch_get_wikidata_entities(entities_with_wikidata_ids, config=config)
        
        # Entitäten mit Wikidata-Details aktualisieren
        for entity_name, details in wikidata_details.items():
            if entity_name not in entity_name_map:
                continue
                
            entity = entity_name_map[entity_name]
            
            if details.get("status") == "found":
                lang = config.get("LANGUAGE", "de")
                entity["wikidata_description"] = details.get("descriptions", {}).get(lang) or details.get("descriptions", {}).get("en")
                entity["wikidata_label"] = details.get("labels", {}).get(lang) or details.get("labels", {}).get("en")
                entity["wikidata_types"] = details.get("types", [])
                entity["wikidata_part_of"] = details.get("part_of", [])
                entity["wikidata_has_parts"] = details.get("has_parts", [])
                entity["wikidata_image_url"] = details.get("image_url")
    
    # 5. DBpedia-Informationen in einem Batch abrufen
    entities_for_dbpedia = {
        entity.get("name"): entity.get("wikipedia_url")
        for entity in entities
        if entity.get("name") and entity.get("wikipedia_url") and is_valid_wikipedia_url(entity.get("wikipedia_url"))
    }
    
    if entities_for_dbpedia and config.get("USE_DBPEDIA", True):
        dbpedia_results = batch_get_dbpedia_info(entities_for_dbpedia, config=config)
        
        # Entitäten mit DBpedia-Informationen aktualisieren
        for entity_name, dbpedia_data in dbpedia_results.items():
            if entity_name not in entity_name_map:
                continue
                
            entity = entity_name_map[entity_name]
            
            if dbpedia_data.get("status") == "found":
                entity["dbpedia_resource_uri"] = dbpedia_data.get("resource_uri")
                entity["dbpedia_abstract"] = dbpedia_data.get("abstract", "")
                entity["dbpedia_types"] = dbpedia_data.get("types", [])
                entity["dbpedia_categories"] = dbpedia_data.get("categories", [])
                entity["dbpedia_subjects"] = dbpedia_data.get("subjects", [])
                entity["dbpedia_part_of"] = dbpedia_data.get("part_of", [])
                entity["dbpedia_has_parts"] = dbpedia_data.get("has_parts", [])
    
    return entities

def batch_process_chunk(text_chunk: str, config=None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Verarbeitet einen Text-Chunk mit optimierter Batch-Verarbeitung.
    
    Args:
        text_chunk: Ein Textfragment zur Verarbeitung
        config: Konfigurationswörterbuch
        
    Returns:
        Tuple aus (Entitäten, Beziehungen)
    """
    if config is None:
        config = get_config()
    
    mode = config.get("MODE", "extract")
    
    # Wähle Extraktionsmethode basierend auf Modus
    from entityextractor.core.extract_api import extract_entities
    from entityextractor.core.generate_api import generate_entities
    
    # Entitäten extrahieren oder generieren
    if mode == "generate":
        raw_entities = generate_entities(text_chunk, config)
    else:
        raw_entities = extract_entities(text_chunk, config)
    
    # Entitäten als Batch verlinken
    linked_entities = batch_link_entities(raw_entities, config)
    
    # Beziehungen ableiten falls gewünscht
    relationships = []
    if config.get("RELATION_EXTRACTION", False):
        from entityextractor.core.relationship_api import infer_entity_relationships
        relationships = infer_entity_relationships(text_chunk, linked_entities, config)
    
    return linked_entities, relationships
