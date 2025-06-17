#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Integrationsmodul für Batch-Services

Dieses Modul stellt Verbindungsfunktionen bereit, die die neuen Batch-Services
mit der bestehenden API-Struktur verbinden und die Umstellung erleichtern.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple

# Importiere Hilfsfunktionen aus den modularen Services
from entityextractor.services.wikipedia import BatchWikipediaService
from entityextractor.services.dbpedia.service import DBpediaService
from entityextractor.core.context import EntityProcessingContext
from entityextractor.services.wikidata import get_batch_wikidata_service
from entityextractor.services.wikidata.search import get_wikidata_ids_for_entities
from entityextractor.services.wikidata.fetchers import get_wikidata_entities
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
    
    # Verwende den modularisierten Service
    import asyncio
    wiki_service = BatchWikipediaService(config)
    
    # Erstelle Entity-Objekte für den Batch-Service
    from entityextractor.models.entity import Entity
    from entityextractor.models.base import LanguageCode
    
    wiki_entities = []
    for name in entity_names:
        import uuid
        entity = Entity(id=str(uuid.uuid4()), name=name)
        entity.label.set(LanguageCode.DE if config.get("LANGUAGE", "de") == "de" else LanguageCode.EN, name)
        wiki_entities.append(entity)
    
    # Verarbeite Entitäten
    wiki_entities = asyncio.get_event_loop().run_until_complete(wiki_service.enrich_entities(wiki_entities))
    
    # Konvertiere in das alte Format für Kompatibilität
    wiki_results = {}
    for entity in wiki_entities:
        name = entity.label.get(LanguageCode.DE) or entity.label.get(LanguageCode.EN)
        if name and entity.get_source('wikipedia'):
            source = entity.get_source('wikipedia')
            wiki_results[name] = {
                'title': source.get('title', ''),
                'url': source.get('url', ''),
                'extract': source.get('extract', ''),
                'thumbnail': source.get('thumbnail', ''),
                'wikidata_id': source.get('wikidata_id', ''),
                'internal_links': source.get('internal_links', []),
                'image_info': source.get('image_info', None),
                'status': source.get('status', 'unknown')
            }
        elif name:
            wiki_results[name] = {
                'title': name,
                'status': 'not_found'
            }
    
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
        # Verwende den modularisierten Service
        wikidata_results = get_wikidata_ids_for_entities(entities_for_wikidata, config)
        
        # Entitäten mit Wikidata-IDs aktualisieren
        for entity_name, service_data in wikidata_results.items():
            if entity_name not in entity_name_map:
                continue
                
            entity = entity_name_map[entity_name]
            
            if service_data and service_data.get("wikidata_data") and service_data["wikidata_data"].entity_id:
                entity["wikidata_id"] = service_data["wikidata_data"].entity_id
    
    # 4. Wikidata-Details in einem Batch abrufen
    entities_with_wikidata_ids = {
        entity.get("name"): entity.get("wikidata_id")
        for entity in entities
        if entity.get("name") and entity.get("wikidata_id")
    }
    
    if entities_with_wikidata_ids:
        # Verwende den modularisierten Service
        wikidata_details = get_wikidata_entities(entities_with_wikidata_ids, config)
        
        # Entitäten mit Wikidata-Details aktualisieren
        for entity_name, service_data in wikidata_details.items():
            if entity_name not in entity_name_map:
                continue
                
            entity = entity_name_map[entity_name]
            
            if service_data and service_data.get("wikidata_data"):
                wikidata_data = service_data["wikidata_data"]
                lang = config.get("LANGUAGE", "de")
                
                # Verwende die neue Pydantic-Modellstruktur
                if wikidata_data.description:
                    entity["wikidata_description"] = wikidata_data.description.get(lang) or wikidata_data.description.get("en")
                
                if wikidata_data.label:
                    entity["wikidata_label"] = wikidata_data.label.get(lang) or wikidata_data.label.get("en")
                
                # Typen aus P31 (instance of) extrahieren
                if wikidata_data.claims and "P31" in wikidata_data.claims:
                    entity["wikidata_types"] = [prop.value for prop in wikidata_data.claims["P31"]]
                
                # Part-of-Beziehungen aus P361 (part of) extrahieren
                if wikidata_data.claims and "P361" in wikidata_data.claims:
                    entity["wikidata_part_of"] = [prop.value for prop in wikidata_data.claims["P361"]]
                
                # Has-parts-Beziehungen aus P527 (has part) extrahieren
                if wikidata_data.claims and "P527" in wikidata_data.claims:
                    entity["wikidata_has_parts"] = [prop.value for prop in wikidata_data.claims["P527"]]
                
                # Bild aus P18 (image) extrahieren
                if wikidata_data.claims and "P18" in wikidata_data.claims and wikidata_data.claims["P18"]:
                    entity["wikidata_image_url"] = wikidata_data.claims["P18"][0].value
    
    # 5. DBpedia-Informationen in einem Batch abrufen
    entities_for_dbpedia = {
        entity.get("name"): entity.get("wikipedia_url")
        for entity in entities
        if entity.get("name") and entity.get("wikipedia_url") and is_valid_wikipedia_url(entity.get("wikipedia_url"))
    }
    
    if entities_for_dbpedia and config.get("USE_DBPEDIA", True):
        # Verwende den neuen DBpediaService
        dbpedia_service = DBpediaService(config)
        
        # Erstelle EntityProcessingContext-Objekte für den Service
        dbpedia_contexts: List[EntityProcessingContext] = []
        # Make sure LanguageCode is imported if not already: from entityextractor.models.base import LanguageCode
        lang_val = config.get("LANGUAGE", "de") # Default to 'de' or get from config

        for name, url in entities_for_dbpedia.items():
            original_entity_dict = entity_name_map.get(name)
            if not original_entity_dict:
                continue # Should not happen if entities_for_dbpedia is derived correctly

            epc = EntityProcessingContext(
                entity_name=name,
                language=lang_val,
                wikipedia_url=url,
                wikipedia_language=lang_val # Assuming Wikipedia language is same as entity language
            )
            setattr(epc, '_original_entity_dict_ref', original_entity_dict) # Store ref to original dict
            
            # If wikidata_id was populated by previous steps, add it to context
            if original_entity_dict.get("wikidata_id"):
                epc.wikidata_id = original_entity_dict["wikidata_id"]

            dbpedia_contexts.append(epc)
        
        # Verarbeite Kontexte
        if dbpedia_contexts:
            asyncio.get_event_loop().run_until_complete(dbpedia_service.process_entities(dbpedia_contexts))
        
            # Entitäten mit DBpedia-Informationen aktualisieren
            for context in dbpedia_contexts:
                original_dict = getattr(context, '_original_entity_dict_ref', None)
                if not original_dict:
                    continue

                dbpedia_service_output = context.get_service_data('dbpedia')
                if dbpedia_service_output and dbpedia_service_output.get('dbpedia_data'):
                    dbpedia_data = dbpedia_service_output['dbpedia_data']
                    if dbpedia_data.status == "linked":
                        original_dict["dbpedia_resource_uri"] = dbpedia_data.uri
                        original_dict["dbpedia_label"] = dbpedia_data.label.get("en") if dbpedia_data.label else None
                        original_dict["dbpedia_abstract"] = dbpedia_data.abstract.get("en", "") if dbpedia_data.abstract else ""
                        original_dict["dbpedia_types"] = dbpedia_data.types if dbpedia_data.types else []
                        original_dict["dbpedia_categories"] = dbpedia_data.categories if dbpedia_data.categories else []
                        # Subjects sind jetzt in categories enthalten
                        original_dict["dbpedia_subjects"] = dbpedia_data.categories if dbpedia_data.categories else []
                        # Beziehungen
                        original_dict["dbpedia_part_of"] = dbpedia_data.part_of if dbpedia_data.part_of else []
                        original_dict["dbpedia_has_parts"] = dbpedia_data.has_parts if dbpedia_data.has_parts else []
                    else:
                        original_dict["dbpedia_status"] = dbpedia_data.status
                        if dbpedia_data.error:
                            original_dict["dbpedia_error"] = dbpedia_data.error
    
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
