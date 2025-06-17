#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
relationship_extraction.py

Funktionen zur Extraktion von Beziehungen zwischen Entitäten.
Unterstützt sowohl die traditionelle Dictionary-basierte als auch
die neue kontextbasierte Architektur.

Optimiert für Batch-Verarbeitung und präzise Named Entity Behandlung.
"""

import logging
import time
from typing import List, Dict, Any, Optional, Union, Set, Tuple

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.services.openai_service import extract_relationships_with_openai
from entityextractor.utils.id_utils import generate_relationship_id
from entityextractor.schemas.service_schemas import validate_relationship

logger = logging.getLogger(__name__)

async def extract_relationships_from_contexts(
    contexts: List[EntityProcessingContext], 
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Extrahiert Beziehungen zwischen den Entitäten in den gegebenen Kontexten.
    Optimiert für korrekte Named Entity Behandlung und Batch-Verarbeitung.
    
    Args:
        contexts: Liste von EntityProcessingContext-Objekten
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von Beziehungs-Dictionaries
    """
    if not contexts:
        return []
        
    start_time = time.time()
    config = config or get_config()
    
    logger.info(f"Extrahiere Beziehungen zwischen {len(contexts)} Entitäten")
    
    # Erstelle eine Liste der relevanten Entitätsdaten mit optimierten Informationen
    entity_data = []
    entity_name_to_id_map = {}  # Für schnelle Zuordnung von Namen zu IDs
    entity_id_to_name_map = {}  # Für schnelle Zuordnung von IDs zu Namen
    entity_id_to_type_map = {}  # Für schnelle Zuordnung von IDs zu Typen
    
    for context in contexts:
        # Grundlegende Entitätsinformationen
        entity_id = context.entity_id
        entity_name = context.entity_name
        entity_type = context.entity_type or ""
        
        # Maps aktualisieren für spätere Validierung und Referenzierung
        entity_name_to_id_map[entity_name] = entity_id
        entity_id_to_name_map[entity_id] = entity_name
        entity_id_to_type_map[entity_id] = entity_type
        
        # Detaillierte Entitätsinformationen für die Beziehungsextraktion
        entity_info = {
            "id": entity_id,
            "name": entity_name,  # Wichtig: Original-Groß-/Kleinschreibung beibehalten
            "type": entity_type,
        }
        
        # Wikipedia-Daten hinzufügen, wenn vorhanden
        service_data = context.get_service_data("wikipedia")
        if service_data and service_data.get("wikipedia_data"):
            wikipedia_data = service_data.get("wikipedia_data")
            
            # Verwende die neue Struktur mit dem Pydantic-Modell
            if wikipedia_data.extract:
                entity_info["abstract"] = wikipedia_data.extract
            
            if wikipedia_data.url:
                entity_info["url"] = wikipedia_data.url
            
            # Sprache aus Wikipedia-Daten übernehmen, wenn vorhanden
            if wikipedia_data.language:
                entity_info["language"] = wikipedia_data.language
                
        # Wikidata-Daten hinzufügen, wenn vorhanden
        service_data = context.get_service_data("wikidata")
        if service_data and service_data.get("wikidata_data"):
            wikidata_data = service_data.get("wikidata_data")
            
            # Verwende die neue Struktur mit dem Pydantic-Modell
            if wikidata_data.claims and "P31" in wikidata_data.claims:
                entity_info["wikidata_types"] = [prop.value for prop in wikidata_data.claims["P31"]]
            
            # Wikidata-ID hinzufügen, wenn vorhanden
            if wikidata_data.entity_id:
                entity_info["wikidata_id"] = wikidata_data.entity_id
                
        # DBpedia-Daten hinzufügen, wenn vorhanden (für zusätzliche Kategorisierung)
        service_data = context.get_service_data("dbpedia")
        if service_data and service_data.get("dbpedia_data") and "subjects" in service_data.get("dbpedia_data", {}):
            entity_info["dbpedia_subjects"] = service_data.get("dbpedia_data", {}).get("subjects", [])
            
        entity_data.append(entity_info)
    
    if not entity_data:
        logger.warning("Keine ausreichenden Entitätsdaten für Beziehungsextraktion vorhanden")
        return []
    
    # Extrahiere den Originaltext aus dem ersten Kontext, falls vorhanden
    original_text = ""
    if contexts and hasattr(contexts[0], 'original_text') and contexts[0].original_text:
        original_text = contexts[0].original_text
    
    # Konvertiere die Entitätsdaten in das Format, das von extract_relationships_with_openai erwartet wird
    openai_entity_format = []
    for entity in entity_data:
        openai_entity = {
            "entity": entity["name"],
            "details": {
                "typ": entity["type"] or "Unbekannt"
            }
        }
        openai_entity_format.append(openai_entity)
    
    # Verwende OpenAI, um Beziehungen zu extrahieren
    # Nutze hier das OpenAI-Service mit den speziellen Prompts für Named Entity Beziehungen
    from entityextractor.services.openai_service import extract_relationships_with_openai
    relationships = extract_relationships_with_openai(openai_entity_format, original_text, config)
    logger.info(f"Extrahiert: {len(relationships)} Rohbeziehungen")
    
    # Validiere und erweitere die extrahierten Beziehungen
    valid_relationships = []
    for rel in relationships:
        # Prüfe, ob Subjekt und Objekt aus der angegebenen Entitätsliste stammen
        subject = rel.get("subject")
        predicate = rel.get("predicate")
        object_ = rel.get("object")
        
        # Falls IDs als Namen angegeben wurden, konvertiere sie zu IDs
        if subject in entity_name_to_id_map:
            subject = entity_name_to_id_map[subject]
            rel["subject"] = subject
            
        if object_ in entity_name_to_id_map:
            object_ = entity_name_to_id_map[object_]
            rel["object"] = object_
        
        # Stellen sicher, dass wir nur Beziehungen zwischen bekannten Entitäten haben
        if subject not in entity_id_to_name_map or object_ not in entity_id_to_name_map:
            logger.debug(f"Ignoriere Beziehung mit unbekannten Entitäten: {rel}")
            continue
            
        # Füge wichtige Metadaten hinzu
        rel["id"] = generate_relationship_id()
        rel["subject_name"] = entity_id_to_name_map[subject]  # Original-Namensform beibehalten
        rel["object_name"] = entity_id_to_name_map[object_]  # Original-Namensform beibehalten
        rel["subject_type"] = entity_id_to_type_map.get(subject, "")
        rel["object_type"] = entity_id_to_type_map.get(object_, "")
        
        # Prädikat in Kleinbuchstaben (gemäß Konvention)
        rel["predicate"] = predicate.lower() if predicate else ""
        
        # Verwende Schema-Validierung, um die Struktur zu prüfen
        if validate_relationship(rel):
            valid_relationships.append(rel)
        else:
            logger.warning(f"Ungültige Beziehungsstruktur: {rel}")
    
    logger.info(f"Validiert: {len(valid_relationships)} gültige Beziehungen von {len(relationships)} extrahierten")
    
    # Aktualisiere die Kontexte mit den validierten Beziehungen
    for context in contexts:
        entity_id = context.entity_id
        context_relationships = []
        
        # Finde Beziehungen, in denen diese Entität vorkommt
        for rel in valid_relationships:
            if rel.get("subject") == entity_id or rel.get("object") == entity_id:
                context_relationships.append(rel)
        
        # Füge Beziehungen zum Kontext hinzu mit allen Metadaten
        for rel in context_relationships:
            subject_id = rel.get("subject")
            predicate = rel.get("predicate")
            object_id = rel.get("object")
            
            # Hole alle zusätzlichen Metadaten für die Beziehung
            metadata = {k: v for k, v in rel.items() 
                       if k not in ["subject", "predicate", "object", "subject_type", "object_type"]}
            
            # Füge die Beziehung mit allen Metadaten hinzu
            relationship_dict = {
                "subject": subject_id,
                "predicate": predicate,
                "object": object_id,
                **metadata
            }
            
            # Füge Typen hinzu, wenn vorhanden
            if "subject_type" in rel:
                relationship_dict["subject_type"] = rel.get("subject_type")
            if "object_type" in rel:
                relationship_dict["object_type"] = rel.get("object_type")
                
            context.add_relationship(relationship_dict)
    
    elapsed = time.time() - start_time
    logger.info(f"Beziehungsextraktion abgeschlossen in {elapsed:.2f}s")
    
    return valid_relationships

async def extract_relationships_from_entities(
    entities: List[Dict[str, Any]], 
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Legacy-Methode zur Extraktion von Beziehungen aus Dictionary-Entitäten.
    
    Args:
        entities: Liste von Dictionary-Entitäten
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von Beziehungs-Dictionaries
    """
    # Konvertiere Dictionary-Entitäten in Kontexte
    contexts = []
    for entity in entities:
        context = EntityProcessingContext(
            entity_name=entity.get("name", ""),
            entity_id=entity.get("id", ""),
            entity_type=entity.get("details", {}).get("type", ""),
        )
        
        # Füge vorhandene Daten hinzu
        if "sources" in entity and "wikipedia" in entity["sources"]:
            context.add_service_data("wikipedia", {
                "wikipedia": entity["sources"]["wikipedia"]
            })
        
        if "sources" in entity and "wikidata" in entity["sources"]:
            context.add_service_data("wikidata", {
                "wikidata": entity["sources"]["wikidata"]
            })
            
        if "sources" in entity and "dbpedia" in entity["sources"]:
            context.add_service_data("dbpedia", {
                "dbpedia_data": entity["sources"]["dbpedia"]
            })
            
        contexts.append(context)
    
    # Extrahiere Beziehungen aus den Kontexten
    return await extract_relationships_from_contexts(contexts, config)
