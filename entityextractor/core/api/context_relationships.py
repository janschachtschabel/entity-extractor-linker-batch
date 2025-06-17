#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
context_relationships.py

API-Modul für die Beziehungsextraktion und -inferenz zwischen Entitäten
mit Unterstützung für die kontextbasierte Architektur.

Diese Implementierung nutzt EntityProcessingContext-Objekte und arbeitet
mit Entity-IDs statt Namen für bessere Konsistenz und Performance.
"""

import logging
import time
import asyncio
from typing import List, Dict, Any, Optional, Union, Set, Tuple

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.utils.openai_utils import call_openai_api
from entityextractor.utils.id_utils import generate_relationship_id
from entityextractor.schemas.service_schemas import validate_relationship
from entityextractor.utils.batch_processing import process_relationships_in_batches

logger = logging.getLogger(__name__)


async def infer_relationships_from_contexts(
    contexts: List[EntityProcessingContext],
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Inferiert Beziehungen zwischen Entitäten in EntityProcessingContext-Objekten.
    
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
    
    # Beziehungen nur inferieren, wenn aktiviert
    if not config.get("RELATION_EXTRACTION", True):
        logger.info("Entity Relationship Inference deaktiviert.")
        return []
    
    logger.info(f"Inferiere Beziehungen zwischen {len(contexts)} Kontexten")
    
    # Erstelle ID-basierte Maps für effiziente Verarbeitung
    id_to_name = {}
    id_to_type = {}
    id_to_inferred = {}
    id_to_description = {}
    
    # Erstelle eine Liste der relevanten Entitätsdaten
    entity_data = []
    
    # Filtere Kontexte nach Typen, falls konfiguriert
    allowed_types = config.get("ALLOWED_ENTITY_TYPES", "auto")
    filtered_contexts = contexts
    
    if allowed_types != "auto" and isinstance(allowed_types, list):
        filtered_contexts = [
            ctx for ctx in contexts
            if ctx.entity_type in allowed_types
        ]
        logger.info(f"Auf {len(filtered_contexts)} Kontexte gefiltert basierend auf erlaubten Typen")
    
    # Extrahiere relevante Daten aus den Kontexten
    for context in filtered_contexts:
        entity_id = context.entity_id
        entity_name = context.entity_name
        entity_type = context.entity_type or ""
        
        # Maps aktualisieren für spätere Referenzierung
        id_to_name[entity_id] = entity_name
        id_to_type[entity_id] = entity_type
        
        # Inferenz-Status aus Output-Daten holen
        output_data = context.get_output()
        inferred_status = output_data.get("details", {}).get("inferred", "explicit")
        id_to_inferred[entity_id] = inferred_status
        
        # Beschreibung extrahieren (bevorzugt aus Wikipedia)
        description = ""
        wikipedia_data = context.get_service_data("wikipedia").get("wikipedia", {})
        if "extract" in wikipedia_data:
            description = wikipedia_data.get("extract", "")[:300]  # Erste 300 Zeichen für Effizienz
        
        # Alternativ aus DBpedia
        if not description:
            dbpedia_data = context.get_service_data("dbpedia").get("dbpedia", {})
            if "abstract" in dbpedia_data:
                description = dbpedia_data.get("abstract", "")[:300]
        
        id_to_description[entity_id] = description
        
        # Entitätsinformationen für die Beziehungsinferenz
        entity_info = {
            "id": entity_id,
            "name": entity_name,
            "type": entity_type,
            "inferred": inferred_status,
            "description": description
        }
        
        entity_data.append(entity_info)
    
    # Erstelle Entity-Info und Entity-Liste für den Prompt
    entity_info_text = ""
    entity_list = ""
    
    for entity in entity_data:
        # Detaillierte Beschreibung für den Entity-Info-Text
        entity_name = entity["name"]
        entity_type = entity["type"]
        description = entity["description"]
        
        entity_info_text += f"{entity_name} ({entity_type}): {description}\n\n"
        
        # Einfache Liste der Entitätsnamen
        entity_list += f"- {entity_name}\n"
    
    # Stelle sicher, dass genügend Entitäten vorhanden sind
    if len(entity_data) < 2:
        logger.warning("Weniger als 2 Entitäten für Beziehungsinferenz vorhanden, überspringe")
        return []
    
    # Inferiere Beziehungen mit angepasstem Prompt für Entity-IDs
    language = config.get("LANGUAGE", "de")
    temperature = config.get("TEMPERATURE_RELATIONSHIPS", 0.5)
    model = config.get("MODEL_RELATIONSHIPS", config.get("MODEL", "gpt-4.1-mini"))
    
    if language == "de":
        system_prompt = """Du bist ein Assistent für das Erstellen von Wissensgraphen. Deine Aufgabe ist es, IMPLIZITE Beziehungen zwischen Entitäten zu identifizieren, basierend auf allgemeinem Wissen und Kontext.

Wichtige Regeln:
1. Subjekt und Objekt MÜSSEN aus der bereitgestellten Entitätsliste stammen
2. Verwende keine Entitäten, die nicht in der Liste stehen
3. Inferiere Beziehungen basierend auf allgemeinem Wissen
4. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an, einen pro Zeile
5. Verwende als Subjekt und Objekt EXAKT die Namen aus der Entitätsliste
6. Verwende ausschließlich kleingeschriebene Prädikate in der dritten Person Singular (z.B. "enthält", "ist_teil_von")
7. Bewahre bei Subjekt und Objekt die originale Groß-/Kleinschreibung
8. Verwende keine erklärenden Sätze oder Einleitungen"""

        user_prompt = f"""Hier sind die extrahierten Entitäten mit kurzen Beschreibungen:

{entity_info_text}

Entitätsliste:
{entity_list}

Bitte identifiziere IMPLIZITE Beziehungen zwischen diesen Entitäten, basierend auf allgemeinem Wissen. Vermeide offensichtliche oder triviale Beziehungen. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an."""
    else:
        system_prompt = """You are an assistant for creating knowledge graphs. Your task is to identify IMPLICIT relationships between entities, based on general knowledge and context.

Important rules:
1. Subject and object MUST be from the provided entity list
2. Do not use entities that are not in the list
3. Infer relationships based on general knowledge
4. Provide the relationships in the format "Subject; Predicate; Object", one per line
5. Use EXACTLY the names from the entity list for subject and object
6. Use only lowercase predicates in the third person singular (e.g. "contains", "is_part_of")
7. Preserve the original capitalization of subject and object
8. Do not use explanatory sentences or introductions"""

        user_prompt = f"""Here are the extracted entities with short descriptions:

{entity_info_text}

Entity list:
{entity_list}

Please identify IMPLICIT relationships between these entities, based on general knowledge. Avoid obvious or trivial relationships. Provide the relationships in the format "Subject; Predicate; Object"."""
    
    # Rufe die OpenAI API auf
    logger.info(f"Rufe OpenAI API für Beziehungsinferenz auf (Modell: {model})")
    start_api_time = time.time()
    
    response = await call_openai_api(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=temperature,
        config=config
    )
    
    api_time = time.time() - start_api_time
    logger.info(f"OpenAI API-Aufruf abgeschlossen in {api_time:.2f}s")
    
    # Verarbeite die Antwort
    relationships = []
    
    if response:
        answer = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        logger.debug(f"API-Antwort (gekürzt): {answer[:150]}...")
        
        # Erstelle Mapping von Namen zu IDs für die Konvertierung
        name_to_id = {name: entity_id for entity_id, name in id_to_name.items()}
        
        # Verarbeite die Antwort Zeile für Zeile
        for line in answer.strip().split("\n"):
            if not line or ";" not in line:
                continue
                
            parts = [p.strip() for p in line.split(";")]
            if len(parts) >= 3:
                subject_name, predicate, object_name = parts[0], parts[1], parts[2]
                
                # Finde die entsprechenden IDs
                subject_id = name_to_id.get(subject_name)
                object_id = name_to_id.get(object_name)
                
                # Stellen sicher, dass beide IDs existieren
                if not subject_id or not object_id:
                    logger.warning(f"Ungültige Entität in Beziehung: {subject_name} -> {predicate} -> {object_name}")
                    continue
                
                # Normalisierung des Prädikats (klein)
                predicate = predicate.lower().strip()
                
                # Erstelle die Beziehung mit Entitätstypen und IDs
                relationship = {
                    "id": generate_relationship_id(),
                    "subject": subject_id,
                    "predicate": predicate,
                    "object": object_id,
                    "subject_name": subject_name,  # Original-Name mit korrekter Groß-/Kleinschreibung
                    "object_name": object_name,    # Original-Name mit korrekter Groß-/Kleinschreibung
                    "inferred": "implicit",
                    "subject_type": id_to_type.get(subject_id, ""),
                    "object_type": id_to_type.get(object_id, ""),
                    "subject_inferred": id_to_inferred.get(subject_id, "explicit"),
                    "object_inferred": id_to_inferred.get(object_id, "explicit"),
                    "confidence": 0.8  # Standard-Konfidenzwert für implizite Beziehungen
                }
                
                # Beziehung validieren
                if validate_relationship(relationship):
                    relationships.append(relationship)
                else:
                    logger.warning(f"Ungültige Beziehungsstruktur: {relationship}")
    
    # Aktualisiere die Kontexte mit den extrahierten Beziehungen
    for context in contexts:
        entity_id = context.entity_id
        
        # Finde Beziehungen, in denen diese Entität vorkommt
        for rel in relationships:
            if rel.get("subject") == entity_id or rel.get("object") == entity_id:
                # Extrahiere die wichtigsten Felder
                subject_id = rel.get("subject")
                predicate = rel.get("predicate")
                object_id = rel.get("object")
                
                # Metadaten für die Beziehung
                metadata = {k: v for k, v in rel.items() 
                           if k not in ["subject", "predicate", "object", "subject_type", "object_type"]}
                
                # Füge die Beziehung zum Kontext hinzu
                context.add_relationship(
                    subject_id, 
                    predicate, 
                    object_id,
                    subject_type=rel.get("subject_type"),
                    object_type=rel.get("object_type"),
                    metadata=metadata
                )
    
    total_time = time.time() - start_time
    logger.info(f"Beziehungsinferenz abgeschlossen: {len(relationships)} Beziehungen in {total_time:.2f}s")
    
    return relationships


async def extract_explicit_relationships_from_contexts(
    contexts: List[EntityProcessingContext],
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Extrahiert explizite Beziehungen zwischen Entitäten basierend auf dem Originaltext.
    
    Args:
        contexts: Liste von EntityProcessingContext-Objekten
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von Beziehungs-Dictionaries
    """
    if not contexts:
        return []
    
    config = config or get_config()
    start_time = time.time()
    
    # Überprüfe, ob Originaltext verfügbar ist
    text = None
    for context in contexts:
        if context.original_text:
            text = context.original_text
            break
    
    if not text:
        logger.warning("Kein Originaltext für die Extraktion expliziter Beziehungen verfügbar")
        return []
    
    logger.info(f"Extrahiere explizite Beziehungen aus Text mit {len(contexts)} Kontexten")
    
    # ID-basierte Maps erstellen
    id_to_name = {ctx.entity_id: ctx.entity_name for ctx in contexts}
    id_to_type = {ctx.entity_id: ctx.entity_type or "" for ctx in contexts}
    name_to_id = {ctx.entity_name: ctx.entity_id for ctx in contexts}
    
    # Entitätsliste für den Prompt
    entity_list = "\n".join([f"- {ctx.entity_name}" for ctx in contexts])
    
    # Inferiere explizite Beziehungen mit angepasstem Prompt
    language = config.get("LANGUAGE", "de")
    temperature = config.get("TEMPERATURE_RELATIONSHIPS", 0.3)  # Niedriger für explizite Beziehungen
    model = config.get("MODEL_RELATIONSHIPS", config.get("MODEL", "gpt-4.1-mini"))
    
    if language == "de":
        system_prompt = """Du bist ein Assistent für das Extrahieren von Beziehungen zwischen Entitäten. Deine Aufgabe ist es, EXPLIZITE Beziehungen zwischen Entitäten im Text zu identifizieren.

Wichtige Regeln:
1. Extrahiere NUR Beziehungen, die EXPLIZIT im Text erwähnt werden
2. Subjekt und Objekt MÜSSEN aus der bereitgestellten Entitätsliste stammen
3. Verwende keine Entitäten, die nicht in der Liste stehen
4. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an, einen pro Zeile
5. Verwende als Subjekt und Objekt EXAKT die Namen aus der Entitätsliste
6. Verwende ausschließlich kleingeschriebene Prädikate in der dritten Person Singular (z.B. "enthält", "ist_teil_von")
7. Bewahre bei Subjekt und Objekt die originale Groß-/Kleinschreibung
8. Füge für jede Beziehung einen vierten Teil hinzu: den Textabschnitt, der die Beziehung belegt
9. Verwende keine erklärenden Sätze oder Einleitungen"""

        user_prompt = f"""Hier ist der Text:

{text}

Hier ist die Liste der extrahierten Entitäten:
{entity_list}

Bitte extrahiere EXPLIZITE Beziehungen zwischen diesen Entitäten, die im Text erwähnt werden. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt; Beleg" an."""
    else:
        system_prompt = """You are an assistant for extracting relationships between entities. Your task is to identify EXPLICIT relationships between entities mentioned in the text.

Important rules:
1. Extract ONLY relationships that are EXPLICITLY mentioned in the text
2. Subject and object MUST be from the provided entity list
3. Do not use entities that are not in the list
4. Provide the relationships in the format "Subject; Predicate; Object; Evidence", one per line
5. Use EXACTLY the names from the entity list for subject and object
6. Use only lowercase predicates in the third person singular (e.g. "contains", "is_part_of")
7. Preserve the original capitalization of subject and object
8. For each relationship, add a fourth part: the text passage that supports the relationship
9. Do not use explanatory sentences or introductions"""

        user_prompt = f"""Here is the text:

{text}

Here is the list of extracted entities:
{entity_list}

Please extract EXPLICIT relationships between these entities that are mentioned in the text. Provide the relationships in the format "Subject; Predicate; Object; Evidence"."""
    
    # Rufe die OpenAI API auf
    logger.info(f"Rufe OpenAI API für explizite Beziehungsextraktion auf (Modell: {model})")
    start_api_time = time.time()
    
    response = await call_openai_api(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=temperature,
        config=config
    )
    
    api_time = time.time() - start_api_time
    logger.info(f"OpenAI API-Aufruf abgeschlossen in {api_time:.2f}s")
    
    # Verarbeite die Antwort
    relationships = []
    
    if response:
        answer = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        logger.debug(f"API-Antwort (gekürzt): {answer[:150]}...")
        
        # Verarbeite die Antwort Zeile für Zeile
        for line in answer.strip().split("\n"):
            if not line or ";" not in line:
                continue
                
            parts = [p.strip() for p in line.split(";")]
            if len(parts) >= 3:
                subject_name, predicate, object_name = parts[0], parts[1], parts[2]
                
                # Extrahiere den Beleg, falls vorhanden
                evidence = parts[3] if len(parts) > 3 else ""
                
                # Finde die entsprechenden IDs
                subject_id = name_to_id.get(subject_name)
                object_id = name_to_id.get(object_name)
                
                # Stellen sicher, dass beide IDs existieren
                if not subject_id or not object_id:
                    logger.warning(f"Ungültige Entität in Beziehung: {subject_name} -> {predicate} -> {object_name}")
                    continue
                
                # Normalisierung des Prädikats (klein)
                predicate = predicate.lower().strip()
                
                # Erstelle die Beziehung mit Entitätstypen und IDs
                relationship = {
                    "id": generate_relationship_id(),
                    "subject": subject_id,
                    "predicate": predicate,
                    "object": object_id,
                    "subject_name": subject_name,
                    "object_name": object_name,
                    "inferred": "explicit",
                    "subject_type": id_to_type.get(subject_id, ""),
                    "object_type": id_to_type.get(object_id, ""),
                    "evidence": evidence,
                    "confidence": 0.95  # Höhere Konfidenz für explizite Beziehungen
                }
                
                # Beziehung validieren
                if validate_relationship(relationship):
                    relationships.append(relationship)
                else:
                    logger.warning(f"Ungültige Beziehungsstruktur: {relationship}")
    
    # Aktualisiere die Kontexte mit den extrahierten Beziehungen
    for context in contexts:
        entity_id = context.entity_id
        
        # Finde Beziehungen, in denen diese Entität vorkommt
        for rel in relationships:
            if rel.get("subject") == entity_id or rel.get("object") == entity_id:
                # Extrahiere die wichtigsten Felder
                subject_id = rel.get("subject")
                predicate = rel.get("predicate")
                object_id = rel.get("object")
                
                # Metadaten für die Beziehung
                metadata = {k: v for k, v in rel.items() 
                           if k not in ["subject", "predicate", "object", "subject_type", "object_type"]}
                
                # Füge die Beziehung zum Kontext hinzu
                context.add_relationship(
                    subject_id, 
                    predicate, 
                    object_id,
                    subject_type=rel.get("subject_type"),
                    object_type=rel.get("object_type"),
                    metadata=metadata
                )
    
    total_time = time.time() - start_time
    logger.info(f"Explizite Beziehungsextraktion abgeschlossen: {len(relationships)} Beziehungen in {total_time:.2f}s")
    
    return relationships


async def process_all_relationships(
    contexts: List[EntityProcessingContext],
    config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Verarbeitet sowohl explizite als auch implizite Beziehungen für eine Liste von Kontexten.
    
    Args:
        contexts: Liste von EntityProcessingContext-Objekten
        config: Konfigurationswörterbuch
        
    Returns:
        Liste aller extrahierten Beziehungen
    """
    if not contexts:
        return []
    
    config = config or get_config()
    
    # Extrahiere explizite Beziehungen, wenn Original-Text vorhanden ist
    has_original_text = any(ctx.original_text for ctx in contexts)
    all_relationships = []
    
    if has_original_text and config.get("EXTRACT_EXPLICIT_RELATIONSHIPS", True):
        explicit_relationships = await extract_explicit_relationships_from_contexts(contexts, config)
        all_relationships.extend(explicit_relationships)
        logger.info(f"Extrahiert: {len(explicit_relationships)} explizite Beziehungen")
    
    # Inferiere implizite Beziehungen, wenn aktiviert
    if config.get("INFER_IMPLICIT_RELATIONSHIPS", True):
        implicit_relationships = await infer_relationships_from_contexts(contexts, config)
        all_relationships.extend(implicit_relationships)
        logger.info(f"Inferiert: {len(implicit_relationships)} implizite Beziehungen")
    
    # Dedupliziere Beziehungen falls nötig
    if config.get("DEDUPLICATE_RELATIONSHIPS", True) and all_relationships:
        from entityextractor.core.process.deduplication import deduplicate_relationships_from_contexts
        all_relationships = deduplicate_relationships_from_contexts(contexts, config)
        logger.info(f"Nach Deduplizierung: {len(all_relationships)} Beziehungen")
    
    return all_relationships
