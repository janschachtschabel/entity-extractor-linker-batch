#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
circular_import_fix.py

Dieses Modul löst zirkuläre Importprobleme zwischen den Kern-Modulen
der kontextbasierten Architektur. Es stellt gemeinsam genutzte Funktionalitäten
bereit, ohne dass die Module sich gegenseitig importieren müssen.
"""

import logging
import uuid
from typing import Dict, Any, List

from entityextractor.core.context import EntityProcessingContext

logger = logging.getLogger(__name__)

def format_contexts_to_result(contexts: List[EntityProcessingContext]) -> Dict[str, Any]:
    """
    Formatiert eine Liste von EntityProcessingContext-Objekten zu einem standardisierten Ergebnisobjekt.
    
    Args:
        contexts: Liste von EntityProcessingContext-Objekten
        
    Returns:
        Ein Dictionary mit den formatierten Daten
    """
    logger.debug("Formatiere %d Kontexte zu Ergebnis", len(contexts))
    
    result = {
        "entities": [],
        "relationships": [],
        "statistics": {
            "total_entities": len(contexts),
            "total_relationships": 0,
            "processing_time": 0,
            "linked": {
                "wikipedia": {"count": 0},
                "wikidata": {"count": 0},
                "dbpedia": {"count": 0}
            }
        }
    }
    
    # Original-Text aus dem ersten Kontext extrahieren (falls vorhanden)
    if contexts and contexts[0].original_text:
        result["original_text"] = contexts[0].original_text
    
    # Berechne, wie viele Entitäten mit welchen Services verknüpft wurden
    for context in contexts:
        # Erstelle ein eigenes Entitätsobjekt für jede Entität
        entity_data = {
            "entity": context.entity_name,
            "id": context.entity_id or str(uuid.uuid4()),
            "details": {
                "typ": context.entity_type,
                "inferred": "explicit",  # Standard, kann später überschrieben werden
            },
            "sources": {}
        }
        
        # Füge weitere Daten aus dem Context hinzu
        if hasattr(context, "output_data") and context.output_data:
            if "details" in context.output_data:
                entity_data["details"].update(context.output_data["details"])
            if "sources" in context.output_data:
                entity_data["sources"].update(context.output_data["sources"])
        
        # Füge die Entität zum Ergebnis hinzu
        result["entities"].append(entity_data)
        
        # Services zählen
        for service in ["wikipedia", "wikidata", "dbpedia"]:
            if hasattr(context, "processed_by_services") and service in context.processed_by_services:
                result["statistics"]["linked"][service]["count"] += 1
        
        # Beziehungen sammeln
        if hasattr(context, "relationships") and context.relationships:
            result["relationships"].extend(context.relationships)
    
    # Gesamtzahl der Beziehungen aktualisieren
    result["statistics"]["total_relationships"] = len(result["relationships"])
    
    return result
