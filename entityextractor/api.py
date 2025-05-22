"""
api.py

Hauptmoduldatei mit der öffentlichen API für den Entity Extractor.
"""

import logging
from entityextractor.config.settings import get_config

# Importiere die neuen API-Funktionen aus den spezifischen Modulen
from entityextractor.core.api.extract import extract_entities
from entityextractor.core.api.generate import generate_entities
from entityextractor.core.api.link import link_entities
from entityextractor.core.api.relationships import infer_entity_relationships
from entityextractor.core.process.orchestrator import process_entities

def extract_and_link_entities(text, config=None):
    """
    Extrahiert Entitäten aus einem Text und verknüpft sie mit Wissensquellen.
    
    Dies ist die Hauptfunktion für die Entitätsextraktion aus einem bestehenden Text.
    Sie nutzt die optimierte Batch-Verarbeitung für maximale Effizienz.
    
    Args:
        text: Der Text, aus dem Entitäten extrahiert werden sollen
        config: Konfigurationswörterbuch (optional) mit Parametern wie:
            - LANGUAGE: Sprache des Textes ("de" oder "en")
            - USE_WIKIDATA: Wikidata-Verknüpfung aktivieren (default: True)
            - USE_DBPEDIA: DBpedia-Verknüpfung aktivieren (default: True)
            - RELATION_EXTRACTION: Beziehungsextraktion aktivieren (default: True)
            - ENABLE_RELATIONS_INFERENCE: Implizite Beziehungen aktivieren (default: False)
            - TEXT_CHUNKING: Text in Chunks aufteilen (default: False)
            - VISUALIZE_GRAPH: Graph visualisieren (default: True)
        
    Returns:
        Ein Dictionary mit extrahierten Entitäten und optional Beziehungen und Visualisierungen
    """
    user_config = config or {}
    user_config["MODE"] = "extract"
    return process_entities(text, user_config)

def generate_and_link_entities(topic, config=None):
    """
    Generiert Entitäten zu einem Thema und verknüpft sie mit Wissensquellen.
    
    Diese Funktion generiert relevante Entitäten zu einem bestimmten Thema
    anstatt sie aus einem Text zu extrahieren. Sie eignet sich besonders,
    um Wissensgraphen zu erstellen oder Lehrmaterial zu ergänzen.
    
    Args:
        topic: Das Thema, zu dem Entitäten generiert werden sollen
        config: Konfigurationswörterbuch (optional) mit Parametern wie:
            - LANGUAGE: Sprache des Themas ("de" oder "en")
            - MAX_ENTITIES: Maximale Anzahl zu generierender Entitäten (default: 10)
            - USE_WIKIDATA: Wikidata-Verknüpfung aktivieren (default: True)
            - USE_DBPEDIA: DBpedia-Verknüpfung aktivieren (default: True)
            - RELATION_EXTRACTION: Beziehungsextraktion aktivieren (default: True)
            - ENABLE_RELATIONS_INFERENCE: Implizite Beziehungen aktivieren (default: False)
            - VISUALIZE_GRAPH: Graph visualisieren (default: True)
        
    Returns:
        Ein Dictionary mit generierten Entitäten und optional Beziehungen und Visualisierungen
    """
    user_config = config or {}
    user_config["MODE"] = "generate"
    return process_entities(topic, user_config)

def create_knowledge_compendium(topic, config=None):
    """
    Erstellt ein umfassendes Wissenskompendium zu einem Thema.
    
    Diese Funktion generiert ein strukturiertes Kompendium mit Entitäten,
    Beziehungen und einem zusammenfassenden Text zum Thema.
    
    Args:
        topic: Das Thema, zu dem ein Kompendium erstellt werden soll
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Ein Dictionary mit dem Kompendium, Entitäten, Beziehungen und Referenzen
    """
    user_config = config or {}
    user_config["MODE"] = "compendium"
    user_config["ENABLE_COMPENDIUM"] = True
    return process_entities(topic, user_config)

__all__ = [
    "extract_and_link_entities",
    "generate_and_link_entities",
    "create_knowledge_compendium",
    "process_entities",  # Für fortgeschrittene Anwendungsfälle
    
    # Einzelne Komponenten für flexible Integration
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships"
]
