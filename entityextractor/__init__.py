"""
Entity Extractor - Ein leistungsstarkes Tool zur Identifizierung und Verknüpfung von Entitäten mit Wissensbasen.

Dieses Paket bietet Funktionen zur Extraktion benannter Entitäten aus Texten, zur Generierung 
von Entitäten zu Themen und zur Verknüpfung mit Wikipedia, Wikidata und DBpedia.
Es enthält auch Funktionen zur Beziehungsextraktion und Knowledge Graph-Visualisierung.
"""

__version__ = "1.1.0"

# Hauptfunktionen für Endbenutzer exportieren
from entityextractor.api import (
    extract_and_link_entities,
    generate_and_link_entities,
    create_knowledge_compendium
)

# Erweiterte Funktionen für fortgeschrittene Benutzer
from entityextractor.core import (
    extract_entities,
    generate_entities,
    link_entities,
    infer_entity_relationships,
    process_entities
)

# Konfigurations-Utility exportieren
from entityextractor.config.settings import get_config

__all__ = [
    # Hauptfunktionen
    "extract_and_link_entities",
    "generate_and_link_entities",
    "create_knowledge_compendium",
    
    # Erweiterte Funktionen
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships",
    "process_entities",
    
    # Hilfsfunktionen
    "get_config",
    
    # Metadaten
    "__version__"
]
