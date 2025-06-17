"""
Entity Extractor - Ein leistungsstarkes Tool zur Identifizierung und Verknüpfung von Entitäten mit Wissensbasen.

Dieses Paket bietet Funktionen zur Extraktion benannter Entitäten aus Texten, zur Generierung 
von Entitäten zu Themen und zur Verknüpfung mit Wikipedia, Wikidata und DBpedia.
Es enthält auch Funktionen zur Beziehungsextraktion und Knowledge Graph-Visualisierung.

Die neue kontextbasierte Architektur ermöglicht eine optimierte Batch-Verarbeitung und
strukturierte Datenübergabe zwischen den verschiedenen Services.
"""

__version__ = "1.2.0"  # Version aktualisiert wegen neuer kontextbasierter Architektur

# Hauptfunktionen für Endbenutzer exportieren
from entityextractor.api import (
    extract_and_link_entities,
    generate_and_link_entities,
    create_knowledge_compendium
)

# Legacy-Funktionen (dictionary-basiert)
from entityextractor.core import (
    extract_entities,
    generate_entities,
    link_entities,
    infer_entity_relationships,
    process_entities
)

# Kontext-basierte Architektur
from entityextractor.core import (
    EntityProcessingContext,
    process_entity,
    process_entities_batch,
    generate_context_statistics,
    visualize_contexts
)

# Konfigurations-Utility exportieren
from entityextractor.config.settings import get_config

__all__ = [
    # Hauptfunktionen
    "extract_and_link_entities",
    "generate_and_link_entities",
    "create_knowledge_compendium",
    
    # Legacy-Funktionen (dictionary-basiert)
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships",
    "process_entities",
    
    # Kontext-basierte Architektur
    "EntityProcessingContext",
    "process_entity",
    "process_entities_batch",
    "generate_context_statistics",
    "visualize_contexts",
    
    # Hilfsfunktionen
    "get_config",
    
    # Metadaten
    "__version__"
]
