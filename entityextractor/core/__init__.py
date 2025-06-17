"""
Core functionality for the Entity Extractor.

Dieses Paket enthält die Kernfunktionalität für die Extraktion, Generierung und Verknüpfung von Entitäten.
Nach der Refaktorierung ist der Code in die Submodule api, process und visualization unterteilt.
Die neue kontextbasierte Architektur ermöglicht eine strukturierte Datenübergabe zwischen Services.
"""

# Kontext-Klasse importieren
from entityextractor.core.context import EntityProcessingContext

# API-Funktionen importieren (legacy)
from entityextractor.core.api.extract import extract_entities
from entityextractor.core.api.generate import generate_entities
from entityextractor.core.api.link import link_entities
from entityextractor.core.api.relationships import infer_entity_relationships

# Process-Funktionen importieren
from entityextractor.core.process.orchestrator import process_entities
from entityextractor.core.process.orchestrator import process_entity, process_entities as process_entities_batch
from entityextractor.core.process.context_statistics import generate_context_statistics

# Visualisierungsfunktionen importieren
from entityextractor.core.visualization import visualize_contexts

__all__ = [
    # Kontext-Klasse
    "EntityProcessingContext",
    
    # Legacy API
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships",
    "process_entities",
    
    # Kontextbasierte API
    "process_entity",
    "process_entities_batch",
    "generate_context_statistics",
    "visualize_contexts"
]
