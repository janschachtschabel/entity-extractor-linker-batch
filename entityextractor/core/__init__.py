"""
Core functionality for the Entity Extractor.

Dieses Paket enthält die Kernfunktionalität für die Extraktion, Generierung und Verknüpfung von Entitäten.
Nach der Refaktorierung ist der Code in die Submodule api, process und visualization unterteilt.
"""

# API-Funktionen importieren
from entityextractor.core.api.extract import extract_entities
from entityextractor.core.api.generate import generate_entities
from entityextractor.core.api.link import link_entities
from entityextractor.core.api.relationships import infer_entity_relationships

# Process-Funktionen importieren
from entityextractor.core.process.orchestrator import process_entities

__all__ = [
    # Core API
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships",
    "process_entities"
]
