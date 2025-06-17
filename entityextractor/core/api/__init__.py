"""
API Module für den Entity Extractor.

Diese Datei exportiert die Hauptfunktionen für den Zugriff auf die API.
Sowohl die ursprüngliche dictionary-basierte als auch die neue kontext-basierte API werden unterstützt.
"""

# Legacy API (dictionary-basiert)
from entityextractor.core.api.extract import extract_entities
from entityextractor.core.api.generate import generate_entities
from entityextractor.core.api.link import link_entities, link_contexts
from entityextractor.core.api.relationships import infer_entity_relationships

# Für Kompatibilität mit bestehenden Skripten
from entityextractor.core.process.orchestrator import process_entities

# Kontext-basierte API
from entityextractor.core.process.orchestrator import process_entity, process_entities as process_entities_batch
from entityextractor.core.process.orchestrator import process_single_pass

__all__ = [
    # Legacy API (dictionary-basiert)
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships",
    "process_entities",  # Zur Kompatibilität mit bestehenden Skripten

    # Kontext-basierte API
    "link_contexts",
    "process_entity",
    "process_entities_batch",
    "process_single_pass"
]
