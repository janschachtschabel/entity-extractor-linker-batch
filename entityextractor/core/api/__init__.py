"""
API Module für den Entity Extractor.

Diese Datei exportiert die Hauptfunktionen für den Zugriff auf die API.
"""

from entityextractor.core.api.extract import extract_entities
from entityextractor.core.api.generate import generate_entities
from entityextractor.core.api.link import link_entities
from entityextractor.core.api.relationships import infer_entity_relationships

# Für Kompatibilität mit bestehenden Skripten
from entityextractor.core.process.orchestrator import process_entities

__all__ = [
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships",
    "process_entities"  # Zur Kompatibilität mit bestehenden Skripten
]
