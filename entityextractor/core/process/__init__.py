"""
Process-Module für den Entity Extractor.

Diese Datei exportiert die Hauptfunktionen für die Verarbeitung von Entitäten und Beziehungen.
"""

from entityextractor.core.process.orchestrator import process_entities
from entityextractor.core.process.deduplication import deduplicate_entities, deduplicate_relationships
from entityextractor.core.process.statistics import generate_statistics
from entityextractor.core.process.result_formatter import format_results

__all__ = [
    "process_entities",
    "deduplicate_entities",
    "deduplicate_relationships",
    "generate_statistics",
    "format_results"
]
