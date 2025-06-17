"""
Process-Module für den Entity Extractor.

Diese Datei exportiert die Hauptfunktionen für die Verarbeitung von Entitäten und Beziehungen.
Unterstützt sowohl die traditionelle dictionary-basierte als auch die neue context-basierte Architektur.
"""

# Legacy-Funktionen (dictionary-basiert)
from entityextractor.core.process.orchestrator import process_entities
from entityextractor.core.process.deduplication import deduplicate_entities, deduplicate_relationships
from entityextractor.core.process.context_statistics import generate_context_statistics as generate_statistics
from entityextractor.core.process.result_formatter import format_results, format_contexts_to_result

# Neue kontext-basierte Funktionen
from entityextractor.core.process.orchestrator import process_entity, process_entities as process_entities_batch
from entityextractor.core.process.context_statistics import generate_context_statistics, format_statistics

__all__ = [
    # Legacy-Funktionen
    "process_entities",
    "deduplicate_entities",
    "deduplicate_relationships",
    "generate_statistics",
    "format_results",
    
    # Kontext-basierte Funktionen
    "process_entity",
    "process_entities_batch",
    "generate_context_statistics",
    "format_statistics",
    "format_contexts_to_result"
]
