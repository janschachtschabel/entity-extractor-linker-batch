"""
Schemas-Paket für den EntityExtractor.

Enthält Schema-Definitionen zur Validierung von Datenstrukturen.
"""

from entityextractor.schemas.service_schemas import (
    validate_wikipedia_data,
    validate_wikidata_data,
    validate_dbpedia_data,
    validate_entity_output,
    validate_relationship,
    validate_enhanced_relationship,
)

from entityextractor.schemas.context_schemas import (
    validate_entity_context,
    validate_batch_context,
    validate_statistics,
    validate_context_service_data,
)

__all__ = [
    # Service-Schema-Validierungen
    "validate_wikipedia_data",
    "validate_wikidata_data",
    "validate_dbpedia_data", 
    "validate_entity_output",
    "validate_relationship",
    "validate_enhanced_relationship",
    
    # Kontext-Schema-Validierungen
    "validate_entity_context",
    "validate_batch_context",
    "validate_statistics",
    "validate_context_service_data",
]
