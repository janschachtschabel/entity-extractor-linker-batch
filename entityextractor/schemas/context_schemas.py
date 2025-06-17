#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kontext-Schemas

Schemas speziell für die Validierung der EntityProcessingContext-Strukturen.
Diese ergänzen die bestehenden Service-Schemas und verbessern die Validierung
in der kontextbasierten Architektur.
"""

import logging
import jsonschema
from typing import Dict, Any, Optional, List, Union

from entityextractor.schemas.service_schemas import (
    WIKIPEDIA_SCHEMA, WIKIDATA_SCHEMA, DBPEDIA_SCHEMA, 
    RELATIONSHIP_SCHEMA, ENTITY_OUTPUT_SCHEMA
)

logger = logging.getLogger(__name__)

# Schema für die Processing-Data eines Kontexts
PROCESSING_DATA_SCHEMA = {
    "type": "object",
    "additionalProperties": True
}

# Schema für die EntityProcessingContext-Serialisierung
ENTITY_CONTEXT_SERIALIZED_SCHEMA = {
    "type": "object",
    "properties": {
        "entity_name": {"type": "string"},
        "entity_id": {"type": "string"},
        "entity_type": {"type": ["string", "null"]},
        "original_text": {"type": ["string", "null"]},
        "citation": {"type": ["string", "null"]},
        "processing_data": {
            "type": "object",
            "additionalProperties": True
        },
        "relationships": {
            "type": "array",
            "items": {"$ref": "#/definitions/relationship"}
        },
        "additional_data": {
            "type": "object",
            "additionalProperties": True
        },
        "output_data": ENTITY_OUTPUT_SCHEMA,
        "processed_by_services": {
            "type": "array",
            "items": {"type": "string"}
        }
    },
    "required": ["entity_name", "entity_id", "output_data"],
    "definitions": {
        "relationship": RELATIONSHIP_SCHEMA
    }
}

# Schema für Batch-Verarbeitungskontext
BATCH_PROCESSING_CONTEXT_SCHEMA = {
    "type": "object",
    "properties": {
        "batch_id": {"type": "string"},
        "contexts": {
            "type": "array",
            "items": {"$ref": "#/definitions/entity_context_reference"}
        },
        "common_original_text": {"type": ["string", "null"]},
        "batch_metadata": {
            "type": "object",
            "additionalProperties": True
        },
        "processing_statistics": {
            "type": "object",
            "additionalProperties": True
        }
    },
    "required": ["batch_id", "contexts"],
    "definitions": {
        "entity_context_reference": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string"},
                "entity_name": {"type": "string"}
            },
            "required": ["entity_id", "entity_name"]
        }
    }
}

# Schema für Statistik-Ausgaben
STATISTICS_SCHEMA = {
    "type": "object",
    "properties": {
        "total_entities": {"type": "integer"},
        "total_relationships": {"type": "integer"},
        "types_distribution": {
            "type": "object",
            "additionalProperties": {"type": "integer"}
        },
        "linked": {
            "type": "object",
            "properties": {
                "wikipedia": {
                    "type": "object",
                    "properties": {
                        "count": {"type": "integer"},
                        "percent": {"type": "number"}
                    }
                },
                "wikidata": {
                    "type": "object",
                    "properties": {
                        "count": {"type": "integer"},
                        "percent": {"type": "number"}
                    }
                },
                "dbpedia": {
                    "type": "object",
                    "properties": {
                        "count": {"type": "integer"},
                        "percent": {"type": "number"}
                    }
                }
            }
        },
        "top_wikipedia_categories": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "count": {"type": "integer"}
                }
            }
        },
        "top_wikidata_types": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "count": {"type": "integer"}
                }
            }
        },
        "entity_connections": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "entity": {"type": "string"},
                    "entity_id": {"type": "string"},
                    "count": {"type": "integer"}
                }
            }
        },
        "relationship_inference": {
            "type": "object",
            "properties": {
                "explicit": {
                    "type": "object",
                    "properties": {
                        "count": {"type": "integer"},
                        "percent": {"type": "number"}
                    }
                },
                "implicit": {
                    "type": "object",
                    "properties": {
                        "count": {"type": "integer"},
                        "percent": {"type": "number"}
                    }
                }
            }
        },
        "avg_processing_times": {
            "type": "object",
            "additionalProperties": {"type": "number"}
        }
    },
    "required": ["total_entities", "total_relationships", "types_distribution", "linked"]
}


def validate_entity_context(context_data: Dict[str, Any]) -> bool:
    """
    Validiert ein serialisiertes EntityProcessingContext-Objekt gegen das Schema.
    
    Args:
        context_data: Die zu validierenden Kontextdaten
        
    Returns:
        True, wenn die Daten valide sind, sonst False
    """
    try:
        jsonschema.validate(context_data, ENTITY_CONTEXT_SERIALIZED_SCHEMA)
        logger.debug("EntityProcessingContext erfolgreich validiert")
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Validierungsfehler für EntityProcessingContext: {str(e)}")
        logger.debug(f"Validierungsfehler Details: {e.message}")
        logger.debug(f"Fehlerpfad: {'.'.join(str(p) for p in e.path)}")
        return False


def validate_batch_context(batch_data: Dict[str, Any]) -> bool:
    """
    Validiert einen Batch-Verarbeitungskontext gegen das Schema.
    
    Args:
        batch_data: Die zu validierenden Batch-Kontextdaten
        
    Returns:
        True, wenn die Daten valide sind, sonst False
    """
    try:
        jsonschema.validate(batch_data, BATCH_PROCESSING_CONTEXT_SCHEMA)
        logger.debug("Batch-Verarbeitungskontext erfolgreich validiert")
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Validierungsfehler für Batch-Kontext: {str(e)}")
        logger.debug(f"Validierungsfehler Details: {e.message}")
        return False


def validate_statistics(stats_data: Dict[str, Any]) -> bool:
    """
    Validiert Statistik-Daten gegen das Schema.
    
    Args:
        stats_data: Die zu validierenden Statistik-Daten
        
    Returns:
        True, wenn die Daten valide sind, sonst False
    """
    try:
        jsonschema.validate(stats_data, STATISTICS_SCHEMA)
        logger.debug("Statistik-Daten erfolgreich validiert")
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Validierungsfehler für Statistik-Daten: {str(e)}")
        logger.debug(f"Validierungsfehler Details: {e.message}")
        return False


def validate_context_service_data(context: 'EntityProcessingContext', service_name: str) -> bool:
    """
    Validiert die Service-Daten eines EntityProcessingContext-Objekts.
    
    Args:
        context: Das zu validierende Kontext-Objekt
        service_name: Name des Services (wikipedia, wikidata, dbpedia)
        
    Returns:
        True, wenn die Daten valide sind, sonst False
    """
    if not context.is_processed_by(service_name):
        logger.warning(f"Kontext wurde nicht von {service_name} verarbeitet")
        return False
        
    service_data = context.get_service_data(service_name)
    if not service_data:
        logger.warning(f"Keine {service_name}-Daten im Kontext gefunden")
        return False
        
    # Wähle das passende Schema basierend auf dem Service-Namen
    schema = None
    if service_name == "wikipedia":
        schema = WIKIPEDIA_SCHEMA
    elif service_name == "wikidata":
        schema = WIKIDATA_SCHEMA
    elif service_name == "dbpedia":
        schema = DBPEDIA_SCHEMA
    else:
        logger.error(f"Unbekannter Service: {service_name}")
        return False
        
    # Validiere gegen das Schema
    try:
        jsonschema.validate(service_data, schema)
        logger.debug(f"{service_name}-Daten im Kontext erfolgreich validiert")
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Validierungsfehler für {service_name}-Daten im Kontext: {str(e)}")
        logger.debug(f"Validierungsfehler Details: {e.message}")
        return False
