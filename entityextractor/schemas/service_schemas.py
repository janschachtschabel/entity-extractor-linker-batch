#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Service-Schemas

Definiert die Schemas für die Datenstrukturen der verschiedenen Services.
Diese Schemas dienen zur Validierung der Service-Ausgaben und zur Dokumentation
der erwarteten Datenstrukturen.
"""

import logging
import jsonschema
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Wikipedia-Schema
WIKIPEDIA_SCHEMA = {
    "type": "object",
    "properties": {
        "wikipedia_data": {
            "type": "object",
            "properties": {
                # Basisinformationen
                "title": {"type": "string"},
                "url": {"type": "string"},
                "extract": {"type": ["string", "null"]},
                "status": {"type": "string", "enum": ["found", "partial", "not_found"]},
                
                # Kategorisierung und Struktur
                "categories": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "internal_links": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                
                # Metadaten
                "pageid": {"type": ["integer", "null"]},
                "ns": {"type": ["integer", "null"]},
                "language": {"type": "string"},
                "redirected_from": {"type": ["string", "null"]},
                
                # Mediendaten
                "thumbnail": {"type": ["string", "null"]},
                
                # Externe Verweise
                "wikidata_id": {"type": ["string", "null"]},
                "multilang": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "url": {"type": "string"},
                            "label": {"type": "string"},
                            "description": {"type": ["string", "null"]}
                        }
                    }
                },
                
                # Debug-Informationen
                "source": {"type": "string"},  # "api", "opensearch", "fallback", etc.
                "needs_fallback": {"type": "boolean"},
                "fallback_attempts": {"type": "integer"}
            },
            "required": ["title", "url", "status"]
        }
    },
    "required": ["wikipedia_data"]
}

# Wikidata-Schema
WIKIDATA_SCHEMA = {
    "type": "object",
    "properties": {
        "wikidata_data": {
            "type": "object",
            "properties": {
                # Basisinformationen
                "id": {"type": "string"},  # z.B. "Q123"
                "url": {"type": "string"},  # z.B. "https://www.wikidata.org/entity/Q123"
                "status": {"type": "string", "enum": ["found", "partial", "not_found", "linked"]},
                
                # Mehrsprachige Labels und Beschreibungen
                "labels": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}  # Sprachcode -> Label
                },
                "descriptions": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}  # Sprachcode -> Beschreibung
                },
                "aliases": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                
                # Semantische Informationen
                "claims": {"type": "object"},  # Rohdaten von Wikidata
                "instance_of": {
                    "type": "array",
                    "items": {"type": "object"}
                },
                "subclass_of": {
                    "type": "array",
                    "items": {"type": "object"}
                },
                
                # Typen und Klassifikation
                "types": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "ontology": {
                    "type": "object",
                    "properties": {
                        "instance_of": {
                            "type": "array",
                            "items": {"type": "object"}
                        },
                        "subclass_of": {
                            "type": "array",
                            "items": {"type": "object"}
                        },
                        "part_of": {
                            "type": "array",
                            "items": {"type": "object"}
                        },
                        "has_parts": {
                            "type": "array",
                            "items": {"type": "object"}
                        }
                    }
                },
                
                # Medien
                "media": {
                    "type": "object",
                    "properties": {
                        "images": {
                            "type": "array", 
                            "items": {"type": "string"}
                        },
                        "image_url": {"type": ["string", "null"]}
                    }
                },
                
                # Externe Identifikatoren
                "external_ids": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                },
                
                # Verbindungen
                "sitelinks": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                },
                
                # Semantische Beziehungen
                "semantics": {
                    "type": "object",
                    "properties": {
                        "main_subject": {
                            "type": "array",
                            "items": {"type": "object"}
                        },
                        "field_of_work": {
                            "type": "array",
                            "items": {"type": "object"}
                        },
                        "applies_to": {
                            "type": "array",
                            "items": {"type": "object"}
                        },
                        "facet_of": {
                            "type": "array",
                            "items": {"type": "object"}
                        }
                    }
                },
                
                # Geografische Daten
                "coordinates": {
                    "type": "object",
                    "properties": {
                        "latitude": {"type": "number"},
                        "longitude": {"type": "number"}
                    }
                },
                
                # Zeitbezogene Daten
                "temporal": {
                    "type": "object",
                    "properties": {
                        "inception": {"type": ["string", "null"]},
                        "dissolution": {"type": ["string", "null"]},
                        "birth_date": {"type": ["string", "null"]},
                        "death_date": {"type": ["string", "null"]}
                    }
                },
                
                # Source und Debug-Informationen
                "source": {"type": "string"},  # "api", "search", "fallback", etc.
                "query_method": {"type": "string"}  # "direct", "search", "backlink", etc.
            },
            "required": ["id", "url", "status"]
        }
    },
    "required": ["wikidata_data"]
}

# DBpedia-Schema
DBPEDIA_SCHEMA = {
    "type": "object",
    "properties": {
        "dbpedia_data": {
            "type": "object",
            "properties": {
                # Basisinformationen
                "uri": {"type": "string"},  # z.B. "http://dbpedia.org/resource/Berlin"
                "resource_uri": {"type": "string"},  # Alias für uri oder alternativ
                "status": {"type": "string", "enum": ["found", "partial", "not_found", "linked"]},
                "source_language": {"type": "string"},  # z.B. "de" oder "en"
                "original_title": {"type": "string"},  # Wikipedia-Titel, aus dem DBpedia-Daten stammen
                
                # Inhalts-Beschreibungen
                "abstract": {"type": ["string", "null"]},  # Zusammenfassung
                "comment": {"type": ["string", "null"]},  # Kurzbeschreibung
                
                # Kategorisierung
                "types": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "categories": {
                    "type": "array", 
                    "items": {"type": "string"}
                },
                "subjects": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                
                # Hierarchische Beziehungen
                "part_of": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "has_parts": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                
                # Geografische Informationen
                "latitude": {"type": ["number", "null"]},
                "longitude": {"type": ["number", "null"]},
                "geo": {
                    "type": "object",
                    "properties": {
                        "lat": {"type": "number"},
                        "long": {"type": "number"},
                        "alt": {"type": "number"}
                    }
                },
                "place": {
                    "type": "object",
                    "properties": {
                        "country": {"type": ["string", "null"]},
                        "region": {"type": ["string", "null"]},
                        "city": {"type": ["string", "null"]}
                    }
                },
                
                # Zeitbezogene Informationen
                "birthDate": {"type": ["string", "null"]},
                "deathDate": {"type": ["string", "null"]},
                "foundingDate": {"type": ["string", "null"]},
                "temporal": {
                    "type": "object",
                    "properties": {
                        "start": {"type": ["string", "null"]},
                        "end": {"type": ["string", "null"]}
                    }
                },
                
                # Externe Verlinkungen
                "isPrimaryTopicOf": {"type": ["string", "null"]},
                "homepage": {"type": ["string", "null"]},
                "externalLinks": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "sameAs": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                
                # Multimedia
                "thumbnail": {"type": ["string", "null"]},
                "depiction": {"type": ["string", "null"]},
                "media": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                
                # Externe Identifikatoren
                "gndId": {"type": ["string", "null"]},
                "viafId": {"type": ["string", "null"]},
                "orcidId": {"type": ["string", "null"]},
                "wikidataId": {"type": ["string", "null"]},
                "external_ids": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                },
                
                # Semantische Eigenschaften
                "industry": {"type": ["string", "null"]},
                "occupation": {"type": ["string", "null"]},
                "field": {"type": ["string", "null"]},
                "influences": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "influenced": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                
                # Metadata
                "format": {"type": "string"},  # "json" oder "xml"
                "lookup_class": {"type": ["string", "null"]},
                "query_method": {"type": "string"},  # "sparql", "lookup", "url"
                
                # Source und Debug-Informationen
                "source": {"type": "string"},  # "sparql", "lookup", "url_conversion", etc.
                "endpoint": {"type": "string"},  # "de.dbpedia.org", "dbpedia.org", etc.
                "use_de": {"type": "boolean"}  # Flag für deutsche DBpedia
            },
            "required": ["uri", "status"]
        }
    },
    "required": ["dbpedia_data"]
}

# Entity-Schema (finales Output-Format)
ENTITY_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "entity": {"type": "string"},
        "id": {"type": "string"},
        "details": {
            "type": "object",
            "properties": {
                "typ": {"type": "string"},
                "inferred": {"type": "string"},
                "extract": {"type": ["string", "null"]},
                "citation": {"type": "string"},
                "citation_start": {"type": "integer"},
                "citation_end": {"type": "integer"}
            },
            "required": ["typ"]
        },
        "sources": {
            "type": "object",
            "properties": {
                "wikipedia": {"type": "object"},
                "wikidata": {"type": "object"},
                "dbpedia": {"type": "object"}
            }
        }
    },
    "required": ["entity", "details", "sources"]
}

# Relationship-Schema
RELATIONSHIP_SCHEMA = {
    "type": "object",
    "properties": {
        "subject": {"type": "string"},
        "predicate": {"type": "string"},
        "object": {"type": "string"},
        "inferred": {"type": "string"},
        "subject_type": {"type": "string"},
        "object_type": {"type": "string"},
        "subject_id": {"type": "string"},
        "object_id": {"type": "string"},
        "subject_label": {"type": "string"},
        "object_label": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "metadata": {"type": "object"},
        "source": {"type": "string"}
    },
    "required": ["subject", "predicate", "object"]
}

# Erweitertes Relationship-Schema mit temporalen und räumlichen Dimensionen
ENHANCED_RELATIONSHIP_SCHEMA = {
    "type": "object",
    "properties": {
        "subject": {"type": "string"},
        "predicate": {"type": "string"},
        "object": {"type": "string"},
        "inferred": {"type": "string"},
        "subject_type": {"type": "string"},
        "object_type": {"type": "string"},
        "subject_id": {"type": "string"},
        "object_id": {"type": "string"},
        "subject_label": {"type": "string"},
        "object_label": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "metadata": {"type": "object"},
        "source": {"type": "string"},
        "temporal": {
            "type": "object",
            "properties": {
                "start": {"type": "string"},  # ISO-8601 Zeitformat
                "end": {"type": ["string", "null"]},  # ISO-8601 Zeitformat
                "precision": {"type": "string", "enum": ["year", "month", "day", "hour", "minute", "second"]},
                "duration": {"type": ["string", "null"]},  # ISO-8601 Dauer-Format
                "is_ongoing": {"type": "boolean"}
            },
            "required": ["start"]
        },
        "spatial": {
            "type": "object",
            "properties": {
                "location": {"type": "string"},
                "coordinates": {
                    "type": "object",
                    "properties": {
                        "latitude": {"type": "number"},
                        "longitude": {"type": "number"},
                        "altitude": {"type": ["number", "null"]}
                    },
                    "required": ["latitude", "longitude"]
                },
                "region": {"type": "string"},
                "country": {"type": "string"}
            }
        },
        "qualifiers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "value": {"type": "string"}
                },
                "required": ["type", "value"]
            }
        }
    },
    "required": ["subject", "predicate", "object"]
}

# Schema für EntityProcessingContext
ENTITY_CONTEXT_SCHEMA = {
    "type": "object",
    "properties": {
        "entity_name": {"type": "string"},
        "entity_id": {"type": "string"},
        "entity_type": {"type": ["string", "null"]},
        "original_text": {"type": ["string", "null"]},
        "processing_info": {"type": "object"},
        "service_data": {
            "type": "object",
            "properties": {
                "wikipedia": {"type": "object"},
                "wikidata": {"type": "object"},
                "dbpedia": {"type": "object"}
            }
        },
        "relationships": {
            "type": "array",
            "items": {"$ref": "#/definitions/relationship"}
        },
        "metadata": {"type": "object"},
        "statistics": {"type": "object"},
        "debug_info": {"type": "object"}
    },
    "required": ["entity_name", "entity_id"],
    "definitions": {
        "relationship": RELATIONSHIP_SCHEMA
    }
}

# Schema für Beziehungsnetzwerke
RELATIONSHIP_NETWORK_SCHEMA = {
    "type": "object",
    "properties": {
        "nodes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "label": {"type": "string"},
                    "type": {"type": "string"},
                    "properties": {"type": "object"},
                    "sources": {"type": "object"}
                },
                "required": ["id", "label"]
            }
        },
        "edges": {
            "type": "array",
            "items": {"$ref": "#/definitions/relationship"}
        },
        "metadata": {"type": "object"}
    },
    "required": ["nodes", "edges"],
    "definitions": {
        "relationship": RELATIONSHIP_SCHEMA
    }
}


def validate_service_data(data: Dict[str, Any], schema: Dict[str, Any], service_name: str) -> bool:
    """
    Validiert Service-Daten gegen ein Schema.
    
    Args:
        data: Die zu validierenden Daten
        schema: Das Schema für die Validierung
        service_name: Name des Services (für Logging)
        
    Returns:
        True, wenn die Daten valide sind, sonst False
    """
    try:
        jsonschema.validate(data, schema)
        logger.debug(f"Daten für Service '{service_name}' erfolgreich validiert")
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Validierungsfehler für '{service_name}'-Daten: {str(e)}")
        # Detaillierte Fehlerinformationen im Debug-Level ausgeben
        logger.debug(f"Validierungsfehler Details: {e.message}")
        logger.debug(f"Fehlerpfad: {'.'.join(str(p) for p in e.path)}")
        logger.debug(f"Schema-Pfad: {'.'.join(str(p) for p in e.schema_path)}")
        return False


def validate_wikipedia_data(data: Dict[str, Any]) -> bool:
    """Validates Wikipedia data.
    Accepts either the full wrapped structure {"wikipedia_data": {...}}
    or just the inner Wikipedia payload. If the latter is provided it will
    be wrapped transparently for schema validation so callers do not have
    to worry about the envelope format.
    """
    # Auto-wrap to expected schema format if necessary
    if "wikipedia_data" not in data:
        data = {"wikipedia_data": data}

    # Ensure required keys exist with sensible defaults
    wp = data["wikipedia_data"]
    if "status" not in wp:
        # Heuristik: hat URL und/oder Extract -> 'found', sonst 'not_found'
        wp["status"] = "found" if wp.get("url") else "not_found"
    return validate_service_data(data, WIKIPEDIA_SCHEMA, "wikipedia_data")
    """Validiert Wikipedia-Daten gegen das Schema"""
    return validate_service_data(data, WIKIPEDIA_SCHEMA, "wikipedia_data")


def validate_wikidata_data(data: Dict[str, Any]) -> bool:
    """Validiert Wikidata-Daten gegen das Schema"""
    return validate_service_data(data, WIKIDATA_SCHEMA, "wikidata_data")


def validate_dbpedia_data(data: Dict[str, Any]) -> (bool, str):
    """Validiert DBpedia-Daten gegen das Schema und gibt (is_valid, error_message) zurück."""
    try:
        import jsonschema
        jsonschema.validate(data, DBPEDIA_SCHEMA)
        logger.debug(f"Daten für Service 'dbpedia_data' erfolgreich validiert")
        return True, ""
    except Exception as e:
        logger.error(f"Validierungsfehler für 'dbpedia_data'-Daten: {str(e)}")
        return False, str(e)



def validate_entity_output(entity_data: Dict[str, Any]) -> bool:
    """Validiert die Ausgabedaten einer Entität"""
    return validate_service_data(entity_data, ENTITY_OUTPUT_SCHEMA, "entity_output")


def validate_relationship(relationship_data: Dict[str, Any]) -> bool:
    """Validiert eine Beziehung"""
    return validate_service_data(relationship_data, RELATIONSHIP_SCHEMA, "relationship")


def validate_enhanced_relationship(relationship_data: Dict[str, Any]) -> bool:
    """Validiert eine erweiterte Beziehung mit temporalen und räumlichen Dimensionen"""
    return validate_service_data(relationship_data, ENHANCED_RELATIONSHIP_SCHEMA, "enhanced_relationship")


def validate_entity_context(context_data: Dict[str, Any]) -> bool:
    """Validiert EntityProcessingContext-Daten"""
    return validate_service_data(context_data, ENTITY_CONTEXT_SCHEMA, "entity_context")


def validate_relationship_network(network_data: Dict[str, Any]) -> bool:
    """Validiert ein Beziehungsnetzwerk"""
    return validate_service_data(network_data, RELATIONSHIP_NETWORK_SCHEMA, "relationship_network")
