"""
Format converter utilities for the Entity Extractor.

This module provides functions for converting between different output formats.
"""

def convert_to_legacy_format(result):
    """
    Convert the new entity format to the legacy format.
    
    Args:
        result: The result from extract_and_link_entities
        
    Returns:
        A list of entities in the legacy format
    """
    if not result or "entities" not in result:
        return []
        
    legacy_entities = []
    
    for entity in result["entities"]:
        legacy_entity = {
            "entity": entity.get("name", ""),
            "details": {
                "typ": entity.get("type", ""),
                "citation": result.get("text", ""),
                "citation_start": 0,
                "citation_end": len(result.get("text", ""))
            },
            "sources": {}
        }
        
        # Add Wikipedia source if available
        if "wikipedia_data" in entity:
            wikipedia_data = entity.get("wikipedia_data", {})
            legacy_entity["sources"]["wikipedia"] = {}
            
            # Copy the most important fields
            if "url" in wikipedia_data:
                legacy_entity["sources"]["wikipedia"]["url"] = wikipedia_data["url"]
            if "extract" in wikipedia_data:
                legacy_entity["sources"]["wikipedia"]["extract"] = wikipedia_data["extract"]
            
            # Also set the old fields for backward compatibility
            if "url" in wikipedia_data:
                entity["wikipedia_url"] = wikipedia_data["url"]
            if "extract" in wikipedia_data:
                entity["wikipedia_extract"] = wikipedia_data["extract"]
        
        # Add Wikidata source if available
        if "wikidata_data" in entity:
            wikidata_data = entity.get("wikidata_data", {})
            legacy_entity["sources"]["wikidata"] = {}
            
            # Copy the most important fields
            if "id" in wikidata_data:
                legacy_entity["sources"]["wikidata"]["id"] = wikidata_data["id"]
            if "description" in wikidata_data:
                legacy_entity["sources"]["wikidata"]["description"] = wikidata_data["description"]
            if "types" in wikidata_data:
                legacy_entity["sources"]["wikidata"]["types"] = wikidata_data["types"]
            
            # Also set the old fields for backward compatibility
            if "id" in wikidata_data:
                entity["wikidata_id"] = wikidata_data["id"]
            if "description" in wikidata_data:
                entity["wikidata_description"] = wikidata_data["description"]
            if "types" in wikidata_data:
                entity["wikidata_types"] = wikidata_data["types"]
        
        # Add DBpedia source if available
        if "dbpedia_data" in entity:
            dbpedia_data = entity.get("dbpedia_data", {})
            legacy_entity["sources"]["dbpedia"] = {}
            
            # Copy the most important fields
            if "resource_uri" in dbpedia_data:
                legacy_entity["sources"]["dbpedia"]["resource_uri"] = dbpedia_data["resource_uri"]
            if "language" in dbpedia_data:
                legacy_entity["sources"]["dbpedia"]["language"] = dbpedia_data["language"]
            if "abstract" in dbpedia_data:
                legacy_entity["sources"]["dbpedia"]["abstract"] = dbpedia_data["abstract"]
            if "types" in dbpedia_data:
                legacy_entity["sources"]["dbpedia"]["types"] = dbpedia_data["types"]
            
            # Also set the old fields for backward compatibility
            if "resource_uri" in dbpedia_data:
                entity["dbpedia_uri"] = dbpedia_data["resource_uri"]
            if "language" in dbpedia_data:
                entity["dbpedia_language"] = dbpedia_data["language"]
            if "abstract" in dbpedia_data:
                entity["dbpedia_abstract"] = dbpedia_data["abstract"]
            if "types" in dbpedia_data:
                entity["dbpedia_types"] = dbpedia_data["types"]
        
        legacy_entities.append(legacy_entity)
    
    return legacy_entities
