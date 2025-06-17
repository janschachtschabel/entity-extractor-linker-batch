"""
entity_linker/converters.py

Enthält Funktionen zur Konvertierung zwischen verschiedenen Datenformaten für das Entity-Linking.
"""

import logging
import uuid
from typing import List, Dict, Any, Optional

from entityextractor.models.entity import Entity
from entityextractor.models.base import LanguageCode

def dict_to_entity(entity_dict: Dict[str, Any], language: str = "de") -> Entity:
    """
    Konvertiert ein Entity-Dictionary in ein Entity-Objekt.
    
    Args:
        entity_dict: Dictionary mit Entity-Informationen
        language: Sprachcode (Standard: "de")
        
    Returns:
        Entity-Objekt
    """
    # UUID entweder aus dem Dictionary nehmen oder neu generieren
    entity_id = entity_dict.get("id") or str(uuid.uuid4())
    name = entity_dict.get("name", "")
    
    # Entity erstellen
    entity = Entity(id=entity_id, name=name)
    
    # Primäre Sprache festlegen
    lang_code = LanguageCode.DE if language == "de" else LanguageCode.EN
    entity.label.set(lang_code, name)
    
    # Weitere Eigenschaften übertragen, falls vorhanden
    if "type" in entity_dict:
        entity.type = entity_dict["type"]
    
    # Wenn Wikipedia-Daten vorhanden sind, diese hinzufügen
    if "wikipedia_url" in entity_dict and entity_dict["wikipedia_url"]:
        entity.add_source("wikipedia", {
            "url": entity_dict["wikipedia_url"],
            "title": entity_dict.get("wikipedia_title", name),
            "extract": entity_dict.get("wikipedia_extract", ""),
            "status": "found"
        })
    
    # Wenn Wikidata-ID vorhanden ist, diese hinzufügen
    if "wikidata_id" in entity_dict and entity_dict["wikidata_id"]:
        entity.wikidata_id = entity_dict["wikidata_id"]
    
    # Wenn DBpedia-Daten vorhanden sind, diese hinzufügen
    if "dbpedia" in entity_dict and entity_dict["dbpedia"]:
        dbpedia_data = entity_dict["dbpedia"]
        entity.add_source("dbpedia", {
            "resource_uri": dbpedia_data.get("resource_uri", ""),
            "abstract": dbpedia_data.get("abstract", ""),
            "subjects": dbpedia_data.get("subjects", []),
            "types": dbpedia_data.get("types", []),
            "categories": dbpedia_data.get("categories", []),
            "part_of": dbpedia_data.get("part_of", []),
            "has_parts": dbpedia_data.get("has_parts", []),
            "status": "found" if dbpedia_data.get("resource_uri") else "not_found"
        })
    
    return entity

def entity_to_dict(entity: Entity) -> Dict[str, Any]:
    """
    Konvertiert ein Entity-Objekt in ein Dictionary.
    
    Args:
        entity: Entity-Objekt
        
    Returns:
        Dictionary mit Entity-Informationen
    """
    # Basis-Informationen
    result = {
        "id": entity.id,
        "name": entity.name
    }
    
    # Typ hinzufügen, wenn vorhanden
    if entity.type:
        result["type"] = entity.type
    
    # Wikipedia-Informationen hinzufügen, wenn vorhanden
    if entity.has_source("wikipedia"):
        wiki_source = entity.sources.get("wikipedia")
        result["wikipedia_url"] = getattr(wiki_source, "url", "") if hasattr(wiki_source, "url") else ""
        result["wikipedia_title"] = getattr(wiki_source, "title", entity.name) if hasattr(wiki_source, "title") else entity.name
        result["wikipedia_extract"] = getattr(wiki_source, "extract", "") if hasattr(wiki_source, "extract") else ""
        result["wikipedia_categories"] = getattr(wiki_source, "categories", []) if hasattr(wiki_source, "categories") else []
        
        # Interne Links
        if hasattr(wiki_source, "internal_links"):
            result["wikipedia_internal_links"] = wiki_source.internal_links
        
        # Thumbnail/Bild
        if hasattr(wiki_source, "thumbnail"):
            result["wikipedia_thumbnail"] = wiki_source.thumbnail
        
        # Detaillierte Bildinformationen
        if hasattr(wiki_source, "image_info") and wiki_source.image_info:
            image_info = wiki_source.image_info
            result["wikipedia_image_info"] = {
                "url": getattr(image_info, "url", "") if hasattr(image_info, "url") else "",
                "width": getattr(image_info, "width", 0) if hasattr(image_info, "width") else 0,
                "height": getattr(image_info, "height", 0) if hasattr(image_info, "height") else 0,
                "mime": getattr(image_info, "mime", "") if hasattr(image_info, "mime") else "",
                "title": getattr(image_info, "title", "") if hasattr(image_info, "title") else ""
            }
        
        # Wikidata-ID (kann aus Wikipedia-Daten stammen)
        if hasattr(wiki_source, "wikidata_id") and wiki_source.wikidata_id and not result.get("wikidata_id"):
            result["wikidata_id"] = wiki_source.wikidata_id
    
    # Wikidata-Informationen hinzufügen, wenn vorhanden
    if entity.wikidata_id:
        result["wikidata_id"] = entity.wikidata_id
    
    if entity.has_source("wikidata"):
        wikidata_source = entity.sources.get("wikidata")
        
        # Standard-URL erstellen, falls keine vorhanden
        default_url = f"https://www.wikidata.org/entity/{entity.wikidata_id}"
        wikidata_url = getattr(wikidata_source, "url", default_url) if hasattr(wikidata_source, "url") else default_url
        
        # Standard-Werte für die verschiedenen Attribute
        empty_dict = {}
        empty_list = []
        
        # Ontologische Informationen mit Standardwerten
        ontology_default = {
            "instance_of": empty_list,
            "subclass_of": empty_list,
            "part_of": empty_list,
            "has_part": empty_list,
            "facet_of": empty_list
        }
        
        # Semantische Informationen mit Standardwerten
        semantics_default = {
            "main_subject": empty_list,
            "main_subject_of": empty_list,
            "field_of_work": empty_list,
            "applies_to": empty_list
        }
        
        # Medien-Informationen mit Standardwerten
        media_default = {
            "image": None,
            "image_url": None
        }
        
        result["wikidata"] = {
            "id": entity.wikidata_id,
            "url": wikidata_url,
            "labels": getattr(wikidata_source, "labels", empty_dict) if hasattr(wikidata_source, "labels") else empty_dict,
            "descriptions": getattr(wikidata_source, "descriptions", empty_dict) if hasattr(wikidata_source, "descriptions") else empty_dict,
            "aliases": getattr(wikidata_source, "aliases", empty_dict) if hasattr(wikidata_source, "aliases") else empty_dict,
            "claims": getattr(wikidata_source, "claims", empty_dict) if hasattr(wikidata_source, "claims") else empty_dict,
            "sitelinks": getattr(wikidata_source, "sitelinks", empty_dict) if hasattr(wikidata_source, "sitelinks") else empty_dict,
            
            # Ontologische Informationen
            "ontology": getattr(wikidata_source, "ontology", ontology_default) if hasattr(wikidata_source, "ontology") else ontology_default,
            
            # Semantische Informationen
            "semantics": getattr(wikidata_source, "semantics", semantics_default) if hasattr(wikidata_source, "semantics") else semantics_default,
            
            # Medien-Informationen
            "media": getattr(wikidata_source, "media", media_default) if hasattr(wikidata_source, "media") else media_default
        }
    
    # DBpedia-Informationen hinzufügen, wenn vorhanden
    if entity.has_source("dbpedia"):
        dbpedia_source = entity.sources.get("dbpedia")
        empty_list = []
        
        result["dbpedia"] = {
            "resource_uri": getattr(dbpedia_source, "uri", "") if hasattr(dbpedia_source, "uri") else "",
            "abstract": getattr(dbpedia_source, "abstract", "") if hasattr(dbpedia_source, "abstract") else "",
            "subjects": getattr(dbpedia_source, "subjects", empty_list) if hasattr(dbpedia_source, "subjects") else empty_list,
            "types": getattr(dbpedia_source, "types", empty_list) if hasattr(dbpedia_source, "types") else empty_list,
            "categories": getattr(dbpedia_source, "categories", empty_list) if hasattr(dbpedia_source, "categories") else empty_list,
            "part_of": getattr(dbpedia_source, "part_of", empty_list) if hasattr(dbpedia_source, "part_of") else empty_list,
            "has_parts": getattr(dbpedia_source, "has_parts", empty_list) if hasattr(dbpedia_source, "has_parts") else empty_list,
            "gnd_id": getattr(dbpedia_source, "gndId", "") if hasattr(dbpedia_source, "gndId") else "",
            "homepage": getattr(dbpedia_source, "homepage", "") if hasattr(dbpedia_source, "homepage") else "",
            "thumbnail": getattr(dbpedia_source, "thumbnail", "") if hasattr(dbpedia_source, "thumbnail") else "",
            "coordinates": None
        }
        
        # Koordinaten nur hinzufügen, wenn latitude und longitude vorhanden sind
        if hasattr(dbpedia_source, "latitude") and hasattr(dbpedia_source, "longitude"):
            if dbpedia_source.latitude is not None and dbpedia_source.longitude is not None:
                result["dbpedia"]["coordinates"] = {
                    "latitude": dbpedia_source.latitude,
                    "longitude": dbpedia_source.longitude
                }
    
    return result

def dicts_to_entities(entity_dicts: List[Dict[str, Any]], language: str = "de") -> List[Entity]:
    """
    Konvertiert eine Liste von Entity-Dictionaries in Entity-Objekte.
    
    Args:
        entity_dicts: Liste von Entity-Dictionaries
        language: Sprachcode (Standard: "de")
        
    Returns:
        Liste von Entity-Objekten
    """
    return [dict_to_entity(entity_dict, language) for entity_dict in entity_dicts]

def entities_to_dicts(entities: List[Entity]) -> List[Dict[str, Any]]:
    """
    Konvertiert eine Liste von Entity-Objekten in Dictionaries.
    
    Args:
        entities: Liste von Entity-Objekten
        
    Returns:
        Liste von Entity-Dictionaries
    """
    return [entity_to_dict(entity) for entity in entities]
