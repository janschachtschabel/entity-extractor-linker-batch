"""
result_formatter.py

Functions for formatting extraction results into a standardized format.
Supports both the traditional format and the new context-based architecture.
"""

import uuid
import datetime
import copy
from collections import Counter
from typing import Dict, List, Any, Optional, Union

from loguru import logger

from entityextractor.utils.source_utils import safe_get, ensure_dict_format
from entityextractor.core.context import EntityProcessingContext

# Logger is imported from loguru


def format_context_to_result(context: EntityProcessingContext) -> Dict[str, Any]:
    """
    Formats an EntityProcessingContext into a standardized result object.
    This function is optimized for the new context-based architecture.
    
    Args:
        context: The EntityProcessingContext with all processed data
        
    Returns:
        A standardized result object with entities and optionally relationships
    """
    # Basisstruktur erstellen
    result = {
        "entities": [],
        "relationships": [],
        "meta": {
            "entity_name": context.entity_name,
            "processing_time": context.get_processing_time(),
            "services_used": list(context.get_available_services()),
            "config": {k: v for k, v in context.config.items() if isinstance(v, (str, int, float, bool, list))}
        }
    }
    
    # Original text, if available
    original_text = context.get_original_text()
    if original_text:
        result["original_text"] = original_text
    
    # Process entity and add to result list
    formatted_entity = format_entity_from_context(context)
    if formatted_entity:
        result["entities"].append(formatted_entity)
        
    # Relationships, if available
    relationships = context.get_relationships()
    if relationships:
        result["relationships"] = relationships
    
    # Debug information, if enabled and available
    if context.config.get("DEBUG", False) and context.debug_info:
        result["debug"] = context.debug_info
    
    # Processing information, if available
    if context.processing_info:
        result["processing_info"] = context.processing_info
    
    return result


def format_entity_from_context(context: EntityProcessingContext) -> Dict[str, Any]:
    """
    Formats an EntityProcessingContext object into a standardized entity result format.
    
    Args:
        context: The EntityProcessingContext to format
        
    Returns:
        A standardized entity object with details and sources
    """
    # Skip if no entity name
    if not context.entity_name:
        return None
    
    # Create base entity object
    entity = {
        "id": context.entity_id,
        "entity": context.entity_name,
        "details": {},
        "sources": {}
    }
    
    # Add minimal metadata to details section - nur grundlegende Informationen
    # 1. Entity type if available
    if context.entity_type:
        entity["details"]["typ"] = context.entity_type
    
    # 2. Inferred status
    entity["details"]["inferred"] = context.get_processing_info("inferred", "explicit")
    
    # 3. Zitationsinformationen hinzufügen, falls vorhanden
    citation = context.get_citation()
    if citation:
        entity["details"]["citation"] = citation
        
        # Prüfe, ob Start- und Endposition der Zitation in processing_data vorhanden sind
        citation_start = context.get_processing_info("citation_start")
        citation_end = context.get_processing_info("citation_end")
        
        if citation_start is not None:
            entity["details"]["citation_start"] = citation_start
        if citation_end is not None:
            entity["details"]["citation_end"] = citation_end
    
    # Entferne alle anderen Details, die in die jeweiligen Service-Bereiche gehören
    
    # Add sources from output_data if available
    if "sources" in context.output_data and isinstance(context.output_data["sources"], dict):
        logger.info(f"Taking sources for '{context.entity_name}' directly from output_data: {list(context.output_data['sources'].keys())}")
        entity["sources"] = copy.deepcopy(context.output_data["sources"])
        
        # Stelle sicher, dass DBpedia-Daten unter sources.dbpedia stehen und nicht als separates Feld
        if "dbpedia" in context.output_data and "dbpedia" not in entity["sources"]:
            entity["sources"]["dbpedia"] = copy.deepcopy(context.output_data["dbpedia"])
            
        # Entferne unnötige Felder
        if "wikipedia" in entity["sources"] and "pageid" in entity["sources"]["wikipedia"]:
            del entity["sources"]["wikipedia"]["pageid"]
    else:
        # Manually extract sources from context
        logger.warning(f"No sources in output_data for '{context.entity_name}', trying manual extraction")
        
        # Wikipedia source
        if "wikipedia" in context.processed_by_services:
            wikipedia_data = context.get_service_data("wikipedia")
            if wikipedia_data:
                entity["sources"]["wikipedia"] = {
                    "label": wikipedia_data.get("label", ""),
                    "url": wikipedia_data.get("url", ""),
                    "extract": wikipedia_data.get("extract", ""),
                    "categories": wikipedia_data.get("categories", []),
                    "internal_links": wikipedia_data.get("internal_links", []),
                    "wikidata_id": wikipedia_data.get("wikidata_id", ""),
                    "pageid": wikipedia_data.get("pageid"),
                    "thumbnail": wikipedia_data.get("thumbnail", ""),
                    "language": wikipedia_data.get("language", ""),
                    "redirected_from": wikipedia_data.get("redirected_from", ""),
                    "status": wikipedia_data.get("status", "not_found"),
                    "source": wikipedia_data.get("source", ""),
                }
                
                # Add fallback information if available
                if "needs_fallback" in wikipedia_data:
                    entity["sources"]["wikipedia"]["needs_fallback"] = wikipedia_data["needs_fallback"]
                if "fallback_attempts" in wikipedia_data:
                    entity["sources"]["wikipedia"]["fallback_attempts"] = wikipedia_data["fallback_attempts"]
                if "fallback_source" in wikipedia_data:
                    entity["sources"]["wikipedia"]["fallback_source"] = wikipedia_data["fallback_source"]
                
                logger.info(f"Wikipedia data for '{context.entity_name}' added manually")
        
        # Wikidata source
        if "wikidata" in context.processed_by_services:
            wikidata_data = context.get_service_data("wikidata")
            if wikidata_data:
                logger.info(f"Wikidata data for '{context.entity_name}' in format_entity_from_context: {list(wikidata_data.keys()) if wikidata_data else 'None'}")
                
                entity["sources"]["wikidata"] = {
                    "id": wikidata_data.get("id", ""),
                    "uri": wikidata_data.get("uri", ""),
                    "label": wikidata_data.get("label", ""),
                    "description": wikidata_data.get("description", ""),
                    "types": wikidata_data.get("types", []),
                    "part_of": wikidata_data.get("part_of", []),
                    "has_parts": wikidata_data.get("has_parts", []),
                    "aliases": wikidata_data.get("aliases", []),
                    "status": wikidata_data.get("status", "not_found"),
                }
                
                logger.info(f"Wikidata data for '{context.entity_name}' added directly")
        
        # DBpedia source
        if "dbpedia" in context.processed_by_services:
            dbpedia_data = context.get_service_data("dbpedia")
            if dbpedia_data:
                entity["sources"]["dbpedia"] = {
                    "uri": dbpedia_data.get("uri", ""),
                    "label": dbpedia_data.get("label", ""),
                    "abstract": dbpedia_data.get("abstract", ""),
                    "categories": dbpedia_data.get("categories", []),
                    "types": dbpedia_data.get("types", []),
                    "part_of": dbpedia_data.get("part_of", []),
                    "has_parts": dbpedia_data.get("has_parts", []),
                    "geo": dbpedia_data.get("geo", {}),
                    "wiki": dbpedia_data.get("wiki", ""),
                    "homepage": dbpedia_data.get("homepage", ""),
                    "image": dbpedia_data.get("image", ""),
                    "status": dbpedia_data.get("status", "not_found"),
                }
                
                logger.info(f"DBpedia data for '{context.entity_name}' added directly")
    
    return entity
    
    # Final check of the sources
    logger.info(f"Final sources for '{entity_name}': {list(formatted_entity['sources'].keys())}")
    for source_name, source_data in formatted_entity['sources'].items():
        logger.info(f"  - {source_name}: {list(source_data.keys()) if isinstance(source_data, dict) else 'Not a dictionary'}")
    
    return formatted_entity


def format_contexts_to_result(contexts: List[EntityProcessingContext], 
                            original_text: Optional[str] = None,
                            relationships: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Formats multiple EntityProcessingContext objects into a common result format.
    
    Args:
        contexts: List of EntityProcessingContext objects
        original_text: Optional original text used for extraction
        relationships: Optional list of relationships between entities
        
    Returns:
        A standardized result object with all entities and relationships
    """
    # Create empty result object
    result = {
        "entities": [],
        "relationships": [],
        "meta": {
            "entity_count": len(contexts),
            "services_used": set(),
            "processing_time": 0.0,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    }
    
    # Add original text if available
    if original_text:
        result["original_text"] = original_text
    else:
        # Try to extract the original text from one of the contexts
        for context in contexts:
            if context.original_text:
                result["original_text"] = context.original_text
                break
    
    # Collect all relationships from all contexts
    all_relationships = []
    if relationships:
        all_relationships.extend(relationships)
    
    # Process entities and relationships
    processing_time_total = 0.0
    entity_id_map = {}  # To map entity names to IDs
    
    # Collect statistics data for top-level fields
    wikipedia_categories = Counter()
    wikidata_types = Counter()
    wikidata_part_of = Counter()
    wikidata_has_parts = Counter()
    dbpedia_subjects = Counter()
    
    for context in contexts:
        # Debug output before formatting
        logger.info(f"Formatting entity '{context.entity_name}' with services: {context.processed_by_services}")
        if "wikidata" in context.processed_by_services:
            wikidata_data = context.get_service_data("wikidata")
            logger.info(f"Wikidata data before formatting for '{context.entity_name}': {list(wikidata_data.keys()) if wikidata_data else 'None'}")
            logger.info(f"Wikidata in sources before formatting: {list(context.output_data['sources'].keys()) if 'sources' in context.output_data else 'No sources'}")
            if 'sources' in context.output_data and 'wikidata' in context.output_data['sources']:
                logger.info(f"Wikidata data in sources before formatting: {list(context.output_data['sources']['wikidata'].keys()) if context.output_data['sources']['wikidata'] else 'Empty dict'}")
        
        # Format entity from old format to new format
        entity = format_entity_from_context(context)
        if entity:
            # Debug output after formatting
            logger.info(f"Formatted entity '{entity['entity']}' with sources: {list(entity['sources'].keys()) if 'sources' in entity else 'No sources'}")
            if 'sources' in entity and 'wikidata' in entity['sources']:
                logger.info(f"Wikidata data after formatting: {list(entity['sources']['wikidata'].keys())}")
            else:
                logger.warning(f"No Wikidata data in the formatted entity '{entity['entity']}'!")
            
            result["entities"].append(entity)
            result["meta"]["services_used"].update(context.processed_by_services)
            
            # Store entity ID for later reference
            entity_id_map[context.entity_name.lower()] = entity["id"]
            
            # Sum up processing time
            processing_time = context.get_processing_info("processing_time", 0.0)
            processing_time_total += processing_time
            
            # Add relationships from the context
            context_relationships = context.get_relationships()
            if context_relationships:
                all_relationships.extend(context_relationships)
                
            # Collect statistics data for top-level fields
            # 1. Wikipedia categories
            if 'sources' in entity and 'wikipedia' in entity['sources']:
                categories = entity['sources']['wikipedia'].get('categories', [])
                for cat in categories:
                    wikipedia_categories[cat] += 1
            
            # 2. Wikidata types
            if 'sources' in entity and 'wikidata' in entity['sources']:
                types = entity['sources']['wikidata'].get('types', [])
                for typ in types:
                    wikidata_types[typ] += 1
                    
                # 3. Wikidata part_of
                part_of_items = entity['sources']['wikidata'].get('part_of', [])
                for item in part_of_items:
                    wikidata_part_of[item] += 1
                    
                # 4. Wikidata has_parts
                has_parts_items = entity['sources']['wikidata'].get('has_parts', [])
                for item in has_parts_items:
                    wikidata_has_parts[item] += 1
            
            # 5. DBpedia subjects
            if 'sources' in entity and 'dbpedia' in entity['sources']:
                subjects = entity['sources']['dbpedia'].get('categories', [])
                for subject in subjects:
                    dbpedia_subjects[subject] += 1
    
    # Deduplicate relationships and add IDs
    unique_relationships = []
    seen_relationships = set()
    
    for rel in all_relationships:
        # Create key for deduplication
        rel_key = f"{rel.get('subject', '').lower()}|{rel.get('predicate', '').lower()}|{rel.get('object', '').lower()}"
        
        if rel_key not in seen_relationships:
            seen_relationships.add(rel_key)
            
            # Add IDs if possible
            if 'subject_id' not in rel and rel.get('subject', '').lower() in entity_id_map:
                rel['subject_id'] = entity_id_map[rel.get('subject', '').lower()]
                
            if 'object_id' not in rel and rel.get('object', '').lower() in entity_id_map:
                rel['object_id'] = entity_id_map[rel.get('object', '').lower()]
            
            unique_relationships.append(rel)
    
    # Add relationships to the result
    result["relationships"] = unique_relationships
    
    # Finalize meta information
    result["meta"]["services_used"] = list(result["meta"]["services_used"])
    result["meta"]["processing_time"] = processing_time_total
    result["meta"]["relationship_count"] = len(unique_relationships)
    
    # Add top-level statistics fields
    # 1. Top Wikipedia categories
    top_wikipedia_categories = sorted(wikipedia_categories.items(), key=lambda x: -x[1])[:10]
    result["top_wikipedia_categories"] = [{"category": cat, "count": count} for cat, count in top_wikipedia_categories]
    
    # 2. Top Wikidata types
    top_wikidata_types = sorted(wikidata_types.items(), key=lambda x: -x[1])[:10]
    result["top_wikidata_types"] = [{"type": typ, "count": count} for typ, count in top_wikidata_types]
    
    # 3. Top Wikidata part_of
    top_wikidata_part_of = sorted(wikidata_part_of.items(), key=lambda x: -x[1])[:10]
    result["top_wikidata_part_of"] = [{"part_of": item, "count": count} for item, count in top_wikidata_part_of]
    
    # 4. Top Wikidata has_parts
    top_wikidata_has_parts = sorted(wikidata_has_parts.items(), key=lambda x: -x[1])[:10]
    result["top_wikidata_has_parts"] = [{"has_parts": item, "count": count} for item, count in top_wikidata_has_parts]
    
    # 5. Top DBpedia subjects
    top_dbpedia_subjects = sorted(dbpedia_subjects.items(), key=lambda x: -x[1])[:10]
    result["top_dbpedia_subjects"] = [{"subject": subject, "count": count} for subject, count in top_dbpedia_subjects]
    
    # Initialize knowledgegraph_visualisation field if not present
    if "knowledgegraph_visualisation" not in result:
        result["knowledgegraph_visualisation"] = []
    
    return result

def format_results(entities, relationships, original_text) -> Dict[str, Any]:
    """
    Formats the extracted entities and relationships into a standardized result object.
    All IDs are UUID4. Relationships reference entities exclusively via subject_id and object_id.
    
    This function is intended for backward compatibility with older code parts.
    For new implementations, format_context_to_result should be used.
    """
    """
    Formatiert die extrahierten Entitäten und Beziehungen in ein standardisiertes Ergebnisobjekt.
    Alle IDs sind UUID4. Beziehungen referenzieren Entitäten ausschließlich über subject_id und object_id. Labels dienen nur der Anzeige.
    """
    # Optional validation: IDs available?
    result = {"entities": [], "relationships": []}
    
    # Check and format relationships
    if relationships and isinstance(relationships, list):
        # Ensure that relationships are correctly formatted
        for rel in relationships:
            if isinstance(rel, dict) and "subject" in rel and "predicate" in rel and "object" in rel:
                result["relationships"].append(rel)
                
        # Debug output
        logger.info(f"Formatted relationships: {len(result['relationships'])}")
        if result["relationships"]:
            logger.info(f"Example relationship: {result['relationships'][0]}")
    else:
        logger.info("No valid relationships found for formatting.")
    
    # Format entities
    for entity in entities:
        # Support for different entity structures
        if isinstance(entity, dict):
            # New data structure uses 'entity' instead of 'name'
            name = entity.get("entity", entity.get("name", ""))
            
            # Extract type from different possible structures
            if "details" in entity and "typ" in entity.get("details", {}):
                entity_type = entity.get("details", {}).get("typ", "")
            else:
                entity_type = entity.get("type", "")
                
            # Extract inference type
            if "details" in entity and "inferred" in entity.get("details", {}):
                inferred = entity.get("details", {}).get("inferred", "explicit")
            else:
                inferred = entity.get("inferred", "explicit")
        elif isinstance(entity, str):
            # Fallback if the entity is directly passed as a string
            name = entity
            entity_type = ""
            inferred = "explicit"
        else:
            # Unknown format, skip
            logger.warning(f"Unknown entity format: {type(entity)}")
            continue
        
        # Extract or generate a citation from the text
        # Support for different entity structures for citation
        if isinstance(entity, dict):
            # Check different possible structures for the citation
            if "citation" in entity:
                citation = entity.get("citation", original_text)
            elif "details" in entity and "citation" in entity.get("details", {}):
                citation = entity.get("details", {}).get("citation", original_text)
            else:
                citation = original_text
        else:
            citation = original_text
            
        # Calculate the position of the citation in the text
        citation_start = original_text.find(citation) if citation != original_text else 0
        citation_end = citation_start + len(citation) if citation_start != -1 else len(original_text)
        
        # Create the formatted entity
        formatted_entity = {
            "entity": name,
            "details": {
                "type": entity_type,
                "inferred": inferred,
                "citation": citation,
                "citation_start": citation_start,
                "citation_end": citation_end
            },
            "sources": {}
        }
        
        # Source information (Wikipedia, Wikidata, etc.)
        # Support for different entity structures for sources
        if isinstance(entity, dict) and "sources" in entity and isinstance(entity.get("sources"), dict):
            # Iterate over all available sources
            for source_name, source_obj in entity["sources"].items():
                # Convert SourceData to a complete dictionary with all attributes
                if hasattr(source_obj, "to_dict"):
                    # Use to_dict if available
                    source_data = source_obj.to_dict()
                    logger.debug(f"Source {source_name} converted with to_dict: {list(source_data.keys()) if source_data else 'Empty dict'}")
                else:
                    # Fallback: Ensure that all attributes and data are copied
                    source_data = {}
                    
                    # Basic attributes for Wikipedia
                    if source_name == "wikipedia":
                        for attr in ["url", "title", "extract", "categories", "internal_links", "thumbnail", "wikidata_id"]:
                            value = safe_get(source_obj, attr)
                            if value is not None:
                                source_data[attr] = value
                    
                    # Basic attributes for Wikidata
                    elif source_name == "wikidata":
                        for attr in ["id", "url", "label", "description", "aliases", "claims", "sitelinks", "official_website", "gnd_id"]:
                            value = safe_get(source_obj, attr)
                            if value is not None:
                                source_data[attr] = value
                    
                    # Generic case for other sources
                    else:
                        for attr in ["id", "url"]:
                            value = safe_get(source_obj, attr)
                            if value is not None:
                                source_data[attr] = value
                    
                    # If data is available, also copy it
                    data = safe_get(source_obj, "data", {})
                    if data:
                        for key, value in data.items():
                            if key not in source_data:
                                source_data[key] = value
                    
                    logger.debug(f"Source {source_name} converted manually: {list(source_data.keys()) if source_data else 'Empty dict'}")
                
                # Add the source to the formatted result
                formatted_entity["sources"][source_name] = source_data
                logger.info(f"Source {source_name} added to the formatted entity '{name}'")
            
            # Debug output
            logger.info(f"Formatted entity '{name}' has the following sources: {list(formatted_entity['sources'].keys())}")
            for source_name in formatted_entity['sources']:
                logger.info(f"  - {source_name}: {list(formatted_entity['sources'][source_name].keys()) if formatted_entity['sources'][source_name] else 'Empty dict'}")
        else:
            logger.warning(f"No sources found for entity '{name}' or invalid format")
            
        # Legacy support for Wikipedia attributes directly in the entity
        if isinstance(entity, dict) and "wikipedia_url" in entity and entity["wikipedia_url"]:
            if "wikipedia" not in formatted_entity["sources"]:
                formatted_entity["sources"]["wikipedia"] = {}
            formatted_entity["sources"]["wikipedia"]["url"] = entity["wikipedia_url"]
            if entity.get("wikipedia_title"):
                formatted_entity["sources"]["wikipedia"]["title"] = entity["wikipedia_title"]
            if entity.get("wikipedia_extract"):
                formatted_entity["sources"]["wikipedia"]["extract"] = entity["wikipedia_extract"]
        # Fallback for flat structure (old entity structure)
        elif isinstance(entity, dict) and entity.get("wikipedia_url"):
            # Fallback for flat structure
            wiki_source = formatted_entity["sources"].setdefault("wikipedia", {})
            wiki_source["label"] = entity.get("wikipedia_title", name)
            wiki_source["url"] = entity.get("wikipedia_url", "")
            if entity.get("wikipedia_extract"):
                wiki_source["extract"] = entity.get("wikipedia_extract", "")
            if entity.get("wikipedia_categories"):
                wiki_source["categories"] = entity.get("wikipedia_categories", [])
            if entity.get("wikipedia_internal_links"):
                wiki_source["internal_links"] = entity.get("wikipedia_internal_links", [])
            if entity.get("wikipedia_thumbnail"):
                wiki_source["thumbnail"] = entity.get("wikipedia_thumbnail", "")
            if entity.get("wikipedia_wikidata_id"):
                wiki_source["wikidata_id"] = entity.get("wikipedia_wikidata_id", "")
        
        # Wikidata information
        # Support for different entity structures for sources
        if isinstance(entity, dict):
            if "sources" in entity and isinstance(entity.get("sources"), dict) and "wikidata" in entity.get("sources", {}):
                # New structure with direct sources dict
                wikidata_source_obj = entity["sources"]["wikidata"]
                
                # Convert SourceData to a complete dictionary with all attributes
                if hasattr(wikidata_source_obj, "to_dict"):
                    # Use to_dict if available
                    wikidata_source = wikidata_source_obj.to_dict()
                else:
                    # Fallback: Ensure that all attributes and data are copied
                    wikidata_source = {}
                    # Basic attributes
                    for attr in ["id", "url", "labels", "descriptions", "aliases", "claims", "sitelinks", "ontology", "semantics", "media"]:
                        value = safe_get(wikidata_source_obj, attr)
                        if value is not None:
                            wikidata_source[attr] = value
                    # If data is available, also copy it
                    data = safe_get(wikidata_source_obj, "data", {})
                    if data:
                        for key, value in data.items():
                            if key not in wikidata_source:
                                wikidata_source[key] = value
            
                formatted_entity["sources"]["wikidata"] = wikidata_source
            # Legacy format: Complete Wikidata object at the top level
            elif "wikidata" in entity and isinstance(entity["wikidata"], dict) and entity["wikidata"].get("id"):
                formatted_entity["sources"]["wikidata"] = entity["wikidata"].copy()
            # Fallback for flat structure
            elif "wikidata_id" in entity:
                wikidata_source = formatted_entity["sources"].setdefault("wikidata", {})
                wikidata_source["id"] = entity.get("wikidata_id", "")
                wikidata_source["url"] = entity.get("wikidata_url", f"https://www.wikidata.org/entity/{entity.get('wikidata_id')}")
                
                # External links
                if entity.get("wikidata_labels"):
                    wikidata_source["labels"] = entity.get("wikidata_labels", {})
                if entity.get("wikidata_descriptions"):
                    wikidata_source["descriptions"] = entity.get("wikidata_descriptions", {})
                if entity.get("wikidata_aliases"):
                    wikidata_source["aliases"] = entity.get("wikidata_aliases", {})
                if entity.get("wikidata_claims"):
                    wikidata_source["claims"] = entity.get("wikidata_claims", {})
                if entity.get("wikidata_ontology"):
                    wikidata_source["ontology"] = entity.get("wikidata_ontology", {})
                if entity.get("wikidata_semantics"):
                    wikidata_source["semantics"] = entity.get("wikidata_semantics", {})
                if entity.get("wikidata_images"):
                    wikidata_source["images"] = entity.get("wikidata_images", [])
                if entity.get("wikidata_facet_of"):
                    wikidata_source["facet_of"] = entity.get("wikidata_facet_of", [])
                
                # Semantische Beziehungen
                if entity.get("wikidata_main_subject"):
                    wikidata_source["main_subject"] = entity.get("wikidata_main_subject", [])
                if entity.get("wikidata_field_of_work"):
                    wikidata_source["field_of_work"] = entity.get("wikidata_field_of_work", [])
                if entity.get("wikidata_applies_to"):
                    wikidata_source["applies_to"] = entity.get("wikidata_applies_to", [])
                
                # Medien
                if entity.get("wikidata_image_url"):
                    wikidata_source["image_url"] = entity.get("wikidata_image_url", "")
                if entity.get("wikidata_images"):
                    wikidata_source["images"] = entity.get("wikidata_images", [])
        
        # DBpedia-Informationen
        # Unterstützung für verschiedene Entitätsstrukturen bei den Quellen
        if isinstance(entity, dict):
            if "sources" in entity and isinstance(entity.get("sources"), dict) and "dbpedia" in entity.get("sources", {}):
                # Neue Struktur mit direktem sources-Dict
                dbpedia_source_obj = entity["sources"]["dbpedia"]
                
                # Konvertiere SourceData in ein vollständiges Dictionary mit allen Attributen
                if hasattr(dbpedia_source_obj, "to_dict"):
                    # Verwende to_dict wenn verfügbar
                    dbpedia_source = dbpedia_source_obj.to_dict()
                else:
                    # Fallback: Stelle sicher, dass alle Attribute und Daten kopiert werden
                    dbpedia_source = {}
                
                    # Liste aller möglichen DBpedia-Attribute
                    dbpedia_attrs = [
                        # 1. Basisinformationen
                        "uri", "resource_uri", "abstract", "types",
                        
                        # 2. Multimedia & Verlinkungen
                        "thumbnail", "homepage", "isPrimaryTopicOf", "externalLinks", "sameAs",
                        
                        # 3. Geografische Informationen
                        "latitude", "longitude",
                        
                        # 4. Kategorisierung & Klassifikation
                        "subjects", "categories",
                        
                        # 5. Zeitbezogene Informationen
                        "birthDate", "deathDate", "foundingDate",
                    
                        # 6. Externe Identifikatoren
                        "gndId", "viafId", "orcidId",
                        
                        # 7. Legacy-Felder
                        "part_of", "has_parts"
                    ]
                    
                    # Basisattribute kopieren
                    for attr in dbpedia_attrs:
                        value = safe_get(dbpedia_source_obj, attr)
                        if value is not None:
                            dbpedia_source[attr] = value
                    
                    # Wenn data vorhanden, auch kopieren
                    data = safe_get(dbpedia_source_obj, "data", {})
                    if data:
                        for key, value in data.items():
                            if key not in dbpedia_source:
                                dbpedia_source[key] = value
            
                    # resource_uri ist das Standardattribut
                    if "uri" in dbpedia_source and "resource_uri" not in dbpedia_source:
                        dbpedia_source["resource_uri"] = dbpedia_source["uri"]
                    
                    formatted_entity["sources"]["dbpedia"] = dbpedia_source
            # Legacy-Format: Vollständiges DBpedia-Objekt auf oberster Ebene
            elif "dbpedia" in entity and isinstance(entity["dbpedia"], dict) and (entity["dbpedia"].get("uri") or entity["dbpedia"].get("resource_uri")):
                formatted_entity["sources"]["dbpedia"] = entity["dbpedia"].copy()
            # Fallback für flache Struktur
            elif any(key in entity for key in ["dbpedia_uri", "dbpedia_abstract", "dbpedia_subjects"]):
                dbpedia_source = formatted_entity["sources"].setdefault("dbpedia", {})
            
                # Detaillierte Bildinformationen
                if entity.get('metadata', {}).get('image_info'):
                    dbpedia_source['image_info'] = entity['metadata']['image_info']
                
                # Koordinaten
                if entity.get('metadata', {}).get('coordinates'):
                    dbpedia_source['coordinates'] = entity['metadata']['coordinates']
                    # For better compatibility with existing tools, also as individual fields
                    dbpedia_source['latitude'] = entity['metadata']['coordinates'].get('lat')
                    dbpedia_source['longitude'] = entity['metadata']['coordinates'].get('lon')
                    
                # 1. Basisinformationen
                if entity.get("dbpedia_uri"):
                    dbpedia_source["uri"] = entity.get("dbpedia_uri", "")
                if entity.get("dbpedia_abstract"):
                    dbpedia_source["abstract"] = entity.get("dbpedia_abstract", "")
                if entity.get("dbpedia_types"):
                    dbpedia_source["types"] = entity.get("dbpedia_types", [])
                
                # 2. Multimedia & Verlinkungen
                if entity.get("dbpedia_thumbnail"):
                    dbpedia_source["thumbnail"] = entity.get("dbpedia_thumbnail", "")
                if entity.get("dbpedia_homepage"):
                    dbpedia_source["homepage"] = entity.get("dbpedia_homepage", "")
                if entity.get("dbpedia_isPrimaryTopicOf"):
                    dbpedia_source["isPrimaryTopicOf"] = entity.get("dbpedia_isPrimaryTopicOf", "")
                if entity.get("dbpedia_externalLinks"):
                    dbpedia_source["externalLinks"] = entity.get("dbpedia_externalLinks", [])
                if entity.get("dbpedia_sameAs"):
                    dbpedia_source["sameAs"] = entity.get("dbpedia_sameAs", [])
            
                # 3. Geografische Informationen
                if entity.get("dbpedia_latitude") and entity.get("dbpedia_longitude"):
                    dbpedia_source["latitude"] = entity.get("dbpedia_latitude")
                    dbpedia_source["longitude"] = entity.get("dbpedia_longitude")
                
                # 4. Kategorisierung & Klassifikation
                if entity.get("dbpedia_subjects"):
                    dbpedia_source["subjects"] = entity.get("dbpedia_subjects", [])
                if entity.get("dbpedia_categories"):
                    dbpedia_source["categories"] = entity.get("dbpedia_categories", [])
                
                # 5. Zeitbezogene Informationen
                if entity.get("dbpedia_birthDate"):
                    dbpedia_source["birthDate"] = entity.get("dbpedia_birthDate")
                if entity.get("dbpedia_deathDate"):
                    dbpedia_source["deathDate"] = entity.get("dbpedia_deathDate")
                if entity.get("dbpedia_foundingDate"):
                    dbpedia_source["foundingDate"] = entity.get("dbpedia_foundingDate")
                
                # 6. Externe Identifikatoren
                if entity.get("dbpedia_gndId"):
                    dbpedia_source["gndId"] = entity.get("dbpedia_gndId", "")
                if entity.get("dbpedia_viafId"):
                    dbpedia_source["viafId"] = entity.get("dbpedia_viafId", "")
                if entity.get("dbpedia_orcidId"):
                    dbpedia_source["orcidId"] = entity.get("dbpedia_orcidId", "")
                
                # Legacy fields for compatibility
                if entity.get("dbpedia_part_of"):
                    dbpedia_source["part_of"] = entity.get("dbpedia_part_of", [])
                if entity.get("dbpedia_has_parts"):
                    dbpedia_source["has_parts"] = entity.get("dbpedia_has_parts", [])
        
        # Add the formatted entity to the result
        result["entities"].append(formatted_entity)
    
    return result


# Legacy wrapper for backward compatibility
def format_legacy_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formats an entity in the old format into the new standardized format.
    
    Args:
        entity: Entity in the old format
        
    Returns:
        Entity in the new format
    """
    # Simple wrapper for existing entities
    # This function can be extended if needed
    return entity
