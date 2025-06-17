#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
context_statistics_top10.py

Functions for generating top-10 statistics from entity contexts.
This module is a companion to context_statistics.py to reduce file size.
"""

from typing import List, Dict, Any
from collections import Counter

from loguru import logger

from entityextractor.core.context import EntityProcessingContext


def extract_wikipedia_statistics(contexts: List[EntityProcessingContext]) -> Dict[str, Dict[str, int]]:
    """
    Extracts top-10 statistics from Wikipedia data in entity contexts.
    
    Args:
        contexts: List of EntityProcessingContext objects
        
    Returns:
        Dictionary with top-10 Wikipedia statistics
    """
    result = {}
    
    # Top Wikipedia categories
    wikipedia_categories = []
    for context in contexts:
        if context.is_processed_by("wikipedia"):
            # Zugriff auf Wikipedia-Daten über sources.wikipedia
            wiki_data = None
            if "sources" in context.output_data and "wikipedia" in context.output_data["sources"]:
                wiki_data = context.output_data["sources"]["wikipedia"]
            elif "wikipedia" in context.output_data:
                # Fallback auf direkte wikipedia-Daten
                wiki_data = context.output_data["wikipedia"]
            
            if wiki_data and "wikipedia" in wiki_data:
                # Struktur wie im Beispiel: output_data.sources.wikipedia.wikipedia
                wiki_data = wiki_data["wikipedia"]
            
            if wiki_data:
                categories = wiki_data.get("categories", [])
                if categories:
                    if isinstance(categories, list):
                        wikipedia_categories.extend(categories)
                    elif isinstance(categories, str):
                        wikipedia_categories.append(categories)
    
    cat_counts = Counter(wikipedia_categories)
    top_cats = sorted(cat_counts.items(), key=lambda x: -x[1])[:10]
    result["wikipedia_categories"] = {c: n for c, n in top_cats}
    
    # Top Wikipedia internal links
    wikipedia_links = []
    for context in contexts:
        if context.is_processed_by("wikipedia"):
            wiki_data = None
            if "sources" in context.output_data and "wikipedia" in context.output_data["sources"]:
                wiki_data = context.output_data["sources"]["wikipedia"]
            elif "wikipedia" in context.output_data:
                wiki_data = context.output_data["wikipedia"]
            
            if wiki_data and "wikipedia" in wiki_data:
                wiki_data = wiki_data["wikipedia"]
            
            if wiki_data:
                links = wiki_data.get("internal_links", [])
                if links:
                    if isinstance(links, list):
                        wikipedia_links.extend(links)
                    elif isinstance(links, str):
                        wikipedia_links.append(links)
    
    link_counts = Counter(wikipedia_links)
    top_links = sorted(link_counts.items(), key=lambda x: -x[1])[:10]
    result["wikipedia_internal_links"] = {l: n for l, n in top_links}
    
    return result


def extract_wikidata_statistics(contexts: List[EntityProcessingContext]) -> Dict[str, Dict[str, int]]:
    """
    Extracts top-10 statistics from Wikidata in entity contexts.
    
    Args:
        contexts: List of EntityProcessingContext objects
        
    Returns:
        Dictionary with top-10 Wikidata statistics
    """
    result = {}
    
    # Top Wikidata instance_of (types)
    wikidata_types = []
    for context in contexts:
        if context.is_processed_by("wikidata"):
            wd_data = None
            if "sources" in context.output_data and "wikidata" in context.output_data["sources"]:
                wd_data = context.output_data["sources"]["wikidata"]
            elif "wikidata" in context.output_data:
                wd_data = context.output_data["wikidata"]
            
            if wd_data:
                instance_of = wd_data.get("instance_of", [])
                if instance_of:
                    if isinstance(instance_of, list):
                        for item in instance_of:
                            if isinstance(item, dict) and "label" in item:
                                wikidata_types.append(item["label"])
                            elif isinstance(item, str):
                                wikidata_types.append(item)
                    elif isinstance(instance_of, dict) and "label" in instance_of:
                        wikidata_types.append(instance_of["label"])
                    elif isinstance(instance_of, str):
                        wikidata_types.append(instance_of)
    
    type_counts = Counter(wikidata_types)
    top_instance = sorted(type_counts.items(), key=lambda x: -x[1])[:10]
    result["wikidata_instance_of"] = {ty: n for ty, n in top_instance}
    
    # Top Wikidata type (Kompatibilität mit altem Namen)
    result["wikidata_type"] = result["wikidata_instance_of"]
    
    # Top Wikidata subclass_of
    wikidata_subclasses = []
    for context in contexts:
        if context.is_processed_by("wikidata"):
            wd_data = None
            if "sources" in context.output_data and "wikidata" in context.output_data["sources"]:
                wd_data = context.output_data["sources"]["wikidata"]
            elif "wikidata" in context.output_data:
                wd_data = context.output_data["wikidata"]
            
            if wd_data:
                subclass_of = wd_data.get("subclass_of", [])
                if subclass_of:
                    if isinstance(subclass_of, list):
                        for item in subclass_of:
                            if isinstance(item, dict) and "label" in item:
                                wikidata_subclasses.append(item["label"])
                            elif isinstance(item, str):
                                wikidata_subclasses.append(item)
                    elif isinstance(subclass_of, dict) and "label" in subclass_of:
                        wikidata_subclasses.append(subclass_of["label"])
                    elif isinstance(subclass_of, str):
                        wikidata_subclasses.append(subclass_of)
    
    subclass_counts = Counter(wikidata_subclasses)
    top_subclass = sorted(subclass_counts.items(), key=lambda x: -x[1])[:10]
    result["wikidata_subclass_of"] = {sc: n for sc, n in top_subclass}
    
    # Top Wikidata part_of
    wikidata_part_of = []
    for context in contexts:
        if context.is_processed_by("wikidata"):
            wd_data = None
            if "sources" in context.output_data and "wikidata" in context.output_data["sources"]:
                wd_data = context.output_data["sources"]["wikidata"]
            elif "wikidata" in context.output_data:
                wd_data = context.output_data["wikidata"]
            
            if wd_data:
                part_of = wd_data.get("part_of", [])
                if part_of:
                    if isinstance(part_of, list):
                        for item in part_of:
                            if isinstance(item, dict) and "label" in item:
                                wikidata_part_of.append(item["label"])
                            elif isinstance(item, str):
                                wikidata_part_of.append(item)
                    elif isinstance(part_of, dict) and "label" in part_of:
                        wikidata_part_of.append(part_of["label"])
                    elif isinstance(part_of, str):
                        wikidata_part_of.append(part_of)
    
    part_of_counts = Counter(wikidata_part_of)
    top_part_of = sorted(part_of_counts.items(), key=lambda x: -x[1])[:10]
    result["wikidata_part_of"] = {po: n for po, n in top_part_of}
    
    # Top Wikidata has_part
    wikidata_has_part = []
    for context in contexts:
        if context.is_processed_by("wikidata"):
            wd_data = None
            if "sources" in context.output_data and "wikidata" in context.output_data["sources"]:
                wd_data = context.output_data["sources"]["wikidata"]
            elif "wikidata" in context.output_data:
                wd_data = context.output_data["wikidata"]
            
            if wd_data:
                has_part = wd_data.get("has_part", [])
                if has_part:
                    if isinstance(has_part, list):
                        for item in has_part:
                            if isinstance(item, dict) and "label" in item:
                                wikidata_has_part.append(item["label"])
                            elif isinstance(item, str):
                                wikidata_has_part.append(item)
                    elif isinstance(has_part, dict) and "label" in has_part:
                        wikidata_has_part.append(has_part["label"])
                    elif isinstance(has_part, str):
                        wikidata_has_part.append(has_part)
    
    has_part_counts = Counter(wikidata_has_part)
    top_has_part = sorted(has_part_counts.items(), key=lambda x: -x[1])[:10]
    result["wikidata_has_part"] = {hp: n for hp, n in top_has_part}
    
    return result


def extract_dbpedia_statistics(contexts: List[EntityProcessingContext]) -> Dict[str, Dict[str, int]]:
    """
    Extracts top-10 statistics from DBpedia data in entity contexts.
    
    Args:
        contexts: List of EntityProcessingContext objects
        
    Returns:
        Dictionary with top-10 DBpedia statistics
    """

    logger.info(f"Starting DBpedia statistics extraction for {len(contexts)} contexts")
    logger.debug("extract_dbpedia_statistics function called")
    
    # Hilfsfunktion zum Extrahieren des Labels aus einem URI
    def extract_label_from_uri(uri):
        if not isinstance(uri, str):
            return str(uri)
            
        # Für DBpedia-URIs (http://dbpedia.org/resource/Category:...)
        if uri.startswith("http://dbpedia.org/resource/"):
            # Entferne den Präfix
            label = uri.replace("http://dbpedia.org/resource/", "")
            # Entferne Category: Präfix, falls vorhanden
            if label.startswith("Category:"):
                label = label.replace("Category:", "")
            # Ersetze Unterstriche durch Leerzeichen für bessere Lesbarkeit
            return label.replace("_", " ")
        # Für andere URIs mit Pfadkomponenten
        elif "/" in uri:
            label = uri.split("/")[-1]
            # Ersetze Unterstriche durch Leerzeichen für bessere Lesbarkeit
            return label.replace("_", " ")
        return uri
    
    # Hilfsfunktion zum Abrufen von DBpedia-Daten aus dem Kontext
    def get_dbpedia_data(context):
        logger.debug(f"Searching for DBpedia data in context for {context.entity_name}")
        
        # Versuche verschiedene Pfade, um die DBpedia-Daten zu finden
        if "dbpedia" in context.output_data:
            logger.debug(f"Found DBpedia data in output_data for {context.entity_name}")
            status = context.output_data["dbpedia"].get("status", "unknown")
            logger.debug(f"DBpedia status for {context.entity_name}: {status}")
            return context.output_data["dbpedia"]
        elif "sources" in context.output_data and "dbpedia" in context.output_data["sources"]:
            logger.debug(f"Found DBpedia data in output_data.sources for {context.entity_name}")
            return context.output_data["sources"]["dbpedia"]
        elif "output" in context.output_data and "dbpedia" in context.output_data["output"]:
            logger.debug(f"Found DBpedia data in output_data.output for {context.entity_name}")
            return context.output_data["output"]["dbpedia"]
        
        logger.debug(f"No DBpedia data found in context for {context.entity_name}")
        return None
            
    def extract_values(data, key):
        values = []
        if not data:
            return values
        items = data.get(key, [])
        if not items:
            return values
            
        # Protokolliere den Typ und Inhalt der Daten für Debugging
        logger.debug(f"Extracting values for key '{key}', data type: {type(items)}, content: {items[:100] if isinstance(items, list) else items}")
        
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and "label" in item:
                    values.append(item["label"])
                elif isinstance(item, str):
                    # Extrahiere das Label aus der URI
                    label = extract_label_from_uri(item)
                    values.append(label)
                    logger.debug(f"Extracted label '{label}' from URI '{item}'")
        elif isinstance(items, dict):
            if "label" in items:
                values.append(items["label"])
            elif "en" in items:
                values.append(items["en"])
            else:
                for v in items.values():
                    if isinstance(v, str):
                        values.append(extract_label_from_uri(v))
                        break
        elif isinstance(items, str):
            label = extract_label_from_uri(items)
            values.append(label)
            logger.debug(f"Extracted label '{label}' from string '{items}'")
        
        logger.debug(f"Extracted {len(values)} values for key '{key}'")
        return values
    
    # Initialize counters
    counters = {
        'types': Counter(),
        'categories': Counter(),
        'part_of': Counter(),
        'has_part': Counter(),
        'subjects': Counter()
    }

    for context in contexts:
        dbpedia_data = get_dbpedia_data(context)
        
        if not dbpedia_data:
            logger.debug(f"Entity {context.entity_name}: No DBpedia data found")
            continue
            
        if dbpedia_data and dbpedia_data.get("status") == "linked":
            logger.debug(f"Entity {context.entity_name}: Processing linked DBpedia data")

            types = extract_values(dbpedia_data, 'types')
            logger.debug(f"Entity {context.entity_name}: Extracted types: {types}")
            categories = extract_values(dbpedia_data, 'categories')
            logger.debug(f"Entity {context.entity_name}: Extracted categories: {categories}")
            part_of = extract_values(dbpedia_data, 'part_of')
            logger.debug(f"Entity {context.entity_name}: Extracted part_of: {part_of}")
            has_part = extract_values(dbpedia_data, 'has_part')
            logger.debug(f"Entity {context.entity_name}: Extracted has_part: {has_part}")
            subjects = extract_values(dbpedia_data, 'subjects')
            logger.debug(f"Entity {context.entity_name}: Extracted subjects: {subjects}")

            # Update counters with extracted values
            for t in types:
                counters['types'][t] += 1
                logger.debug(f"Incremented types counter for '{t}' to {counters['types'][t]}")
            for c in categories:
                counters['categories'][c] += 1
                logger.debug(f"Incremented categories counter for '{c}' to {counters['categories'][c]}")
            for p in part_of:
                counters['part_of'][p] += 1
                logger.debug(f"Incremented part_of counter for '{p}' to {counters['part_of'][p]}")
            for h in has_part:
                counters['has_part'][h] += 1
                logger.debug(f"Incremented has_part counter for '{h}' to {counters['has_part'][h]}")
            for s in subjects:
                counters['subjects'][s] += 1
                logger.debug(f"Incremented subjects counter for '{s}' to {counters['subjects'][s]}")
        else:
            logger.debug(f"Entity {context.entity_name}: DBpedia data not linked or not found")

    # Create result dictionary
    result = {
        "dbpedia_types": dict(counters['types'].most_common(10)),
        "dbpedia_categories": dict(counters['categories'].most_common(10)),
        "dbpedia_part_of": dict(counters['part_of'].most_common(10)),
        "dbpedia_has_part": dict(counters['has_part'].most_common(10)),
        "dbpedia_subjects": dict(counters['subjects'].most_common(10))
    }

    logger.info(f"DBpedia statistics extracted: Types={len(result['dbpedia_types'])}, Categories={len(result['dbpedia_categories'])}, PartOf={len(result['dbpedia_part_of'])}, HasPart={len(result['dbpedia_has_part'])}, Subjects={len(result['dbpedia_subjects'])}")
    return result


def extract_relationship_statistics(all_relationships: List[Dict]) -> Dict[str, Dict[str, int]]:
    """
    Extracts top-10 statistics from relationships.
    
    Args:
        all_relationships: List of relationship dictionaries
        
    Returns:
        Dictionary with top-10 relationship statistics
    """
    result = {}
    
    # --------------------------------------------------
    # Global Deduplikation, damit jede unique Triple-
    # Variante (inkl. inferred-Status) nur einmal
    # berücksichtigt wird.
    # --------------------------------------------------
    from entityextractor.config.settings import get_config
    cfg = get_config()

    # Deduplicate only if flag is True
    if cfg.get("STATISTICS_DEDUPLICATE_RELATIONSHIPS", True):
        unique_rel_map: Dict[tuple, Dict] = {}
        for rel in all_relationships:
            key = (
                rel.get("subject"),
                rel.get("predicate"),
                rel.get("object"),
                str(rel.get("inferred", "explicit")).lower()
            )
            if key not in unique_rel_map:
                unique_rel_map[key] = rel
        relationships = list(unique_rel_map.values())
    else:
        # Keep duplicates when deduplication is disabled
        relationships = all_relationships

    # Top Relationship Predicates
    predicate_counts = Counter()
    for rel in relationships:
        predicate = rel.get("predicate")
        if predicate:
            predicate_counts[predicate] += 1
    
    top_predicates = sorted(predicate_counts.items(), key=lambda x: -x[1])[:10]
    result["predicates"] = {p: n for p, n in top_predicates}
    
    # Relationship Inference Status (explicit vs implicit)
    relationship_inference_counts = Counter()
    for rel in relationships:
        inferred_value = rel.get("inferred", "explicit")

        # Possible representations:
        #   - Boolean: True for implicit/inferred, False for explicit
        #   - String: "implicit" / "explicit" (case-insensitive)
        #   - String: legacy "inferred" meaning implicit
        status: str
        if isinstance(inferred_value, bool):
            status = "implicit" if inferred_value else "explicit"
        elif isinstance(inferred_value, str):
            val_lower = inferred_value.lower().strip()
            if val_lower in {"implicit", "inferred", "true", "yes", "1"}:
                status = "implicit"
            else:
                status = "explicit"
        else:
            # Fallback for unexpected types
            status = "explicit"

        relationship_inference_counts[status] += 1
    
    total_rels = len(relationships) or 1  # Vermeidet Division durch Null
    result["relationship_inference"] = {}
    for status, count in relationship_inference_counts.items():
        result["relationship_inference"][status] = {
            "count": count,
            "percent": round(count / total_rels * 100, 1)
        }
    
    return result


def extract_entity_inference_statistics(contexts: List[EntityProcessingContext]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Extracts entity inference statistics.
    
    Args:
        contexts: List of EntityProcessingContext objects
        
    Returns:
        Dictionary with entity inference statistics
    """
    result = {}
    
    # Entity Inference Status
    entity_inference_counts = Counter()
    for context in contexts:
        inferred_status = context.output_data.get("details", {}).get("inferred", "explicit")
        entity_inference_counts[inferred_status] += 1
    
    total_entities = len(contexts) or 1  # Vermeidet Division durch Null
    result["entity_inference"] = {}
    for status, count in entity_inference_counts.items():
        result["entity_inference"][status] = {
            "count": count,
            "percent": round(count / total_entities * 100, 1)
        }
    
    return result
