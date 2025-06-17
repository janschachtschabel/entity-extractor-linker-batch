#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
context_statistics.py

Functions for generating statistics about entities and relationships
with support for the context-based architecture.
"""

import time
import json
from typing import List, Dict, Any, Optional, Set, Tuple, Union
from collections import Counter

import logging
logger = logging.getLogger(__name__)

from entityextractor.core.context import EntityProcessingContext
from entityextractor.utils.category_utils import filter_category_counts
from entityextractor.core.process.context_statistics_top10 import (
    extract_wikipedia_statistics,
    extract_wikidata_statistics,
    extract_dbpedia_statistics,
    extract_relationship_statistics,
    extract_entity_inference_statistics
)


def generate_context_statistics(contexts: List[EntityProcessingContext], include_details: bool = False) -> Dict[str, Any]:
    """
    Generates comprehensive statistics directly from EntityProcessingContext objects.
    
    Args:
        contexts: List of EntityProcessingContext objects
        include_details: If True, detailed statistics are generated
        
    Returns:
        A dictionary with statistics
    """
    logger.info(f"Generiere Statistiken für {len(contexts)} Entitäten...")
    
    # Basis-Statistiken
    stats = {
        "total_entities": len(contexts),
        "total_relationships": 0,
        "top10": {}
    }
    
    # Service-Statistiken
    service_counts = {
        "wikipedia": 0,
        "wikidata": 0,
        "dbpedia": 0
    }
    
    # Beziehungen sammeln (werden anschließend dedupliziert)
    all_relationships: List[Dict[str, Any]] = []
    
    for context in contexts:
        # Service-Nutzung zählen
        for service in service_counts.keys():
            if context.is_processed_by(service):
                service_counts[service] += 1
        
        # Beziehungen sammeln
        rels = context.get_relationships()
        all_relationships.extend(rels)
    
    # --------------------------------------------------
    from entityextractor.config.settings import get_config
    cfg = get_config()

    # --------------------------------------------------
    # Grund-Deduplikation nach eindeutiger ID, um Mehrfachspeicherung
    # derselben Beziehung in verschiedenen Kontexten auszuschließen.
    # --------------------------------------------------
    id_map = {}
    for rel in all_relationships:
        rid = rel.get("id")
        if rid and rid not in id_map:
            id_map[rid] = rel
    if id_map:
        all_relationships = list(id_map.values())

    if cfg.get("STATISTICS_DEDUPLICATE_RELATIONSHIPS", True):
        # Einfaches Triple-Dedup, um Doppelzählungen zu vermeiden
        triple_map = {}
        for rel in all_relationships:
            key = (
                rel.get("subject"),
                rel.get("predicate"),
                rel.get("object"),
                str(rel.get("inferred", "explicit")).lower()
            )
            if key not in triple_map:
                triple_map[key] = rel
        unique_relationships = list(triple_map.values())
    else:
        # Ohne Deduplikation exakt die Anzahl im Output übernehmen
        unique_relationships = all_relationships

    # Aktualisiere Gesamtanzahl der Beziehungen nach Deduplikation
    stats["total_relationships"] = len(unique_relationships)
    
    # Type distribution
    type_counts = Counter()
    for context in contexts:
        entity_type = context.entity_type or "Unknown"
        type_counts[entity_type] += 1
    
    stats["types_distribution"] = dict(type_counts)
    
    # Linking success rates
    wiki_count = sum(1 for ctx in contexts if ctx.is_processed_by("wikipedia"))
    wikidata_count = sum(1 for ctx in contexts if ctx.is_processed_by("wikidata"))
    
    # For DBpedia, we need to check if the entity is actually linked (status="linked")
    dbpedia_count = 0
    for ctx in contexts:
        # Check different possible paths for DBpedia data
        dbpedia_data = None
        
        # Direct dbpedia field at entity level
        if hasattr(ctx, 'dbpedia'):
            dbpedia_data = ctx.dbpedia
        # In output_data directly
        elif "dbpedia" in ctx.output_data:
            dbpedia_data = ctx.output_data["dbpedia"]
        # In sources
        elif "sources" in ctx.output_data and "dbpedia" in ctx.output_data["sources"]:
            dbpedia_data = ctx.output_data["sources"]["dbpedia"]
        # In output
        elif "output" in ctx.output_data and "dbpedia" in ctx.output_data["output"]:
            dbpedia_data = ctx.output_data["output"]["dbpedia"]
        
        # Check if the entity is actually linked
        if dbpedia_data and isinstance(dbpedia_data, dict) and dbpedia_data.get("status") == "linked":
            dbpedia_count += 1
            
        # Debug logging für DBpedia-Entitäten
        if ctx.is_processed_by("dbpedia") and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Entity {ctx.entity_name} - DBpedia data found: {bool(dbpedia_data)}, Status: {dbpedia_data.get('status') if dbpedia_data else 'N/A'}")


    
    total = len(contexts) or 1  # Avoid division by zero
    stats["linked"] = {
        "wikipedia": {"count": wiki_count, "percent": round(wiki_count / total * 100, 1)},
        "wikidata": {"count": wikidata_count, "percent": round(wikidata_count / total * 100, 1)},
        "dbpedia": {"count": dbpedia_count, "percent": round(dbpedia_count / total * 100, 1)}
    }
    
    # Extrahiere Top-10 Statistiken aus den ausgelagerten Funktionen
    
    # 1. Wikipedia, Wikidata und DBpedia Statistiken (Top-10)
    wikipedia_stats = extract_wikipedia_statistics(contexts)
    for key, value in wikipedia_stats.items():
        stats["top10"][key] = value

    wikidata_stats = extract_wikidata_statistics(contexts)
    for key, value in wikidata_stats.items():
        stats["top10"][key] = value

    dbpedia_stats = extract_dbpedia_statistics(contexts)
    for key, value in dbpedia_stats.items():
        stats["top10"][key] = value

    # Relationship statistics (top10 etc.)
    stats["top10"].update(extract_relationship_statistics(unique_relationships))
    
    # 3. Entity Inference Statistiken
    entity_inference_stats = extract_entity_inference_statistics(contexts)
    for key, value in entity_inference_stats.items():
        stats["top10"][key] = value
    
    # 4. Korrigiere die relationship_inference Statistik, um explicit und implicit zu unterscheiden
    explicit_count = sum(1 for rel in all_relationships if rel.get('inferred') == 'explicit')
    implicit_count = sum(1 for rel in all_relationships if rel.get('inferred') == 'implicit')
    total_rels = len(all_relationships) or 1  # Vermeide Division durch Null
    stats["top10"]["relationship_inference"] = {
        "explicit": {"count": explicit_count, "percent": round(explicit_count / total_rels * 100, 1)},
        "implicit": {"count": implicit_count, "percent": round(implicit_count / total_rels * 100, 1)}
    }
    
    logger.info(f"Statistiken für {len(contexts)} Entitäten generiert.")
    return stats


def format_statistics(stats: Dict[str, Any]) -> str:
    """
    Formats statistics as a human-readable string.
    
    Args:
        stats: Statistics dictionary
        
    Returns:
        Formatted string
    """
    output = []
    
    # Basic stats
    output.append(f"Total entities: {stats.get('total_entities', 0)}")
    output.append(f"Total relationships: {stats.get('total_relationships', 0)}")
    
    # Type distribution
    if "types_distribution" in stats:
        output.append("\nEntity types:")
        for typ, count in stats["types_distribution"].items():
            output.append(f"  {typ}: {count}")
    
    # Linking rates
    if "linked" in stats:
        output.append("\nLinking rates:")
        for service, info in stats["linked"].items():
            output.append(f"  {service}: {info.get('count', 0)} ({info.get('percent', 0.0)}%)")
    
    # Entity inference
    if "top10" in stats and "entity_inference" in stats["top10"]:
        output.append("\nEntity inference:")
        for typ, info in stats["top10"]["entity_inference"].items():
            output.append(f"  {typ}: {info.get('count', 0)} ({info.get('percent', 0.0)}%)")
    
    # Top relationships
    if "top10" in stats and "predicates" in stats["top10"]:
        output.append("\nTop relationships:")
        for pred, count in stats["top10"]["predicates"].items():
            output.append(f"  {pred}: {count}")
    
    # Relationship inference
    if "top10" in stats and "relationship_inference" in stats["top10"]:
        output.append("\nRelationship inference:")
        for typ, info in stats["top10"]["relationship_inference"].items():
            output.append(f"  {typ}: {info.get('count', 0)} ({info.get('percent', 0.0)}%)")
    
    # Top Wikipedia categories
    if "top10" in stats and "wikipedia_categories" in stats["top10"]:
        output.append("\nTop Wikipedia categories:")
        for cat, count in stats["top10"]["wikipedia_categories"].items():
            output.append(f"  {cat}: {count}")
    
    # Top Wikipedia internal links
    if "top10" in stats and "wikipedia_internal_links" in stats["top10"]:
        output.append("\nTop Wikipedia internal links:")
        for link, count in stats["top10"]["wikipedia_internal_links"].items():
            output.append(f"  {link}: {count}")
    
    # Top Wikidata types
    if "top10" in stats and "wikidata_instance_of" in stats["top10"]:
        output.append("\nTop Wikidata types:")
        for typ, count in stats["top10"]["wikidata_instance_of"].items():
            output.append(f"  {typ}: {count}")
    
    # Top Wikidata subclass_of
    if "top10" in stats and "wikidata_subclass_of" in stats["top10"]:
        output.append("\nTop Wikidata subclasses:")
        for cls, count in stats["top10"]["wikidata_subclass_of"].items():
            output.append(f"  {cls}: {count}")
    
    # Top Wikidata part_of
    if "top10" in stats and "wikidata_part_of" in stats["top10"]:
        output.append("\nTop Wikidata part_of:")
        for part, count in stats["top10"]["wikidata_part_of"].items():
            output.append(f"  {part}: {count}")
    
    # Top Wikidata has_part
    if "top10" in stats and "wikidata_has_part" in stats["top10"]:
        output.append("\nTop Wikidata has_part:")
        for part, count in stats["top10"]["wikidata_has_part"].items():
            output.append(f"  {part}: {count}")
    
    # Top DBpedia types
    if "top10" in stats and "dbpedia_types" in stats["top10"]:
        output.append("\nTop DBpedia types:")
        for typ, count in stats["top10"]["dbpedia_types"].items():
            output.append(f"  {typ}: {count}")
    
    # Top DBpedia categories
    if "top10" in stats and "dbpedia_categories" in stats["top10"]:
        output.append("\nTop DBpedia categories:")
        for cat, count in stats["top10"]["dbpedia_categories"].items():
            output.append(f"  {cat}: {count}")
    
    # Top DBpedia part_of
    if "top10" in stats and "dbpedia_part_of" in stats["top10"]:
        output.append("\nTop DBpedia part_of:")
        for part, count in stats["top10"]["dbpedia_part_of"].items():
            output.append(f"  {part}: {count}")
    
    # Top DBpedia has_part
    if "top10" in stats and "dbpedia_has_part" in stats["top10"]:
        output.append("\nTop DBpedia has_part:")
        for part, count in stats["top10"]["dbpedia_has_part"].items():
            output.append(f"  {part}: {count}")
    
    # Top DBpedia subjects
    if "top10" in stats and "dbpedia_subjects" in stats["top10"]:
        output.append("\nTop DBpedia subjects:")
        for subj, count in stats["top10"]["dbpedia_subjects"].items():
            output.append(f"  {subj}: {count}")
    
    return "\n".join(output)
