"""
statistics.py

Funktionen zum Generieren von Statistiken über Entitäten und Beziehungen.
"""

import logging
from entityextractor.utils.category_utils import filter_category_counts

def generate_statistics(result):
    """
    Generiert umfassende Statistiken über Entitäten und Beziehungen.
    
    Diese Funktion wurde aus dem Orchestrator ausgelagert, um die Codebase 
    modularer und wartbarer zu machen.
    
    Args:
        result: Ein Ergebnisobjekt mit entities und relationships
        
    Returns:
        Ein Dictionary mit Statistiken
    """
    entities = result.get("entities", [])
    relationships = result.get("relationships", [])
    
    # Grundlegende Statistiken
    stats = {
        "total_entities": len(entities),
        "total_relationships": len(relationships)
    }
    
    # Typverteilung
    type_counts = {}
    for entity in entities:
        details = entity.get("details", {})
        entity_type = details.get("typ", "Unbekannt")
        if entity_type:
            type_counts[entity_type] = type_counts.get(entity_type, 0) + 1
    
    stats["types_distribution"] = type_counts
    
    # Linking-Erfolgsquoten
    wiki_count = sum(1 for e in entities if e.get("sources", {}).get("wikipedia", {}).get("url", ""))
    wikidata_count = sum(1 for e in entities if e.get("sources", {}).get("wikidata", {}).get("id", ""))
    dbpedia_count = sum(1 for e in entities if e.get("sources", {}).get("dbpedia", {}).get("resource_uri", ""))
    
    total = len(entities) or 1  # Vermeide Division durch Null
    stats["linked"] = {
        "wikipedia": {"count": wiki_count, "percent": round(wiki_count / total * 100, 1)},
        "wikidata": {"count": wikidata_count, "percent": round(wikidata_count / total * 100, 1)},
        "dbpedia": {"count": dbpedia_count, "percent": round(dbpedia_count / total * 100, 1)}
    }
    
    # Top Wikipedia-Kategorien
    cat_counts = {}
    for entity in entities:
        categories = entity.get("sources", {}).get("wikipedia", {}).get("categories", [])
        for cat in categories:
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
    
    # Nutze category_utils zum Filtern irrelevanter Kategorien
    filtered_cat_counts = filter_category_counts(cat_counts)
    top_cats = sorted(filtered_cat_counts.items(), key=lambda x: -x[1])[:10]
    stats["top_wikipedia_categories"] = [{"category": c, "count": n} for c, n in top_cats]
    
    # Top Wikidata-Typen
    wd_counts = {}
    for entity in entities:
        types = entity.get("sources", {}).get("wikidata", {}).get("types", [])
        for ty in types:
            wd_counts[ty] = wd_counts.get(ty, 0) + 1
    
    top_wd = sorted(wd_counts.items(), key=lambda x: -x[1])[:10]
    stats["top_wikidata_types"] = [{"type": ty, "count": n} for ty, n in top_wd]
    
    # Top Wikidata part_of
    collect_property_stats(entities, "wikidata", "part_of", stats, "top_wikidata_part_of")
    
    # Top Wikidata has_parts
    collect_property_stats(entities, "wikidata", "has_parts", stats, "top_wikidata_has_parts")
    
    # Top DBpedia-Subjects
    collect_property_stats(entities, "dbpedia", "subjects", stats, "top_dbpedia_subjects")
    
    # Top DBpedia part_of
    collect_property_stats(entities, "dbpedia", "part_of", stats, "top_dbpedia_part_of")
    
    # Top DBpedia has_parts
    collect_property_stats(entities, "dbpedia", "has_parts", stats, "top_dbpedia_has_parts")
    
    # Entitätsverbindungen analysieren
    conn_map = {}
    for rel in relationships:
        subj = rel.get("subject")
        obj = rel.get("object")
        if subj and obj:
            conn_map.setdefault(subj, set()).add(obj)
            conn_map.setdefault(obj, set()).add(subj)
    
    entity_conn_list = [{"entity": ent, "count": len(neighbors)} for ent, neighbors in conn_map.items()]
    entity_conn_list.sort(key=lambda x: -x["count"])
    stats["entity_connections"] = entity_conn_list[:10]  # Top 10 vernetzte Entitäten
    
    return stats

def collect_property_stats(entities, source, property_name, stats, target_key, max_items=10):
    """
    Helfer-Funktion zum Sammeln von Eigenschaftsstatistiken.
    
    Args:
        entities: Liste von Entitäten
        source: Quellname (z.B. "wikidata", "dbpedia")
        property_name: Name der Eigenschaft (z.B. "part_of")
        stats: Statistik-Dictionary zum Aktualisieren
        target_key: Zielschlüssel für die Statistik
        max_items: Maximale Anzahl von Top-Elementen
    """
    counts = {}
    
    for entity in entities:
        values = entity.get("sources", {}).get(source, {}).get(property_name, [])
        
        # Behandle sowohl Listen als auch einzelne Werte
        if not isinstance(values, list):
            values = [values] if values else []
        
        for value in values:
            if value:  # Überspringe leere Werte
                counts[value] = counts.get(value, 0) + 1
    
    # Sortiere und beschränke auf Top-N
    top_items = sorted(counts.items(), key=lambda x: -x[1])[:max_items]
    
    # Formatiere das Ergebnis
    stats[target_key] = [
        {property_name: item, "count": count}
        for item, count in top_items
    ]
