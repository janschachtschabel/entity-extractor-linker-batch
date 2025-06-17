"""
graph_builder.py

Funktionen zum Erstellen von Wissensgraphen aus Entitäten und Beziehungen.
"""

import networkx as nx
import logging
from difflib import SequenceMatcher
import re

def build_graph(entities, relationships, config=None):
    """
    Erstellt einen gerichteten Graphen aus Entitäten und Beziehungen (jetzt auf Basis von UUIDs).
    
    Args:
        entities (list): Liste von Entitäten (jede mit 'id', 'entity', 'details', 'sources').
        relationships (list): Liste von Beziehungen (jede mit 'subject_id', 'object_id', ...).
        config (dict, optional): Konfigurationsoptionen für den Graph-Builder.
        
    Returns:
        nx.DiGraph: Der erstellte Graph.
    """
    if config is None:
        config = {}

    print(f"DEBUG: Building graph from {len(entities)} entities and {len(relationships)} relationships (UUID mode)")
    logging.info(f"Building graph from {len(entities)} entities and {len(relationships)} relationships (UUID mode)")

    G = nx.DiGraph()
    entity_count = 0
    skipped_entities = 0

    # Mapping von Entity-UUID zu Metadaten
    uuid_to_entity = {}
    for entity in entities:
        entity_id = entity.get("id")
        if not entity_id:
            print(f"DEBUG: Skipping entity without ID: {entity.get('entity', 'unknown')}")
            skipped_entities += 1
            continue
            
        name = entity.get("entity", "")
        details = entity.get("details", {})
        entity_type = details.get("typ", "")
        inferred = details.get("inferred", "")
        wikipedia_url = entity.get("sources", {}).get("wikipedia", {}).get("url", "")
        wikidata_id = entity.get("sources", {}).get("wikidata", {}).get("id", "")
        dbpedia_uri = entity.get("sources", {}).get("dbpedia", {}).get("uri", "")
        color = get_color_for_entity_type(entity_type.lower())
        uuid_to_entity[entity_id] = {
            "name": name,
            "entity_type": entity_type,
            "inferred": inferred,
            "wikipedia_url": wikipedia_url,
            "wikidata_id": wikidata_id,
            "dbpedia_uri": dbpedia_uri,
            "color": color
        }
        # Knoten mit UUID als Schlüssel
        G.add_node(
            entity_id,
            name=name,
            entity_type=entity_type,
            inferred=inferred,
            wikipedia_url=wikipedia_url,
            wikidata_id=wikidata_id,
            dbpedia_uri=dbpedia_uri,
            color=color
        )
        entity_count += 1

    print(f"DEBUG: Added {entity_count} entities to graph, skipped {skipped_entities} entities without IDs")

    # Füge auch Entitäten aus Beziehungen hinzu, falls sie noch nicht existieren
    rel_entity_count = 0
    for rel in relationships:
        for role in ["subject_id", "object_id"]:
            eid = rel.get(role)
            if eid and eid not in G.nodes():
                # Fallback: Minimaler Knoten mit UUID, Name aus rel falls vorhanden
                name = rel.get("subject") if role == "subject_id" else rel.get("object")
                entity_type = rel.get("subject_type") if role == "subject_id" else rel.get("object_type")
                color = get_color_for_entity_type(entity_type.lower())
                G.add_node(
                    eid,
                    name=name,
                    entity_type=entity_type,
                    inferred="explicit",
                    color=color
                )
                rel_entity_count += 1

    print(f"DEBUG: Added {rel_entity_count} additional entities from relationships")

    # Füge Beziehungen als Kanten hinzu (UUID-basiert)
    edge_count = 0
    skipped_edges = 0
    for rel in relationships:
        # Unterstütze sowohl subject_id/object_id als auch subject/object Formate
        subject_id = rel.get("subject_id") or rel.get("subject")
        object_id = rel.get("object_id") or rel.get("object")
        predicate = rel.get("predicate", "")
        inferred = rel.get("inferred", "")
        
        print(f"DEBUG: Processing relationship: {subject_id} --[{predicate}]--> {object_id}")
        
        if not subject_id or not object_id or not predicate:
            print(f"DEBUG: Skipping invalid relationship: missing subject_id, object_id, or predicate")
            skipped_edges += 1
            continue
        style = "solid" if inferred == "explicit" else "dashed"
        G.add_edge(
            subject_id,
            object_id,
            label=predicate,
            predicate=predicate,
            inferred=inferred,
            style=style,
            subject_type=rel.get("subject_type", ""),
            object_type=rel.get("object_type", "")
        )
        edge_count += 1

    print(f"DEBUG: Added {edge_count} edges to graph, skipped {skipped_edges} invalid relationships")
    logging.info(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges (UUID mode)")
    
    # If we have entities but no edges, we'll create a simple layout for isolated nodes
    if G.number_of_nodes() > 0 and G.number_of_edges() == 0:
        print("DEBUG: Graph has nodes but no edges, will create a visualization with isolated nodes")
    
    return G


def normalize_entity_name(name):
    """
    Normalisiert einen Entitätsnamen für besseres Matching.
    
    Args:
        name (str): Der zu normalisierende Name.
        
    Returns:
        str: Der normalisierte Name.
    """
    if not name:
        return ""
        
    # Konvertiere zu Kleinbuchstaben
    normalized = name.lower()
    
    # Entferne Sonderzeichen und überflüssige Leerzeichen
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

def find_entity_in_graph(G, entity_name, entity_map=None):
    """
    Findet eine Entität im Graphen, auch mit Fuzzy-Matching.
    
    Args:
        G (nx.DiGraph): Der Graph, in dem gesucht werden soll.
        entity_name (str): Der Name der zu suchenden Entität.
        entity_map (dict, optional): Mapping von normalisierten Namen zu Original-Namen.
        
    Returns:
        str or None: Der gefundene Entitätsname oder None, wenn nicht gefunden.
    """
    if entity_name in G:
        return entity_name
        
    # Normalisiere den Suchbegriff
    normalized_search = normalize_entity_name(entity_name)
    
    # Versuche exaktes Matching mit normalisierten Namen
    if entity_map and normalized_search in entity_map:
        return entity_map[normalized_search]
        
    # Versuche Fuzzy-Matching
    best_match = None
    best_score = 0.0
    threshold = 0.8  # Minimale Ähnlichkeit für ein Match
    
    for node in G.nodes():
        normalized_node = normalize_entity_name(node)
        
        # Berechne Ähnlichkeit
        score = similarity_score(normalized_search, normalized_node)
        
        if score > threshold and score > best_score:
            best_match = node
            best_score = score
            
    if best_match:
        logging.debug(f"Fuzzy-matched '{entity_name}' to '{best_match}' with score {best_score:.2f}")
        
    return best_match

def similarity_score(str1, str2):
    """
    Berechnet einen Ähnlichkeitswert zwischen zwei Strings.
    
    Args:
        str1 (str): Erster String.
        str2 (str): Zweiter String.
        
    Returns:
        float: Ähnlichkeitswert zwischen 0 und 1.
    """
    # Verwende SequenceMatcher für die Ähnlichkeitsberechnung
    return SequenceMatcher(None, str1, str2).ratio()

def get_color_for_entity_type(entity_type):
    """
    Bestimmt eine Farbe für einen Entitätstyp.
    
    Args:
        entity_type (str): Der Entitätstyp.
        
    Returns:
        str: Ein Farbcode.
    """
    # Basisfarben für häufige Entitätstypen
    base_colors = {
        "person": "#ffe6e6",
        "organisation": "#e6f0ff",
        "location": "#e7ffe6",
        "event": "#fff6e6",
        "concept": "#f0e6ff",
        "work": "#ffe6cc",
        "theory": "#e6ccff",
        "subject": "#ccffcc"
    }
    
    # Normalisiere den Entitätstyp
    normalized_type = entity_type.lower() if entity_type else ""
    
    # Versuche, eine passende Basisfarbe zu finden
    for key, color in base_colors.items():
        if key in normalized_type:
            return color
            
    # Fallback auf eine Standardfarbe
    return "#f2f2f2"
