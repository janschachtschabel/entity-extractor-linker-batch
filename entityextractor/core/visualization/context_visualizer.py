#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
context_visualizer.py

Erweiterung der Visualisierungskomponenten für die kontextbasierte Architektur.
Ermöglicht die direkte Visualisierung von EntityProcessingContext-Objekten.
"""

import os
import logging
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

import networkx as nx
from entityextractor.core.context import EntityProcessingContext
from entityextractor.core.process.result_formatter import format_contexts_to_result
from .graph_builder import build_graph, get_color_for_entity_type
from .renderer import render_graph_to_png, render_graph_to_html

logger = logging.getLogger(__name__)

async def visualize_contexts(
    contexts: List[EntityProcessingContext],
    config: Optional[Dict[str, Any]] = None,
    output_name: Optional[str] = None
) -> Dict[str, str]:
    """
    Generiert PNG- und HTML-Visualisierungen direkt aus EntityProcessingContext-Objekten.
    
    Args:
        contexts: Liste von EntityProcessingContext-Objekten
        config: Konfigurationsoptionen für die Visualisierung
        output_name: Optionaler Basisname für die Ausgabedateien (ohne Dateiendung)
        
    Returns:
        Ein Dictionary mit den Pfaden zu den erzeugten Visualisierungen oder leeres Dict bei Fehler
    """
    if not contexts:
        logger.warning("Keine Kontexte zur Visualisierung vorhanden")
        return {}
        
    if config is None:
        config = {}
        
    # Überprüfe, ob die Visualisierung aktiviert ist
    if not config.get("ENABLE_GRAPH_VISUALIZATION", False):
        logger.info("Graph-Visualisierung ist deaktiviert (ENABLE_GRAPH_VISUALIZATION=False)")
        return {}
    
    try:
        # Extrahiere alle Beziehungen aus den Kontexten
        all_relationships = []
        for context in contexts:
            all_relationships.extend(context.get_relationships())
            
        # Wenn keine Beziehungen vorhanden sind, versuche implizit zu erstellen (falls konfiguriert)
        if not all_relationships and config.get("AUTO_CREATE_RELATIONSHIPS", False):
            logger.info("Keine Beziehungen gefunden, versuche automatische Erstellung")
            import asyncio
            from entityextractor.core.relationship_extraction import extract_relationships_from_contexts
            all_relationships = await extract_relationships_from_contexts(contexts, config)
            
        # Überprüfe erneut auf Beziehungen
        if not all_relationships:
            logger.warning("Keine Beziehungen zur Visualisierung vorhanden, Visualisierung wird übersprungen")
            return {}
            
        # Konvertiere die Kontexte in ein formatiertes Ergebnis
        result = format_contexts_to_result(contexts)
        
        # Bereite Ausgabedateipfade vor
        output_dir = config.get("OUTPUT_DIR", "./output")
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Erzeuge Zeitstempel für eindeutige Dateinamen
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Erstelle Dateinamen
        base_filename = output_name or f"knowledge_graph_{timestamp}"
        png_filename = os.path.join(output_dir, f"{base_filename}.png")
        html_filename = os.path.join(output_dir, f"{base_filename}_interactive.html")
        
        logger.info(f"Erstelle Knowledge Graph Visualisierung - PNG: {png_filename}, HTML: {html_filename}")
        
        # Baue den Graphen aus Entitäten und Beziehungen
        G = build_context_graph(contexts, all_relationships, config)
        
        if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
            logger.error("Graph hat keine Knoten oder Kanten, Visualisierung wird übersprungen")
            return {}
            
        # Rendere den Graphen als PNG und HTML
        png_path = render_graph_to_png(G, png_filename, config)
        html_path = render_graph_to_html(G, html_filename, config)
        
        logger.info(f"Knowledge Graph PNG gespeichert: {png_path}")
        logger.info(f"Interaktive Knowledge Graph HTML gespeichert: {html_path}")
        
        return {"png": png_path, "html": html_path}
        
    except Exception as e:
        logger.error(f"Fehler bei der Graph-Visualisierung: {str(e)}", exc_info=True)
        return {}


def build_context_graph(
    contexts: List[EntityProcessingContext],
    relationships: List[Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None
) -> nx.DiGraph:
    """
    Erstellt einen gerichteten Graphen direkt aus EntityProcessingContext-Objekten.
    
    Args:
        contexts: Liste von EntityProcessingContext-Objekten
        relationships: Liste von Beziehungen
        config: Konfigurationsoptionen für den Graph-Builder
        
    Returns:
        Der erstellte Graph
    """
    if config is None:
        config = {}
        
    logger.info(f"Erstelle Graph aus {len(contexts)} Kontexten und {len(relationships)} Beziehungen")
    
    G = nx.DiGraph()
    
    # Mapping von Entitäts-IDs zu Metadaten
    id_to_name = {}
    id_to_type = {}
    
    # Füge alle Entitäten als Knoten hinzu
    for context in contexts:
        entity_id = context.entity_id
        entity_name = context.entity_name
        entity_type = context.entity_type or ""
        
        # Speichere Mappings für spätere Referenz
        id_to_name[entity_id] = entity_name
        id_to_type[entity_id] = entity_type
        
        # Bestimme die Knotenfarbe basierend auf dem Entitätstyp
        color = get_color_for_entity_type(entity_type.lower())
        
        # Extrahiere Wikipedia-URL und Wikidata-ID, falls vorhanden
        wikipedia_data = context.get_service_data("wikipedia").get("wikipedia", {})
        wikipedia_url = wikipedia_data.get("url", "")
        
        wikidata_data = context.get_service_data("wikidata").get("wikidata", {})
        wikidata_id = wikidata_data.get("id", "")
        
        # Bestimme den Inferenz-Status
        output_data = context.get_output()
        inferred = output_data.get("details", {}).get("inferred", "explicit")
        
        # Füge den Knoten hinzu
        G.add_node(
            entity_id,
            name=entity_name,
            entity_type=entity_type,
            inferred=inferred,
            wikipedia_url=wikipedia_url,
            wikidata_id=wikidata_id,
            color=color
        )
    
    # Füge Beziehungen als Kanten hinzu
    for rel in relationships:
        subject_id = rel.get("subject")
        object_id = rel.get("object")
        predicate = rel.get("predicate", "")
        
        if not subject_id or not object_id or not predicate:
            continue
            
        # Überprüfe, ob beide Entitäten im Graphen vorhanden sind
        if subject_id not in G.nodes() or object_id not in G.nodes():
            # Versuche, fehlende Knoten hinzuzufügen
            for node_id in [subject_id, object_id]:
                if node_id not in G.nodes():
                    # Suche nach dem Kontext mit dieser ID
                    matching_context = next((ctx for ctx in contexts if ctx.entity_id == node_id), None)
                    
                    if matching_context:
                        # Kontext gefunden, füge Knoten hinzu
                        entity_name = matching_context.entity_name
                        entity_type = matching_context.entity_type or ""
                        color = get_color_for_entity_type(entity_type.lower())
                        
                        G.add_node(
                            node_id,
                            name=entity_name,
                            entity_type=entity_type,
                            inferred="explicit",
                            color=color
                        )
                    else:
                        # Kontext nicht gefunden, füge minimalen Knoten hinzu
                        entity_name = id_to_name.get(node_id, f"Entity {node_id}")
                        entity_type = id_to_type.get(node_id, "")
                        color = get_color_for_entity_type(entity_type.lower())
                        
                        G.add_node(
                            node_id,
                            name=entity_name,
                            entity_type=entity_type,
                            inferred="reference",
                            color=color
                        )
        
        # Bestimme den Inferenz-Status und Kantenstil
        inferred = rel.get("inferred", "explicit")
        style = "solid" if inferred == "explicit" else "dashed"
        
        # Füge die Kante hinzu
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
    
    logger.info(f"Graph erstellt mit {G.number_of_nodes()} Knoten und {G.number_of_edges()} Kanten")
    return G
