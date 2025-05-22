"""
visualizer.py

Hauptmodul für die Visualisierung von Wissensgraphen.
"""

import os
import logging
import datetime
from pathlib import Path

from .graph_builder import build_graph
from .renderer import render_graph_to_png, render_graph_to_html

def visualize_graph(result, config=None):
    """
    Generiert PNG- und HTML-Visualisierungen des Wissensgraphen.
    
    Args:
        result (dict): Das Ergebnis der Entitätsextraktion und -verlinkung.
        config (dict, optional): Konfigurationsoptionen für die Visualisierung.
        
    Returns:
        dict or None: Ein Dictionary mit den Pfaden zu den erzeugten Visualisierungen oder None bei Fehler.
    """
    if config is None:
        config = {}
        
    # Überprüfe, ob die Visualisierung aktiviert ist
    if not config.get("ENABLE_GRAPH_VISUALIZATION", False):
        return None
        
    # Überprüfe, ob Beziehungsextraktion aktiviert ist
    if not config.get("RELATION_EXTRACTION", False):
        logging.warning("Graph visualization requires RELATION_EXTRACTION=True, skipping.")
        return None
        
    # Bereite Ausgabedateipfade vor und protokolliere Status
    output_dir = config.get("OUTPUT_DIR", "./output")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Erzeuge Zeitstempel für eindeutige Dateinamen
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Erstelle Dateinamen mit Zeitstempel
    base_filename = f"knowledge_graph_{timestamp}"
    png_filename = os.path.join(output_dir, f"{base_filename}.png")
    html_filename = os.path.join(output_dir, f"{base_filename}_interactive.html")
    
    logging.info(f"Graph visualization enabled - PNG: {png_filename}, HTML: {html_filename}")
    
    entities = result.get("entities", [])
    relationships = result.get("relationships", [])
    
    # Wenn keine Beziehungen vorhanden sind, breche die Visualisierung ab, um Fehler zu vermeiden
    if not relationships:
        logging.error("Graph visualization aborted: no relationships available.")
        return None
        
    try:
        # Baue den Graphen aus Entitäten und Beziehungen
        G = build_graph(entities, relationships, config)
        
        if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
            logging.error("Graph visualization aborted: graph has no nodes or edges.")
            return None
            
        # Rendere den Graphen als PNG und HTML
        png_path = render_graph_to_png(G, png_filename, config)
        html_path = render_graph_to_html(G, html_filename, config)
        
        logging.info(f"Knowledge Graph PNG gespeichert: {png_path}")
        print(f"Knowledge Graph PNG gespeichert: {png_path}")
        
        logging.info(f"Interaktive Knowledge Graph HTML gespeichert: {html_path}")
        print(f"Interaktive Knowledge Graph HTML gespeichert: {html_path}")
        
        # Füge die Visualisierungspfade zum Ergebnis hinzu
        if "knowledgegraph_visualisation" not in result:
            result["knowledgegraph_visualisation"] = []
            
        # Speichere die vollständigen Pfade und Dateinamen für einfachen Zugriff
        result["knowledgegraph_visualisation"].append({
            "static": os.path.basename(png_path),
            "static_path": png_path,
            "interactive": os.path.basename(html_path),
            "interactive_path": html_path,
            "timestamp": timestamp
        })
        
        return {"png": png_path, "html": html_path}
        
    except Exception as e:
        logging.error(f"Error during graph visualization: {str(e)}", exc_info=True)
        return None
