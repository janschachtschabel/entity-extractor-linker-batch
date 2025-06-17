"""
visualizer.py

Hauptmodul für die Visualisierung von Wissensgraphen.
"""

import os
import logging
import datetime
from pathlib import Path

from .graph_builder import build_graph
from .png_renderer import render_graph_to_png
from .html_renderer import render_graph_to_html

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
        
    # ---------------------------------------------
    # Debug-Print-Filter: Unterdrückt alle print-Aufrufe,
    # die mit "DEBUG:" beginnen, sofern weder DEBUG_MODE
    # gesetzt ist noch das globale Log-Level DEBUG ist.
    # ---------------------------------------------
    import builtins
    debug_enabled = config.get("DEBUG_MODE", False) or logging.getLogger().isEnabledFor(logging.DEBUG)
    _orig_print = builtins.print  # Backup

    def _filtered_print(*args, **kwargs):  # type: ignore
        if args and isinstance(args[0], str) and str(args[0]).startswith("DEBUG:") and not debug_enabled:
            return  # Schlucke Debug-Ausgabe
        _orig_print(*args, **kwargs)

    builtins.print = _filtered_print

    # Überprüfe, ob die Visualisierung aktiviert ist
    if not config.get("ENABLE_GRAPH_VISUALIZATION", False):
        logging.debug("Graph visualization is disabled in config")
        return None
    else:
        logging.debug("Graph visualization is enabled in config")
        
    # Überprüfe, ob Beziehungsextraktion aktiviert ist
    if not config.get("RELATION_EXTRACTION", False):
        print("DEBUG: RELATION_EXTRACTION is disabled in config, but we'll try to visualize entities anyway")
        # We'll continue anyway and just visualize entities if there are any
        
    # Bereite Ausgabedateipfade vor und protokolliere Status
    # Verwende GRAPH_OUTPUT_DIR mit Fallback auf OUTPUT_DIR und dann auf ./output
    output_dir = config.get("GRAPH_OUTPUT_DIR", config.get("OUTPUT_DIR", "./output"))
    
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        print(f"DEBUG: Ausgabeverzeichnis existiert oder wurde erstellt: {os.path.abspath(output_dir)}")
    except Exception as e:
        print(f"DEBUG: Fehler beim Erstellen des Ausgabeverzeichnisses: {str(e)}")
        logging.error(f"Fehler beim Erstellen des Ausgabeverzeichnisses: {str(e)}")
        return None
    
    # Stelle sicher, dass der Ausgabeordner existiert und protokolliere ihn
    logging.info(f"Using graph output directory: {os.path.abspath(output_dir)}")
    print(f"DEBUG: Verwende Graph-Ausgabeverzeichnis: {os.path.abspath(output_dir)}")
    
    # Erzeuge Zeitstempel für eindeutige Dateinamen
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Erstelle Dateinamen mit Zeitstempel
    base_filename = f"knowledge_graph_{timestamp}"
    png_filename = os.path.join(output_dir, f"{base_filename}.png")
    html_filename = os.path.join(output_dir, f"{base_filename}_interactive.html")
    
    logging.info(f"Graph visualization enabled - PNG: {png_filename}, HTML: {html_filename}")
    
    entities = result.get("entities", [])
    relationships = result.get("relationships", [])
    
    print(f"DEBUG: Found {len(entities)} entities and {len(relationships)} relationships for visualization")
    
    # Wenn keine Beziehungen vorhanden sind, erstellen wir trotzdem eine Visualisierung nur mit Entitäten
    if not relationships:
        print("DEBUG: No relationships found, will create a visualization with only entities")
        logging.warning("No relationships found, creating visualization with only entities")
        # Create empty relationships for isolated nodes if needed
        if len(entities) > 0:
            print(f"DEBUG: Creating visualization with {len(entities)} isolated entities")
        else:
            print("DEBUG: No entities found either, cannot create visualization")
            logging.error("Graph visualization aborted: no entities or relationships available.")
            return None
        
    try:
        # Baue den Graphen aus Entitäten und Beziehungen
        print("DEBUG: Building graph from entities and relationships")
        G = build_graph(entities, relationships, config)
        
        if G.number_of_nodes() == 0:
            print("DEBUG: Graph has no nodes, aborting visualization")
            logging.error("Graph visualization aborted: graph has no nodes.")
            return None
            
        print(f"DEBUG: Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
            
        # Rendere den Graphen als PNG und HTML
        print("DEBUG: Rendering graph to PNG")
        try:
            # Stelle sicher, dass der Ausgabeordner existiert
            os.makedirs(os.path.dirname(png_filename), exist_ok=True)
            print(f"DEBUG: Output directory created or exists: {os.path.dirname(png_filename)}")
            
            # Verwende eine lokale Kopie der render_graph_to_png-Funktion, um Importprobleme zu vermeiden
            from .png_renderer import render_graph_to_png as render_png
            png_path = render_png(G, png_filename, config)
            
            # Überprüfe, ob die PNG-Datei tatsächlich erstellt wurde
            if png_path and os.path.exists(png_path):
                file_size = os.path.getsize(png_path)
                print(f"DEBUG: PNG-Visualisierung erfolgreich erstellt: {png_path} (Größe: {file_size} Bytes)")
                if file_size == 0:
                    print("DEBUG: WARNING - PNG file has zero size!")
                    logging.warning(f"PNG file has zero size: {png_path}")
            else:
                print(f"DEBUG: PNG-Datei wurde nicht gefunden nach dem Rendering: {png_filename}")
                png_path = None
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"DEBUG: Fehler beim Rendern der PNG-Visualisierung: {str(e)}")
            print(f"DEBUG: Fehlerdetails: {error_details}")
            logging.error(f"Fehler beim Rendern der PNG-Visualisierung: {str(e)}")
            logging.error(f"Fehlerdetails: {error_details}")
            png_path = None
        print("DEBUG: Rendering graph to HTML")
        html_path = render_graph_to_html(G, html_filename, config)
        
        # Wenn nur die HTML-Visualisierung erfolgreich war, geben wir trotzdem ein Ergebnis zurück
        if not html_path:
            print("DEBUG: Failed to render HTML visualization")
            logging.error("Failed to render HTML visualization")
            return None
            
        # Wenn die PNG-Visualisierung fehlgeschlagen ist, protokollieren wir das, aber brechen nicht ab
        if not png_path:
            print("DEBUG: Failed to render PNG visualization, but HTML was successful")
            logging.warning("Failed to render PNG visualization, but HTML was successful")
        
        logging.info(f"Knowledge Graph PNG gespeichert: {png_path}")
        print(f"Knowledge Graph PNG gespeichert: {png_path}")
        
        logging.info(f"Interaktive Knowledge Graph HTML gespeichert: {html_path}")
        print(f"Interaktive Knowledge Graph HTML gespeichert: {html_path}")
        
        # Gib einen deutlichen Hinweis, wie die HTML-Datei geöffnet werden kann
        print("\n" + "=" * 80)
        print(f"WICHTIG: Um die interaktive HTML-Visualisierung anzuzeigen, öffnen Sie diese Datei im Browser:")
        print(f"\n  {os.path.abspath(html_path)}\n")
        print("Sie können die Datei durch Doppelklick im Datei-Explorer öffnen oder die URL in Ihren Browser kopieren.")
        print("=" * 80 + "\n")
        
        # Versuche, die HTML-Datei im Standardbrowser zu öffnen (optional)
        try:
            import webbrowser
            # Nur versuchen, wenn explizit in der Konfiguration aktiviert
            if config.get("AUTO_OPEN_HTML_VISUALIZATION", False):
                print("Versuche, die HTML-Visualisierung automatisch im Browser zu öffnen...")
                webbrowser.open(f"file://{os.path.abspath(html_path)}")
                print("Browser wurde gestartet. Falls sich kein Fenster geöffnet hat, öffnen Sie die Datei manuell.")
        except Exception as e:
            print(f"Konnte den Browser nicht automatisch öffnen: {str(e)}")
            print("Bitte öffnen Sie die HTML-Datei manuell in Ihrem Browser.")
            # Fehler beim Öffnen des Browsers sollten nicht den gesamten Prozess abbrechen
        
        # Füge die Visualisierungspfade zum Ergebnis hinzu
        # Stelle sicher, dass wir die Liste initialisieren
        if "knowledgegraph_visualization" not in result:
            result["knowledgegraph_visualization"] = []
            
        # Erstelle die Visualisierungsinformationen basierend auf den verfügbaren Dateien
        visualization_info = {
            "interactive": os.path.basename(html_path),
            "interactive_path": os.path.abspath(html_path),
            "timestamp": timestamp
        }
        
        # Füge PNG-Informationen hinzu, wenn verfügbar
        if png_path:
            visualization_info["static"] = os.path.basename(png_path)
            visualization_info["static_path"] = os.path.abspath(png_path)
        
        # Füge die Informationen zum Ergebnis hinzu (einheitlicher Schlüssel)
        result["knowledgegraph_visualization"] = [visualization_info]
        
        print(f"DEBUG: Visualisierungspfade zum Ergebnis hinzugefügt: {visualization_info}")
        
        # Rückgabe der verfügbaren Pfade
        return_dict = {"html": os.path.abspath(html_path)}
        if png_path:
            return_dict["png"] = os.path.abspath(png_path)
            
        return return_dict
        
    except Exception as e:
        print(f"DEBUG: Error during graph visualization: {str(e)}")
        logging.error(f"Error during graph visualization: {str(e)}", exc_info=True)
        return None
