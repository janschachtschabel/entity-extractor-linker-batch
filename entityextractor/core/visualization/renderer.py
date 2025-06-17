"""
renderer.py

Hauptmodul für die Visualisierung von Wissensgraphen in verschiedene Ausgabeformate.
Dieses Modul delegiert die eigentliche Rendering-Arbeit an spezialisierte Module.
"""

import logging
import os
import networkx as nx
from pathlib import Path

# Importiere die spezialisierten Renderer
from .common import ensure_output_directory, GRAPH_STYLE, GRAPH_NODE_STYLE, GRAPH_EDGE_LENGTH
from .png_renderer import render_graph_to_png
from .html_renderer import render_graph_to_html

def visualize_graph(G, output_dir, filename_prefix="knowledge_graph", config=None):
    """
    Visualisiert einen Graphen als PNG und HTML.
    
    Args:
        G: NetworkX DiGraph Objekt
        output_dir: Ausgabeverzeichnis
        filename_prefix: Präfix für die Dateinamen
        config: Konfigurationswörterbuch
        
    Returns:
        Dictionary mit absoluten Pfaden zu den erzeugten Visualisierungen
    """
    if not G or G.number_of_nodes() == 0:
        print("DEBUG: Cannot visualize empty graph")
        logging.error("Cannot visualize empty graph")
        return {}
        
    # Stelle sicher, dass das Ausgabeverzeichnis existiert
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"DEBUG: Ausgabeverzeichnis erstellt/existiert: {os.path.abspath(output_dir)}")
    
    # Erstelle Dateinamen für die Visualisierungen
    png_path = os.path.join(output_dir, f"{filename_prefix}.png")
    html_path = os.path.join(output_dir, f"{filename_prefix}.html")
    
    print(f"DEBUG: Versuche, Graph als PNG zu speichern nach: {os.path.abspath(png_path)}")
    print(f"DEBUG: Versuche, Graph als HTML zu speichern nach: {os.path.abspath(html_path)}")
    
    # Rendere den Graphen als PNG und HTML
    png_result = render_graph_to_png(G, png_path, config)
    html_result = render_graph_to_html(G, html_path, config)
    
    # Überprüfe, ob die Dateien tatsächlich existieren
    if png_result and os.path.exists(png_result):
        print(f"DEBUG: PNG-Datei erfolgreich erstellt: {png_result}")
    else:
        print(f"DEBUG: FEHLER - PNG-Datei wurde nicht erstellt oder ist nicht auffindbar: {png_path}")
    
    if html_result and os.path.exists(html_result):
        print(f"DEBUG: HTML-Datei erfolgreich erstellt: {html_result}")
        print(f"HINWEIS: Um die interaktive HTML-Visualisierung anzuzeigen, öffnen Sie die folgende Datei in einem Browser: {html_result}")
    else:
        print(f"DEBUG: FEHLER - HTML-Datei wurde nicht erstellt oder ist nicht auffindbar: {html_path}")
    
    # Erstelle ein Dictionary mit den Ergebnissen
    visualization_info = {}
    if png_result:
        visualization_info["png"] = png_result
    if html_result:
        visualization_info["html"] = html_result
        
    # Für die Rückwärtskompatibilität
    result = {}
    if visualization_info:
        result["knowledgegraph_visualization"] = [visualization_info]
        result["knowledgegraph_visualisation"] = [visualization_info]  # Alternative Schreibweise für Rückwärtskompatibilität
        print(f"DEBUG: Visualisierungsinformationen wurden in das Ergebnis-Dictionary eingefügt")
    
    # Gib die absoluten Pfade zurück
    return {"png": os.path.abspath(png_path) if png_result else None, 
            "html": os.path.abspath(html_path) if html_result else None}

# Die Funktionen render_graph_to_png und render_graph_to_html wurden in separate Module ausgelagert
# und werden von dort importiert.
    # Stelle sicher, dass das Ausgabeverzeichnis existiert
    output_dir = os.path.dirname(output_path)
    if output_dir:
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            print(f"DEBUG: PNG output directory created or exists: {os.path.abspath(output_dir)}")
        except Exception as e:
            print(f"DEBUG: Error creating PNG output directory: {str(e)}")
            logging.error(f"Error creating PNG output directory: {str(e)}")
            return None
    
    try:
        print(f"DEBUG: Rendering graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        logging.info(f"Rendering graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        
        if G.number_of_nodes() == 0:
            print("DEBUG: Cannot render graph with no nodes.")
            logging.error("Cannot render graph with no nodes.")
            return None
            
        # We can render graphs with no edges (isolated nodes)
        if G.number_of_edges() == 0:
            print("DEBUG: Graph has no edges, will render isolated nodes")
            logging.warning("Graph has no edges, rendering isolated nodes only.")
            
        # Wähle den Stil basierend auf der Konfiguration
        style = config.get("GRAPH_STYLE", GRAPH_STYLE) if config else GRAPH_STYLE
        
        plt.figure(figsize=(12, 10))
        
        # Bestimme das Layout basierend auf dem Stil und der Graphgröße
        if G.number_of_edges() == 0:
            # Für Graphen ohne Kanten verwenden wir ein kreisförmiges Layout
            print("DEBUG: Using circular layout for graph with isolated nodes")
            pos = nx.circular_layout(G)
        elif G.number_of_nodes() < 20:
            # Für kleinere Graphen verwenden wir Kamada-Kawai für ein ästhetischeres Layout
            print("DEBUG: Using Kamada-Kawai layout for small graph")
            pos = nx.kamada_kawai_layout(G)
        else:
            # Für größere Graphen verwenden wir das schnellere Spring-Layout
            print("DEBUG: Using spring layout for larger graph")
            pos = nx.spring_layout(G, k=0.3, iterations=50)
            
            # Verbesserte Überlappungsprävention mit mehr Iterationen und größerem Mindestabstand
            min_dist = 0.2  # Größerer Mindestabstand als vorher
            max_iterations = 100  # Mehr Iterationen für bessere Verteilung
            
            # Ausgangsgröße des Layouts - vergrößern, damit Knoten mehr Platz haben
            scale_factor = 1.2
            for node in pos:
                pos[node][0] *= scale_factor
                pos[node][1] *= scale_factor
            
            # Iterativ versuchen, Überlappungen zu beseitigen
            for iteration in range(max_iterations):
                overlap = False
                for n1, p1 in pos.items():
                    for n2, p2 in pos.items():
                        if n1 != n2:
                            dx = p1[0] - p2[0]
                            dy = p1[1] - p2[1]
                            dist = (dx**2 + dy**2)**0.5
                            
                            if dist < min_dist:
                                # Stärkere Korrektur als vorher (0.6 statt 0.5)
                                factor = min_dist / (dist + 1e-6) - 1.0
                                pos[n1][0] += dx * factor * 0.6
                                pos[n1][1] += dy * factor * 0.6
                                pos[n2][0] -= dx * factor * 0.6
                                pos[n2][1] -= dy * factor * 0.6
                                overlap = True
                                
                # Wenn keine Überlappungen mehr, frühzeitig beenden
                if not overlap:
                    logging.debug(f"Optimierung des Layouts nach {iteration+1} Iterationen abgeschlossen")
                    break
                
                # Wenn mehr als die Hälfte der Iterationen durch sind und noch Überlappungen existieren,
                # dann den Mindestabstand schrittweise verringern, um eine Lösung zu finden
                if iteration > max_iterations // 2 and overlap:
                    min_dist *= 0.98
        
        normalized_entity_types, type_color_map, edge_color = get_entity_type_color_map(G, config, style)
        # Use only the color map and edge_color from the helper.
        # node_size, font_size, and alpha can be set as needed after this point, if required for plotting.
        node_colors = [type_color_map.get(normalized_entity_types[node], 'gray') for node in G.nodes()]
        node_size = 800
        font_size = 10
        alpha = 0.9
        # For the legend: map entity types to colors
        types_with_colors = {typ: type_color_map[typ] for typ in sorted(set(normalized_entity_types.values()))}

        
        # Sammle mehrfache Kanten zwischen gleichen Knoten, um sie zu biegen
        edge_count = {}
        for u, v in G.edges():
            if (u, v) in edge_count:
                edge_count[(u, v)] += 1
            else:
                edge_count[(u, v)] = 1
        
        # Zeichne Kanten mit unterschiedlichen Stilen und Farben für explizite und implizite Beziehungen
        edge_styles = []
        edge_widths = []
        edge_colors = []
        edge_connectionstyles = []
        
        # Zähle Kanten zwischen gleichen Knotenpaaren
        edge_index = {}
        
        for u, v, data in G.edges(data=True):
            # Bestimme den Stil basierend auf implizit/explizit
            if data.get('inferred') == 'implicit':
                edge_styles.append('dashed')
                edge_widths.append(1.0)
                edge_colors.append('#777777')  # Hellgrau für implizite Beziehungen
            else:  # explicit
                edge_styles.append('solid')
                edge_widths.append(1.8)
                edge_colors.append('#333333')  # Dunkelgrau für explizite Beziehungen
            
            # Bestimme gebogene Verbindungsstile für mehrfache Kanten
            key = (u, v)
            if edge_count[key] > 1:
                if key not in edge_index:
                    edge_index[key] = 0
                else:
                    edge_index[key] += 1
                
                # Berechne Biegungsgrad basierend auf der Kantenanzahl und dem Index
                idx = edge_index[key]
                total = edge_count[key]
                rad = 0.3 + (idx * 0.2)  # Zunehmender Biegungsgrad
                
                # Alternierende Biegung nach links und rechts
                if idx % 2 == 0:
                    connectionstyle = f'arc3,rad={rad}'
                else:
                    connectionstyle = f'arc3,rad=-{rad}'
                
                edge_connectionstyles.append(connectionstyle)
            else:
                edge_connectionstyles.append('arc3,rad=0')  # Gerade Linie
        
        # Verwende nur die Namen als Labels, ohne Typinformation
        node_labels = {}
        for node in G.nodes():
            name = G.nodes[node].get('name', str(node))
            # Großschreiben der Entitätsnamen
            if name and isinstance(name, str):
                name = name.capitalize()  
            node_labels[node] = name
        
        # Zeichne zuerst nur die Knoten
        nx.draw_networkx_nodes(
            G, pos, 
            node_color=node_colors,
            node_size=node_size,  # Standard-Knotengröße
            alpha=alpha
        )
        
        # Zeichne die Knotenlabels
        nx.draw_networkx_labels(
            G, pos,
            labels=node_labels,
            font_size=font_size,
            font_weight='bold'
        )
        
        # Zeichne die Kanten mit individuellen Verbindungsstilen
        for i, (u, v) in enumerate(G.edges()):
            nx.draw_networkx_edges(
                G, pos, 
                edgelist=[(u, v)],
                width=edge_widths[i],
                style=edge_styles[i],
                edge_color=[edge_colors[i]],
                arrows=True,
                arrowsize=15,
                alpha=alpha,
                connectionstyle=edge_connectionstyles[i]
            )
        
        # Zeichne Kantenbeschriftungen (Prädikate)
        edge_labels = {(u, v): data.get('predicate', '') for u, v, data in G.edges(data=True)}
        nx.draw_networkx_edge_labels(
            G, pos,
            edge_labels=edge_labels,
            font_size=font_size-2,
            font_color='#222222',
            alpha=0.8
        )
        
        # Füge eine Legende für die Entitätstypen hinzu
        legend_elements = [Line2D([0], [0], marker='o', color='w', markerfacecolor=color, 
                                  markersize=10, label=entity_type) 
                           for entity_type, color in sorted(types_with_colors.items())]
        
        # Füge Legendenelemente für die Kantentypen hinzu
        legend_elements.append(Line2D([0], [0], color=edge_color, lw=1.5, label='Explizite Beziehung'))
        legend_elements.append(Line2D([0], [0], color=edge_color, lw=1, linestyle='dashed', label='Implizite Beziehung'))
        
        plt.legend(handles=legend_elements, loc='upper right')
        plt.axis('off')
        plt.tight_layout()
        
        # Speichere die Grafik
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()
        
        abs_path = os.path.abspath(output_path)
        logging.info(f"Graph als PNG gespeichert: {abs_path}")
        print(f"DEBUG: Graph als PNG gespeichert: {abs_path}")
        return abs_path
    except Exception as e:
        print(f"DEBUG: Fehler beim Speichern des PNG-Graphen: {str(e)}")
        logging.error(f"Fehler beim Speichern des PNG-Graphen: {str(e)}")
        return None

def render_graph_to_html(G, output_path, config=None):
    """Rendert einen Graphen als interaktive HTML-Visualisierung.
    
    Args:
        G: NetworkX DiGraph Objekt
        output_path: Pfad zum Speichern der HTML-Datei
        config: Konfigurationswörterbuch
        
    Returns:
        Absoluter Pfad zur gespeicherten HTML-Datei oder None bei Fehler
    """
    # Stelle sicher, dass das Ausgabeverzeichnis existiert
    output_dir = os.path.dirname(output_path)
    if output_dir:
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            print(f"DEBUG: HTML output directory created or exists: {os.path.abspath(output_dir)}")
            logging.info(f"Ensuring output directory exists: {os.path.abspath(output_dir)}")
        except Exception as e:
            print(f"DEBUG: Error creating HTML output directory: {str(e)}")
            logging.error(f"Error creating HTML output directory: {str(e)}")
            return None
    try:
        print(f"DEBUG: Rendering interactive graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        logging.info(f"Rendering interactive graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        
        if G.number_of_nodes() == 0:
            print("DEBUG: Cannot render graph with no nodes.")
            logging.error("Cannot render graph with no nodes.")
            return None
            
        # We can render graphs with no edges (isolated nodes)
        if G.number_of_edges() == 0:
            print("DEBUG: Graph has no edges, will render isolated nodes in HTML")
            logging.warning("Graph has no edges, rendering isolated nodes only in HTML.")
            
        # Wähle Stil, Node-Stil und Kantenlänge basierend auf der Konfiguration
        style = config.get("GRAPH_STYLE", GRAPH_STYLE) if config else GRAPH_STYLE
        node_style = config.get("GRAPH_NODE_STYLE", GRAPH_NODE_STYLE) if config else GRAPH_NODE_STYLE
        edge_length = config.get("GRAPH_EDGE_LENGTH", GRAPH_EDGE_LENGTH) if config else GRAPH_EDGE_LENGTH
        
        # Bestimme das Layout für die Knoten und Kantenlänge
        # Anpassung der Kantenlänge basierend auf der Konfiguration
        k_factor = 0.3  # Standard
        if edge_length == "compact":
            k_factor = 0.15  # 50% kürzer
            min_dist = 0.1
            scale_factor = 0.9
        elif edge_length == "extended":
            k_factor = 0.45  # 50% länger
            min_dist = 0.3
            scale_factor = 1.5
        else:  # "standard"
            k_factor = 0.3
            min_dist = 0.2
            scale_factor = 1.2
            
        if G.number_of_edges() == 0:
            # Für Graphen ohne Kanten verwenden wir ein kreisförmiges Layout
            print("DEBUG: Using circular layout for HTML graph with isolated nodes")
            pos = nx.circular_layout(G)
        elif G.number_of_nodes() < 20:
            print("DEBUG: Using Kamada-Kawai layout for small HTML graph")
            pos = nx.kamada_kawai_layout(G)
        else:
            print("DEBUG: Using spring layout for larger HTML graph")
            pos = nx.spring_layout(G, k=k_factor, iterations=50)
            # Überlappungsprävention
            max_iterations = 100
            for node in pos:
                pos[node][0] *= scale_factor
                pos[node][1] *= scale_factor
            for iteration in range(max_iterations):
                overlap = False
                for n1, p1 in pos.items():
                    for n2, p2 in pos.items():
                        if n1 != n2:
                            dx = p1[0] - p2[0]
                            dy = p1[1] - p2[1]
                            dist = (dx**2 + dy**2)**0.5
                            if dist < min_dist:
                                factor = min_dist / (dist + 1e-6) - 1.0
                                pos[n1][0] += dx * factor * 0.6
                                pos[n1][1] += dy * factor * 0.6
                                pos[n2][0] -= dx * factor * 0.6
                                pos[n2][1] -= dy * factor * 0.6
                                overlap = True
                if not overlap:
                    break
                if iteration > max_iterations // 2 and overlap:
                    min_dist *= 0.98
        # Erstelle interaktives Netzwerk mit PyVis
        print(f"DEBUG: Creating PyVis network with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
        net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff", font_color="#222", notebook=False)
        
        # Wichtig: Zuerst alle Knoten zum Netzwerk hinzufügen, bevor Kanten hinzugefügt werden
        print("DEBUG: Adding nodes to PyVis network")
        # Wiederverwendung der statischen Positionen und Invertierung von Y für übereinstimmende Ausrichtung
        scale_px = 500
        pos_inter = {node: (coords[0] * scale_px, -coords[1] * scale_px) for node, coords in pos.items()}
        
        # Verwende die gemeinsame Hilfsfunktion für Farbzuordnung
        normalized_entity_types, type_color_map, edge_color = get_entity_type_color_map(G, config, style)
        
        # Füge Knoten hinzu
        for node in G.nodes():
            x, y = pos_inter.get(node, (0, 0))
            node_data = G.nodes[node]
            name = node_data.get('name', str(node))
            entity_type = node_data.get('entity_type', '')
            if not entity_type:
                entity_type = 'Unknown'
            normalized_type = normalized_entity_types.get(node, 'Unknown')
            color = {'background': type_color_map.get(normalized_type, '#f2f2f2'), 'border': '#222222'}

            # Erstelle einen verbesserten Tooltip mit allen verfügbaren Informationen und korrekter HTML-Formatierung
            title = f"<div style='padding:5px;'>"
            title += f"<div style='font-size:14px;font-weight:bold;margin-bottom:5px;'>{name}</div>"  # Name in Fettdruck
            
            if entity_type:
                title += f"<div style='margin-bottom:5px;'><i>Typ: {entity_type}</i></div>"
            
            # Füge alle verfügbaren Links und IDs hinzu
            wiki_url = node_data.get('wikipedia_url', '')
            if wiki_url:
                wiki_name = wiki_url.split('/')[-1].replace('_', ' ')
                title += f"<div style='margin-bottom:3px;'>Wikipedia: <a href='{wiki_url}' target='_blank' style='color:#0645ad;text-decoration:underline;'>{wiki_name}</a></div>"
                title += f"<div style='margin-bottom:3px;font-size:11px;color:#666;word-break:break-all;'>URL: {wiki_url}</div>"
            
            wikidata_id = node_data.get('wikidata_id', '')
            if wikidata_id:
                wikidata_url = f"https://www.wikidata.org/wiki/{wikidata_id}"
                title += f"<div style='margin-bottom:3px;'>Wikidata: <a href='{wikidata_url}' target='_blank' style='color:#0645ad;text-decoration:underline;'>{wikidata_id}</a></div>"
                title += f"<div style='margin-bottom:3px;font-size:11px;color:#666;word-break:break-all;'>URL: {wikidata_url}</div>"
            
            dbpedia_uri = node_data.get('dbpedia_uri', '')
            if dbpedia_uri:
                dbpedia_short = dbpedia_uri.split('/')[-1]
                title += f"<div style='margin-bottom:3px;'>DBpedia: <a href='{dbpedia_uri}' target='_blank' style='color:#0645ad;text-decoration:underline;'>{dbpedia_short}</a></div>"
                title += f"<div style='margin-bottom:3px;font-size:11px;color:#666;word-break:break-all;'>URI: {dbpedia_uri}</div>"
            
            title += "</div>"
            
            # Node-Styling basierend auf dem GRAPH_NODE_STYLE Parameter
            if node_style == "label_above":
                # Kleiner Kreis mit Label darüber - verwende leeren Kreis und platziere Text darüber
                net.add_node(
                    node, 
                    label=name,
                    title=title,
                    color=color, 
                    x=x, y=y, 
                    physics=False,  # Physics deaktiviert, aber Knoten können bewegt werden
                    shape='dot',    # Verwende dot statt circle
                    size=10,        # Kleinere Kreise
                    font={'size': 12, 'face': 'arial', 'background': 'rgba(255,255,255,0.8)'},
                    labelHighlightBold=True,
                    fixed=False,    # Nicht fixiert, damit Knoten bewegt werden können
                    font_vadjust=-20  # Verschiebe Label nach oben
                )
            elif node_style == "label_below":
                # Kleiner Kreis mit Label darunter - verwende leeren Kreis und platziere Text darunter
                net.add_node(
                    node, 
                    label=name,
                    title=title,
                    color=color, 
                    x=x, y=y, 
                    physics=False,  # Physics deaktiviert, aber Knoten können bewegt werden
                    shape='dot',    # Verwende dot statt circle
                    size=10,        # Kleinere Kreise
                    font={'size': 12, 'face': 'arial', 'background': 'rgba(255,255,255,0.8)'},
                    labelHighlightBold=True,
                    fixed=False,    # Nicht fixiert, damit Knoten bewegt werden können
                    font_vadjust=20  # Verschiebe Label nach unten
                )
            else:  # "label_inside" oder default
                # Großer Kreis mit Label im Kreis (Standard Vis.js Verhalten)
                net.add_node(
                    node, 
                    label=name,
                    title=title,
                    color=color, 
                    x=x, y=y, 
                    physics=False,
                    shape='circle', 
                    size=25,  # Größerer Kreis für Text
                    font={'size': 12, 'face': 'arial'},
                )
        # Set interaction options: allow node drag, zoom, pan; disable hover
        net.set_options('''{"interaction": {"dragNodes": true, "dragView": true, "zoomView": true, "hover": false}, "physics": {"enabled": false}}''')

            
        # Füge Kanten hinzu
        print(f"DEBUG: Adding {G.number_of_edges()} edges to PyVis network")
        edge_count = 0
        
        # Sammle alle Kanten zwischen den gleichen Knoten, um sie später zu versetzen
        edge_groups = {}
        for u, v, data in G.edges(data=True):
            key = (u, v)
            if key not in edge_groups:
                edge_groups[key] = []
            edge_groups[key].append(data)
        
        # Füge jede Kante mit angepasster Position hinzu
        for (u, v), edges_data in edge_groups.items():
            num_edges = len(edges_data)
            
            for i, data in enumerate(edges_data):
                edge_count += 1
                label = data.get("predicate", "")
                is_implicit = data.get("inferred", "") == "implicit"
                subject_type = data.get("subject_type", "")
                object_type = data.get("object_type", "")
                
                # Erstelle ausführlichen Tooltip für die Kante mit verbessertem HTML-Markup
                title = f"<div style='padding:5px;'>"
                title += f"<div style='font-size:14px;font-weight:bold;margin-bottom:5px;'>{label}</div>"
                title += f"<div style='margin-bottom:3px;'>Type: <b>{['Explicit', 'Implicit'][is_implicit]}</b></div>"
                if subject_type or object_type:
                    title += f"<div style='margin-bottom:3px;'>Subject Type: {subject_type}</div>"
                    title += f"<div style='margin-bottom:3px;'>Object Type: {object_type}</div>"
                title += "</div>"
                
                # Verschiedene Farben für explizite/implizite Beziehungen
                rel_color = '#999999' if is_implicit else '#333333'
                width = 1.5 if is_implicit else 3
                
                # Debug-Ausgabe für jede Kante
                print(f"DEBUG: Adding edge {edge_count}: {u} -> {v} with label '{label}'")
                
                # Berechne Versatz für mehrere Kanten zwischen den gleichen Knoten
                smooth_type = {}
                if num_edges > 1:
                    # Verwende verschiedene Kurventypen für mehrere Kanten
                    smooth_type = {
                        "type": "curvedCW",
                        "roundness": 0.2 + (i * 0.15)  # Zunehmende Kurvenrundheit für jede Kante
                    }
                    # Kürze lange Labels ab, um Überlappungen zu reduzieren
                    if len(label) > 15:
                        label = label[:12] + "..."
                else:
                    smooth_type = {"type": "continuous"}
                
                # Füge die Kante mit angepassten Eigenschaften hinzu
                net.add_edge(
                    u, v, 
                    label=label, 
                    title=title,
                    color=rel_color, 
                    arrows={"to": {"enabled": True, "scaleFactor": 1}},
                    dashes=is_implicit, 
                    width=width,
                    font={"size": 11, "color": "#333333", "background": "rgba(255,255,255,0.7)", "strokeWidth": 0}, 
                    smooth=smooth_type,
                    physics=False
                )
            
        # Speichere interaktives HTML direkt
        net.write_html(output_path)
        
        # Füge zusätzliche Netzwerkoptionen hinzu für bessere Lesbarkeit
        net.set_options('''
        {
            "interaction": {
                "dragNodes": true,
                "dragView": true,
                "zoomView": true,
                "hover": true,
                "tooltipDelay": 200
            },
            "physics": {
                "enabled": false
            },
            "edges": {
                "font": {
                    "background": "rgba(255,255,255,0.7)"
                },
                "smooth": {
                    "type": "continuous",
                    "forceDirection": "none"
                }
            }
        }
        ''')
        
        # Injiziere HTML-Legende am Seitenanfang
        # Verbesserte Legende mit deutlicherer Darstellung und fester Position
        legend_html = '''
        <div style="padding:12px; background:#f9f9f9; border:1px solid #ddd; margin:10px auto; 
                    border-radius:5px; font-size:13px; max-width:90%; text-align:center; 
                    box-shadow:0 2px 4px rgba(0,0,0,0.1); position:relative; z-index:1000;">
            <h3 style="margin-top:0; margin-bottom:8px; color:#333;">Knowledge Graph</h3>
        '''
        
        # Legende für Entitätstypen (sortiert für konsistente Anzeige)
        legend_html += '<div style="margin:8px 0"><b>Entity Types:</b> '
        for typ, color in sorted(type_color_map.items()):
            if typ and typ != 'Unknown':
                legend_html += f'<span style="background:{color};border:1px solid #444;padding:2px 6px;margin-right:6px;display:inline-block;font-size:12px;border-radius:3px;">{typ}</span>'
        legend_html += '</div>'
        
        # Legende für Beziehungstypen mit deutlicherer Darstellung
        legend_html += '<div style="margin:8px 0"><b>Relationships:</b> '
        legend_html += '<span style="border-bottom:2px solid #333;padding:2px 6px;margin-right:8px;display:inline-block;font-size:12px;">Explicit</span>'
        legend_html += '<span style="border-bottom:2px dashed #555;padding:2px 6px;display:inline-block;font-size:12px;">Implicit</span>'
        legend_html += '</div></div>'
        
        # Füge die Legende nach dem <body>-Tag ein und stelle sicher, dass sie sichtbar ist
        with open(output_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Verbesserte Methode zum Einfügen der Legende
        # Erstelle eine feste Legende am oberen Rand des Bildschirms
        fixed_legend_html = '''
        <div style="position:fixed; top:0; left:0; right:0; background:#f9f9f9; border-bottom:1px solid #ddd; 
                    padding:12px; font-size:13px; text-align:center; z-index:1000; 
                    box-shadow:0 2px 4px rgba(0,0,0,0.1);">
            <h3 style="margin-top:0; margin-bottom:8px; color:#333;">Knowledge Graph</h3>
        '''
        
        # Legende für Entitätstypen (sortiert für konsistente Anzeige)
        fixed_legend_html += '<div style="margin:8px 0"><b>Entity Types:</b> '
        for typ, color in sorted(type_color_map.items()):
            if typ and typ != 'Unknown':
                fixed_legend_html += f'<span style="background:{color};border:1px solid #444;padding:2px 6px;margin-right:6px;display:inline-block;font-size:12px;border-radius:3px;">{typ}</span>'
        fixed_legend_html += '</div>'
        
        # Legende für Beziehungstypen mit deutlicherer Darstellung
        fixed_legend_html += '<div style="margin:8px 0"><b>Relationships:</b> '
        fixed_legend_html += '<span style="border-bottom:2px solid #333;padding:2px 6px;margin-right:8px;display:inline-block;font-size:12px;">Explicit</span>'
        fixed_legend_html += '<span style="border-bottom:2px dashed #555;padding:2px 6px;display:inline-block;font-size:12px;">Implicit</span>'
        fixed_legend_html += '</div></div>'
        
        # Füge die Legende direkt nach dem <body>-Tag ein
        if '<body>' in html_content:
            html_content = html_content.replace('<body>', '<body>\n' + fixed_legend_html + '\n')
        
        # Füge Abstand zum Netzwerk hinzu, damit es unter der festen Legende erscheint
        html_content = html_content.replace('<div id="mynetwork"', '<div style="padding-top:120px;"><div id="mynetwork"')
        html_content = html_content.replace('</body>', '</div></body>')
        
        # Schreibe den geänderten HTML-Inhalt zurück
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        abs_path = os.path.abspath(output_path)
        logging.info(f"Graph als HTML gespeichert: {abs_path}")
        print(f"DEBUG: Graph als HTML gespeichert: {abs_path}")
        return abs_path
        
    except Exception as e:
        print(f"DEBUG: Fehler beim Speichern des HTML-Graphen: {str(e)}")
        logging.error(f"Fehler beim Speichern des HTML-Graphen: {str(e)}")
        return None
