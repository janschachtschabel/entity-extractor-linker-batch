"""
renderer.py

Funktionen zum Rendern von Wissensgraphen in verschiedene Ausgabeformate.
"""

import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import os
import logging
import math
from pathlib import Path
from pyvis.network import Network

def render_graph_to_png(G, output_path, config=None):
    """
    Rendert einen gerichteten Graphen als statisches PNG-Bild.
    
    Args:
        G (nx.DiGraph): Der zu rendernde Graph.
        output_path (str): Der Ausgabepfad für die PNG-Datei.
        config (dict, optional): Konfigurationsoptionen für das Rendering.
        
    Returns:
        str: Der Pfad zur erzeugten PNG-Datei.
    """
    try:
        logging.info(f"Rendering graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        
        if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
            logging.error("Cannot render graph with no nodes or edges.")
            return None
            
        # Erstelle Ausgabeverzeichnis, falls nicht vorhanden
        output_dir = os.path.dirname(output_path)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Wähle den Stil basierend auf der Konfiguration
        style = config.get("GRAPH_STYLE", "modern") if config else "modern"
        
        plt.figure(figsize=(12, 10))
        
        # Bestimme das Layout basierend auf dem Stil und der Graphgröße
        if G.number_of_nodes() < 20:
            # Für kleinere Graphen verwenden wir Kamada-Kawai für ein ästhetischeres Layout
            pos = nx.kamada_kawai_layout(G)
        else:
            # Für größere Graphen verwenden wir das schnellere Spring-Layout
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
        
                # --- Helper for entity type normalization and color assignment ---
        def get_entity_type_color_map(G, config, style):
            import matplotlib.cm as cm
            import matplotlib.colors as mcolors
            entity_types = set()
            normalized_entity_types = {}
            for node, data in G.nodes(data=True):
                entity_type = data.get('entity_type', '')
                name = data.get('name', str(node))
                if (not entity_type or entity_type == 'Unknown') and config and 'ENTITY_TYPES' in config:
                    entity_types_map = config.get('ENTITY_TYPES', {})
                    for k, v in entity_types_map.items():
                        if k.strip().lower() == name.strip().lower():
                            entity_type = v
                            G.nodes[node]['entity_type'] = entity_type
                            break
                if not entity_type:
                    entity_type = 'Unknown'
                normalized_type = entity_type.capitalize()
                entity_types.add(normalized_type)
                normalized_entity_types[node] = normalized_type
            # Use a large colormap if needed
            sorted_types = sorted(entity_types)
            n_types = len(sorted_types)
            if style == "minimal":
                base_colors = ['#333333', '#555555', '#777777', '#999999', '#BBBBBB', '#DDDDDD']
                colors = (base_colors * ((n_types // len(base_colors)) + 1))[:n_types]
                edge_color = '#555555'
            elif style == "classic":
                base_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00', '#ffff33']
                colors = (base_colors * ((n_types // len(base_colors)) + 1))[:n_types]
                edge_color = '#000000'
            else:
                # Use tab20 or hsv for modern/large graphs
                if n_types <= 20:
                    cmap = cm.get_cmap('tab20', n_types)
                else:
                    cmap = cm.get_cmap('hsv', n_types)
                colors = [mcolors.to_hex(cmap(i)) for i in range(n_types)]
                edge_color = '#333333'
            type_color_map = {typ: colors[i] for i, typ in enumerate(sorted_types)}
            return normalized_entity_types, type_color_map, edge_color
        # --- End helper ---

        normalized_entity_types, type_color_map, edge_color = get_entity_type_color_map(G, config, style)
        # Use only the color map and edge_color from the helper.
        # node_size, font_size, and alpha can be set as needed after this point, if required for plotting.
        node_colors = [type_color_map.get(normalized_entity_types[node], 'gray') for node in G.nodes()]
        node_size = 800
        font_size = 10
        alpha = 0.9
        # For the legend: map entity types to colors
        types_with_colors = {typ: type_color_map[typ] for typ in sorted(set(normalized_entity_types.values()))}

        
        # Zeichne Kanten mit unterschiedlichen Stilen und Farben für explizite und implizite Beziehungen
        edge_styles = []
        edge_widths = []
        edge_colors = []
        
        for _, _, data in G.edges(data=True):
            if data.get('inferred') == 'implicit':
                edge_styles.append('dashed')
                edge_widths.append(1.0)
                edge_colors.append('#777777')  # Hellgrau für implizite Beziehungen
            else:  # explicit
                edge_styles.append('solid')
                edge_widths.append(1.8)
                edge_colors.append('#333333')  # Dunkelgrau für explizite Beziehungen
        
        # Verwende nur die Namen als Labels, ohne Typinformation
        node_labels = {}
        for node in G.nodes():
            name = G.nodes[node].get('name', str(node))
            # Großschreiben der Entitätsnamen
            if name and isinstance(name, str):
                name = name.capitalize()  
            node_labels[node] = name
        
        # Zeichne den Graphen
        nx.draw_networkx(
            G, pos, 
            labels=node_labels,
            node_color=node_colors,
            node_size=node_size,  # Standard-Knotengröße
            font_size=font_size,
            font_weight='bold',
            arrowsize=15,
            width=edge_widths,
            style=edge_styles,
            edge_color=edge_colors,
            arrows=True,
            alpha=alpha
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
        
        # Speichere das Bild
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logging.info(f"Knowledge Graph PNG gespeichert: {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Fehler beim Rendern des PNG-Graphen: {str(e)}", exc_info=True)
        return None

def render_graph_to_html(G, output_path, config=None):
    """
    Rendert einen gerichteten Graphen als interaktive HTML-Visualisierung.
    
    Args:
        G (nx.DiGraph): Der zu rendernde Graph.
        output_path (str): Der Ausgabepfad für die HTML-Datei.
        config (dict, optional): Konfigurationsoptionen für das Rendering.
        
    Returns:
        str: Der Pfad zur erzeugten HTML-Datei.
    """
    try:
        logging.info(f"Rendering interactive graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        
        if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
            logging.error("Cannot render graph with no nodes or edges.")
            return None
            
        # Erstelle Ausgabeverzeichnis, falls nicht vorhanden
        output_dir = os.path.dirname(output_path)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Wähle Stil, Node-Stil und Kantenlänge basierend auf der Konfiguration
        style = config.get("GRAPH_STYLE", "modern") if config else "modern"
        node_style = config.get("GRAPH_NODE_STYLE", "label_above") if config else "label_above"
        edge_length = config.get("GRAPH_EDGE_LENGTH", "standard") if config else "standard"
        
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
            
        if G.number_of_nodes() < 20:
            pos = nx.kamada_kawai_layout(G)
        else:
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
        net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff", font_color="#222", notebook=False)
        # Wiederverwendung der statischen Positionen und Invertierung von Y für übereinstimmende Ausrichtung
        scale_px = 500
        pos_inter = {node: (coords[0] * scale_px, -coords[1] * scale_px) for node, coords in pos.items()}
        
                # Use shared helper for normalization and color assignment
        import matplotlib.cm as cm
        import matplotlib.colors as mcolors
        def get_entity_type_color_map(G, config, style):
            entity_types = set()
            normalized_entity_types = {}
            for node, data in G.nodes(data=True):
                entity_type = data.get('entity_type', '')
                name = data.get('name', str(node))
                if (not entity_type or entity_type == 'Unknown') and config and 'ENTITY_TYPES' in config:
                    entity_types_map = config.get('ENTITY_TYPES', {})
                    for k, v in entity_types_map.items():
                        if k.strip().lower() == name.strip().lower():
                            entity_type = v
                            G.nodes[node]['entity_type'] = entity_type
                            break
                if not entity_type:
                    entity_type = 'Unknown'
                normalized_type = entity_type.capitalize()
                entity_types.add(normalized_type)
                normalized_entity_types[node] = normalized_type
            sorted_types = sorted(entity_types)
            n_types = len(sorted_types)
            if style == "minimal":
                base_colors = ['#333333', '#555555', '#777777', '#999999', '#BBBBBB', '#DDDDDD']
                colors = (base_colors * ((n_types // len(base_colors)) + 1))[:n_types]
                edge_color = '#555555'
            elif style == "classic":
                base_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00', '#ffff33']
                colors = (base_colors * ((n_types // len(base_colors)) + 1))[:n_types]
                edge_color = '#000000'
            else:
                if n_types <= 20:
                    cmap = cm.get_cmap('tab20', n_types)
                else:
                    cmap = cm.get_cmap('hsv', n_types)
                colors = [mcolors.to_hex(cmap(i)) for i in range(n_types)]
                edge_color = '#333333'
            type_color_map = {typ: colors[i] for i, typ in enumerate(sorted_types)}
            return normalized_entity_types, type_color_map, edge_color
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

            # Tooltip für alle Node-Stile gleich aufbauen
            title = f"<b>{name}</b>"
            if entity_type:
                title += f"<br><i>Typ: {entity_type}</i>"
            # Füge nur wichtige Links hinzu (ohne IDs)
            wiki_url = node_data.get('wikipedia_url', '')
            if wiki_url:
                wiki_name = wiki_url.split('/')[-1].replace('_', ' ')
                title += f"<br>Wikipedia: <a href='{wiki_url}' target='_blank'>{wiki_name}</a>"
            
            # Node-Styling basierend auf dem GRAPH_NODE_STYLE Parameter
            if node_style == "label_above":
                # Kleiner Kreis mit Label darüber (manueller Abstand mit Newline)
                # Label wird beim ersten Rendern über dem Kreis angezeigt
                net.add_node(
                    node, 
                    label=name + '\n\n',  # Zwei Zeilenumbrüche schieben Label nach oben
                    title=title,
                    color=color, 
                    x=x, y=y, 
                    physics=False,
                    shape='circle', 
                    size=14,  # Kleine Kreise
                    font={'size': 12, 'face': 'arial', 'background': 'rgba(255,255,255,0.8)'},
                )
            elif node_style == "label_below":
                # Kleiner Kreis mit Label darunter
                net.add_node(
                    node, 
                    label='\n\n' + name,  # Zwei Zeilenumbrüche schieben Label nach unten 
                    title=title,
                    color=color, 
                    x=x, y=y, 
                    physics=False,
                    shape='circle', 
                    size=14,  # Kleine Kreise
                    font={'size': 12, 'face': 'arial', 'background': 'rgba(255,255,255,0.8)'},
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
        net.set_options('''{
            "interaction": {
                "dragNodes": true,
                "dragView": true,
                "zoomView": true,
                "hover": false
            },
            "physics": {
                "enabled": false
            }
        }''')
        # Optionally, try to further prevent node overlap (vis.js limitation)
        # Legend injection remains unchanged below

            
        # Füge Kanten hinzu
        for u, v, data in G.edges(data=True):
            label = data.get("predicate", "")
            is_implicit = data.get("inferred", "") == "implicit"
            subject_type = data.get("subject_type", "")
            object_type = data.get("object_type", "")
            
            # Erstelle ausführlichen Tooltip für die Kante
            title = f"\u003cb\u003e{label}\u003c/b\u003e"
            title += f"\u003cbr\u003eType: {['explicit', 'implicit'][is_implicit]}"
            if subject_type or object_type:
                title += f"\u003cbr\u003eSubject Type: {subject_type}\u003cbr\u003eObject Type: {object_type}"
            
            # Verschiedene Farben für explizite/implizite Beziehungen
            rel_color = '#999999' if is_implicit else '#333333'
            width = 1 if is_implicit else 2
            
            net.add_edge(
                u, v, 
                label=label, 
                title=title,
                color=rel_color, 
                arrows="to",
                dashes=is_implicit, 
                width=width,
                font={"size": 11}, 
                smooth=False
            )
            
        # Speichere interaktives HTML direkt
        net.write_html(output_path)
        
        # Injiziere HTML-Legende am Seitenanfang
        legend_html = '<div style="padding:8px; background:#f9f9f9; border:1px solid #ddd; margin:0 auto 8px auto; border-radius:5px; font-size:12px; max-width:800px; text-align:center;">'
        legend_html += '<h4 style="margin-top:0; margin-bottom:5px;">Knowledge Graph</h4>'
        
        # Legende für Entitätstypen (sortiert für konsistente Anzeige)
        legend_html += '<div style="margin:5px 0"><b>Entity Types:</b> '
        for typ, color in sorted(type_color_map.items()):
            if typ and typ != 'Unknown':
                legend_html += f'<span style="background:{color};border:1px solid #444;padding:1px 4px;margin-right:4px;display:inline-block;font-size:11px;">{typ}</span>'
        legend_html += '</div>'
        
        # Legende für Beziehungstypen
        legend_html += '<div style="margin:5px 0"><b>Relationships:</b> '
        legend_html += '<span style="border-bottom:1px solid #333;padding:1px 4px;margin-right:5px;display:inline-block;font-size:11px;">Explicit</span>'
        legend_html += '<span style="border-bottom:1px dashed #555;padding:1px 4px;display:inline-block;font-size:11px;">Implicit</span>'
        legend_html += '</div></div>'
        
        # Füge die Legende nach dem <body>-Tag ein
        with open(output_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
            
        if '<body>' in html_content:
            html_content = html_content.replace('<body>', '<body>\n' + legend_html + '\n')
            
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logging.info(f"Interaktive Knowledge Graph HTML gespeichert: {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Fehler beim Rendern des HTML-Graphen: {str(e)}", exc_info=True)
        return None
        return None
