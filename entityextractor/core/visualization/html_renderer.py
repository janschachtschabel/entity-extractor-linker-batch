"""
Funktionen zum Rendern von Graphen als interaktive HTML-Visualisierungen.
"""
import logging
import os
import sys
import networkx as nx
from pathlib import Path

# Versuche, PyVis zu importieren, mit Fehlerbehandlung
try:
    from pyvis.network import Network
except ImportError as e:
    print(f"DEBUG: Fehler beim Importieren von PyVis: {str(e)}")
    print(f"DEBUG: Python-Pfad: {sys.path}")
    logging.error(f"Fehler beim Importieren von PyVis: {str(e)}")
    # Versuche, PyVis zu installieren, wenn es nicht gefunden wird
    try:
        import pip
        print("DEBUG: Versuche, PyVis zu installieren...")
        pip.main(['install', 'pyvis'])
        from pyvis.network import Network
        print("DEBUG: PyVis erfolgreich installiert und importiert")
    except Exception as install_error:
        print(f"DEBUG: Konnte PyVis nicht installieren: {str(install_error)}")
        logging.error(f"Konnte PyVis nicht installieren: {str(install_error)}")

from .common import get_entity_type_color_map, ensure_output_directory, GRAPH_NODE_STYLE, GRAPH_EDGE_LENGTH

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
    if not ensure_output_directory(output_path):
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
        style = config.get("GRAPH_STYLE", "modern") if config else "modern"
        node_style = config.get("GRAPH_NODE_STYLE", GRAPH_NODE_STYLE) if config else GRAPH_NODE_STYLE
        edge_length = config.get("GRAPH_EDGE_LENGTH", GRAPH_EDGE_LENGTH) if config else GRAPH_EDGE_LENGTH
        
        # Bestimme das Layout für die Knoten und Kantenlänge
        if G.number_of_nodes() < 20:
            spring_length = edge_length
        else:
            # Für größere Graphen längere Kanten verwenden
            spring_length = edge_length * 1.5
            
        # Verwende die gemeinsame Hilfsfunktion für Farbzuordnung
        normalized_entity_types, type_color_map, _ = get_entity_type_color_map(G, config, style)
        
        # Erstelle interaktives Netzwerk mit PyVis
        print(f"DEBUG: Creating PyVis network with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
        
        # Definiere Netzwerkoptionen direkt
        options = {
            "interaction": {
                "dragNodes": True,
                "dragView": True,
                "zoomView": True,
                "hover": False
            },
            "physics": {
                "enabled": False
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
        
        # Übergebe Optionen direkt bei der Erstellung
        try:
            net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff", font_color="#222", notebook=False)
            # Wir setzen die Optionen später, da einige PyVis-Versionen keine options-Parameter akzeptieren
        except Exception as e:
            print(f"DEBUG: Error creating PyVis network: {str(e)}")
            logging.error(f"Error creating PyVis network: {str(e)}")
            return None
        
        # ----- Positionierung der Knoten -----
        # Layout-Modus bestimmen ("spring" = PNG-ähnlich, "hierarchical" = Baum)
        # Unterstützte Layouts: spring (Fruchterman-Reingold), kamada_kawai (KK), hierarchical
        layout_mode = config.get("HTML_LAYOUT", "spring") if config else "spring"
        # Passe Kantenlängen gemäß Benutzerwunsch an: +50 % Mindestlänge, −30 % Maximallänge
        min_edge_length = int((config.get("HTML_MIN_EDGE_LENGTH", 120) if config else 120) * 1.5)
        max_edge_length = int((config.get("HTML_MAX_EDGE_LENGTH", 250) if config else 250) * 0.7)

        print(f"DEBUG: Adding nodes to PyVis network (layout_mode={layout_mode})")

        # Maßstab für spring-Koordinaten (niedriger Wert vermeidet überlange Kanten)
        scale_px = 600
        pos = {}
        if layout_mode == "spring":
            # ===== SPRING LAYOUT (Fruchterman-Reingold, Standard wie PNG) =====
            # ===== SPRING LAYOUT (wie PNG) =====
            if G.number_of_edges() == 0:
                import networkx as nx
                pos = nx.circular_layout(G)
            else:
                import networkx as nx
                pos = nx.spring_layout(G, k=0.5, iterations=100, seed=42)
            # Koordinaten invertieren (y) und skalieren
            pos_inter = {n: (c[0] * scale_px, -c[1] * scale_px) for n, c in pos.items()}
            # Auto-Korrektur des Seitenverhältnisses: verbreitert X, wenn Graph stark gestreckt ist
            if len(pos_inter) > 1:
                xs = [p[0] for p in pos_inter.values()]
                ys = [p[1] for p in pos_inter.values()]
                width_span = max(xs) - min(xs)
                height_span = max(ys) - min(ys)
                if height_span > width_span * 1.2:
                    widen_factor = (height_span / (width_span + 1e-6)) * 0.8
                    pos_inter = {n: (x * widen_factor, y) for n, (x, y) in pos_inter.items()}
        elif layout_mode in ("kamada_kawai", "kk"):
            # ===== KAMADA-KAWAI LAYOUT (gleichmäßige Kantenlängen, gute Mindestabstände) =====
            import networkx as nx
            pos = nx.kamada_kawai_layout(G, weight=None)
            pos_inter = {n: (c[0] * scale_px, -c[1] * scale_px) for n, c in pos.items()}
            # Auto-Korrektur des Seitenverhältnisses: verbreitert X, wenn Graph stark gestreckt ist
            if len(pos_inter) > 1:
                xs = [p[0] for p in pos_inter.values()]
                ys = [p[1] for p in pos_inter.values()]
                width_span = max(xs) - min(xs)
                height_span = max(ys) - min(ys)
                if height_span > width_span * 1.2:
                    widen_factor = (height_span / (width_span + 1e-6)) * 0.8
                    pos_inter = {n: (x * widen_factor, y) for n, (x, y) in pos_inter.items()}
        else:
            # ===== HIERARCHICAL LAYOUT =====
            # Vis.js berechnet Positionen automatisch
            pos_inter = {}

        
        # Zweiter Schritt: einfache Nachbearbeitung, um Überlappungen zu vermeiden
        # Wir verschieben Knoten voneinander weg, wenn sie zu nah (< min_px) beieinander liegen.
        import math
        min_px = 120  # Mindestabstand in Pixeln (+50 %)
        moved = True
        max_push_iter = 50
        iter_count = 0
        while moved and iter_count < max_push_iter:
            moved = False
            iter_count += 1
            for n1 in G.nodes():
                for n2 in G.nodes():
                    if n1 >= n2:
                        continue
                    x1, y1 = pos_inter[n1]
                    x2, y2 = pos_inter[n2]
                    dx = x2 - x1
                    dy = y2 - y1
                    dist = math.hypot(dx, dy)
                    if dist < min_px and dist > 1e-6:
                        # Schiebe beide Knoten auseinander
                        push = (min_px - dist) / 2.0
                        # Normalisiere
                        ux = dx / dist
                        uy = dy / dist
                        pos_inter[n1] = (x1 - ux * push, y1 - uy * push)
                        pos_inter[n2] = (x2 + ux * push, y2 + uy * push)
                        moved = True
        
        # Füge Knoten hinzu (bei hierarchical ohne feste Koordinaten)
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
                
            
            wikidata_id = node_data.get('wikidata_id', '')
            if wikidata_id:
                wikidata_url = f"https://www.wikidata.org/wiki/{wikidata_id}"
                title += f"<div style='margin-bottom:3px;'>Wikidata: <a href='{wikidata_url}' target='_blank' style='color:#0645ad;text-decoration:underline;'>{wikidata_id}</a></div>"
                
            
            dbpedia_uri = node_data.get('dbpedia_uri', '')
            if dbpedia_uri:
                dbpedia_short = dbpedia_uri.split('/')[-1]
                title += f"<div style='margin-bottom:3px;'>DBpedia: <a href='{dbpedia_uri}' target='_blank' style='color:#0645ad;text-decoration:underline;'>{dbpedia_short}</a></div>"
                
            
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
        if layout_mode == "hierarchical":
            net.set_options('''{
                "interaction": {"dragNodes": true, "dragView": true, "zoomView": true, "hover": true},
                "layout": {"hierarchical": {"enabled": true, "direction": "UD", "sortMethod": "hubsize", "levelSeparation": 180, "nodeSpacing": 150}},
                "physics": {"enabled": false}
            }''')
        else:
            net.set_options('''{"interaction": {"dragNodes": true, "dragView": true, "zoomView": true, "hover": true}, "physics": {"enabled": false}}''')

            
        # Füge Kanten hinzu
        print(f"DEBUG: Adding {G.number_of_edges()} edges to PyVis network")
        total_edges_added = 0
        
        # Erstelle ein Dictionary, um die Anzahl der Kanten zwischen Knotenpaaren zu zählen
        # Unabhängig von der Richtung
        edge_count = {}
        for u, v, data in G.edges(data=True):
            # Normalisiere das Knotenpaar (kleinerer Knoten zuerst)
            norm_key = (u, v) if u <= v else (v, u)
            if norm_key not in edge_count:
                edge_count[norm_key] = 0
            edge_count[norm_key] += 1
            
        print(f"DEBUG: Edge count between node pairs: {edge_count}")             
            
        # Sammle alle Kanten zwischen den gleichen Knoten, um sie später zu versetzen
        edge_groups = {}
        for u, v, data in G.edges(data=True):
            key = (u, v)
            if key not in edge_groups:
                edge_groups[key] = []
            edge_groups[key].append(data)
            
            # Prüfe, ob es mehrere Kanten zwischen den Knoten gibt (unabhängig von der Richtung)
            norm_key = (u, v) if u <= v else (v, u)
            if edge_count[norm_key] > 1:
                # Wenn es mehrere Kanten gibt, markiere sie für gebogene Darstellung
                if len(edge_groups[key]) == 1 and not data.get("_multiple", False):
                    data["_multiple"] = True
        
        # Füge jede Kante mit angepasster Position hinzu
        for (u, v), edges_data in edge_groups.items():
            num_edges = len(edges_data)
            
            for i, data in enumerate(edges_data):
                # Überspringe Dummy-Einträge
                if data.get("_dummy", False):
                    continue
                    
                total_edges_added += 1
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
                edge_length = max(min_edge_length, min(max_edge_length, int((min_edge_length+max_edge_length)/2)))
                
                # Debug-Ausgabe für jede Kante
                print(f"DEBUG: Adding edge {total_edges_added}: {u} -> {v} with label '{label}'")
                
                # Berechne Versatz für mehrere Kanten zwischen den gleichen Knoten
                smooth_type = {}
                
                # Prüfe, ob es mehrere Kanten zwischen den Knoten gibt (unabhängig von der Richtung)
                norm_key = (u, v) if u <= v else (v, u)
                # Verwende die tatsächliche Anzahl der Kanten zwischen diesem Knotenpaar
                has_multiple_edges = edge_count.get(norm_key, 0) > 1
                print(f"DEBUG: Edge {u} -> {v}: has_multiple_edges={has_multiple_edges}, count={edge_count.get(norm_key, 0)}")
                
                # Stelle sicher, dass wir IMMER nur bei mehreren Kanten zwischen denselben Knoten gebogene Linien verwenden
                # Setze den Standard auf gerade Linien
                smooth_type = {"type": "continuous", "forceDirection": "none"}
                
                # Prüfe explizit, ob diese Kante Teil eines Paares mit mehreren Kanten ist
                norm_key = (u, v) if u <= v else (v, u)
                has_multiple_edges = edge_count.get(norm_key, 0) > 1
                print(f"DEBUG: Edge {u} -> {v}: has_multiple_edges={has_multiple_edges}, count={edge_count.get(norm_key, 0)}")
                
                # Forciere gerade Linien, wenn es nur eine Kante zwischen den Knoten gibt
                if not has_multiple_edges:
                    smooth_type = {"type": "continuous", "forceDirection": "none"}
                
                if has_multiple_edges:
                    # Berechne einen eindeutigen Index für diese Kante zwischen diesem Knotenpaar
                    edge_index = 0
                    edge_count_for_pair = 0
                    for idx, (eu, ev, _) in enumerate(G.edges(data=True)):
                        if (eu == u and ev == v) or (eu == v and ev == u):
                            if (eu == u and ev == v):
                                edge_index = edge_count_for_pair
                            edge_count_for_pair += 1
                    
                    # Berechne eine gleichmäßigere Verteilung der Kurven basierend auf der Gesamtzahl
                    total_edges = edge_count.get(norm_key, 0)
                    if total_edges > 1:
                        # Berechne den Winkel basierend auf der Position dieser Kante
                        angle_step = 0.4 / total_edges
                        if edge_index % 2 == 0:
                            # Gerade Indizes: positive Kurve (im Uhrzeigersinn)
                            curve_value = 0.2 + (angle_step * (edge_index // 2))
                            curve_direction = "curvedCW"
                        else:
                            # Ungerade Indizes: negative Kurve (gegen den Uhrzeigersinn)
                            curve_value = 0.2 + (angle_step * (edge_index // 2))
                            curve_direction = "curvedCCW"
                            
                        smooth_type = {
                            "type": curve_direction,
                            "roundness": curve_value
                        }
                        
                        # Optimiere Textposition für gebogene Kanten
                        # Abwechselnd horizontale und mittige Ausrichtung
                        font_align = "horizontal" if edge_index % 2 == 0 else "middle"
                        
                        # Kürze lange Labels ab, um Überlappungen zu reduzieren
                        if len(label) > 15:
                            label = label[:12] + "..."
                    else:
                        # Wenn es nur eine Kante gibt, verwende eine gerade Linie
                        smooth_type = {"type": "continuous", "forceDirection": "none"}
                else:
                    # Gerade Kanten für einzelne Verbindungen
                    smooth_type = {"type": "continuous", "forceDirection": "none"}
                
                # Füge die Kante mit angepassten Eigenschaften hinzu
                font_options = {
                    "size": 11, 
                    "color": "#333333", 
                    "background": "rgba(255,255,255,0.7)", 
                    "strokeWidth": 0
                }
                
                # Füge zusätzliche Font-Optionen für gebogene Kanten hinzu
                if has_multiple_edges and 'font_align' in locals():
                    font_options["align"] = font_align
                    # Verschiebe Text leicht nach außen bei gebogenen Kanten
                    font_options["vadjust"] = -10
                    
                    # Bei gebogenen Kanten positionieren wir die Beschriftung weiter vom Mittelpunkt entfernt
                    # und fügen einen Hintergrund hinzu, um die Lesbarkeit zu verbessern
                    font_options["background"] = "rgba(255,255,255,0.9)"
                    font_options["strokeWidth"] = 2
                    font_options["strokeColor"] = "rgba(255,255,255,0.9)"
                
                net.add_edge(
                    u, v, 
                    label=label, 
                    title=title,
                    color=rel_color, 
                    arrows={"to": {"enabled": True, "scaleFactor": 1}},
                    dashes=is_implicit, 
                    width=width,
                    font=font_options, 
                    smooth=smooth_type,
                    physics=False
                )
            
        # Füge zusätzliche Netzwerkoptionen hinzu für bessere Lesbarkeit und größere Knotenabstände
        # Wir verwenden keine set_options-Methode mehr, da sie in einigen PyVis-Versionen nicht verfügbar ist
        # Stattdessen werden die Optionen direkt in der HTML-Datei nach dem Speichern eingefügt
        print("DEBUG: Network options will be injected directly into HTML file")
        
        # Speichere interaktives HTML direkt
        try:
            net.write_html(output_path)
            print(f"DEBUG: HTML file saved to {output_path}")
            
            # Füge Netzwerkoptionen direkt in die HTML-Datei ein
            try:
                with open(output_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Füge JavaScript-Optionen nach dem Netzwerk-Initialisierungscode ein
                network_options = '''
                network.setOptions({
                    interaction: {
                        dragNodes: true,
                        dragView: true,
                        zoomView: true,
                        hover: true,
                        tooltipDelay: 200
                    },
                    nodes: {
                        shape: 'dot',
                        size: 20,  // Größere Knoten
                        font: {
                            size: 14,
                            face: 'arial',
                            strokeWidth: 3,
                            strokeColor: '#ffffff'
                        },
                        margin: 12  // Mehr Abstand um die Knoten
                    },
                    physics: {
                        enabled: true,  // Physik aktivieren für initiale Positionierung
                        stabilization: {
                            iterations: 500,  // Deutlich mehr Iterationen für stabileres Layout
                            fit: true
                        },
                        solver: 'forceAtlas2Based',  // Verwende forceAtlas2Based für bessere Knotenverteilung
                        forceAtlas2Based: {
                            gravitationalConstant: -3000,  // Stärkere Abstoßung
                            centralGravity: 0.001,  // Minimale Zentralgravitation
                            springLength: 400,  // Extrem lange Federn für maximalen Abstand
                            springConstant: 0.01,  // Sehr weiche Federn
                            damping: 0.4,
                            avoidOverlap: 1.0  // Maximale Überlappungsvermeidung
                        },
                        minVelocity: 0.3,
                        maxVelocity: 50,
                        timestep: 0.3
                    },
                    edges: {
                        font: {
                            size: 12,
                            background: "rgba(255,255,255,0.9)",
                            strokeWidth: 2,
                            strokeColor: "rgba(255,255,255,0.9)"
                        },
                        width: 1.5,
                        selectionWidth: 2,
                        smooth: {
                            type: "continuous",
                            forceDirection: "none"
                        },
                        length: 250  // Längere Kanten für mehr Abstand (150 ist Standard)
                    },
                    layout: {
                        improvedLayout: true,
                        hierarchical: {
                            enabled: false
                        }
                    }
                });
                
                // Nach der initialen Stabilisierung die Physik ausschalten
                setTimeout(function() {
                    network.setOptions({ physics: { enabled: false } });
                }, 3000);
                '''
                
                # Suche nach der Stelle, an der das Netzwerk initialisiert wird
                if 'var network = new vis.Network(container, data, options);' in html_content:
                    html_content = html_content.replace(
                        'var network = new vis.Network(container, data, options);',
                        'var network = new vis.Network(container, data, options);\n' + network_options
                    )
                    
                    # Schreibe den geänderten HTML-Inhalt zurück
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    print("DEBUG: Network options injected into HTML file")
                else:
                    print("DEBUG: Could not find network initialization in HTML file")
            except Exception as e:
                print(f"DEBUG: Error injecting network options: {str(e)}")
                # Fehler beim Einfügen der Optionen sollte nicht zum Abbruch führen
        except Exception as e:
            print(f"DEBUG: Error writing HTML file: {str(e)}")
            logging.error(f"Error writing HTML file: {str(e)}")
            return None
        
        # Injiziere HTML-Legende am Seitenanfang
        # Verbesserte Legende mit deutlicherer Darstellung und fester Position
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
        
        # Füge die Legende direkt nach dem <body>-Tag ein und stelle sicher, dass sie sichtbar ist
        with open(output_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Verbesserte Methode zum Einfügen der Legende
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
