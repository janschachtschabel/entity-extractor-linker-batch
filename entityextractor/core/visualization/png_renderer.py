"""
Funktionen zum Rendern von Graphen als PNG-Dateien.
"""
import logging
import os
import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.lines import Line2D

from .common import get_entity_type_color_map, ensure_output_directory

def render_graph_to_png(G, output_path, config=None):
    """Rendert einen Graphen als PNG-Datei.
    
    Args:
        G: NetworkX DiGraph Objekt
        output_path: Pfad zum Speichern der PNG-Datei
        config: Konfigurationswörterbuch
        
    Returns:
        Absoluter Pfad zur gespeicherten PNG-Datei oder None bei Fehler
    """
    # Stelle sicher, dass das Ausgabeverzeichnis existiert
    if not ensure_output_directory(output_path):
        return None
        
    # Importiere NetworkX explizit im Funktionsbereich
    import networkx as nx
    
    try:
        print(f"DEBUG: Rendering graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        logging.info(f"Rendering graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges to {output_path}")
        
        if G.number_of_nodes() == 0:
            print("DEBUG: Cannot render graph with no nodes.")
            logging.error("Cannot render graph with no nodes.")
            return None
            
        # Prüfe, ob matplotlib verfügbar ist
        try:
            import matplotlib
            print(f"DEBUG: Matplotlib Version: {matplotlib.__version__}")
        except Exception as e:
            print(f"DEBUG: Problem mit Matplotlib: {str(e)}")
            logging.error(f"Problem mit Matplotlib: {str(e)}")
            return None
            
        # We can render graphs with no edges (isolated nodes)
        if G.number_of_edges() == 0:
            print("DEBUG: Graph has no edges, will render isolated nodes")
            logging.warning("Graph has no edges, rendering isolated nodes only.")
            
        # Wähle den Stil basierend auf der Konfiguration
        style = config.get("GRAPH_STYLE", "modern") if config else "modern"
        
        plt.figure(figsize=(16, 12))
        
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
            pos = nx.spring_layout(G, k=0.5, iterations=100)
            
            # Verbesserte Überlappungsprävention mit mehr Iterationen und größerem Mindestabstand
            min_dist = 0.3  # Größerer Mindestabstand als vorher
            max_iterations = 100  # Mehr Iterationen für bessere Verteilung
            
            # Ausgangsgröße des Layouts - vergrößern, damit Knoten mehr Platz haben
            scale_factor = 1.5
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
        node_size = 500
        font_size = 10
        alpha = 0.9
        # For the legend: map entity types to colors
        types_with_colors = {typ: type_color_map[typ] for typ in sorted(set(normalized_entity_types.values()))}

        # Zähle die Anzahl der Kanten zwischen Knotenpaaren (unabhängig von der Richtung)
        edge_count = {}
        for u, v in G.edges():
            # Normalisiere die Knotenreihenfolge, um bidirektionale Kanten zu erfassen
            if u > v:
                key = (v, u)
            else:
                key = (u, v)
                
            if key in edge_count:
                edge_count[key] += 1
            else:
                edge_count[key] = 1
        
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
            u_v = (u, v)
            v_u = (v, u)
            
            # Normalisiere die Knotenreihenfolge für die Zählung
            norm_key = (u, v) if u <= v else (v, u)
            total_edges = edge_count.get(norm_key, 1)
            
            # Nur gebogene Kanten verwenden, wenn es mindestens 2 Kanten zwischen den Knoten gibt
            if total_edges > 1:
                if u_v not in edge_index:
                    edge_index[u_v] = 0
                else:
                    edge_index[u_v] += 1
                
                # Berechne Biegungsgrad basierend auf der Kantenanzahl und dem Index
                idx = edge_index[u_v]
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
            # Berechne die Knotengröße in Matplotlib-Koordinaten
            # Die Knotengröße in draw_networkx_nodes ist in Punkten^2, daher nehmen wir die Wurzel und multiplizieren mit einem Faktor
            # Für bessere Pfeilspitzen-Platzierung verwenden wir einen etwas größeren Wert
            node_radius = (node_size ** 0.5) * 0.015
            
            # Zeichne die Kante mit angepassten Pfeilspitzen, die nur bis zur äußeren Kante der Kreise reichen
            nx.draw_networkx_edges(
                G, pos, 
                edgelist=[(u, v)],
                width=edge_widths[i],
                style=edge_styles[i],
                edge_color=[edge_colors[i]],
                arrows=True,
                arrowsize=15,  # Größe der Pfeilspitze
                alpha=alpha,
                connectionstyle=edge_connectionstyles[i],
                # Lasse die Pfeilspitzen vor den Knoten enden
                arrowstyle='-|>',  # Pfeilspitze mit gerader Basis
                node_size=node_size,  # Knotengröße für die Berechnung des Endpunkts
                min_source_margin=node_radius,  # Mindestabstand vom Startknoten
                min_target_margin=node_radius   # Mindestabstand vom Zielknoten
            )
        
        # Zeichne Kantenbeschriftungen (Prädikate) mit optimierter Positionierung je nach Biegung
        edge_labels_list = []
        edge_label_pos = {}
        
        # Sammle alle Kanten und ihre Beschriftungen
        for i, (u, v, data) in enumerate(G.edges(data=True)):
            label = data.get('predicate', '')
            if not label:
                continue
                
            # Belasse vollständiges Prädikat ohne Kürzung
                
            edge_labels_list.append((u,v,label,edge_connectionstyles[i]))
            
            # Prüfe, ob diese Kante gebogen ist
            is_curved = edge_connectionstyles[i] != 'arc3,rad=0'
            
            if is_curved:
                # Skip, wenn eine der Knotenpositionen fehlt (kann bei gefilterten/isolierten Knoten vorkommen)
                if u not in pos or v not in pos:
                    continue

                # Extrahiere den Radius aus dem connectionstyle
                try:
                    rad = float(edge_connectionstyles[i].split('=')[1])
                except (ValueError, IndexError):
                    rad = 0.3  # Fallback auf Standardwert
                
                # Berechne eine optimierte Position für das Label
                # Bei gebogenen Kanten verschieben wir das Label weiter nach außen
                # und passen die Position basierend auf der Biegungsrichtung an
                pos_u, pos_v = pos[u], pos[v]
                mid_x = (pos_u[0] + pos_v[0]) / 2
                mid_y = (pos_u[1] + pos_v[1]) / 2
                
                # Berechne den Vektor senkrecht zur Kante
                dx = pos_v[0] - pos_u[0]
                dy = pos_v[1] - pos_u[1]
                length = (dx**2 + dy**2)**0.5
                if length > 0:
                    # Normalisiere und drehe um 90 Grad
                    nx, ny = -dy/length, dx/length
                    
                    # Verschiebe das Label in Richtung der Biegung
                    offset = 0.15 * abs(rad) * (1.5 if abs(rad) > 0.2 else 1.0)
                    if rad > 0:
                        edge_label_pos[(u, v)] = (mid_x + nx * offset, mid_y + ny * offset)
                    else:
                        edge_label_pos[(u, v)] = (mid_x - nx * offset, mid_y - ny * offset)
        
        # Zeichne die Kantenbeschriftungen einzeln, um Überlappungen bei Gegenbögen zu reduzieren
        import networkx as _nx_lbl
        for (u,v,label,conn) in edge_labels_list:
            if u not in pos or v not in pos:
                continue
            # Wähle label_pos abhängig von Rad-Parameter der Kurve
            label_pos = 0.3 if 'rad=-' in conn else 0.7 if 'rad=' in conn and 'rad=-' not in conn else 0.5
            _nx_lbl.draw_networkx_edge_labels(
                G,
                pos,
                edge_labels={(u,v): label},
                font_size=font_size-2,
                font_color='#222222',
                alpha=0.8,
                bbox={"boxstyle": "round,pad=0.2", "fc": "white", "ec": "none", "alpha": 0.7},
                label_pos=label_pos
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
        import traceback
        error_details = traceback.format_exc()
        print(f"DEBUG: Fehler beim Speichern des PNG-Graphen: {str(e)}")
        print(f"DEBUG: Fehlerdetails: {error_details}")
        logging.error(f"Fehler beim Speichern des PNG-Graphen: {str(e)}")
        logging.error(f"Fehlerdetails: {error_details}")
        return None
