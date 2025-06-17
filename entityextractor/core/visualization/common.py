"""
Gemeinsame Funktionen und Konstanten für die Visualisierung von Graphen.
"""
import logging
import os
from pathlib import Path

# Standard-Konfigurationswerte
GRAPH_STYLE = "modern"  # modern, classic, minimal
GRAPH_NODE_STYLE = "label_inside"  # label_inside, label_above, label_below
GRAPH_EDGE_LENGTH = 200  # Standardlänge für Kanten in Pixeln

def get_entity_type_color_map(G, config=None, style="modern"):
    """Erstellt eine Farbzuordnung für Entitätstypen.
    
    Args:
        G: NetworkX DiGraph Objekt
        config: Konfigurationswörterbuch
        style: Visualisierungsstil (modern, classic, minimal)
        
    Returns:
        Tuple mit (normalisierte_typen, typ_farbe_map, kanten_farbe)
    """
    # Verwende den übergebenen Stil oder den Standardwert
    style = config.get("GRAPH_STYLE", GRAPH_STYLE) if config else GRAPH_STYLE
    
    # Normalisiere Entitätstypen (entferne Namensraum-Präfixe)
    normalized_entity_types = {}
    all_types = set()
    
    for node in G.nodes():
        entity_type = G.nodes[node].get('entity_type', '')
        
        # Verwende den letzten Teil des Typs (nach dem letzten Slash oder Doppelpunkt)
        if entity_type:
            if '/' in entity_type:
                normalized_type = entity_type.split('/')[-1]
            elif ':' in entity_type:
                normalized_type = entity_type.split(':')[-1]
            else:
                normalized_type = entity_type
                
            # Kapitalisiere den ersten Buchstaben
            normalized_type = normalized_type.capitalize()
            
            # Speichere den normalisierten Typ
            normalized_entity_types[node] = normalized_type
            all_types.add(normalized_type)
        else:
            normalized_entity_types[node] = 'Unknown'
            all_types.add('Unknown')
    
    # Vordefinierte Farben für häufige Entitätstypen
    predefined_colors = {
        'Person': '#ff9999',
        'Organisation': '#99ccff',
        'Ort': '#99ff99',
        'Location': '#99ff99',
        'Event': '#ffcc99',
        'Work': '#cc99ff',
        'Concept': '#ffff99',
        'Agent': '#ff99cc',
        'Place': '#99ffcc',
        'Species': '#ccff99',
        'Fachgebiet': '#9999ff',
        'Wissenschaftler': '#ff9999',
        'Theorie': '#ffcc99',
        'Physikalisches_konzept': '#ffff99',
        'Unknown': '#eeeeee'
    }
    
    # Moderne Farbpalette (sanftere Farben)
    modern_colors = [
        '#8dd3c7', '#ffffb3', '#bebada', '#fb8072', '#80b1d3',
        '#fdb462', '#b3de69', '#fccde5', '#d9d9d9', '#bc80bd',
        '#ccebc5', '#ffed6f', '#a6cee3', '#1f78b4', '#b2df8a'
    ]
    
    # Klassische Farbpalette (kräftigere Farben)
    classic_colors = [
        '#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00',
        '#ffff33', '#a65628', '#f781bf', '#999999', '#66c2a5',
        '#fc8d62', '#8da0cb', '#e78ac3', '#a6d854', '#ffd92f'
    ]
    
    # Minimale Farbpalette (Grautöne)
    minimal_colors = [
        '#f7f7f7', '#d9d9d9', '#bdbdbd', '#969696', '#737373',
        '#525252', '#252525', '#000000'
    ]
    
    # Wähle die Farbpalette basierend auf dem Stil
    if style == "modern":
        color_palette = modern_colors
        edge_color = '#555555'
    elif style == "classic":
        color_palette = classic_colors
        edge_color = '#000000'
    elif style == "minimal":
        color_palette = minimal_colors
        edge_color = '#000000'
    else:  # Fallback auf modern
        color_palette = modern_colors
        edge_color = '#555555'
    
    # Erstelle die Farbzuordnung für die Entitätstypen
    type_color_map = {}
    color_index = 0
    
    # Zuerst die vordefinierten Farben verwenden
    for entity_type in sorted(all_types):
        if entity_type in predefined_colors:
            type_color_map[entity_type] = predefined_colors[entity_type]
        else:
            # Dann die Farbpalette durchlaufen
            type_color_map[entity_type] = color_palette[color_index % len(color_palette)]
            color_index += 1
    
    return normalized_entity_types, type_color_map, edge_color

def ensure_output_directory(output_path):
    """Stellt sicher, dass das Ausgabeverzeichnis existiert.
    
    Args:
        output_path: Pfad zur Ausgabedatei
    
    Returns:
        True bei Erfolg, False bei Fehler
    """
    output_dir = os.path.dirname(output_path)
    if output_dir:
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            print(f"DEBUG: Output directory created or exists: {os.path.abspath(output_dir)}")
            logging.info(f"Ensuring output directory exists: {os.path.abspath(output_dir)}")
            return True
        except Exception as e:
            print(f"DEBUG: Error creating output directory: {str(e)}")
            logging.error(f"Error creating output directory: {str(e)}")
            return False
    return True
