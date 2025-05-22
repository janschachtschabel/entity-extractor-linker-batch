#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Beispielskript zur Demonstration der Graph-Visualisierung mit Beziehungen.
Dieses Skript zeigt, wie ein Knowledge Graph aus Entitäten und Beziehungen
erstellt und visualisiert werden kann.
"""

import sys
import json
import logging
import os
from pathlib import Path

# Konfiguriere Logging und Ausgabe
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
sys.stdout.reconfigure(encoding='utf-8')

# Importiere die API-Funktionen
from entityextractor.api import extract_and_link_entities
from entityextractor.core.visualization.visualizer import visualize_graph
from entityextractor.core.visualization.graph_builder import build_graph

# Beispieltext mit klaren Beziehungen
text = """
Albert Einstein entwickelte die Relativitätstheorie im Jahr 1905.
Die Relativitätstheorie revolutionierte die Physik und führte zu neuen Erkenntnissen über Raum und Zeit.
Einstein erhielt 1921 den Nobelpreis für Physik, allerdings nicht für die Relativitätstheorie, 
sondern für seine Erklärung des photoelektrischen Effekts.
"""

def main():
    # Erstelle Ausgabeverzeichnis
    output_dir = "./output"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Konfiguration für die Entitätsextraktion und Visualisierung
    config = {
        # LLM-Einstellungen
        "MODEL": "gpt-4.1-mini",
        "TEMPERATURE": 0.1,
        
        # Entitäts- und Beziehungseinstellungen
        "MAX_ENTITIES": 10,
        "RELATION_EXTRACTION": True,
        "INFER_RELATIONSHIPS": True,
        
        # Wissensquellen
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": False,
        "USE_DBPEDIA": False,
        
        # Visualisierungseinstellungen
        "ENABLE_GRAPH_VISUALIZATION": True,
        "GRAPH_STYLE": "modern",
        "GRAPH_OUTPUT_DIR": output_dir
    }
    
    # Extrahiere Entitäten und Beziehungen
    logging.info("Extrahiere Entitäten und Beziehungen...")
    result = extract_and_link_entities(text, config)
    
    # Prüfe, ob Beziehungen extrahiert wurden
    relationships = result.get("relationships", [])
    if not relationships:
        logging.warning("Keine Beziehungen extrahiert. Erstelle manuelle Beispielbeziehungen.")
        
        # Erstelle manuelle Beispielbeziehungen, falls keine extrahiert wurden
        entities = result.get("entities", [])
        entity_names = [e.get("entity") for e in entities]
        
        if len(entity_names) >= 3:
            # Füge manuelle Beziehungen hinzu
            relationships = [
                {
                    "subject": entity_names[0],
                    "predicate": "entwickelte",
                    "object": entity_names[1],
                    "inferred": "explicit",
                    "subject_type": "Person",
                    "object_type": "Theorie"
                },
                {
                    "subject": entity_names[1],
                    "predicate": "revolutionierte",
                    "object": entity_names[2],
                    "inferred": "explicit",
                    "subject_type": "Theorie",
                    "object_type": "Fachgebiet"
                }
            ]
            result["relationships"] = relationships
            logging.info(f"Manuelle Beziehungen hinzugefügt: {len(relationships)}")
    
    # Visualisiere den Graphen
    logging.info(f"Visualisiere Graphen mit {len(result.get('entities', []))} Entitäten und {len(relationships)} Beziehungen...")
    visualization_result = visualize_graph(result, config)
    
    if visualization_result:
        logging.info(f"Visualisierung erfolgreich erstellt:")
        logging.info(f"PNG: {visualization_result.get('png')}")
        logging.info(f"HTML: {visualization_result.get('html')}")
    else:
        logging.error("Visualisierung fehlgeschlagen.")
    
    # Gib das Ergebnis aus
    logging.info("Gebe Ergebnis aus...")
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
