#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Beispielskript zur Demonstration der Knowledge-Graph-Funktionalität.

Dieses Skript extrahiert Entitäten und Beziehungen aus einem Text und
visualisiert sie als Knowledge Graph.
"""

from entityextractor.api import extract_and_link_entities
import json
import sys
import logging
import os
from pathlib import Path

# Konfiguriere Logging und Ausgabe
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
sys.stdout.reconfigure(encoding='utf-8')

# Erstelle Ausgabeverzeichnis
output_dir = "./output"
Path(output_dir).mkdir(parents=True, exist_ok=True)

# Beispieltext über Johann Amos Comenius
text = """
Johann Amos Comenius (1592-1670) war ein bedeutender Pädagoge und Philosoph. 
Er veröffentlichte 1632 sein Hauptwerk "Didactica Magna" (Große Unterrichtslehre), 
das als Grundlage der modernen Pädagogik gilt. Comenius entwickelte das Konzept 
des lebenslangen Lernens und betonte die Bedeutung der Bildung für alle Menschen, 
unabhängig von sozialem Status oder Geschlecht. Seine didaktischen Prinzipien 
beeinflussen bis heute den Unterricht und die Lehrerbildung.
"""

# Vollständige JSON-Ausgabe
result = extract_and_link_entities(
    text,
    {
        # === LLM Provider Parameters ===
        "LLM_BASE_URL": "https://api.openai.com/v1",  # Base URL für LLM API
        "MODEL": "gpt-4.1-mini",   # LLM-Modell
        "OPENAI_API_KEY": None,    # API-Key aus Umgebungsvariable (None) oder Angabe
        "MAX_TOKENS": 15000,       # Maximale Tokenanzahl pro Anfrage
        "TEMPERATURE": 0.2,        # Sampling-Temperature

        # === Data Source Parameters ===
        "USE_WIKIPEDIA": True,     # Wikipedia-Verknüpfung aktivieren
        "USE_WIKIDATA": True,      # Wikidata-Verknüpfung aktivieren
        "USE_DBPEDIA": True,       # DBpedia-Verknüpfung aktivieren
        "DBPEDIA_USE_DE": False,   # Deutsche DBpedia nutzen
        "DBPEDIA_LOOKUP_API": True, # DBPedia Lookup API als Backup
        "LANGUAGE": "de",          # Sprache (de, en)
        
        # === Entity Extraction Parameters ===
        "MAX_ENTITIES": 20,        # Max. Anzahl Entitäten
        "ALLOWED_ENTITY_TYPES": "auto", # Entitätstypen automatisch filtern
        
        # === Relationship Parameters ===
        "RELATION_EXTRACTION": True,  # Relationsextraktion aktivieren
        "ENABLE_RELATIONS_INFERENCE": True,  # Implizite Relationen aktivieren
        "MAX_RELATIONS": 20,  # Maximale Anzahl an Beziehungen
        
        # === Graph Visualization Parameters ===
        "ENABLE_GRAPH_VISUALIZATION": True,
        "GRAPH_OUTPUT_DIR": output_dir,
        "GRAPH_FORMAT": "both",  # Erzeuge sowohl PNG als auch HTML
        "GRAPH_STYLE": "modern",    # Visueller Stil (modern=Pastellfarben, classic=Kräftige Farben, minimal=Grautöne)
        "GRAPH_NODE_STYLE": "label_above",  # Node-Stil (label_above=kleine Kreise mit Label darüber, label_below=kleine Kreise mit Label darunter, label_inside=große Kreise mit Label)
        "GRAPH_EDGE_LENGTH": "standard"    # Kantenlänge (standard=normale Distanz, compact=50% kürzer, extended=50% länger)
    }
)

logging.info("Gebe finale Ergebnisse aus...")

# Ausgabe der extrahierten Entitäten
logging.info(f"Extrahierte Entitäten: {len(result['entities'])}")
for entity in result['entities']:
    print(f"Entität: {entity['entity']} (Typ: {entity['details']['typ']})")

# Direkter Zugriff auf die Beziehungsdaten
relationships_data = []

# Versuche, die Beziehungen aus dem Orchestrator zu erhalten
if 'relationships' in result and isinstance(result['relationships'], list):
    relationships_data = result['relationships']
    logging.info(f"Beziehungen im Ergebnis gefunden: {len(relationships_data)}")
else:
    logging.warning("Keine Beziehungen im Ergebnis gefunden.")

# Ausgabe der Beziehungen
logging.info(f"Extrahierte Beziehungen: {len(relationships_data)}")
print("\nBeziehungen:")

if relationships_data:
    # Ausgabe der Struktur der ersten Beziehung
    print("\nErste Beziehung (Struktur):\n")
    print(json.dumps(relationships_data[0], indent=2, ensure_ascii=False))
    print("\nAlle Beziehungen:")
    
    for i, rel in enumerate(relationships_data):
        print(f"\nBeziehung {i+1}:")
        subject = rel.get('subject', 'Unbekannt')
        predicate = rel.get('predicate', 'Unbekannt')
        object_ = rel.get('object', 'Unbekannt')
        subject_type = rel.get('subject_type', '')
        object_type = rel.get('object_type', '')
        inferred = rel.get('inferred', 'explicit')
        print(f"  {subject} ({subject_type}) -- {predicate} --> {object_} ({object_type}) [{inferred}]")
else:
    print("  Keine Beziehungen gefunden.")
    
# Speichere die Beziehungen explizit in einer separaten Datei
with open(os.path.join(output_dir, "relationships.json"), "w", encoding="utf-8") as f:
    json.dump(relationships_data, f, indent=2, ensure_ascii=False)

logging.info(f"Beziehungen wurden separat in {os.path.join(output_dir, 'relationships.json')} gespeichert.")


# Speichern der vollständigen Ergebnisse als JSON
with open(os.path.join(output_dir, "knowledge_graph_results.json"), "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

logging.info(f"Ergebnisse wurden im Verzeichnis {output_dir} gespeichert.")
logging.info(f"Visualisierungen wurden erstellt: PNG und HTML im Verzeichnis {output_dir}.")
