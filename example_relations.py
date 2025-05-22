#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Beispiel für die Verwendung der Entity Relationship Inference-Funktionalität.
"""

import json
from entityextractor.api import extract_and_link_entities
import logging
import os

def main():
    # Beispieltext
    example_text = (
        "Die Industrielle Revolution begann im späten 18. Jahrhundert in Großbritannien und veränderte die Wirtschaft grundlegend. "
        "James Watt verbesserte die Dampfmaschine, was die Produktion revolutionierte. "
        "Eli Whitney erfand die Baumwollentkörnungsmaschine (Cotton Gin), die die Textilproduktion beschleunigte. "
        "Karl Marx kritisierte in seinem Werk 'Das Kapital' die sozialen Auswirkungen der Industrialisierung. "
        "Die Arbeiterbewegung entstand als Reaktion auf die schlechten Arbeitsbedingungen in den Fabriken. "
        "In Deutschland führte Otto von Bismarck Sozialversicherungen ein, um die Arbeiterklasse zu befrieden."
    )
    
    # Hinweis für implizite Beziehungen
    print("Hinweis: Implizite Beziehungen sind solche, die nicht direkt im Text erwähnt werden,")
    print("sondern aus dem Kontext abgeleitet werden können. Zum Beispiel könnte eine implizite")
    print("Beziehung zwischen 'Industrielle Revolution' und 'Arbeiterbewegung' bestehen, obwohl")
    print("diese nicht direkt im Text miteinander verbunden sind.")
    
    # Konfiguration definieren
    config = {
        # === LLM PROVIDER SETTINGS ===
        "LLM_BASE_URL": "https://api.openai.com/v1",  # Base-URL für LLM API
        "MODEL": "gpt-4.1-mini",                      # LLM-Modell (empfohlen: gpt-4.1-mini, gpt-4o-mini)
        "OPENAI_API_KEY": None,                       # API-Key setzen oder aus Umgebungsvariable (Standard: None)
        "MAX_TOKENS": 16000,                          # Maximale Tokenanzahl pro Anfrage
        "TEMPERATURE": 0.2,                           # Sampling-Temperatur

        # === LANGUAGE SETTINGS ===
        "LANGUAGE": "en",           # Sprache der Verarbeitung (de oder en)

        # === TEXT PROCESSING SETTINGS ===
        "TEXT_CHUNKING": False,     # Text-Chunking aktivieren (False = ein LLM-Durchgang)
        "TEXT_CHUNK_SIZE": 1000,    # Chunk-Größe in Zeichen
        "TEXT_CHUNK_OVERLAP": 50,   # Überlappung zwischen Chunks in Zeichen

        # === ENTITY EXTRACTION SETTINGS ===
        "MODE": "extract",               # Modus: extract oder generate
        "MAX_ENTITIES": 20,              # Maximale Anzahl extrahierter Entitäten
        "ALLOWED_ENTITY_TYPES": "auto",  # Automatische Filterung erlaubter Entitätstypen
        "ENABLE_ENTITY_INFERENCE": False, # Implizite Entitätserkennung aktivieren

        # === RELATIONSHIP EXTRACTION AND INFERENCE ===
        "RELATION_EXTRACTION": True,         # Relationsextraktion aktivieren
        "ENABLE_RELATIONS_INFERENCE": False,  # Implizite Relationen aktivieren
        "MAX_RELATIONS": 15,                  # Maximale Anzahl Beziehungen pro Prompt

        # === CORE DATA SOURCE SETTINGS ===
        "USE_WIKIPEDIA": True,          # Wikipedia-Verknüpfung aktivieren (immer True)
        "USE_WIKIDATA": True,          # Wikidata-Verknüpfung aktivieren
        "USE_DBPEDIA": False,           # DBpedia-Verknüpfung aktivieren
        "DBPEDIA_USE_DE": False,        # Deutsche DBpedia nutzen (Standard: False = englische DBpedia)
        "ADDITIONAL_DETAILS": False,    # Zusätzliche Details aus allen Wissensquellen abrufen (mehr Infos aber langsamer)

        # === DBpedia Lookup API Fallback ===
        "DBPEDIA_LOOKUP_API": True,       # Fallback via DBpedia Lookup API aktivieren
        "DBPEDIA_SKIP_SPARQL": False,     # SPARQL-Abfragen überspringen und nur Lookup-API verwenden
        "DBPEDIA_LOOKUP_MAX_HITS": 5,     # Maximale Trefferzahl für Lookup-API
        "DBPEDIA_LOOKUP_CLASS": None,     # Optionale DBpedia-Ontology-Klasse für Lookup-API (derzeit ungenutzt)
        "DBPEDIA_LOOKUP_FORMAT": "xml",   # Response-Format: "json", "xml" (empfohlen) oder "beide" (maximale Details)

        # === COMPENDIUM SETTINGS ===
        "ENABLE_COMPENDIUM": False,           # Kompendium-Generierung aktivieren
        "COMPENDIUM_LENGTH": 8000,            # Anzahl der Zeichen für das Kompendium (ca. 4 A4-Seiten)
        "COMPENDIUM_EDUCATIONAL_MODE": False,  # Bildungsmodus für Kompendium aktivieren

        # === KNOWLEDGE GRAPH VISUALIZATION SETTINGS ===
        "ENABLE_GRAPH_VISUALIZATION": True,  # Statische PNG- und interaktive HTML-Ansicht aktivieren (erfordert RELATION_EXTRACTION=True)

        # === KNOWLEDGE GRAPH COMPLETION (KGC) ===
        "ENABLE_KGC": False,   # Knowledge-Graph-Completion aktivieren (Vervollständigung mit impliziten Relationen)
        "KGC_ROUNDS": 3,       # Anzahl der KGC-Runden

        # === STATISCHER GRAPH mit NetworkX-Layouts (PNG) ===
        "GRAPH_LAYOUT_METHOD": "spring",          # Layout: "kamada_kawai" (ohne K-/Iter-Param) oder "spring" (Fruchterman-Reingold)
        "GRAPH_LAYOUT_K": None,                   # (Spring-Layout) Ideale Kantenlänge (None=Standard)
        "GRAPH_LAYOUT_ITERATIONS": 50,            # (Spring-Layout) Anzahl der Iterationen
        "GRAPH_PHYSICS_PREVENT_OVERLAP": True,    # (Spring-Layout) Überlappungsprävention aktivieren
        "GRAPH_PHYSICS_PREVENT_OVERLAP_DISTANCE": 0.1,  # (Spring-Layout) Mindestabstand zwischen Knoten
        "GRAPH_PHYSICS_PREVENT_OVERLAP_ITERATIONS": 50, # (Spring-Layout) Iterationen zur Überlappungsprävention
        "GRAPH_PNG_SCALE": 0.30,                  # Skalierungsfaktor für statisches PNG-Layout (Standard 0.33)

        # === INTERAKTIVER GRAPH mit PyVis (HTML) ===
        "GRAPH_HTML_INITIAL_SCALE": 10,           # Anfangs-Zoom (network.moveTo scale): >1 rauszoomen, <1 reinzoomen

        # === TRAINING DATA COLLECTION SETTINGS ===
        "COLLECT_TRAINING_DATA": False,  # Trainingsdaten für Fine-Tuning sammeln
        "OPENAI_TRAINING_DATA_PATH": "entity_extractor_training_openai.jsonl",  # Pfad für Entitäts-Trainingsdaten
        "OPENAI_RELATIONSHIP_TRAINING_DATA_PATH": "entity_relationship_training_openai.jsonl",  # Pfad für Beziehungs-Trainingsdaten

        # === RATE LIMITER AND TIMEOUT SETTINGS ===
        "TIMEOUT_THIRD_PARTY": 20,       # Timeout für externe Dienste (Wikipedia, Wikidata, DBpedia)
        "RATE_LIMIT_MAX_CALLS": 3,       # Maximale Anzahl Aufrufe pro Zeitraum
        "RATE_LIMIT_PERIOD": 1,          # Zeitraum in Sekunden
        "RATE_LIMIT_BACKOFF_BASE": 1,    # Basiswert für exponentielles Backoff
        "RATE_LIMIT_BACKOFF_MAX": 60,    # Maximale Wartezeit bei Backoff
        "USER_AGENT": "EntityExtractor/1.0", # HTTP User-Agent-Header
        "WIKIPEDIA_MAXLAG": 5,           # Maxlag-Parameter für Wikipedia-API

        # === CACHING SETTINGS ===
        "CACHE_ENABLED": True,   # Caching global aktivieren oder deaktivieren
        "CACHE_DIR": os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache"),    # Verzeichnis für Cache-Dateien innerhalb des Pakets (bei Bedarf erstellen)
        "CACHE_DBPEDIA_ENABLED": True,              # Caching für DBpedia-SPARQL-Abfragen aktivieren
        "CACHE_WIKIDATA_ENABLED": True,             # (Optional) Caching für Wikidata-API aktivieren
        "CACHE_WIKIPEDIA_ENABLED": True,            # (Optional) Caching für Wikipedia-API-Anfragen aktivieren

        # === LOGGING AND DEBUG SETTINGS ===
        "SHOW_STATUS": True,            # Statusmeldungen anzeigen
        "SUPPRESS_TLS_WARNINGS": True   # TLS-Warnungen unterdrücken
    }

    logging.info("Starte Entitäten-Extraktion, -Verknüpfung und -Inference in test_relationships.py")
    print(f"\nExtrahiere und verknüpfe Entitäten aus dem Text und inferiere Beziehungen...")
    result = extract_and_link_entities(example_text, config)
    
    # Log: Start der finalen Ergebnis-Ausgabe
    logging.info("Beginne Ausgabe der finalen Ergebnisse in test_relationships.py")
    # Ausgabe formatieren
    if isinstance(result, dict) and "entities" in result and "relationships" in result:
        # Übersichtliche Kurzfassung der Entitäten
        print("\nExtrahierte Entitäten:")
        print("-" * 100)
        print(f"{'Nr':3} | {'Name':25} | {'Typ':15} | {'Inferred':10} | {'Wikipedia':25} | {'Wikidata':15} | {'DBpedia':20}")
        print("-" * 100)
        
        for i, entity in enumerate(result["entities"]):
            # Basisinformationen
            name = entity.get("entity", "")[:25]
            entity_type = ""
            
            # Typ aus verschiedenen möglichen Quellen extrahieren
            if "entity_type" in entity:
                entity_type = entity["entity_type"]
            elif "type" in entity:
                entity_type = entity["type"]
            elif "details" in entity and "typ" in entity["details"]:
                entity_type = entity["details"]["typ"]
            
            # Wikipedia-Informationen
            wiki_label = ""
            wiki_url = ""
            if "sources" in entity and "wikipedia" in entity["sources"]:
                wiki_label = entity["sources"]["wikipedia"].get("label", "")[:25]
                wiki_url = entity["sources"]["wikipedia"].get("url", "")
            
            # Wikidata-Informationen
            wikidata_id = ""
            wikidata_label = ""
            if "sources" in entity and "wikidata" in entity["sources"]:
                wikidata_id = entity["sources"]["wikidata"].get("id", "")
                wikidata_label = entity["sources"]["wikidata"].get("label", "")[:15]
            
            # DBpedia-Informationen
            dbpedia_title = ""
            dbpedia_uri = ""
            if "sources" in entity and "dbpedia" in entity["sources"]:
                src = entity["sources"]["dbpedia"]
                # title aus dbpedia_title oder title
                dbpedia_title = src.get("dbpedia_title", "")[:20] or src.get("title", "")[:20]
                # uri aus resource_uri oder uri
                dbpedia_uri = src.get("resource_uri", "") or src.get("uri", "")
            # Anzeige: bevorzugt Titel, ansonsten URI
            dbpedia_display = dbpedia_title or dbpedia_uri
            
            # Inferred aus Details
            inferred = entity.get('details', {}).get('inferred', entity.get('inferred', ''))
            
            # Zeile ausgeben
            print(f"{i+1:3} | {name:25} | {entity_type:15} | {inferred:10} | {wiki_label:25} | {wikidata_id:15} | {dbpedia_display:20}")
        
        print("-" * 100)
        print(f"Insgesamt {len(result['entities'])} Entitäten gefunden.")
        
        # Beziehungen nach explizit und implizit trennen
        explicit_relationships = [rel for rel in result["relationships"] if rel.get("inferred", "") == "explicit"]
        implicit_relationships = [rel for rel in result["relationships"] if rel.get("inferred", "") == "implicit"]
        
        # Explizite Beziehungen ausgeben
        print("\nExplizite Beziehungen (direkt im Text erwähnt):")
        print("-" * 140)
        print(f"{'Nr':3} | {'Subjekt':25} | {'SubjTyp':12} | {'SubjInf':10} | {'Prädikat':20} | {'Objekt':25} | {'ObjTyp':12} | {'ObjInf':10}")
        print("-" * 140)
        
        if explicit_relationships:
            for i, rel in enumerate(explicit_relationships):
                subject = rel['subject'][:25]
                subject_type = rel.get('subject_type', '')[:12]
                subject_inf = rel.get('subject_inferred', '')
                predicate = rel['predicate'][:20]
                obj = rel['object'][:25]
                object_type = rel.get('object_type', '')[:12]
                object_inf = rel.get('object_inferred', '')
                
                print(f"{i+1:3} | {subject:25} | {subject_type:12} | {subject_inf:10} | {predicate:20} | {obj:25} | {object_type:12} | {object_inf:10}")
        else:
            print("Keine expliziten Beziehungen gefunden.")
            
        print("-" * 140)
        print(f"Insgesamt {len(explicit_relationships)} explizite Beziehungen gefunden.")
        
        # Implizite Beziehungen ausgeben
        print("\nImplizite Beziehungen (aus dem Kontext abgeleitet):")
        print("-" * 140)
        print(f"{'Nr':3} | {'Subjekt':25} | {'SubjTyp':12} | {'SubjInf':10} | {'Prädikat':20} | {'Objekt':25} | {'ObjTyp':12} | {'ObjInf':10}")
        print("-" * 140)
        
        if implicit_relationships:
            for i, rel in enumerate(implicit_relationships):
                subject = rel['subject'][:25]
                subject_type = rel.get('subject_type', '')[:12]
                subject_inf = rel.get('subject_inferred', '')
                predicate = rel['predicate'][:20]
                obj = rel['object'][:25]
                object_type = rel.get('object_type', '')[:12]
                object_inf = rel.get('object_inferred', '')
                
                print(f"{i+1:3} | {subject:25} | {subject_type:12} | {subject_inf:10} | {predicate:20} | {obj:25} | {object_type:12} | {object_inf:10}")
        else:
            print("Keine impliziten Beziehungen gefunden.")
            
        print("-" * 140)
        print(f"Insgesamt {len(implicit_relationships)} implizite Beziehungen gefunden.")
        
        # Gesamtzahl der Beziehungen
        print(f"\nGesamtzahl der Beziehungen: {len(result['relationships'])}")
        
        # Nur Wikipedia-URLs anzeigen
        print("\nWikipedia-URLs:")
        for i, entity in enumerate(result["entities"]):
            if "sources" in entity and "wikipedia" in entity["sources"] and "url" in entity["sources"]["wikipedia"]:
                name = entity.get("entity", "")
                url = entity["sources"]["wikipedia"].get("url", "")
                if url:
                    print(f"{i+1}. {name}: {url}")
        
        # Statistiken anzeigen (aus JSON-Ergebnis)
        stats = result.get("statistics", {})
        print("\nStatistiken:")
        # Gesamt
        print(f"  Gesamtentitäten: {stats.get('total_entities', 0)}")
        # Typverteilung
        print("\n  Typverteilung:")
        for typ, count in stats.get('types_distribution', {}).items():
            print(f"    {typ}: {count}")
        # Linking-Erfolg
        print("\n  Linking-Erfolg:")
        for source, data in stats.get('linked', {}).items():
            print(f"    {source.capitalize()}: {data['count']} ({data['percent']:.1f}%)")
        # Top Wikipedia Kategorien
        print("\n  Top 10 Wikipedia-Kategorien:")
        for c in stats.get('top_wikipedia_categories', []):
            print(f"    {c['category']}: {c['count']}")
        # Top Wikidata Typen
        print("\n  Top 10 Wikidata-Typen:")
        for t in stats.get('top_wikidata_types', []):
            print(f"    {t['type']}: {t['count']}")
        # Entitätsverbindungen
        print("\n  Entitätsverbindungen (Top 10):")
        for ec in stats.get('entity_connections', [])[:10]:
            print(f"    {ec['entity']}: {ec['count']}")
        # Top Wikidata part_of
        print("\n  Top 10 Wikidata 'part_of':")
        for po in stats.get('top_wikidata_part_of', []):
            print(f"    {po['part_of']}: {po['count']}")
        # Top Wikidata has_parts
        print("\n  Top 10 Wikidata 'has_parts':")
        for hp in stats.get('top_wikidata_has_parts', []):
            print(f"    {hp['has_parts']}: {hp['count']}")
        # Top DBpedia part_of
        print("\n  Top 10 DBpedia 'part_of':")
        for po in stats.get('top_dbpedia_part_of', []):
            print(f"    {po['part_of']}: {po['count']}")
        # Top DBpedia has_parts
        print("\n  Top 10 DBpedia 'has_parts':")
        for hp in stats.get('top_dbpedia_has_parts', []):
            print(f"    {hp['has_parts']}: {hp['count']}")
        # Top DBpedia-Subjects
        print("\n  Top 10 DBpedia-Subjects:")
        for sub in stats.get('top_dbpedia_subjects', []):
            print(f"    {sub['subject']}: {sub['count']}")
    else:
        print("\nKeine Entitäten oder Beziehungen gefunden oder RELATION_EXTRACTION ist nicht aktiviert.")

    # Log final results
    logging.info("Final results have been outputted in test_relationships.py")

if __name__ == "__main__":
    main()
