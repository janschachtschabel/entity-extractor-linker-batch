#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Beispiel für langen Text (~4000 Zeichen).
MODE=generate, nur Wikipedia-Integration, MAX_ENTITIES=10.
"""

from entityextractor.api import extract_and_link_entities
import json, logging, sys, os

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO)

text = """Seit den frühen 1990er Jahren verfolgt Deutschland das Ziel, seine Energieversorgung grundlegend zu transformieren. Die Energiewende hat das übergeordnete Ziel, den Ausstoß von Treibhausgasen zu reduzieren und zugleich die Versorgungssicherheit zu gewährleisten. Dabei spielen erneuerbare Energien wie Windkraft, Photovoltaik und Biomasse eine zentrale Rolle. Technische Innovationen in Speichertechnologien und intelligenten Netzen ermöglichen eine immer effizientere Integration fluktuierender Stromquellen. Forschungsinstitute wie das Fraunhofer ISE und das Deutsche Zentrum für Luft- und Raumfahrt (DLR) treiben die Entwicklung von Hochleistungsspeichern und Microgrid-Lösungen voran. Politische und wirtschaftliche Rahmenbedingungen, darunter das Erneuerbare-Energien-Gesetz (EEG), schaffen Anreize für Investitionen in saubere Technologien.
Auf gesellschaftlicher Ebene fördert die Bundesregierung über Programme wie den Klimaschutzplan 2050 die Akzeptanz von Energieeffizienzmaßnahmen und Elektromobilität. Kommunale Energieversorger und Stadtwerke investieren in Ladeinfrastruktur für Elektrofahrzeugen und entwickeln integrierte Konzepte zur Sektorkopplung. Die Kosten für erneuerbare Anlagen sind in den letzten Jahren erheblich gesunken, wodurch Investitionen für private Haushalte und Unternehmen attraktiver geworden sind. Gleichzeitig erfordert die Umstellung auf erneuerbare Energien Anpassungen im Netzbetrieb und Flexibilitätsmarkt. Digitale Plattformen zur Steuerung von Verbrauchern und Prosumer-Modellen gewinnen an Bedeutung und ermöglichen eine dynamische Bilanzierung von Einspeisung und Verbrauch. Marktmechanismen wie Redispatch 2.0 und Kapazitätsmärkte stellen sicher, dass Netzausbau und Betrieb auch bei hoher Volatilität stabil bleiben.
Im internationalen Kontext kooperiert Deutschland im Rahmen der EU-Klimapolitik und globaler Klimaabkommen wie dem Pariser Abkommen, um verbindliche Emissionssenkungen zu vereinbaren. Technologietransfer und gemeinsame Forschungsprojekte mit Partnern in Nordamerika, Asien und Afrika fördern den weltweiten Ausbau sauberer Energien. Die Rolle von grünem Wasserstoff als Energiespeicher und Transformationsmedium gewinnt zunehmend an Bedeutung, da er in der Industrie und im Schwerlastverkehr fossile Brennstoffe ersetzen kann. Pilotprojekte in Schleswig-Holstein und Bayern untersuchen die Machbarkeit von Sektorenkopplung mit Wasserstoffspeichern. Langfristig zielt die Bundesrepublik darauf ab, eine klimaneutrale Wirtschaft zu erreichen und gleichzeitig die wirtschaftliche Wettbewerbsfähigkeit zu erhalten. Die Herausforderung liegt darin, technologische, regulatorische und soziale Aspekte in Einklang zu bringen, um das Ziel einer nachhaltigen Energieversorgung zu realisieren."""

config = {
    # === LLM PROVIDER SETTINGS ===
    "LLM_BASE_URL": "https://api.openai.com/v1",  # Base-URL für LLM API
    "MODEL": "gpt-4.1-mini",                      # LLM-Modell (empfohlen: gpt-4.1-mini, gpt-4o-mini)
    "OPENAI_API_KEY": None,                       # API-Key aus Umgebungsvariable
    "MAX_TOKENS": 16000,                          # Maximale Tokenanzahl pro Anfrage
    "TEMPERATURE": 0.2,                           # Sampling-Temperatur

    # === LANGUAGE SETTINGS ===
    "LANGUAGE": "de",                             # Sprache der Verarbeitung (de oder en)

    # === TEXT PROCESSING SETTINGS ===
    "TEXT_CHUNKING": True,                         # Text-Chunking aktivieren (False = ein LLM-Durchgang)
    "TEXT_CHUNK_SIZE": 2000,                       # Chunk-Größe in Zeichen
    "TEXT_CHUNK_OVERLAP": 50,                      # Überlappung zwischen Chunks in Zeichen

    # === ENTITY EXTRACTION SETTINGS ===
    "MODE": "extract",                           # Modus: extract oder generate
    "MAX_ENTITIES": 15,                            # Maximale Anzahl extrahierter Entitäten
    "ALLOWED_ENTITY_TYPES": "auto",              # Automatische Filterung erlaubter Entitätstypen
    "ENABLE_ENTITY_INFERENCE": False,              # Implizite Entitätserkennung aktivieren

    # === RELATIONSHIP EXTRACTION AND INFERENCE ===
    "RELATION_EXTRACTION": True,                   # Relationsextraktion aktivieren
    "ENABLE_RELATIONS_INFERENCE": False,           # Implizite Relationen aktivieren
    "MAX_RELATIONS": 15,                           # Maximale Anzahl Beziehungen pro Prompt

    # === CORE DATA SOURCE SETTINGS ===
    "USE_WIKIPEDIA": True,                        # Wikipedia-Verknüpfung aktivieren (immer True)
    "USE_WIKIDATA": False,                        # Wikidata-Verknüpfung aktivieren
    "USE_DBPEDIA": False,                         # DBpedia-Verknüpfung aktivieren
    "DBPEDIA_USE_DE": False,                      # Deutsche DBpedia nutzen (Standard: False = englische DBpedia)
    "ADDITIONAL_DETAILS": False,                  # Zusätzliche Details aus allen Wissensquellen abrufen

    # === DBpedia Lookup API Fallback ===
    "DBPEDIA_LOOKUP_API": True,                   # Fallback via DBpedia Lookup API aktivieren
    "DBPEDIA_SKIP_SPARQL": False,                 # SPARQL-Abfragen überspringen und nur Lookup-API verwenden
    "DBPEDIA_LOOKUP_MAX_HITS": 5,                 # Maximale Trefferzahl für Lookup-API
    "DBPEDIA_LOOKUP_CLASS": None,                 # Optionale DBpedia-Ontology-Klasse für Lookup-API
    "DBPEDIA_LOOKUP_FORMAT": "xml",             # Response-Format: "json", "xml" oder "beide"

    # === COMPENDIUM SETTINGS ===
    "ENABLE_COMPENDIUM": False,                   # Kompendium-Generierung aktivieren
    "COMPENDIUM_LENGTH": 8000,                    # Anzahl der Zeichen für das Kompendium (ca. 4 A4-Seiten)
    "COMPENDIUM_EDUCATIONAL_MODE": False,         # Bildungsmodus für Kompendium aktivieren

    # === KNOWLEDGE GRAPH VISUALIZATION SETTINGS ===
    "ENABLE_GRAPH_VISUALIZATION": True,           # Statische PNG- und interaktive HTML-Ansicht aktivieren

    # === KNOWLEDGE GRAPH COMPLETION (KGC) ===
    "ENABLE_KGC": True,                           # Knowledge-Graph-Completion aktivieren
    "KGC_ROUNDS": 3,                              # Anzahl der KGC-Runden

    # === STATISCHER GRAPH mit NetworkX-Layouts (PNG) ===
    "GRAPH_LAYOUT_METHOD": "spring",             # Layoutmethode für statisches PNG: "spring" oder "kamada_kawai"
    "GRAPH_LAYOUT_K": None,                       # Ideale Kantenlänge im Spring-Layout (None=Standard)
    "GRAPH_LAYOUT_ITERATIONS": 50,                # Iterationen für Spring-Layout
    "GRAPH_PHYSICS_PREVENT_OVERLAP": True,        # Überlappungsprävention im Spring-Layout aktivieren
    "GRAPH_PHYSICS_PREVENT_OVERLAP_DISTANCE": 0.1,# Mindestabstand zwischen Knoten
    "GRAPH_PHYSICS_PREVENT_OVERLAP_ITERATIONS": 50,# Iterationen zur Überlappungsprävention
    "GRAPH_PNG_SCALE": 0.30,                      # Skalierungsfaktor für das statische PNG-Layout (Standard 0.33)

    # === INTERAKTIVER GRAPH mit PyVis (HTML) ===
    "GRAPH_HTML_INITIAL_SCALE": 10,               # Anfangs-Zoom im interaktiven HTML-Graph

    # === TRAINING DATA COLLECTION SETTINGS ===
    "COLLECT_TRAINING_DATA": False,               # Trainingsdaten für Fine-Tuning sammeln
    "OPENAI_TRAINING_DATA_PATH": "entity_extractor_training_openai.jsonl",  # Pfad für Entitäts-Trainingsdaten
    "OPENAI_RELATIONSHIP_TRAINING_DATA_PATH": "entity_relationship_training_openai.jsonl",  # Pfad für Beziehungs-Trainingsdaten

    # === RATE LIMITER AND TIMEOUT SETTINGS ===
    "TIMEOUT_THIRD_PARTY": 20,                    # Timeout für externe Dienste (Sekunden)
    "RATE_LIMIT_MAX_CALLS": 3,                    # Maximale Anzahl Aufrufe pro Zeitraum
    "RATE_LIMIT_PERIOD": 1,                       # Zeitraum (Sekunden) für das Rate-Limiter-Fenster
    "RATE_LIMIT_BACKOFF_BASE": 1,                 # Basiswert für exponentielles Backoff bei HTTP 429
    "RATE_LIMIT_BACKOFF_MAX": 60,                 # Maximale Backoff-Dauer (Sekunden) bei HTTP 429
    "USER_AGENT": "EntityExtractor/1.0",        # HTTP User-Agent-Header für alle API-Anfragen
    "WIKIPEDIA_MAXLAG": 5,                       # Maxlag-Parameter für Wikipedia-API-Anfragen

    # === CACHING SETTINGS ===
    "CACHE_ENABLED": True,                        # Caching global aktivieren oder deaktivieren
    "CACHE_DIR": os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache"),  # Verzeichnis für Cache-Dateien
    "CACHE_DBPEDIA_ENABLED": True,                # Caching für DBpedia-SPARQL-Abfragen aktivieren
    "CACHE_WIKIDATA_ENABLED": True,               # Caching für Wikidata-API aktivieren
    "CACHE_WIKIPEDIA_ENABLED": True,              # Caching für Wikipedia-API-Anfragen aktivieren

    # === LOGGING AND DEBUG SETTINGS ===
    "SHOW_STATUS": True,                          # Statusmeldungen anzeigen
    "SUPPRESS_TLS_WARNINGS": True                 # TLS-Warnungen unterdrücken
}

result = extract_and_link_entities(text, config)
logging.info("Ergebnisse für langen Text:")

# Tabellarische Ausgabe der Entitäten und Beziehungen
if isinstance(result, dict) and "entities" in result and "relationships" in result:
    entities = result["entities"]
    relationships = result["relationships"]
else:
    entities = result
    relationships = []

# Entitäten-Tabelle
print("\nExtrahierte Entitäten:")
print("-" * 100)
print(f"{'Nr':3} | {'Name':25} | {'Typ':15} | {'Inferred':10} | {'Wikipedia':25} | {'Wikidata':15} | {'DBpedia':20}")
print("-" * 100)
for i, entity in enumerate(entities, start=1):
    # Prüfe verschiedene mögliche Strukturen der Entitäten
    if "entity" in entity:
        name = entity.get("entity", "")[:25]
        details = entity.get("details", {})
        etype = details.get("typ", "")[:15]
        inferred = details.get("inferred", "")[:10]
        sources = entity.get("sources", {})
        wiki = sources.get("wikipedia", {}).get("url", "")[:25]
        wikidata = sources.get("wikidata", {}).get("id", "")[:15]
        dbpedia = sources.get("dbpedia", {}).get("url", "")[:20]
    else:
        # Fallback für ältere Entitätsstruktur
        name = entity.get("name", "")[:25]
        etype = entity.get("type", "")[:15]
        inferred = entity.get("inferred", "explicit")[:10]
        wiki = entity.get("wikipedia_url", "")[:25]
        wikidata = entity.get("wikidata_id", "")[:15]
        dbpedia = entity.get("dbpedia_abstract", "")[:20]
    
    print(f"{i:3} | {name:25} | {etype:15} | {inferred:10} | {wiki:25} | {wikidata:15} | {dbpedia:20}")
print("-" * 100)
print(f"Insgesamt {len(entities)} Entitäten gefunden.")

# Beziehungen-Tabelle
if relationships:
    explicit = [r for r in relationships if r.get("inferred") == "explicit"]
    implicit = [r for r in relationships if r.get("inferred") == "implicit"]

    # Map Entity-Namen auf Entity-Inferenzstatus
    entity_inf_map = {ent.get("entity", ""): ent.get("details", {}).get("inferred", "") for ent in entities}

    print("\nExplizite Beziehungen:")
    print("-" * 140)
    print(f"{'Nr':3} | {'Subjekt':25} | {'SubjTyp':12} | {'SubjInf':10} | {'Prädikat':20} | {'Objekt':25} | {'ObjTyp':12} | {'ObjInf':10}")
    print("-" * 140)
    for i, rel in enumerate(explicit, start=1):
        full_subj = rel.get("subject", "")
        subj = full_subj[:25]
        stype = rel.get("subject_type", "")[:12]
        subject_inf = entity_inf_map.get(full_subj, "")[:10]
        pred = rel.get("predicate", "")[:20]
        full_obj = rel.get("object", "")
        obj = full_obj[:25]
        otype = rel.get("object_type", "")[:12]
        object_inf = entity_inf_map.get(full_obj, "")[:10]
        print(f"{i:3} | {subj:25} | {stype:12} | {subject_inf:10} | {pred:20} | {obj:25} | {otype:12} | {object_inf:10}")
    print("-" * 140)
    print(f"Insgesamt {len(explicit)} explizite Beziehungen gefunden.")

    print("\nImplizite Beziehungen:")
    print("-" * 140)
    print(f"{'Nr':3} | {'Subjekt':25} | {'SubjTyp':12} | {'SubjInf':10} | {'Prädikat':20} | {'Objekt':25} | {'ObjTyp':12} | {'ObjInf':10}")
    print("-" * 140)
    for i, rel in enumerate(implicit, start=1):
        full_subj = rel.get("subject", "")
        subj = full_subj[:25]
        stype = rel.get("subject_type", "")[:12]
        subject_inf = entity_inf_map.get(full_subj, "")[:10]
        pred = rel.get("predicate", "")[:20]
        full_obj = rel.get("object", "")
        obj = full_obj[:25]
        otype = rel.get("object_type", "")[:12]
        object_inf = entity_inf_map.get(full_obj, "")[:10]
        print(f"{i:3} | {subj:25} | {stype:12} | {subject_inf:10} | {pred:20} | {obj:25} | {otype:12} | {object_inf:10}")
    print("-" * 140)
    print(f"Insgesamt {len(implicit)} implizite Beziehungen gefunden.")
else:
    print("Keine Beziehungen gefunden.")

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
# Top 10 DBpedia-Subjects
print("\n  Top 10 DBpedia-Subjects:")
for sub in stats.get('top_dbpedia_subjects', []):
    print(f"    {sub['subject']}: {sub['count']}")
