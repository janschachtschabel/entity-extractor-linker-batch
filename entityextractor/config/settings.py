"""
Default configuration settings for the Entity Extractor.

This module defines the default configuration settings used throughout
the application. These settings can be overridden by providing a custom
configuration dictionary when calling the entity extraction functions.
"""

import os

# Default configuration for entity extraction
DEFAULT_CONFIG = {
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
    "MAX_ENTITIES": 15,              # Maximale Anzahl extrahierter Entitäten
    "ALLOWED_ENTITY_TYPES": "auto",  # Automatische Filterung erlaubter Entitätstypen
    "ENABLE_ENTITY_INFERENCE": False, # Implizite Entitätserkennung aktivieren

    # === RELATIONSHIP EXTRACTION AND INFERENCE ===
    "RELATION_EXTRACTION": True,         # Relationsextraktion aktivieren
    "ENABLE_RELATIONS_INFERENCE": False,  # Implizite Relationen aktivieren
    "MAX_RELATIONS": 15,                  # Maximale Anzahl Beziehungen pro Prompt

    # === CORE DATA SOURCE SETTINGS ===
    "USE_WIKIPEDIA": True,          # Wikipedia-Verknüpfung aktivieren (immer True)
    "USE_WIKIDATA": False,          # Wikidata-Verknüpfung aktivieren
    "USE_DBPEDIA": False,           # DBpedia-Verknüpfung aktivieren
    "DBPEDIA_USE_DE": False,        # Deutsche DBpedia nutzen (Standard: False = englische DBpedia)

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
    "ENABLE_GRAPH_VISUALIZATION": False,  # Statische PNG- und interaktive HTML-Ansicht aktivieren (erfordert RELATION_EXTRACTION=True)
    "GRAPH_STYLE": "modern",              # Visueller Stil: "modern" (Pastellfarben), "classic" (Kräftige Farben), "minimal" (Grautöne)
    "GRAPH_NODE_STYLE": "label_above",    # Knotendarstellung: "label_above" (kleine Kreise, Label darüber), "label_below" (kleine Kreise, Label darunter), "label_inside" (große Kreise mit Label)
    "GRAPH_EDGE_LENGTH": "standard",      # Kantenlänge: "standard" (normale Distanz), "compact" (50% kürzer), "extended" (50% länger)
    "GRAPH_OUTPUT_DIR": "./output",       # Ausgabeverzeichnis für Visualisierungen
    
    # === KNOWLEDGE GRAPH COMPLETION (KGC) ===
    "ENABLE_KGC": False,                  # Knowledge-Graph-Completion aktivieren (Vervollständigung mit impliziten Relationen)
    "KGC_ROUNDS": 3,                      # Anzahl der KGC-Runden

    # === TRAINING DATA COLLECTION SETTINGS ===
    "COLLECT_TRAINING_DATA": False,  # Trainingsdaten für Fine-Tuning sammeln
    "OPENAI_TRAINING_DATA_PATH": "entity_extractor_training_openai.jsonl",  # Pfad für Entitäts-Trainingsdaten
    "OPENAI_RELATIONSHIP_TRAINING_DATA_PATH": "entity_relationship_training_openai.jsonl",  # Pfad für Beziehungs-Trainingsdaten

    # === RATE LIMITER AND TIMEOUT SETTINGS ===
    "TIMEOUT_THIRD_PARTY": 15,       # Timeout für externe Dienste (Wikipedia, Wikidata, DBpedia)
    "RATE_LIMIT_MAX_CALLS": 3,       # Maximale Anzahl Aufrufe pro Zeitraum
    "RATE_LIMIT_PERIOD": 1,          # Zeitraum in Sekunden
    "RATE_LIMIT_BACKOFF_BASE": 1,    # Basiswert für exponentielles Backoff
    "RATE_LIMIT_BACKOFF_MAX": 60,    # Maximale Wartezeit bei Backoff
    "USER_AGENT": "EntityExtractor/1.0", # HTTP User-Agent-Header
    "WIKIPEDIA_MAXLAG": 5,           # Maxlag-Parameter für Wikipedia-API

    # === CACHING SETTINGS ===
    "CACHE_ENABLED": True,   # Caching global aktivieren oder deaktivieren
    "CACHE_DIR": os.path.join(os.path.dirname(__file__), "../cache"),    # Verzeichnis für Cache-Dateien innerhalb des entityextractor-Pakets
    "CACHE_WIKIPEDIA_ENABLED": True,            # (Optional) Caching für Wikipedia-API-Anfragen aktivieren
    "CACHE_WIKIDATA_ENABLED": True,             # (Optional) Caching für Wikidata-API aktivieren
    "CACHE_DBPEDIA_ENABLED": True,              # Caching für DBpedia-SPARQL-Abfragen aktivieren

    # === LOGGING AND DEBUG SETTINGS ===
    "SHOW_STATUS": True,            # Statusmeldungen anzeigen
    "SUPPRESS_TLS_WARNINGS": True   # TLS-Warnungen unterdrücken
}

def get_config(user_config=None):
    """
    Get a configuration dictionary with user overrides applied.
    
    Args:
        user_config: Optional user configuration dictionary to override defaults
        
    Returns:
        A configuration dictionary with user overrides applied to defaults
    """
    config = DEFAULT_CONFIG.copy()
    
    if user_config:
        config.update(user_config)
        
    # Wenn API-Key nicht vorhanden, aus Umgebungsvariable laden
    if not config.get("OPENAI_API_KEY"):
        config["OPENAI_API_KEY"] = os.environ.get("OPENAI_API_KEY")
    

    return config
