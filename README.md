# Entity Extractor & Linker (LLM-basiert)

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/janschachtschabel/entity-extractor-linker)

Entity Extractor & Linker ist ein flexibles, modulares Tool zur automatisierten Extraktion und Generierung von Named Entities in bzw. zu beliebigen Texten. Es kann erkannte Entitäten direkt mit Informationen aus Wikipedia, Wikidata und DBpedia verknüpfen – inklusive mehrsprachiger Ausgaben (Deutsch, Englisch). Die Ergebnisse liegen in einer strukturierten JSON-Ausgabe vor, die Metadaten zu Entitäten und (optional) Beziehungen enthält. Beziehungen können als explizite (aus dem Text extrahierte) oder implizite (logisch geschlussfolgerte) Triple (Subjekt–Prädikat–Objekt) generiert und in interaktiven Knowledge Graphen visualisiert werden.

**NEU**: Version 1.2.0 führt eine optimierte kontextbasierte Architektur ein, die Batch-Verarbeitung von Entitäten und strukturierte Datenübergabe zwischen Services ermöglicht, was zu besserer Performance und erweiterten Statistiken führt.

## Inhaltsverzeichnis

- [Installation](#installation)
- [Funktionen](#funktionen)
- [API-Referenz](#api-referenz)
- [Datenschema](#datenschema)
- [Anwendungsbeispiele](#anwendungsbeispiele)
- [Projektstruktur](#projektstruktur)
- [Architektur-Übersicht](#architektur-übersicht)
- [Konfiguration](#konfiguration)
- [Lizenz](#lizenz)
- [Autor](#autor)

## Installation

```bash
# Repository klonen
git clone https://github.com/janschachtschabel/entity-extractor-linker.git
cd entity-extractor-linker

# Option 1: Entwicklungsinstallation (empfohlen)
pip install -e .

# Option 2: Produktion
pip install entity-extractor-linker
```

Setze anschließend den OpenAI API Key in der Umgebungsvariable:

```bash
export OPENAI_API_KEY="<dein_api_key>"
```

## Funktionen

### Kernfunktionen
- **Entitäten extrahieren**: Direkt aus Texten identifizieren (extrahieren).
- **Entitäten generieren**: Kontextbasiert neue Entitäten vorschlagen (generieren).
- **Beziehungsextraktion**: Explizite Beziehungen (Subjekt; Prädikat; Objekt) im Text erkennen.
- **Entitäteninferenz**: Implizite logische Knoten ergänzen und Knowledge Graph vervollständigen.
- **Beziehungsinferenz**: Implizite logische Verbindungen ergänzen und Knowledge Graph vervollständigen.
- **Graph-Visualisierung**: Erzeuge statische PNG-Graphen oder interaktive HTML-Ansichten.
- **Kompendium-Generierung**: Erstellung eines kompendialen (zusammenfassenden) Textes mit Referenzen.

### Neue kontextbasierte Architektur (v1.2.0)
- **Optimierte Batch-Verarbeitung**: Effiziente Verarbeitung mehrerer Entitäten in Batches.
- **Strukturierte Datenübergabe**: Verbesserte Kommunikation zwischen Services mit `EntityProcessingContext`.
- **Erweiterte Statistiken**: Detaillierte Statistiken direkt aus Kontexten mit Prozesszeiten und Beziehungsinferenzstatus.
- **Direkte Kontext-Visualisierung**: Erzeugung von Knowledge Graphs direkt aus `EntityProcessingContext`-Objekten.
- **Verbesserte Schema-Validierung**: Umfassende Validierung für alle Aspekte der kontextbasierten Verarbeitung.

### Technische Features
- **Trainingsdaten-Generierung**: Speichere Entity- und Relationship-Daten als JSONL für OpenAI Fine-Tuning.
- **LLM-Schnittstelle**: Kompatibel mit OpenAI-API, anpassbare Basis-URL und Modell.
- **Wissensquellen-Integration**: Wikipedia, Wikidata, DBpedia (SPARQL + Lookup API Fallback).
- **Caching**: Zwischenspeicherung von API-Antworten für schnellere wiederholte Zugriffe.
- **Ratelimiter**: Fängt Fehler mit Ratelimits der Wissensquellen ab.
- **Mehrsprachigkeit**: Optimierte Unterstützung für Deutsch und Englisch mit korrekter Behandlung von Named Entities.

## API-Referenz

### High-Level Convenience

`process_entities` ist der empfohlene Einstiegspunkt – er erkennt automatisch, ob du Text extrahieren, Entitäten generieren oder ein Kompendium erstellen möchtest.  
Die älteren Convenience-Funktionen existieren weiterhin (Alias aufs gleiche Backend) und können für Klarheit verwendet werden, sind aber nicht mehr zwingend nötig.

| Funktion | Status | Kurzbeschreibung |
| -------- | ------- | ---------------- |
| `process_entities(input_data, config)` | **empfohlen** | Universeller Wrapper; erkennt Modus automatisch |
| `extract_and_link_entities(text, config)` | Alias | Spezialisierte Funktion für reine Text-Extraktion |
| `generate_and_link_entities(topic, config)` | Alias | Generiert thematische Entitäten und verknüpft sie |
| `create_knowledge_compendium(topic, config)` | Alias | Erstellt ein Kompendium inkl. Graph-Visualisierung |

Alle Funktionen sind `async`. Beispiel:

```python
import asyncio, json
from typing import Dict, Any
from entityextractor.api import process_entities

async def main() -> None:
    print("Starte Prozess …\n")
    sample_text = "Berlin ist die Hauptstadt Deutschlands."
    config: Dict[str, Any] = {"LANGUAGE": "de"}
    result: Dict[str, Any] = await process_entities(sample_text, config)
    print(json.dumps(result, ensure_ascii=False, indent=2))

asyncio.run(main())
```

### Beispielskripte (Ordner `examples/`)

| Script | Zweck |
|--------|-------|
| `examples/example_extract.py` | Einfaches Extraktions-Beispiel für einen Text |
| `examples/generate_location_geo.py` | Generiert Orte inkl. Geo-Koordinaten und verlinkt sie |
| `examples/generate_compendium_qa_quantenmechanik.py` | Erstellt Thema-Kompendium & Frage-Antwort-Paare |
| `examples/minimal_generate.py` | Minimaler Einstieg in die Entitätsgenerierung |

Starte ein Skript direkt über die Kommandozeile, z.B.:

```bash
python examples/generate_location_geo.py
```

Alle Skripte greifen intern auf `await process_entities(...)` zurück und können als Vorlage für eigene Workflows dienen.

#### Low-Level-Helpers
Weitere Hilfsfunktionen findest du im Modul `entityextractor.core.api` (`extract_entities`, `link_entities`, `infer_entity_relationships` …).

## Datenschema
Eine ausführliche Beschreibung befindet sich in [`docs/DataModelsSchemas.md`](docs/DataModelsSchemas.md).
Kurzfassung des Ergebnis-JSONs:

```jsonc
{
  "entities": [
    {
      "entity": "Albert Einstein",
      "details": {
        "typ": "Person",
        "inferred": "explicit"
      },
      "sources": {
        "wikipedia": { /* … */ },
        "wikidata":  { /* … */ },
        "dbpedia":   { /* … */ }
      }
    }
  ],
  "relationships": [
    {
      "subject": "Albert Einstein",
      "predicate": "entwickelte",
      "object": "Relativitätstheorie",
      "inferred": "explicit",
      "subject_type": "Person",
      "object_type": "Theorie"
    }
  ],
  "statistics": {
    "total_entities": 1,
    "total_relationships": 1,

    "types_distribution": {
      "Person": 1,
      "Theorie": 1
    },

    "linked": {
      "wikipedia": { "count": 1, "percent": 100.0 },
      "wikidata":  { "count": 1, "percent": 100.0 },
      "dbpedia":   { "count": 0, "percent": 0.0 }
    },

    "entity_connections": [
      { "entity": "Albert Einstein",        "count": 1 },
      { "entity": "Relativitätstheorie",    "count": 1 }
    ],

    "top10": {
      "wikidata_part_of":   { "Physik": 1 },
      "wikidata_has_part":  {},
      "dbpedia_part_of":    {},
      "dbpedia_has_part":   {},
      "dbpedia_subjects":   {}
    }
  },

  "knowledgegraph_visualisation": {
    "static": "knowledge_graph.png",
    "interactive": "knowledge_graph_interactive.html"
  }
}
```

**Statistik-Felder – Kurzüberblick**

| Feld | Beschreibung |
|------|--------------|
| `total_entities` | Anzahl aller verarbeiteten Entitäten |
| `total_relationships` | Gesamtzahl erkannter Beziehungen |
| `types_distribution` | Häufigkeit pro Entitätstyp |
| `linked` | Für Wikipedia / Wikidata / DBpedia: Treffer-Anzahl & Prozentanteil |
| `entity_connections` | Anzahl eindeutiger Verknüpfungen pro Entität |
| `top10.wikidata_part_of` usw. | Top-Beziehungen und DBpedia-Subjects (max. 10) |

Weitere Felder können je nach aktivierten Services hinzukommen (z. B. Laufzeiten oder Fallback-Statistiken).

Nur wenn die strikten Validierungskriterien eines Services erfüllt sind (z. B. URI + EN-Label + EN-Abstract bei DBpedia), wird dessen `status` auf `linked` gesetzt.

## Anwendungsbeispiele

### Einfache Entitätsextraktion

```python
import json
from entityextractor.api import extract_and_link_entities

text = "Albert Einstein war ein theoretischer Physiker."
config = {
    "LANGUAGE": "de",               # oder "en" für Englisch
    "MODEL": "gpt-4o-mini",         # oder ein anderes unterstütztes OpenAI-Modell
    "USE_WIKIPEDIA": True,          # Verlinkt Entitäten mit Wikipedia
    "RELATION_EXTRACTION": True,    # Extrahiert Beziehungen zwischen Entitäten
}
result = extract_and_link_entities(text, config)
print(json.dumps(result, ensure_ascii=False, indent=2))
```

### Kontextbasierte Verarbeitung (Neu in v1.2.0)

```python
from entityextractor import EntityProcessingContext, process_entity, visualize_contexts

# Erstellen eines Verarbeitungskontexts
context = EntityProcessingContext(
    entity_name="Albert Einstein", 
    entity_type="Person",
    original_text="Albert Einstein entwickelte die Relativitätstheorie."
)

# Asynchrone Verarbeitung des Kontexts
import asyncio

async def process_example():
    # Verarbeite den Kontext mit allen aktivierten Services
    await process_entity(context.entity_name, context.entity_type, context.entity_id, context.original_text)
    
    # Zeige eine Zusammenfassung des verarbeiteten Kontexts
    context.log_summary()
    
    # Visualisiere den Kontext (erstellt PNG und HTML)
    visualize_contexts([context], output_name="einstein_graph")
    
    # Hole die strukturierte Ausgabe
    output = context.get_output()
    print(f"Entity: {output['entity']}")
    print(f"Wikipedia URL: {output.get('sources', {}).get('wikipedia', {}).get('url', 'Nicht gefunden')}")

# Führe das Beispiel aus
loop = asyncio.get_event_loop()
loop.run_until_complete(process_example())
```

### Entitäten generieren

```python
from entityextractor.api import generate_and_link_entities

topic = "Künstliche Intelligenz"
config = {
    "LANGUAGE": "de",
    "MAX_ENTITIES": 10,             # Maximale Anzahl zu generierender Entitäten
    "ENABLE_RELATIONS_INFERENCE": True,  # Inferiert Beziehungen zwischen Entitäten
    "ENABLE_GRAPH_VISUALIZATION": True,  # Erstellt eine Visualisierung
}
result = generate_and_link_entities(topic, config)
```

### Wissenskompendium erstellen

```python
from entityextractor.api import create_knowledge_compendium

topic = "Quantenphysik"
config = {
    "LANGUAGE": "de",
    "COMPENDIUM_LENGTH": 500,       # Länge des Kompendiums in Zeichen
    "USE_WIKIDATA": True,           # Nutzt auch Wikidata als Wissensquelle
    "USE_DBPEDIA": True,            # Nutzt auch DBpedia als Wissensquelle
}
result = create_knowledge_compendium(topic, config)
```

### Beispiel-Musteroutput

```json
{
  "entities": [
    {
      "entity": "Albert Einstein",
      "details": {
        "typ": "Person",
        "inferred": "explicit",
        "citation": "Albert Einstein entwickelte die Relativitätstheorie.",
        "citation_start": 0,
        "citation_end": 52
      },
      "sources": {
        "wikipedia": {
          "label": "Albert Einstein",
          "url": "https://en.wikipedia.org/wiki/Albert_Einstein"
        }
      }
    }
  ],
  "relationships": [
    {
      "subject": "Albert Einstein",
      "predicate": "entwickelte",
      "object": "Relativitätstheorie",
      "inferred": "explicit",
      "subject_type": "Person",
      "object_type": "Theorie",
      "subject_inferred": "explicit",
      "object_inferred": "explicit"
    }
  ],
  "statistics": {
    "total_entities": 2,
    "types_distribution": {
      "Person": 1,
      "Theorie": 1
    },
    "linked": {
      "wikipedia": 2,
      "wikidata": 1,
      "dbpedia": 0
    },
    "entity_connections": [
      { "entity": "Albert Einstein", "count": 1 },
      { "entity": "Relativitätstheorie", "count": 1 }
    ],
    "top_wikidata_part_of": [],
    "top_wikidata_has_parts": [],
    "top_dbpedia_part_of": [],
    "top_dbpedia_has_parts": [],
    "top_dbpedia_subjects": []
  },
  "knowledgegraph_visualisation": {
    "static": "knowledge_graph.png",
    "interactive": "knowledge_graph_interactive.html"
  },
  "compendium": {
    "text": "Albert Einstein war ein theoretischer Physiker, der die Relativitätstheorie entwickelte.",
    "references": [
      "https://en.wikipedia.org/wiki/Albert_Einstein",
      "https://de.wikipedia.org/wiki/Relativitätstheorie"
    ]
  }
}
```

## Projektstruktur

```plaintext
.
├── entityextractor/                  # Hauptpaket
│   ├── api.py                        # Öffentliche API-Schnittstelle
│   ├── main.py
│   ├── models/                       # Pydantic-Datenmodelle
│   ├── schemas/                      # JSON-Schemas & Datendefinitionen
│   ├── cache/                        # Zwischengespeicherte Daten (LLM, API)
│   ├── config/
│   │   └── settings.py               # Default-Konfigurationen
│   ├── core/
│   │   ├── api/
│   │   ├── process/
│   │   └── visualization/
│   ├── services/
│   │   ├── wikipedia/                # Wikipedia-Integration
│   │   ├── wikidata/                 # Wikidata-Integration mit Cache
│   │   ├── dbpedia/                  # DBpedia (SPARQL & Lookup)
│   │   ├── translation_service.py    # Übersetzungsdienst (optional)
│   │   └── entity_manager.py         # Orchestriert Service-Calls
│   ├── prompts/                      # Prompt-Vorlagen
│   └── utils/                        # Hilfsfunktionen (Caching, Logging …)
├── examples/                         # Ausführbare Beispielskripte
│   ├── generate_location_geo.py
│   ├── generate_compendium_qa_quantenmechanik.py
│   └── minimal_generate.py
├── requirements.txt                  # Produktionsabhängigkeiten
└── README.md
```
├── lib/                              # Externe Bibliotheken
├── entityextractor/                  # Hauptpaket
│   ├── __init__.py
│   ├── main.py
│   ├── api.py                        # Öffentliche API-Schnittstelle
│   ├── cache/                        # Zwischengespeicherte Daten (z. B. LLM Outputs, API-Antworten)
│   │   └── ...
│   ├── config/                       # Konfigurationsdateien
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── core/                         # Kernlogik
│   │   ├── __init__.py
│   │   ├── extractor.py              # Basisimplementierung für Entitätsextraktion
│   │   ├── generator.py              # Basisimplementierung für Entitätsgenerierung
│   │   ├── linker.py                 # Basisimplementierung für Entitätsverknüpfung
│   │   ├── entity_inference.py       # Inferenz impliziter Entitäten
│   │   ├── api/                      # Konsolidierte API-Schicht
│   │   │   ├── __init__.py
│   │   │   ├── extract.py            # API für Entitätsextraktion
│   │   │   ├── generate.py           # API für Entitätsgenerierung
│   │   │   ├── link.py               # API für Entitätsverknüpfung
│   │   │   └── relationships.py      # API für Beziehungsinferenz
│   │   ├── process/                  # Verarbeitungsprozesse
│   │   │   ├── __init__.py
│   │   │   ├── deduplication.py      # Deduplizierungslogik
│   │   │   ├── orchestrator.py       # Workflowsteuerung
│   │   │   ├── result_formatter.py   # Formatierung der Ergebnisse
│   │   │   └── statistics.py         # Generierung von Statistiken
│   │   └── visualization/            # Visualisierungskomponenten
│   │       ├── __init__.py
│   │       ├── graph_builder.py      # Erstellung von Knowledge Graphs
│   │       └── renderer.py           # Rendering in PNG und HTML
│   ├── prompts/                      # Prompt-Definitionen
│   │   ├── __init__.py
│   │   ├── compendium_prompts.py
│   │   ├── deduplication_prompts.py
│   │   ├── entity_inference_prompts.py
│   │   ├── extract_prompts.py
│   │   ├── generation_prompts.py
│   │   ├── relationship_prompts.py
│   │   └── schema_prompts.py
│   ├── services/                     # Dienste und Schnittstellen
│   │   ├── __init__.py
│   │   ├── dbpedia_service.py
│   │   ├── openai_service.py
│   │   ├── schema_service.py
│   │   ├── wikidata_service.py
│   │   └── wikipedia_service.py
│   └── utils/                        # Hilfsfunktionen
│       ├── __init__.py
│       ├── cache_utils.py
│       ├── logging_utils.py
│       ├── openai_utils.py
│       ├── text_utils.py
│       ├── unpack_utils.py
│       └── web_utils.py
├── example_extract.py               # Beispiel: Entitätsextraktion
├── example_generate.py              # Beispiel: Entitätsgenerierung
├── example_generate_simple.py       # Einfaches Beispiel: Entitätsgenerierung
├── example_relations.py             # Beispiel: Beziehungsinferenz
├── example_knowledgegraph.py        # Beispiel: Knowledge Graph
├── example_chunking.py              # Beispiel: Text-Chunking
├── example_compendium_person.py     # Beispiel: Wissenskompendium
├── requirements-dev.txt             # Entwicklungsabhängigkeiten
├── requirements.txt                 # Produktionsabhängigkeiten
├── setup.py                         # Setupscript für pip
├── NOTICE                           # Rechtliche Hinweise
└── LICENSE                          # Lizenzinformationen
```

## Architektur-Übersicht

Die Entity-Extractor-Bibliothek folgt einer klaren Layer-Architektur:

```
┌─────────────────────────────────────────────────────────┐
│                  Öffentliche API (sync/async)           │
│            entityextractor.api.process_entities         │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│                API-/Orchestrator-Layer                 │
│            core/api/*  ·  core/process/orchestrator.py │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│            Kontext- & Prozess-Layer (Business-Logic)   │
│   core/context.py · core/process/* · models/ · schemas/│
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│                 Service-Layer (I/O)                    │
│ services/wikipedia · wikidata · dbpedia · translation  │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│           Utilities & Infrastruktur (querschnitt)      │
│              utils/* · cache/* · prompts/*             │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│                  Externe APIs & Datenquellen           │
│      OpenAI · Wikipedia · Wikidata · DBpedia           │
└─────────────────────────────────────────────────────────┘
```

### Erklärung der Architektur-Schichten

1. **Öffentliche API** (`entityextractor.api`)
   - Stellt einfache Funktionen für Endbenutzer bereit
   - Fokus auf intuitive Benutzerfreundlichkeit und Flexibilität
   - Unterstützt sowohl dictionary-basierte als auch kontext-basierte Architekturen

2. **API-/Orchestrator-Schicht** (`core/api/*`, `core/process/orchestrator.py`)
   - Integriert Basisfunktionalität mit zusätzlichen Prozessen
   - Bietet die Hauptimplementierungen für alle öffentlichen Funktionen
   - Exportiert sowohl dictionary-basierte als auch kontextbasierte API-Funktionen

3. **Kontext- & Prozess-Layer** (`core/context.py`, `core/process/*`, `models/`, `schemas/`)
   - Zentrale `EntityProcessingContext`-Klasse zur strukturierten Datenübergabe
   - Validierung, Geschäftslogik und Workflow-Management
   - Verwaltung von Beziehungen, Statistiken und Metadaten

4. **Service-Layer (I/O)** (`services/wikipedia`, `services/wikidata`, `services/dbpedia`, `services/translation`)
   - Asynchrone Kommunikation mit externen Wissensquellen und APIs
   - Kapselt Caching, Timeout-Handling und Fallback-Strategien

5. **Utilities & Infrastruktur** (`utils/*`, `cache/*`, `prompts/*`)
   - Querschnittsfunktionen wie Logging, Token-Handling, Prompt-Vorlagen und persistentes Caching
   - Wird von allen oberen Schichten genutzt und enthält keine Geschäftslogik

6. **Externe APIs & Datenquellen** (OpenAI, Wikipedia, Wikidata, DBpedia …)
   - Systeme außerhalb des Projekts, die über das Service-Layer angebunden werden

## Funktionsweise

Die Entity Extractor Bibliothek implementiert eine mehrschichtige Verarbeitungspipeline:

1. **Entitätsextraktionspipeline**: Aufbereitung von Texten, Chunking, Tokenanalyse und LLM-basierte Extraktion mit intelligenter Zusammenführung.
2. **Wikipedia-Integration**: Verknüpfung erkannter Entitäten mit Wikipedia-Artikeln und Extraktion von Zusammenfassungen.
3. **Wikidata-Integration**: Abruf von Wikidata-IDs, Typen und Beschreibungen.
4. **DBpedia-Integration**: Nutzung von DBpedia für zusätzliche strukturierte Informationen.
5. **Sprachübergreifende Verarbeitung**: Automatische Übersetzung und Suche in Deutsch und Englisch.

## Tipps und Best Practices

- Für große Texte aktiviere das Text-Chunking (`TEXT_CHUNKING=True`).
- Verwende die entsprechende Sprache für bessere Ergebnisse (`LANGUAGE="de"` oder `"en"`).
- Für detaillierte Entitätsinformationen aktiviere Wikidata und DBpedia.
- Experimentiere mit verschiedenen LLM-Modellen für unterschiedliche Anwendungsfälle.
- Setze `COLLECT_TRAINING_DATA=True`, um Trainingsdaten für Fine-Tuning zu sammeln.

## Konfiguration

Alle Einstellungen liegen in `entityextractor/config/settings.py` unter `DEFAULT_CONFIG`. Die folgende Tabelle zeigt die wichtigsten Parameter (vollständige Liste siehe Datei).

| Kategorie | Parameter | Standard | Beschreibung |
|-----------|-----------|----------|--------------|
| **LLM** | `MODEL` | `"gpt-4.1-mini"` | OpenAI-Modell für Prompts |
| | `TEMPERATURE` | `0.2` | Sampling-Temperatur |
| | `MAX_TOKENS` | `16000` | Max. Token pro Anfrage |
| **Sprache & Modus** | `LANGUAGE` | `"en"` | Verarbeitungssprache (`"de"`/`"en"`) |
| | `MODE` | `"extract"` | `extract` oder `generate` |
| **Extraktion & Generierung** | `MAX_ENTITIES` | `10` | Begrenzung extrahierter oder generierter Entitäten |
| | `ENABLE_ENTITY_INFERENCE` | `False` | Implizite Entitäten erkennen |
| **Beziehungen** | `RELATION_EXTRACTION` | `False` | Explizite Triple-Extraktion |
| | `ENABLE_RELATIONS_INFERENCE` | `False` | Implizite Relationen erkennen |
| | `MAX_RELATIONS` | `15` | Begrenzung extrahierter Relationen |
| **Wissensquellen** | `USE_WIKIPEDIA` | `True` | Wikipedia-Verknüpfung (immer aktiv) |
| | `USE_WIKIDATA` | `False` | Wikidata-Verknüpfung |
| | `USE_DBPEDIA` | `False` | DBpedia-Verknüpfung |
| | `DBPEDIA_LOOKUP_API` | `True` | Lookup-API-Fallback aktivieren |
| **Kompendium & QA** | `ENABLE_COMPENDIUM` | `False` | Kompendium generieren |
| | `ENABLE_QA_PAIRS` | `False` | Frage-Antwort-Paare erstellen |
| **Visualisierung** | `ENABLE_GRAPH_VISUALIZATION` | `False` | Knowledge-Graph PNG/HTML erzeugen |
| **Caching** | `CACHE_ENABLED` | `True` | Globales Caching aktivieren |
| | `CACHE_DIR` | *Projektordner*/cache | Speicherort Cache |
| **Debug & Logging** | `LOG_LEVEL` | `"INFO"` | Globales Log-Level |
| | `DEBUG_MODE` | `False` | Zusätzliche Debug-Ausgaben |

*(Weitere Parameter wie Ratelimiter, Timeouts, Training-Data-Export etc. siehe `settings.py`.)                               | Typ                | Standardwert                                 | Beschreibung                                                                                          |
|-----------------------------------------|--------------------|----------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `LANGUAGE`                              | string             | `"de"`                                       | Sprache (de oder en)                                                                                  |
| `MODEL`                                 | string             | `"gpt-4o-mini"`                            | OpenAI API Modell                                                                                     |
| `MAX_TOKENS`                            | integer            | `16000`                                      | Maximale Tokenanzahl pro Anfrage                                                                       |
| `TEMPERATURE`                           | float              | `0.2`                                        | Sampling-Temperatur                                                                                   |
| `MODE`                                  | string             | `"extract"`                                | Modus: `extract` oder `generate`                                                                       |
| `MAX_ENTITIES`                          | integer            | `15`                                         | Maximale Anzahl extrahierter Entitäten                                                                 |
| `MAX_RELATIONS`                         | integer            | `15`                                         | Maximale Anzahl Beziehungen pro Prompt                                                                 |
| `USE_WIKIPEDIA`                         | boolean            | `True`                                       | Wikipedia-Verknüpfung aktivieren (immer `True`)                                                        |
| `DBPEDIA_LOOKUP_API`                    | boolean            | `True`                                       | Fallback via DBpedia Lookup API aktivieren                                                              |

## Ausgabestruktur

Die Ausgabe liefert eine JSON-Struktur mit folgenden Feldern:

- **entities**: Liste erkannter Entitäten mit `entity`, `details` und `sources`.
- **relationships**: Liste von Triple-Objekten. Jede Beziehung besteht aus:
  - **subject**: Quell-Entität
  - **predicate**: Beziehungsart (z.B. "veröffentlichte")
  - **object**: Ziel-Entität
  - **inferred**: "explizit" oder "implizit"
  - **subject_type**: Typ der Quell-Entität
  - **object_type**: Typ der Ziel-Entität
  - **subject_inferred**: explizit/implizit für Quell-Entität
  - **object_inferred**: explizit/implizit für Ziel-Entität
- **statistics**: Objekt mit Statistiken zu Entitäten und Relationen:
  - **total_entities**: Gesamtanzahl der erkannten Entitäten
  - **types_distribution**: Verteilung der Entitätstypen (Typ → Anzahl)
  - **linked**: Verlinkungserfolg nach Quelle (`wikipedia`, `wikidata`, `dbpedia`)
  - **top_wikipedia_categories**: Top-10 Wikipedia-Kategorien nach Häufigkeit
  - **top_wikidata_types**: Top-10 Wikidata-Typen nach Häufigkeit
  - **entity_connections**: Anzahl eindeutiger Verknüpfungen pro Entität
  - **top_wikidata_part_of**, **top_wikidata_has_parts**, **top_dbpedia_part_of**, **top_dbpedia_has_parts**, **top_dbpedia_subjects**: Weitere Top-Statistiken für Teil-Beziehungen und DBpedia-Subjects
- **compendium**: Objekt mit `text` (kompendialer Text) und `references` (Liste der verwendeten Quellen-URLs)

## Lizenz

Dieses Projekt ist unter der Apache 2.0 Lizenz veröffentlicht. Details siehe [LICENSE](LICENSE).

Weitere rechtliche Hinweise findest du in der [NOTICE](NOTICE).

## Autor

**Jan Schachtschabel**
