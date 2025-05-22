# Entity Extractor & Linker (LLM-basiert)

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/janschachtschabel/entity-extractor-linker)

Entity Extractor & Linker ist ein flexibles, modulares Tool zur automatisierten Extraktion und Generierung von Named Entities in bzw. zu beliebigen Texten. Es kann erkannte Entitäten direkt mit Informationen aus Wikipedia, Wikidata und DBpedia verknüpfen – inklusive mehrsprachiger Ausgaben (Deutsch, Englisch). Die Ergebnisse liegen in einer strukturierten JSON-Ausgabe vor, die Metadaten zu Entitäten und (optional) Beziehungen enthält. Beziehungen können als explizite (aus dem Text extrahierte) oder implizite (logisch geschlussfolgerte) Triple (Subjekt–Prädikat–Objekt) generiert und in interaktiven Knowledge Graphen visualisiert werden.

## Inhaltsverzeichnis

- [Installation](#installation)
- [Funktionen](#funktionen)
- [Anwendungsbeispiele](#anwendungsbeispiele)
- [Projektstruktur](#projektstruktur)
- [Architektur-Übersicht](#architektur-übersicht)
- [Funktionsweise](#funktionsweise)
- [Tipps und Best Practices](#tipps-und-best-practices)
- [Konfiguration](#konfiguration)
- [Ausgabestruktur](#ausgabestruktur)
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

- **Entitäten extrahieren**: Direkt aus Texten identifizieren (extrahieren).
- **Entitäten generieren**: Kontextbasiert neue Entitäten vorschlagen (generieren).
- **Beziehungsextraktion**: Explizite Beziehungen (Subjekt; Prädikat; Objekt) im Text erkennen.
- **Entitäteninferenz**: Implizite logische Knoten ergänzen und Knowledge Graph vervollständigen.
- **Beziehungsinferenz**: Implizite logische Verbindungen ergänzen und Knowledge Graph vervollständigen.
- **Knowledge Graph Completion (KGC)**: Fehlende Relationen in mehreren Runden automatisch generieren.
- **Graph-Visualisierung**: Erzeuge statische PNG-Graphen oder interaktive HTML-Ansichten.
- **Kompendium-Generierung**: Erstellung eines kompendialen (zusammenfassenden) Textes mit Referenzen (optional mit Optimierungen für Bildung).
- **Trainingsdaten-Generierung**: Speichere Entity- und Relationship-Daten als JSONL für OpenAI Fine-Tuning.
- **LLM-Schnittstelle**: Kompatibel mit OpenAI-API, anpassbare Basis-URL und Modell.
- **Wissensquellen-Integration**: Wikipedia, Wikidata, DBpedia (SPARQL + Lookup API Fallback).
- **Caching**: Zwischenspeicherung von API-Antworten für schnellere wiederholte Zugriffe.
- **Ratelimiter**: Fängt Fehler mit Ratelimits der Wissensquellen ab.
- **Statistiken**: Vorberechnete Statistiken u.a. zu gehäuft auftretenen Kategorien.

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
├── .pytest_cache/                    # pytest Cache-Verzeichnis
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

Die Entity Extractor Bibliothek basiert auf einer geschichteten Architektur:

```
┌─────────────────────────────────────────────────────────┐
│                      Öffentliche API                    │
│ entityextractor.api (extract_and_link_entities, etc.)  │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│                      API-Schicht                        │
│     core/api/extract.py, api/generate.py, etc.         │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│                 Prozess-Schicht                         │
│   core/process/orchestrator.py, deduplication.py, etc. │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│                 Basis-Implementierung                   │
│     core/extractor.py, core/generator.py, etc.         │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│                 Externe Dienste                         │
│     OpenAI API, Wikipedia, Wikidata, DBpedia           │
└─────────────────────────────────────────────────────────┘
```

### Erklärung der Architektur-Schichten

1. **Öffentliche API** (entityextractor.api)
   - Stellt einfache Funktionen für Endbenutzer bereit
   - Fokus auf intuitive Benutzerfreundlichkeit und Flexibilität

2. **API-Schicht** (core/api/*)
   - Integriert Basisfunktionalität mit zusätzlichen Prozessen
   - Bietet die Hauptimplementierungen für alle öffentlichen Funktionen

3. **Prozess-Schicht** (core/process/*)
   - Verarbeitet und optimiert die Ergebnisse (Deduplizierung, Formatierung, etc.)
   - Steuert den Workflow durch Orchestrator-Komponenten

4. **Basis-Implementierung** (core/*.py)
   - Grundlegende Funktionalität für Extraktion, Generierung, etc.
   - Fokus auf einzelne Funktionen ohne zusätzliche Verkettung

5. **Externe Dienste**
   - Integration mit OpenAI API für LLM-Anfragen
   - Anbindung an Wissensquellen (Wikipedia, Wikidata, DBpedia)

## Funktionsweise

Die Entity Extractor Bibliothek implementiert eine mehrschichtige Verarbeitungspipeline:

1. **Entitätsextraktionspipeline**: Aufbereitung von Texten, Chunking, Tokenanalyse und LLM-basierte Extraktion mit intelligenter Zusammenführung.
2. **Wikipedia-Integration**: Verknüpfung erkannter Entitäten mit Wikipedia-Artikeln und Extraktion von Zusammenfassungen.
3. **Wikidata-Integration**: Abruf von Wikidata-IDs, Typen und Beschreibungen.
4. **DBpedia-Integration**: Nutzung von DBpedia für zusätzliche strukturierte Informationen.
5. **Sprachübergreifende Verarbeitung**: Automatische Übersetzung und Suche in Deutsch und Englisch.
6. **Knowledge Graph Completion (KGC)**: Iterative Vervollständigung fehlender Relationen.

## Tipps und Best Practices

- Für große Texte aktiviere das Text-Chunking (`TEXT_CHUNKING=True`).
- Verwende die entsprechende Sprache für bessere Ergebnisse (`LANGUAGE="de"` oder `"en"`).
- Für detaillierte Entitätsinformationen aktiviere Wikidata und DBpedia.
- Experimentiere mit verschiedenen LLM-Modellen für unterschiedliche Anwendungsfälle.
- Setze `COLLECT_TRAINING_DATA=True`, um Trainingsdaten für Fine-Tuning zu sammeln.

## Konfiguration

Alle Einstellungen liegen in `entityextractor/config/settings.py` unter `DEFAULT_CONFIG`. Wichtige Optionen:

| Parameter                               | Typ                | Standardwert                                 | Beschreibung                                                                                          |
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
