# Projektstruktur

Dieses Dokument bietet einen vollständigen Überblick über den Aufbau des Repositories `entity-extractor-linker-batch` (Python ≥ 3.8).  Es ergänzt die Kurzfassung im `PROJECT_STRUCTURE.md` an der Wurzel des Projekts und beschreibt, welche Rolle jede wesentliche Datei bzw. jedes Verzeichnis spielt.

> Hinweis: **v1.2.0** führte die kontext-basierte Architektur ein. Wo sinnvoll, wird zwischen _Legacy_- und _Context_-Pfad unterschieden.

---

## Verzeichnisbaum (vereinfachte Übersicht)

```plaintext
.
├── docs/                          # Ausführliche Entwickler-Dokumentation (dieses Verzeichnis)
├── entityextractor/               # Haupt-Python-Package
│   ├── __init__.py
│   ├── api.py                     # Öffentliche High-Level-API (dictionary-basiert)
│   ├── main.py                    # Legacy Entry-Point (CLI Wrapper)
│   ├── main_new.py                # Neuer Entry-Point (Context-Pipeline)
│   ├── cache/                     # Persistenter Antwort-Cache für Services
│   ├── config/                    # Konfigurationsdateien & Defaults
│   ├── core/                      # Kernlogik (Extraktion, Generierung, Verarbeitung)
│   ├── prompts/                   # Prompt-Vorlagen für LLM-Aufrufe
│   ├── services/                  # Wissensquellen-Integrationen (Wikipedia, …)
│   ├── utils/                     # Allgemeine Hilfsfunktionen
│   └── visualization/             # Knowledge-Graph-Renderer
├── examples/                      # Minimal-Beispiele & Snippets
├── tests/                         # (künftig) automatisierte Tests
├── requirements.txt               # Produktionsabhängigkeiten
├── requirements-dev.txt           # Entwicklungs-/CI-Abhängigkeiten (optional)
├── setup.py                       # Packaging- und Installations-Metadaten
└── README.md                      # Projekt-Entry-Point für Anwender
```

---

## Wichtige Verzeichnisse & Dateien

### 1. `docs/`
Detaillierte Beschreibungen der Services, Datenmodelle und Architektur.

| Datei | Zweck |
|-------|-------|
| `WikipediaService.md` | Ablauf, Fallback-Strategien und Datenformat der Wikipedia-Integration |
| `WikidataService.md` | Wie Wikidata abgefragt und validiert wird |
| `DBpediaService.md` | SPARQL-Batching, Lookup-Fallbacks und Linking-Kriterien |
| `ContextArchitecture.md` | Motivation und Lebenszyklus des `EntityProcessingContext` |
| `DataModelsSchemas.md` | Pydantic-Modelle & Dataclasses für alle Knowledge-Sources |
| `ProjectStructure.md` | **Dieses Dokument** |

### 2. `entityextractor/`
Das Hauptpaket, aus dem ein `pip install -e .` ein Import-able Package macht.

#### a. `api.py`
Stellt die Funktionen `extract_and_link_entities`, `generate_and_link_entities`, `create_knowledge_compendium` und `process_entities` bereit. Intern werden die Core-Module eingebunden, sodass Anwender nur einen Import benötigen.

#### b. `core/`
Kernschicht, unterteilt in:

* **`core/context.py`** – Definiert `EntityProcessingContext`.
* **`core/api/`** – Thin-Wrapper um Core-Funktionen 👉 bietet Backwards-Kompatibilität zwischen Legacy- und Context-Aufrufen.
* **`core/process/`** – Orchestriert den End-to-End-Workflow (Deduplication, Result-Formatting, Statistik-Berechnung).

#### c. `services/`
Jedes Unterverzeichnis ist ein Micro-Service nach dem gleichen Muster (async, batch-fähig, kontextbasiert):

| Service | Dateien | Besondere Hinweise |
|---------|---------|--------------------|
| **Wikipedia** | `service.py`, `async_fetchers.py`, `fallbacks.py`, `formatters.py` | Ruft MediaWiki API ab, liefert Multilingual-Daten |
| **Wikidata**  | *analog* | Batcher über `/w/api.php?action=wbgetentities` |
| **DBpedia**   | *analog* | SPARQL-VALUES-Queries & Lookup API, strenge Linking-Kriterien |
| **openai_service.py** | Kapselt alle OpenAI-Aufrufe (+ Caching) |

Jeder Service erbt von `services/base_service.BaseService`, das einen `aiohttp.ClientSession` verwaltet.

#### d. `utils/`
Sammelbecken für wiederverwendbare Helfer (Logging, Async-Rate-Limiter, Text-Utils, Web-Utils …).

#### e. `visualization/`
Renderer für PNG (`png_renderer.py`) und interaktive HTML-Graphen (`html_renderer.py` via pyvis).

### 3. `config/`
`settings.py` enthält `DEFAULT_CONFIG` sowie eine `get_config`-Funktion, die User-Overrides mergen.

### 4. `prompts/`
Prompt-Vorlagen, versioniert separat, um das Model-Tuning zu vereinfachen.

### 5. `examples/`
Kleinste lauffähige Skripte zum schnellen Testen der Public-API.

---

## Build- & Packaging-Dateien

| Datei | Inhalt |
|-------|--------|
| `setup.py` | PyPI-Metadaten, Konsolen-Entry-Point `entityextractor` |
| `requirements.txt` | Minimale Runtime-Dependencies |
| `requirements-dev.txt` | Optionale Dev-/CI-Tools (pytest, ruff …) |

---

## Hinweise für neue Beiträge

1. **Dateigröße ≤ 500 Zeilen** pro Modul einhalten.  
2. **Asynchrone Batch-Verarbeitung** bevorzugen.  
3. **Strenge Typisierung**: Pydantic 2.x + Dataclasses verwenden.  
4. **Dokumentation**: Zu jedem neuen Modul ein kurzes Markdown im `docs/`-Ordner.

---

*Letzte Aktualisierung: 2025-06-15*
