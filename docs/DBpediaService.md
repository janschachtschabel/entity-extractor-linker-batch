# DBpediaService – Funktionsweise & Fallback-Strategien

*Stand: 2025-06-15*

Dieses Dokument erläutert den **DBpediaService** des Projekts *entityextractorbatch*: Aufbau, Datenfluss, Fallbacks, Konfiguration und Best-Practices. Es richtet sich an Entwickler:innen, die den Service anpassen oder Fehler debuggen wollen.

---
## 1. Modulübersicht

| Datei | Zweck (max. 500–600 Zeilen) |
|-------|-----------------------------|
| `service.py` | Zentrale Klasse `DBpediaService`: orchestriert Cache-Check, SPARQL-Batch, Fallbacks, Statistik. |
| `async_fetchers.py` | Asynchrone Low-Level-Loader (SPARQL GET/JSON, Lookup API). |
| `async_sparql.py` | Hilfsroutinen zum Erstellen & Abfeuern von SPARQL-Queries. |
| `formatters.py` | Wandelt rohe SPARQL/Lookup-Antworten in das Standard-Schema. Enthält `DBpediaData`-Klasse mit `is_valid()` & `to_dict()`. |
| `fallbacks.py` | Alternative Strategien (Sprache, alternativer Endpoint, DBpedia Lookup API). |
| `utils.py` | Diverse Helfer (URI-Normalisierung, Language Mapping, Timeout Handling). |
| `batch_service.py` | Utilities für größere Pipeline-Runs (derzeit sekundär). |

Alle Teilmodule verwenden **`asyncio` + `aiohttp`**, gemeinsame Session und Info/Debug-Logging via `loguru`.

---
## 2. End-to-End-Datenfluss

```mermaid
graph TD;
    A[EntityProcessingContext] -->|process_entity| B(DBpediaService)
    B --> C{Cache-Hit?}
    C -- Ja --> X[add_service_data]
    C -- Nein --> D[Batch SPARQL (English URI)]
    D -->|valid| X
    D -- invalid / none --> E[Fallback-Pipeline]
    E -->|Erfolg| X
```

1. **Cache**: JSON-Einträge basierend auf `dbpedia_uri` werden unter `CACHE_DIR/dbpedia/` gehalten.
2. **Batch-SPARQL** (primärer Weg):  
   * URIs werden **immer englisch** generiert (`http://dbpedia.org/resource/{English_Title}`), siehe Memory-Anforderung.
   * Query nutzt `VALUES`-Klausel für effiziente Mehrfach-Abfragen (siehe `async_sparql.py`).
   * Antwort wird von `formatters.py` in `DBpediaData` überführt.
3. **Validierung** (`DBpediaData.is_valid()`): Objekt ist nur dann _linked_, wenn **URI, englisches Label und englischer Abstract** vorhanden sind.
4. **Fallback-Kette** (falls SPARQL kein valides Ergebnis liefert):
   1. **Language Endpoint Fallback** – testet alternativen DBpedia-Endpoint (de.dbpedia.org), **falls `DBPEDIA_USE_DE=True`**. (Standard: deaktiviert)
   2. **Alternative SPARQL-Endpoint** – schwenkt von HTTPS→HTTP oder auf Mirror.
   3. **DBpedia Lookup API** – Keyword-Suche, danach Einzel-Fetch der Entity.

Jeder Schritt wird übersprungen, wenn er bereits zuvor fehlgeschlagen ist (keine endlosen Wiederholungen).

---
## 3. Datenstruktur (`sources["dbpedia"]`)

```jsonc
"sources": {
  "dbpedia": {
    "status": "linked",            // oder "not linked"
    "uri": "http://dbpedia.org/resource/Zugspitze",
    "label": "Zugspitze",
    "abstract": "The Zugspitze is the highest mountain in Germany…", // EN
    "types": ["dbo:Mountain", "schema:Place"],
    "categories": ["Mountains of Bavaria", …],
    "part_of": ["Alps"],
    "has_parts": [],
    "latitude": 47.421,
    "longitude": 10.985,
    "wikiPage": "https://en.wikipedia.org/wiki/Zugspitze",
    "image": "http://commons.wikimedia.org/…/Zugspitze.jpg",
    "fallback_source": "lookup_api",   // optional
    "fallback_attempts": 1              // optional
  }
}
```
*Kein* doppeltes Nesting, kompatibel mit Statistics-Modul.

---
## 4. Fallback-Strategien

| Nr. | Modul-Funktion | Idee | Abbruch-Kriterium / Erfolg |
|----|----------------|------|---------------------------|
| 1 | `apply_language_fallback` | Query an deutschem Endpoint, wenn `DBPEDIA_USE_DE=True`. | Valider `DBpediaData`. |
| 2 | `apply_alt_endpoint_fallback` | SPARQL-Mirror (z. B. `https://dbpedia.org/sparql` → `http://dbpedia.org/sparql`). | s. o. |
| 3 | `apply_lookup_fallback` | DBpedia Lookup API → anschließend Details-Fetch. | s. o. |

Jede Funktion annotiert `fallback_source` und erhöht `fallback_attempts`.

---
## 5. Konfig-Schlüssel (settings.py)

| Schlüssel | Bedeutung | Default |
|-----------|-----------|---------|
| `DBPEDIA_USE_FALLBACKS` | Fallback-Kette aktiv? | `True` |
| `DBPEDIA_USE_DE` | Deutschen Endpoint zulassen? | `False` |
| `DBPEDIA_BATCH_SIZE` | SPARQL-Batch-Größe | 15 |
| `DBPEDIA_TIMEOUT` | Timeout je Request (Sek.) | 30 |
| `DBPEDIA_LOOKUP_API` | Basis-URL Lookup API | `https://lookup.dbpedia.org/api/search` |
| `USER_AGENT` | HTTP Header | `EntityExtractor/1.0` |

---
## 6. Logging & Statistik

`loguru` mit Service-Tag `dbpedia`:
* INFO: Cache-Hits, #URIs pro Batch, valid/invalid, Fallback-Erfolg.  
* DEBUG: Vollständige SPARQL-Query, JSON-Rückgaben (gekürzt), Retry-Details.

`DBpediaService.get_statistics()` liefert z. B.:
```jsonc
{
  "linked_entities": 42,
  "failed_entities": 5,
  "api_calls": {"sparql": 10, "lookup": 3},
  "fallback_usage": {"language": 1, "alt_endpoint": 2, "lookup_api": 3}
}
```

---
## 7. Erweiterung & Best-Practices

1. **Neue SPARQL-Felder**: Query in `async_sparql.py` anpassen, dann `formatters.py` ergänzen.  
2. **Zusätzlicher Fallback**: Neue async-Funktion in `fallbacks.py`; in `service.py` Kette einfügen.  
3. **Strict Linking**: Beibehalten! – Änderungen immer in `DBpediaData.is_valid()` spiegeln.  
4. **Performance**: Prüfen Sie API-Limit bevor `DBPEDIA_BATCH_SIZE` erhöhen. Session-Reuse ist bereits implementiert.  
5. **Timeouts**: Siehe Memory 149bbb32 → 30 s empfohlen.

---
## 8. Quick-Start

```python
from entityextractor.services.dbpedia.service import DBpediaService
from entityextractor.core.context import EntityProcessingContext

service = DBpediaService()
ctx = EntityProcessingContext(entity_name="Zugspitze")

import asyncio
asyncio.run(service.process_entity(ctx))

print(ctx.output_data["sources"]["dbpedia"])  # → formatiertes Dict
```

---
**Maintainer-Kontakt**: winds…@example.com
