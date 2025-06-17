# EntityExtractorBatch – Prozessablauf im **Mode `extract`**

_Vollausbau mit Relationsextraktion, Compendium-Generierung, Question-Answering (QA) und Knowledge-Graph (KG)._  
(Stand: 2025-06-16)

---

## 1  Konfiguration laden

| Schlüssel                          | Bedeutung                                                                    |
|------------------------------------|-------------------------------------------------------------------------------|
| `MODE = "extract"`                | Aktiviert den Extraktions-Workflow.                                           |
| `LANGUAGE`                         | Primäre Inhaltssprache (z. B. `de`).                                          |
| `RELATION_EXTRACTION = True`       | Extrahiert Beziehungen zwischen Entitäten.                                    |
| `ENABLE_ENTITY_INFERENCE`          | Erkennt implizite Entitäten.                                                  |
| `ENABLE_RELATIONS_INFERENCE`       | Erkennt implizite Relationen.                                                 |
| `USE_WIKIPEDIA / DBPEDIA / WIKIDATA` | Aktiviert die jeweilige Verlinkung.                                          |
| `COMPENDIUM = True` (implizit)     | Erstellt einen narrativen Text.                                               |
| `QA = True` (implizit)             | Generiert Frage-Antwort-Paare.                                                |
| `KNOWLEDGE_GRAPH = True` (implizit)| Erzeugt RDF / Graph-Struktur.                                                |

Alle Einstellungen kommen aus `settings.DEFAULT_CONFIG` und können vom Benutzer überschrieben werden.

---

## 2  Text-Vorverarbeitung

1. **Chunking** *(optional)*  
   – Aktiviert durch `TEXT_CHUNKING`.  
   – Teilt den Text in überlappende Blöcke (`TEXT_CHUNK_SIZE`, `TEXT_CHUNK_OVERLAP`).
2. **LLM-Prompt** „Extract Entities“  
   – Liefert Roh-Entitäten (Name, Typ, ggf. Wikipedia-Links).

---

## 3  Entity-Kontext erstellen

Jede erkannte Entität wird in einem `EntityProcessingContext` gekapselt:

```text
entity_name
└── details            # Typ, inferred-Flag …
└── processing_data    # interne Zwischenergebnisse
└── service_data       # Ergebnisse externer Services (wikipedia, dbpedia …)
└── output_data        # Finale Daten (dbpedia, wikidata …)
```

---

## 4  Orchestrator – Batch-Verarbeitung

### 4.1  Wikipedia-Service

1. **Lazy Singleton** – wird bei Bedarf erstellt (respektiert `LANGUAGE`).
2. **Batch-Call** `process_entity_batch`:
   * Cache-Check → API-Fetch → Fallback-Kette (Langlinks, OpenSearch, BS4 …).
   * Speichert Ergebnis unter `sources.wikipedia`.
   * Englische Varianten in `labels['en']`, `label_en`, `url_alt` → unsichtbar für Endnutzer, aber für DBpedia nutzbar.

### 4.2  DBpedia-Service *(falls aktiviert)*

1. **Lazy Singleton** – Erst­initialisierung an dieser Stelle.
2. Nutzt englisches Label → konstruiert URI `http://dbpedia.org/resource/<Label>`.
3. **Batch-SPARQL (VALUES)** gegen `dbpedia.org/sparql` → Fallbacks: alternative Endpoints, Lookup-API.
4. **Strenge Linking-Kriterien**: URI + engl. Label + engl. Abstract ⇒ `status = linked`; sonst `not_found`.
5. Speichert Ergebnis unter `dbpedia`.

### 4.3  Wikidata-Service *(falls aktiviert)*

Abfrage der Wikidata-ID & Claims; Ergebnis unter `wikidata`.

### 4.4  Relationsextraktion

1. Prompt mit Entitätsliste an LLM.  
2. Optionale Inferenz-Schritte.  
3. Ergebnisse (`relations`) werden validiert & IDs zugeordnet.

---

## 5  Knowledge-Graph

Aus Entitäten + Relationen entsteht ein RDF/NetworkX-Graph.  
Export-Optionen: Turtle, JSON-LD, Bild (PNG/SVG).

---

## 6  Compendium-Service

1. Sammelt Entitäts-Extrakte & Referenzen.  
2. Baut prompt → LLM → narrativer Text.  
3. Sortiert Referenzen so, dass Links der eingestellten `LANGUAGE` zuerst erscheinen.

---

## 7  Question-Answering (QA-Service)

Erstellt automatisch QA-Paare aus Entitätsdaten oder generiert sie via LLM.  
Speichert unter `qa` → Liste aus `{question, answer, source_ids}`.

---

## 8  Zusammenführung & Ausgabe

Alle Kontexte werden serialisiert zu einem Gesamtergebnis-JSON:

```jsonc
{
  "entity": "Albert Einstein",
  "sources": {
    "wikipedia": { … },
    "dbpedia":   { … },
    "wikidata":  { … }
  },
  "relations": [ … ],
  "compendium": {
    "text": "…", "references": [ … ]
  },
  "qa": [ … ],
  "knowledge_graph": "@graphIRI"
}
```

---

## 9  Aufräumen

`EntityManager.close()` schließt nur die Dienste, die wirklich instanziiert wurden – Sessions werden sauber beendet, Caches geschrieben.

---

### Timing & Logging

* **INFO**: Start/Ende jeder Phase, Statistiken (Cache-Treffer, Erfolgsraten).  
* **DEBUG**: API-URLs, SPARQL-Queries, Fallbackgründe.  
* **WARNING/ERROR**: Netzwerk-Fehler, Validation-Problems.

---

## Quick-Reference

1. Text/Chunks → LLM (Entitäten)  
2. Wikipedia-Batch → deutsche Extrakte (+ engl. Label intern)  
3. DBpedia-Batch (engl. URI)  
4. Wikidata → IDs & Claims  
5. Relationen  
6. KG  
7. Compendium  
8. QA  
9. JSON-Export & Logging
