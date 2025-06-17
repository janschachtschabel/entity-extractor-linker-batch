# EntityExtractorBatch – Prozessablauf im **Mode `generate`**

_Der „Generate“-Modus erstellt eigenständig Inhalte (Compendium-ähnliche Texte, QA, ggf. Knowledge-Graph), ohne dass ein Ausgangstext mitgeliefert werden muss._  
(Stand: 2025-06-16)

---

## 1  Konfiguration laden

| Schlüssel                              | Bedeutung                                                               |
|----------------------------------------|--------------------------------------------------------------------------|
| `MODE = "generate"`                  | Aktiviert den Generierungs-Workflow.                                      |
| `TOPIC` / `PROMPT`                     | Thema oder Fragestellung, aus der das System Informationen generiert.    |
| `MAX_ENTITIES`, `MAX_RELATIONS`        | Obergrenzen für Entity/Relation-Generierung.                             |
| `LANGUAGE`                             | Zielsprache für alle Ausgaben.                                           |
| `COMPENDIUM = True` (Standard)         | Erzwingt Text-Generierung.                                               |
| `QA = True` (optional)                 | Erstellt QA-Paare.                                                      |
| `KNOWLEDGE_GRAPH = True` (optional)    | Baut Knowledge-Graph aus den generierten Infos.                          |
| `USE_WIKIPEDIA / DBPEDIA / WIKIDATA`   | Externe Wissensquellen zur Validierung/Anreicherung.                     |

---

## 2  Themen-Analyse & Seed-Prompt

1. **Topic → Prompt**  
   – Aus dem `TOPIC` wird ein System-Prompt erstellt („Provide a structured overview …“).  
   – LLM liefert:  
     * Entitäten (Name, Typ, DE/EN Labels, Initiale Wikipedia-Links)  
     * Optionale Kern-Fragen  
     * Erste Beziehungen

2. **EntityProcessingContext** wird für jede gelieferte Entität angelegt (analog zum `extract`-Modus).

---

## 3  Orchestrator – Wissensanreicherung

### 3.1  Wikipedia-Service

* Läuft identisch wie im Extract-Modus, um verlässliche Primärdaten zu erhalten.

### 3.2  DBpedia- & Wikidata-Service (optional)

* Dienen zur Validierung und Anreicherung (Label EN, Abstract, Typen, Geo …)
* Strenge Linking-Kriterien bleiben erhalten.

### 3.3  Relation-Inferenz *(optional)*

* Wenn `RELATION_EXTRACTION` oder `ENABLE_RELATIONS_INFERENCE` gesetzt sind, wird ein zusätzlicher LLM-Prompt verwendet, um ein vollständiges Beziehungsnetz basierend auf den angereicherten Entitäten zu erzeugen.

---

## 4  Wissenskonstrukte erzeugen

### 4.1  Compendium-Service

1. **Outline-Prompt** (LLM): Gliedert das Thema in sinnvolle Abschnitte.  
2. **Section-Prompts**: Für jede Sektion wird ein kurzer Text mit Zitieren der Wikipedia-Extrakte generiert.  
3. **Referenz-Liste** wird aufgebaut (sortiert nach `LANGUAGE`).

### 4.2  Question-Answering-Service *(QA)*

* Nutzt Entitäten + Compendium als Kontext, um n Fragen zu generieren (`MAX_QA`), inklusive kurzer präziser Antworten.

### 4.3  Knowledge-Graph *(KG)*

* Baut RDF/JSON-LD-Graph aus Entitäten & Relationen.  
* Optionale Visualisierung (Graphviz / vis.js).

---

## 5  Ausgabeformat

```jsonc
{
  "topic": "Quantenmechanik – Grundlagen und Anwendungen",
  "compendium": { "text": "…", "references": [ … ] },
  "entities": [ { "entity": "Superposition", "sources": { … } }, … ],
  "relations": [ … ],
  "qa": [ { "question": "…", "answer": "…" } ],
  "knowledge_graph": "@graphIRI"
}
```

---

## 6  Cache & Sessions

* Wikipedia/DBpedia/Wikidata-Cache werden normal genutzt.  
* Lazy-Instanzierung der Services sorgt für minimale Start-Zeit.

---

## 7  Logging & Monitoring

* **INFO**: Generierungs-Start, erzeugte Entitäten/Relationen, LLM-Token-Verbrauch.  
* **DEBUG**: Prompt-Texte, API-Antworten, SPARQL-Queries.  
* **WARNING/ERROR**: Fallback-Gründe, fehlende Daten, Zeitüberschreitungen.
