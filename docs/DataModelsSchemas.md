# Datenmodelle & Schemas

*Stand: 2025-06-15*

Alle strukturierten Daten im Projekt basieren auf **Pydantic**-Modellen (API-Layer) und leichten Dataclasses (intern). Diese Datei liefert einen Überblick über die wichtigsten Klassen in `entityextractor/models/` und ihre Rolle in der Pipeline.

---
## 1. Modell-Landschaft

| Datei | Kernklassen | Einsatzzweck |
|-------|-------------|--------------|
| `base.py` | `LanguageCode`, `LocalizedText`, `SourceData` | Grundbausteine: mehrsprachige Strings und flexible Quell-Datenstruktur mit Dict-Semantik. |
| `data_models.py` | `WikipediaLanguageData`, `WikipediaMultilangData`, `WikidataProperty`, `WikidataData`, `DBpediaData`, `EntityData` | Validierte Pydantic-Schemas für Service-Outputs und die Haupt-Entität. |
| `entity.py` | `Entity` (Domain-Objekt) | Höher-level API für Geschäftslogik (z. B. Merge, Vergleich). |
| `relationship.py` | `Relationship` | Repräsentiert Triples (subject-predicate-object) für Knowledge Graph. |

---
## 2. Grundlagen aus `base.py`

### 2.1 `LanguageCode` (Enum)
Standardisierte ISO-Sprachcodes (`EN`, `DE`, …) ➔ Hilft, Tippfehler zu vermeiden.

### 2.2 `LocalizedText`
```python
a = LocalizedText(en="Hello", de="Hallo")
print(a.get("en"))  # "Hello"
```
* Vorteile: einheitliche Methode `to_dict()` erzeugt `{ "en": "Hello", "de": "Hallo" }`.

### 2.3 `SourceData`
Flexibler Container für Daten aus **einer** Quelle (z. B. Wikipedia). Unterstützt Dict-Syntax **und** Attribute:
```python
s = SourceData(id="123", url="https://…")
s["label"] = "Albert Einstein"
print(s.label)  # → "Albert Einstein"
```

---
## 3. Service-Schemas in `data_models.py`

### 3.1 Wikipedia
* **`WikipediaLanguageData`** – Label, URL, Description *für eine Sprache*.
* **`WikipediaMultilangData`** – Bündelt bis zu 10 `WikipediaLanguageData`-Einträge (`en`, `de`, `fr`, …).

### 3.2 Wikidata
* **`WikidataProperty`** – Wert + Typ + optionale Qualifier.  
* **`WikidataData`** – Enthält `label`/`description` (mehrsprachig), `aliases` und `claims` (`dict[pid] → list[WikidataProperty]`).

### 3.3 DBpedia
* **`DBpediaData`** – URI, multiling. Label & Abstract, Listen von `types`, `categories`, `has_part`, `part_of`, Geo-Koordinaten (lat / lon) u. a. Enthält außerdem `status` & `error` für Kontrollfluss.

### 3.4 Entität als Ganzes
* **`EntityData`** – Aggregiert alle obigen Ergebnisse + Metafelder (`entity_id`, `entity_type`, `language`).  
*Ersetzt nicht* den Runtime-`EntityProcessingContext`, bietet aber schlanke Validation bei API-Ein/Ausgabe.

---
## 4. Beziehungsschema

`relationship.py` definiert eine kleine Pydantic-Klasse
```python
class Relationship(BaseModel):
    subject: str  # URI / ID
    predicate: str  # P-URI
    object: str  # URI / Literal
    source: Optional[str]
    confidence: float = Field(ge=0, le=1)
```
Das ermöglicht spätere RDF- oder KG-Exports.

---
## 5. Best Practices für Entwickler:innen

1. **Service-Outputs**: Gebe niemals rohe JSON zurück. Verwende das passende Modell (`DBpediaData`, …) und rufe `.dict(exclude_none=True)` auf.
2. **Mehrsprachigkeit**: Speichere immer `en` + Originalsprache, damit Downstream (DBpedia) mit Englisch arbeiten kann.
3. **Fehlermanagement**: Nutze `status` & `error`-Felder der Modelle, statt separate Flags im Code zu verteilen.
4. **Forward-Kompatibilität**: Füge neue Felder als `Optional[...] = None` hinzu – breaking-changes vermeiden.

---
## 6. Schema-Validierung vs. Laufzeit-Flexibilität

* **Pydantic** sorgt beim Einlesen/Schreiben externer Daten für **harten** Check.  
* Der **`EntityProcessingContext`** (Runtime) bleibt bewusst flexibler; Services können schrittweise Daten beifügen, bevor sie endgültig in ein Modell gegossen werden (z. B. beim JSON-Export).

---
**Maintainer-Kontakt**: winds…@example.com
