#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""example_extract.py

Demonstriert die Extraktion + Verlinkung von ca. acht Entitäten aus einem Beispieltext
und gibt die Resultate als übersichtliche Tabellen aus:

1. Entitäten (Name, Typ, Inferred-Status, Wikipedia-URL, Wikidata-ID, DBpedia-URI)
2. Beziehungen / Triples (Subject, Predicate, Object, Inferred)
3. Statistiken (Schlüssel → Wert)

Das Skript benötigt **keine zusätzlichen Abhängigkeiten** (rein stdlib).
"""
import asyncio
import json
import os
import sys
from typing import Dict, Any, List
from loguru import logger

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_table(headers: List[str], rows: List[List[Any]], title: str) -> None:
    """Very small helper to print unicode tables without external deps."""
    if not rows:
        print(f"\n{title}: – (keine Daten)\n")
        return

    # column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(str(cell)))

    sep = "+".join("-" * (w + 2) for w in widths)
    def fmt_row(cols: List[Any]) -> str:
        return "|".join(f" {str(c)[:w].ljust(w)} " for c, w in zip(cols, widths))

    print(f"\n{title}:\n")
    print(sep)
    print(fmt_row(headers))
    print(sep)
    for r in rows:
        print(fmt_row(r))
    print(sep)

# ---------------------------------------------------------------------------
# Boilerplate to allow `python examples/example_extract.py` without install
# ---------------------------------------------------------------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from entityextractor.api import process_entities

# ---------------------------------------------------------------------------
# Example text (≈ 8 Entitäten)
# ---------------------------------------------------------------------------
SAMPLE_TEXT = (
    "Albert Einstein entwickelte 1905 in Bern die spezielle Relativitätstheorie. "
    "Sein berühmtes Manuskript wurde später in Berlin vorgestellt. "
    "Isaac Newton erforschte zuvor in Cambridge die Gravitation. "
    "Beide Physiker veränderten das Verständnis von Raum und Zeit."
)

CONFIG = {  # Debug disabled, INFO level only
    "LANGUAGE": "de",
    "MODE": "extract",
    "USE_WIKIPEDIA": True,
    "USE_WIKIDATA": True,
    "USE_DBPEDIA": True,
    "LOG_LEVEL": "INFO",
}

# ---------------------------------------------------------------------------
# Configure Loguru to show only INFO and above
logger.remove()
logger.add(sys.stderr, level="INFO")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    print("Starte Prozess …\n")
    result: Dict[str, Any] = await process_entities(SAMPLE_TEXT, CONFIG)

    # 1) Tabelle Entitäten
    entity_rows: List[List[str]] = []
    for ent in result.get("entities", []):
        sources = ent.get("sources", {})
        entity_rows.append([
            ent.get("entity", ""),
            ent.get("details", {}).get("typ", ""),
            ent.get("details", {}).get("inferred", ""),
            sources.get("wikipedia", {}).get("url", "–"),
            sources.get("wikidata", {}).get("id", "–"),
            sources.get("dbpedia", {}).get("uri")
            or ent.get("dbpedia", {}).get("uri", "–"),
        ])
    _print_table(
        ["Name", "Typ", "Inference", "Wikipedia", "Wikidata", "DBpedia"],
        entity_rows,
        "Entitäten"
    )

    # 2) Tabelle Beziehungen
    # Map entity IDs auf Namen & Typen
    id_to_name = {e.get("id", idx): e.get("entity") for idx, e in enumerate(result.get("entities", []))}
    id_to_type = {e.get("id", idx): e.get("details", {}).get("typ", "") for idx, e in enumerate(result.get("entities", []))}

    rel_rows: List[List[str]] = []
    for r in result.get("relationships", []):
        subj_id = r.get("subject")
        obj_id = r.get("object")
        rel_rows.append([
            id_to_name.get(subj_id, subj_id),
            id_to_type.get(subj_id, ""),
            r.get("predicate", ""),
            id_to_name.get(obj_id, obj_id),
            id_to_type.get(obj_id, ""),
            r.get("inferred", ""),
        ])
    _print_table(
        ["Subjekt", "Subjekt-Typ", "Prädikat", "Objekt", "Objekt-Typ", "Inference"],
        rel_rows,
        "Beziehungen / Triples"
    )

    print("\nFertig. \U0001F389")

if __name__ == "__main__":
    asyncio.run(main())
