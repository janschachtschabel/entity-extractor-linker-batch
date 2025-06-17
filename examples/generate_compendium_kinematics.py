#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate ten entities related to *Kinematik in der Physik* and assemble a
short compendium text based solely on the Wikipedia extracts returned by the
pipeline.

The script demonstrates how to
1. call ``process_entities`` in *generate* mode,
2. limit the number of entities, and
3. post-process the results into a human-readable compendium paragraph.
"""

import sys
import os
import asyncio
import json
from typing import Dict, Any, List

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


from entityextractor.api import process_entities 

TOPIC = "Kinematik in der Physik"


async def main() -> None:
    print(f"Generating entities for topic: '{TOPIC}'\n")

    config: Dict[str, Any] = {
        "LANGUAGE": "de",
        "MODE": "generate",
        "MAX_ENTITIES": 10,
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": False,
        "USE_DBPEDIA": False,
        "ENABLE_COMPENDIUM": True,            # Kompendium-Generierung aktivieren
        "COMPENDIUM_LENGTH": 8000,            # Anzahl der Zeichen für das Kompendium (ca. 4 A4-Seiten)
        "COMPENDIUM_EDUCATIONAL_MODE": True,  # Bildungsmodus für Kompendium aktivieren
    }

    result = await process_entities(TOPIC, user_config=config)

    # Raw JSON for inspection (optional)
    print("\n### RAW JSON OUTPUT ###\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # Show compendium returned by the service
    if result.get("compendium"):
        print("\n=== COMPENDIUM ===\n")
        print(result["compendium"])

    # Print references as a simple Markdown table 
    refs: List[str] | None = result.get("references")
    if refs:
        print("\n=== REFERENCES ===\n")
        # Build table header
        print("| Nr. | Quelle |")
        print("|-----|--------|")
        for ref in refs:
            # Each ref looks like "(1) https://..." – split into number and URL
            if ref.startswith("(") and ")" in ref:
                num, url = ref.split(")", 1)
                num = num.strip("(")
                url = url.strip()
            else:
                num, url = "", ref
            print(f"| {num} | {url} |")


if __name__ == "__main__":
    asyncio.run(main())
