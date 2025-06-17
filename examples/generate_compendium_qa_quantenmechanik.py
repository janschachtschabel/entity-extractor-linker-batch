#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate entities for *Quantenmechanik* and create
1) a detailed compendium text and
2) a set of QA pairs for didactic purposes.

Demonstrates usage of ``process_entities`` with compendium and QA generation
enabled.
"""

import sys
import os
import asyncio
import json
from typing import Dict, Any, List

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Ensure UTF-8 console on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from entityextractor.api import process_entities

TOPIC = "Quantenmechanik"


async def main() -> None:
    print(f"Generating entities, compendium and QA pairs for topic: '{TOPIC}'\n")

    config: Dict[str, Any] = {
        "LANGUAGE": "de",
        "MODE": "generate",
        "MAX_ENTITIES": 10,
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": False,
        "USE_DBPEDIA": False,
        # Compendium
        "ENABLE_COMPENDIUM": True,
        "COMPENDIUM_LENGTH": 8000,
        "COMPENDIUM_EDUCATIONAL_MODE": True,
        # QA Pairs
        "ENABLE_QA_PAIRS": True,      # QA-Pairs generell aktivieren/deaktivieren
        "QA_PAIR_COUNT": 10,
        "QA_PAIR_LENGTH": 220,
    }

    result = await process_entities(TOPIC, user_config=config)

    # Raw JSON for inspection (optional)
    print("\n### RAW JSON OUTPUT ###\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # Show compendium text
    compendium = result.get("compendium")
    if compendium:
        print("\n=== COMPENDIUM ===\n")
        print(compendium)

    # Show references
    refs: List[str] | None = result.get("references")
    if refs:
        print("\n=== REFERENCES ===\n")
        print("| Nr. | Quelle |")
        print("|-----|--------|")
        for ref in refs:
            if ref.startswith("(") and ")" in ref:
                num, url = ref.split(")", 1)
                num = num.strip("(")
                url = url.strip()
            else:
                num, url = "", ref
            print(f"| {num} | {url} |")

    # Show QA pairs
    qa_pairs: List[Dict[str, str]] | None = result.get("qa_pairs")
    if qa_pairs:
        print("\n=== QA PAIRS ===\n")
        for i, pair in enumerate(qa_pairs, 1):
            q = pair.get("question", "?")
            a = pair.get("answer", "-")
            print(f"{i}. Q: {q}\n   A: {a}\n")


if __name__ == "__main__":
    asyncio.run(main())
