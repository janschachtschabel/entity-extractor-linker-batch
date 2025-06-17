#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minimal English example that *generates* entities for a topic and links them
using the high-level ``process_entities`` API. Mirrors the structure of
*minimal_generate.py*.
"""

import sys
import os
import asyncio
import json

# Make sure the local package is importable when running this script directly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Windows console Unicode hygiene
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception as exc:
        print(f"Error reconfiguring stdout/stderr: {exc}", file=sys.stderr)

from entityextractor.api import process_entities

# Topic for which entities should be *generated* (not extracted)
TOPIC = "Optics in physics"


async def main():
    print("Starting English entity generation and linking…")

    # Configuration: generation mode in English. All other settings fall back
    # to defaults in *settings.py*.
    config = {
        "LANGUAGE": "en",
        "MODE": "generate",
        "MAX_ENTITIES": 10,
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": True,
        "USE_DBPEDIA": True,
    }

    result = await process_entities(TOPIC, user_config=config)

    print("\n=== RAW JSON OUTPUT ===\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
