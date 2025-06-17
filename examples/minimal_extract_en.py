#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minimal English example of entity extraction and linking (Wikipedia default) with
raw JSON output. Mirrors the structure of *minimal_extract.py*.
"""

import sys
import os
import asyncio
import json

# Add the project root directory to sys.path so that Python can import
# the local *entityextractor* package when this script is executed
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Ensure UTF-8 stdout/stderr on Windows so that Unicode in logs / output is not
# mangled when running in the default Windows console.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception as exc:
        print(f"Error reconfiguring stdout/stderr: {exc}", file=sys.stderr)

from entityextractor.api import process_entities

# Sample English text for extraction. Feel free to edit.
SAMPLE_TEXT = (
    "Mount Everest is Earth's highest mountain above sea level, located in the "
    "Mahalangur Himal sub-range of the Himalayas. The international border "
    "between Nepal and the Tibet Autonomous Region of China runs across its "
    "summit."
)


async def main():
    print("Starting English entity extraction and linking…")

    # Basic configuration: English language. Everything else falls back to
    # defaults from *settings.py* (LOG_LEVEL, caching, etc.).
    config = {
        "LANGUAGE": "en",
        "MODE": "extract",
        "USE_WIKIPEDIA": True,  # Wikipedia linking is always on by default
        # Enable other sources as desired – uncomment if needed
        "USE_WIKIDATA": True,
        "USE_DBPEDIA": True,
    }

    result = await process_entities(SAMPLE_TEXT, user_config=config)

    print("\n=== RAW JSON OUTPUT ===\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
