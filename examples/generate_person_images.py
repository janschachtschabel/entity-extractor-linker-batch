#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example: Generate persons for a topic and list their portrait images.

This script asks the *entityextractor* to **generate** entities for a topic
where people are expected (here: *Nobel Prize in Physics*).  It restricts the
LLM to the entity type *Person* and, after linking, prints a simple Markdown
 table that shows the image URL found either in Wikipedia or Wikidata.

Run with:
    python examples/generate_person_images.py
"""

import sys
import os
import asyncio
import json
from typing import Dict, Any, List

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from entityextractor.api import process_entities  # noqa: E402

TOPIC = "Nobel Prize in Physics"  # very likely to yield persons


async def main() -> None:
    print(f"Generating person entities for topic: '{TOPIC}'\n")

    config: Dict[str, Any] = {
        "LANGUAGE": "en",
        "MODE": "generate",
        "MAX_ENTITIES": 10,
        "ALLOWED_ENTITY_TYPES": ["Person"],  # only persons
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": True,
        "USE_DBPEDIA": False,  # images come mainly from WP/Wikidata, skip to speed up
    }

    result = await process_entities(TOPIC, user_config=config)

    print("\n### RAW JSON OUTPUT ###\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # Collect image URLs per entity
    rows: List[List[str]] = []
    for ent in result.get("entities", []):
        # Entity name can appear under different keys depending on mode
        name = ent.get("entity") or ent.get("name") or ent.get("entity_name")

        # Wikipedia image
        wp_img = None
        wp = ent.get("sources", {}).get("wikipedia") or ent.get("wikipedia")
        if wp:
            wp_img = (
                wp.get("image_url")
                or wp.get("image")
                or wp.get("thumbnail")
            )

        # Wikidata image
        wd_img = None
        wd = ent.get("sources", {}).get("wikidata") or ent.get("wikidata")
        if wd:
            wd_img = wd.get("image") or wd.get("image_url")

        rows.append([
            name or "-",
            wp_img or "(n/a)",
            wd_img or "(n/a)",
        ])

    # Print Markdown table
    if rows:
        print("\n### Images found\n")
        print("| Person | Wikipedia Image | Wikidata Image |")
        print("|--------|-----------------|---------------|")
        for person, wp_url, wd_url in rows:
            print(f"| {person} | {wp_url} | {wd_url} |")
    else:
        print("No entities returned.")


if __name__ == "__main__":
    asyncio.run(main())
