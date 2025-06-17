#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate German city entities and list their geo-coordinates.

This example asks the *entityextractor* to **generate** entities that are
cities located in Germany, shows the raw JSON result, and afterwards prints a
Markdown table with latitude/longitude data obtained from Wikipedia or
Wikidata.
"""

import sys
import os
import asyncio
import json
from typing import Dict, Any, List, Optional, Tuple

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Ensure proper UTF-8 output on Windows terminals
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from entityextractor.api import process_entities 

TOPIC = "Große Städte in Deutschland"  # LLM should generate German cities


def _extract_coords(source: Optional[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    """Return (lat, lon) if present in a source dict."""
    if not source:
        return None, None

    # Common key variants
    if "lat" in source and "lon" in source:
        return source["lat"], source["lon"]
    if "latitude" in source and "longitude" in source:
        return source["latitude"], source["longitude"]
    if "coordinates" in source and isinstance(source["coordinates"], dict):
        coords = source["coordinates"]
        lat = coords.get("lat") or coords.get("latitude")
        lon = coords.get("lon") or coords.get("longitude")
        return lat, lon
    return None, None


async def main() -> None:
    print(f"Generating city entities for topic: '{TOPIC}'\n")

    config: Dict[str, Any] = {
        "LANGUAGE": "de",  # German prompt
        "MODE": "generate",
        "MAX_ENTITIES": 10,
        "ALLOWED_ENTITY_TYPES": ["Location"],  # restrict to locations
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": True,
        "USE_DBPEDIA": False,
    }

    result = await process_entities(TOPIC, user_config=config)

    # Raw JSON output first
    print("\n### RAW JSON OUTPUT ###\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # Extract geo coordinates into table rows
    rows: List[List[str]] = []
    for ent in result.get("entities", []):
        city = ent.get("entity") or ent.get("name") or ent.get("entity_name")

        # Try Wikipedia first
        wp_lat, wp_lon = _extract_coords(
            (ent.get("sources", {}) or {}).get("wikipedia") or ent.get("wikipedia")
        )
        # Try Wikidata if missing
        if wp_lat is None:
            wd_lat, wd_lon = _extract_coords(
                (ent.get("sources", {}) or {}).get("wikidata") or ent.get("wikidata")
            )
        else:
            wd_lat = wd_lon = None

        lat = wp_lat if wp_lat is not None else wd_lat
        lon = wp_lon if wp_lon is not None else wd_lon

        rows.append([
            city or "-",
            f"{lat:.6f}" if lat is not None else "(n/a)",
            f"{lon:.6f}" if lon is not None else "(n/a)",
        ])

    # Print Markdown table
    if rows:
        print("\n### Geo coordinates\n")
        print("| City | Latitude | Longitude |")
        print("|------|----------|-----------|")
        for city, lat, lon in rows:
            print(f"| {city} | {lat} | {lon} |")
    else:
        print("No entities returned.")


if __name__ == "__main__":
    asyncio.run(main())
