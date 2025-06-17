#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minimal example of entity generation (topic based), linking with Wikipedia and results as raw JSON
"""

import sys
import os
import asyncio
import json

# Add the project root directory to sys.path
# This allows Python to find the 'entityextractor' module
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set stdout/stderr to UTF-8 on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception as e:
        print(f"Error reconfiguring stdout/stderr: {e}", file=sys.stderr)

from entityextractor.api import process_entities

# Sample text for entity extraction
SAMPLE_TEXT = """
Optik im Fach Physik.
"""

async def main():
    print("Starting entity extraction and linking...")
    
    # Prepare configuration
    config = {
        "LANGUAGE": "de",
        "MODE": "generate",
        "MAX_ENTITIES": 10,
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": True,   
        "USE_DBPEDIA": True,    
    }


    # Call the high-level API with configuration
    result = await process_entities(SAMPLE_TEXT, user_config=config)
     
    # Print the raw JSON output
    print("\n=== RAW JSON OUTPUT ===\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))
        
if __name__ == "__main__":
    asyncio.run(main())
