"""Example script: Extraction with entity & relation inference enabled.

This example demonstrates how to run the entity-extraction pipeline in
*extract* mode on a large German text while
  • automatically chunking the input text,
  • inferring implicit entities (ENABLE_ENTITY_INFERENCE), and
  • extracting + inferring relationships between entities (RELATION_EXTRACTION
    + ENABLE_RELATIONS_INFERENCE).

Run via:
    python examples/extract_inference_photosynthesis.py

Prerequisite: environment variable OPENAI_API_KEY must be set or passed via
configuration.
"""

import os
import sys
import asyncio
import json

# Ensure project root is on PYTHONPATH when executed directly from examples/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from entityextractor.api import process_entities

# ---------------------------------------------------------------------------
# Input text (~400 characters) with ~7–8 entities
# ---------------------------------------------------------------------------

TEXT_DE = (
    "Albert Einstein entwickelte die Relativitätstheorie in Bern, "
    "während Marie Curie in Paris Pionierarbeit zur Radioaktivität leistete. "
    "Die NASA landete Apollo 11 mit Neil Armstrong 1969 auf dem Mond, "
    "während SpaceX mit der Rakete Falcon 9 in Cape Canaveral neue Maßstäbe setzte. "
    "Zeitgleich betreibt die ESA das Weltraumteleskop Gaia, das Milliarden Sterne der Milchstraße kartiert."
)

# ---------------------------------------------------------------------------
# Configuration – enable inference features
# ---------------------------------------------------------------------------

CONFIG = {
    "MODE": "extract",
    "LANGUAGE": "de",
    "ENABLE_ENTITY_INFERENCE": False,       # implicit entities
    "RELATION_EXTRACTION": True,          # explicit relation extraction
    "ENABLE_RELATIONS_INFERENCE": True,   # implicit relation inference
    "STATISTICS_DEDUPLICATE_RELATIONSHIPS": True,  # Bei Statistiken Beziehungen deduplizieren
    "ENABLE_GRAPH_VISUALIZATION": True,
    # === TRAINING DATA COLLECTION SETTINGS ===
    "COLLECT_TRAINING_DATA": False,  # Trainingsdaten für Fine-Tuning sammeln
}

# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

async def main():
    result = await process_entities(TEXT_DE, user_config=CONFIG)
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
