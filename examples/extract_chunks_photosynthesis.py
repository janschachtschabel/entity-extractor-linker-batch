"""Example script: Extraction with *automatic* chunking on a large text input.

This example demonstrates how to use the Entity Extractor in *extract* mode
using the built-in automatic chunking.  A ~4 000-character German text about
Photosynthese (photosynthesis) is split into chunks of roughly 1 500
characters that overlap by 100 characters to preserve context at the chunk
boundaries.  Each chunk is processed individually and the extracted entities
are printed.

Run the script via:

    python examples/extract_chunks_photosynthesis.py

Make sure the environment variable OPENAI_API_KEY is set or provide the key
via the configuration dictionary.
"""

import os
import sys
import asyncio
import json

# Add project root to PYTHONPATH so that "import entityextractor" works
# even when this script is run from the "examples" directory directly.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from entityextractor.api import process_entities



# ---------------------------------------------------------------------------
# 1. Large input text (≈4 000 characters)
# ---------------------------------------------------------------------------

TEXT_DE = (
    "Photosynthese ist der fundamentale biologische Prozess, durch den grüne "
    "Pflanzen, Algen und bestimmte Bakterien Lichtenergie nutzen, um aus "
    "Kohlendioxid und Wasser energiereiche Kohlenhydrate aufzubauen. Die "
    "grundlegende Reaktionsgleichung lautet: 6 CO₂ + 6 H₂O → C₆H₁₂O₆ + 6 O₂. "
    "Dabei wird Lichtenergie in chemische Energie umgewandelt, die in den "
    "Bindungen von Glukose gespeichert wird. Die Photosynthese findet in den "
    "Chloroplasten der Pflanzenzellen statt, genauer gesagt in den Thylakoid-"
    "membranen, wo die lichtabhängigen Reaktionen ablaufen, sowie im Stroma, "
    "wo die lichtunabhängigen, auch Calvin-Zyklus genannten Reaktionen "
    "durchgeführt werden. In den lichtabhängigen Reaktionen wird mithilfe des "
    "Pigmentes Chlorophyll Licht absorbiert, Wasser gespalten und Sauerstoff "
    "freigesetzt. Gleichzeitig entstehen ATP und NADPH als kurzfristige "
    "Energiespeicher. Der Calvin-Zyklus nutzt dann ATP und NADPH, um CO₂ zu "
    "Glukose zu reduzieren. Ein Schlüsselenzym des Calvin-Zyklus ist RuBisCO, "
    "das CO₂ an Ribulose-1,5-bisphosphat bindet. Trotz seiner zentralen Rolle "
    "ist RuBisCO relativ ineffizient und kann fälschlicherweise auch O₂ "
    "fixieren, was zur Photorespiration führt und die Nettoeffizienz der "
    "Photosynthese senkt. Pflanzen haben unterschiedlich angepasste Stoffwechsel-"
    "wege wie den C₄-Weg oder die CAM-Photosynthese entwickelt, um dieses "
    "Problem in heißen bzw. trockenen Umgebungen zu umgehen.\n\n"

    "Neben der Bedeutung für die Pflanzen selbst ist die Photosynthese von "
    "globaler Relevanz: Sie setzt den Großteil des atmosphärischen Sauerstoffs "
    "frei und bildet die Grundlage nahezu aller Nahrungsnetze. Die dabei "
    "aufgebaute Biomasse dient als primäre Energiequelle für heterotrophe "
    "Organismen. Historisch betrachtet veränderte die Anreicherung von "
    "Sauerstoff durch photosynthetische Cyanobakterien bereits in der "
    "frühen Erdgeschichte die Atmosphäre drastisch, was als Große "
    "Oxidationsereignis bekannt ist.\n\n"

    "Die Effizienz der Photosynthese wird von verschiedenen Faktoren beeinflusst. "
    "Dazu zählen Lichtintensität, Lichtqualität, Temperatur, Verfügbarkeit von "
    "Wasser und CO₂ sowie die Nährstoffversorgung, insbesondere mit Stickstoff "
    "und Magnesium, die Bestandteile des Chlorophylls sind. Forschung in den "
    "Biowissenschaften und der Agrarindustrie versucht, die Photosynthese zu "
    "optimieren, um die landwirtschaftlichen Erträge zu steigern und zugleich "
    "den steigenden CO₂-Konzentrationen in der Atmosphäre entgegenzuwirken. "
    "Ansätze reichen von gentechnischer Modifikation der RuBisCO-Aktivität bis "
    "zum Einsatz künstlicher Lichtquellen in vertikalen Farmen.\n\n"

    "Darüber hinaus inspiriert die Photosynthese auch technische Entwicklungen. "
    "Im Feld der künstlichen Photosynthese werden Materialsysteme erforscht, die "
    "ähnlich wie Chloroplasten Lichtenergie in chemische Energie umwandeln "
    "können, beispielsweise zur Wasserstoffgewinnung. Solche Technologien "
    "könnten langfristig helfen, erneuerbare Energieträger bereitzustellen und "
    "fossile Brennstoffe zu ersetzen.\n\n"

    "Zusammenfassend ist die Photosynthese nicht nur ein essentieller Prozess "
    "für das Pflanzenwachstum, sondern auch ein Eckpfeiler des globalen "
    "Kohlenstoffkreislaufs und ergo des Klimas. Ein tiefes Verständnis ihrer "
    "Mechanismen ermöglicht es, Lösungen für Ernährungssicherheit, "
    "Klimawandel und Energieversorgung zu entwickeln."
)


# ---------------------------------------------------------------------------
# 2. Configuration – enable extract mode (default) and disable extras
# ---------------------------------------------------------------------------

CONFIG = {
    "MODE": "extract",            # Explicit for clarity
    "LANGUAGE": "de",             # Language of the input text
    "ENABLE_COMPENDIUM": False,    # Disable generation extras
    "QA_PAIR_COUNT": 0             # Skip QA generation for speed
}


# ---------------------------------------------------------------------------
# 3. Run extraction (automatic chunking handled inside process_entities)
# ---------------------------------------------------------------------------

async def main():
    result = await process_entities(TEXT_DE, user_config={
        **CONFIG,
        "TEXT_CHUNKING": True,        # Enable automatic chunking
        "TEXT_CHUNK_SIZE": 1500,
        "TEXT_CHUNK_OVERLAP": 100,
    })
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
