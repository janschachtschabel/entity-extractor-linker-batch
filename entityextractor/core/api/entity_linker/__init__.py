"""
entity_linker/__init__.py

Exportiert die Hauptfunktionen für das Entity-Linking.
"""

from entityextractor.core.api.entity_linker.main import link_entities
from entityextractor.core.api.entity_linker.wikidata import (
    extract_wikidata_ids,
    fetch_wikidata_details,
    link_with_wikidata
)

__all__ = [
    'link_entities',
    'extract_wikidata_ids',
    'fetch_wikidata_details',
    'link_with_wikidata'
]
