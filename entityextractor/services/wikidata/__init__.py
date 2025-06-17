#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wikidata-Service-Modul für die Entitätsverknüpfung mit Wikidata.

Dieses Modul stellt Services für die Verknüpfung von Entitäten mit Wikidata-Einträgen bereit.
Es unterstützt Batch-Verarbeitung, asynchrone Anfragen und verschiedene Fallback-Mechanismen.
"""

from typing import Dict, Any, Optional

from entityextractor.services.wikidata.service import WikidataService, process_entities_strict_pipeline_wikidata


def get_wikidata_service(config: Optional[Dict[str, Any]] = None) -> WikidataService:
    """
    Factory-Funktion für den WikidataService.
    
    Args:
        config: Optionale Konfiguration für den WikidataService
        
    Returns:
        Eine Instanz des WikidataService
    """
    return WikidataService(config)


def get_batch_wikidata_service(config: Optional[Dict[str, Any]] = None):
    """
    Factory-Funktion für den BatchWikidataService.
    
    Diese Funktion erstellt eine Instanz des BatchWikidataService, der als Adapter
    für den WikidataService dient und die Batch-Verarbeitung von Entitäten ermöglicht.
    Der BatchWikidataService nutzt intern den WikidataService und dessen Fallback-Mechanismen,
    einschließlich der direkten Suche, Sprachfallbacks und Synonymfallbacks.
    
    Args:
        config: Optionale Konfiguration für den BatchWikidataService
        
    Returns:
        Eine Instanz des BatchWikidataService
    """
    # Import hier, um zirkuläre Importe zu vermeiden
    from entityextractor.services.wikidata.batch_service import BatchWikidataService
    return BatchWikidataService(config)
