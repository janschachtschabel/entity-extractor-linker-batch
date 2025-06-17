#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BatchDBpediaService - Direkte Implementierung für die Batch-Verarbeitung mit dem DBpedia-Service.

Diese Klasse ersetzt den alten Wrapper (batch_dbpedia_service.py) und bietet
die gleiche Funktionalität direkt im DBpedia-Service-Modul.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional, Union

from entityextractor.models.entity import Entity
from entityextractor.core.context import EntityProcessingContext
from entityextractor.config.settings import get_config
from entityextractor.utils.logging_utils import get_service_logger
from entityextractor.services.dbpedia.service import DBpediaService

# Logger konfigurieren
logger = get_service_logger(__name__, 'dbpedia')

class BatchDBpediaService:
    """
    BatchDBpediaService - Service für die Batch-Verarbeitung von Entitäten mit DBpedia.
    
    Diese Klasse dient als Schnittstelle für die Batch-Verarbeitung und nutzt
    intern den DBpediaService für die eigentliche Verarbeitung.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialisiert den BatchDBpediaService.
        
        Args:
            config: Optionale Konfiguration, die an den Hauptservice weitergegeben wird.
        """
        self.config = config or get_config()
        self.logger = logger
        self._dbpedia_service = None
        
    @property
    def dbpedia_service(self):
        """Lazy-loading für den DBpediaService."""
        if self._dbpedia_service is None:
            self._dbpedia_service = DBpediaService(self.config)
        return self._dbpedia_service
    
    def get_stats(self) -> Dict[str, int]:
        """
        Gibt die aktuellen Verarbeitungsstatistiken zurück.
        
        Returns:
            Ein Dictionary mit den Statistiken
        """
        if hasattr(self, '_dbpedia_service') and self._dbpedia_service is not None:
            return getattr(self._dbpedia_service, 'stats', {})
        return {}
    
    async def enrich_entities(self, entities: List[Entity]) -> List[Entity]:
        """
        Reichert eine Liste von Entitäten mit DBpedia-Daten an.
        
        Args:
            entities: Liste von Entity-Objekten, die angereichert werden sollen.
            
        Returns:
            Die gleiche Liste von Entitäten, nun mit DBpedia-Daten angereichert.
        """
        if not entities:
            self.logger.warning("Keine Entitäten zur Anreicherung übergeben")
            return entities
            
        self.logger.debug(f"Starte Anreicherung von {len(entities)} Entitäten mit DBpedia-Daten")
        
        try:
            # Erstelle Verarbeitungskontexte für jede Entität
            contexts = []
            for entity in entities:
                context = EntityProcessingContext(
                    entity_name=entity.name,
                    entity_id=getattr(entity, 'id', None),
                    entity_type=getattr(entity, 'type', None),
                    original_text=getattr(entity, 'original_text', None)
                )
                # Set the language from config or default to 'de'
                context.language = self.config.get('LANGUAGE', 'de')
                # Copy the label from entity if it exists
                if hasattr(entity, 'label'):
                    context.label = entity.label
                else:
                    # Fallback: Create a simple label from the entity name
                    context.label = {context.language: entity.name}
                # Wichtig: Verknüpfe die ursprüngliche Entität mit dem Kontext
                context.entity = entity
                contexts.append(context)
                
            # Verarbeite alle Kontexte mit dem DBpediaService
            await self._link_contexts(contexts)
            
            # Aktualisiere die ursprünglichen Entity-Objekte
            for context in contexts:
                if hasattr(context, 'entity') and context.entity and hasattr(context, 'dbpedia_data'):
                    entity = context.entity
                    if 'uri' in context.dbpedia_data:
                        entity.dbpedia_uri = context.dbpedia_data['uri']
                    
                    # Wichtig: Registriere DBpedia als Quelle in der sources-Liste der Entität
                    entity.add_source('dbpedia', context.dbpedia_data)
            
        except Exception as e:
            self.logger.error(f"Fehler bei der Anreicherung der Entitäten: {str(e)}", 
                           exc_info=self.config.get('DEBUG', False))
            
            # Im Fehlerfall versuchen, so viele Entitäten wie möglich zu retten
            if 'contexts' in locals():
                for context in contexts:
                    if hasattr(context, 'entity') and context.entity and not hasattr(context, 'dbpedia_data'):
                        # Markiere die Entität als Fehler
                        context.entity.add_source('dbpedia', {
                            'status': 'error',
                            'error': str(e)
                        })
        
        return entities
    
    async def _link_contexts(self, contexts: List[EntityProcessingContext]) -> None:
        """
        Verarbeitet eine Liste von EntityProcessingContext-Objekten mit dem DBpediaService.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        # Batch-Verarbeitung mit dem DBpediaService
        batch_size = self.config.get('DBPEDIA_BATCH_SIZE', 10)
        
        # Verarbeite die Kontexte in Batches
        for i in range(0, len(contexts), batch_size):
            batch = contexts[i:i+batch_size]
            await self.dbpedia_service.process_batch(batch)
    
    async def process_entity(self, context: EntityProcessingContext) -> None:
        """
        Verarbeitet eine einzelne Entität.
        
        Args:
            context: Der Verarbeitungskontext der Entität
        """
        # Verarbeite den Kontext mit dem DBpediaService
        await self.dbpedia_service.process_entity(context)
        
        # Aktualisiere die ursprüngliche Entity, falls vorhanden
        if hasattr(context, 'entity') and context.entity and hasattr(context, 'dbpedia_data'):
            entity = context.entity
            if 'uri' in context.dbpedia_data:
                entity.dbpedia_uri = context.dbpedia_data['uri']
            
            # Wichtig: Registriere DBpedia als Quelle in der sources-Liste der Entität
            entity.add_source('dbpedia', context.dbpedia_data)


# Kompatibilitätsfunktion für die alte API
async def batch_get_dbpedia_info(urls_or_titles, config=None):
    """
    Verarbeitet eine Liste von URLs oder Titeln mit dem DBpediaService.
    
    Diese Funktion dient als Kompatibilitätsschicht für bestehenden Code,
    der die alte batch_get_dbpedia_info-Funktion verwendet.
    
    Args:
        urls_or_titles: Liste von URLs oder Titeln
        config: Optionale Konfiguration
        
    Returns:
        Dictionary mit den Ergebnissen
    """
    service = BatchDBpediaService(config)
    
    # Erstelle Kontexte für jede URL oder jeden Titel
    contexts = []
    for item in urls_or_titles:
        context = EntityProcessingContext(entity_name=item)
        if item.startswith('http'):
            context.set_processing_info('url', item)
        contexts.append(context)
    
    # Verarbeite die Kontexte
    await service._link_contexts(contexts)
    
    # Erstelle das Ergebnis-Dictionary
    results = {}
    for context in contexts:
        item = context.entity_name
        if hasattr(context, 'dbpedia_data'):
            results[item] = context.dbpedia_data
        else:
            results[item] = {'status': 'error', 'error': 'Keine DBpedia-Daten gefunden'}
    
    return results
