#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BatchWikipediaService - Direkte Implementierung für die Batch-Verarbeitung mit dem Wikipedia-Service.

Diese Klasse ersetzt den alten Wrapper (batch_wikipedia_service_enhanced.py) und bietet
die gleiche Funktionalität direkt im Wikipedia-Service-Modul.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional, Union

from entityextractor.models.entity import Entity
from entityextractor.core.context import EntityProcessingContext
from entityextractor.config.settings import get_config
from entityextractor.utils.logging_utils import get_service_logger
from entityextractor.services.wikipedia.service import WikipediaService

# Logger konfigurieren
logger = get_service_logger(__name__, 'wikipedia')

class BatchWikipediaService:
    """
    BatchWikipediaService - Service für die Batch-Verarbeitung von Entitäten mit Wikipedia.
    
    Diese Klasse dient als Schnittstelle für die Batch-Verarbeitung und nutzt
    intern den WikipediaService für die eigentliche Verarbeitung.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialisiert den BatchWikipediaService.
        
        Args:
            config: Optionale Konfiguration, die an den Hauptservice weitergegeben wird.
        """
        self.config = config or get_config()
        self.logger = logger
        self._wikipedia_service = None
        self.user_agent = self.config.get('USER_AGENT', 'EntityExtractor/1.0')
        
    @property
    def wikipedia_service(self):
        """Lazy-loading für den WikipediaService."""
        if self._wikipedia_service is None:
            self._wikipedia_service = WikipediaService(self.config)
        return self._wikipedia_service
    
    def get_stats(self) -> Dict[str, int]:
        """
        Gibt die aktuellen Verarbeitungsstatistiken zurück.
        
        Returns:
            Ein Dictionary mit den Statistiken
        """
        if hasattr(self, '_wikipedia_service') and self._wikipedia_service is not None:
            return getattr(self._wikipedia_service, 'stats', {})
        return {}
    
    async def enrich_entities(self, entities: List[Entity]) -> List[Entity]:
        """
        Reichert eine Liste von Entitäten mit Wikipedia-Daten an.
        
        Args:
            entities: Liste von Entity-Objekten, die angereichert werden sollen.
            
        Returns:
            Die gleiche Liste von Entitäten, nun mit Wikipedia-Daten angereichert.
        """
        if not entities:
            self.logger.warning("Keine Entitäten zur Anreicherung übergeben")
            return entities
            
        self.logger.debug(f"Starte Anreicherung von {len(entities)} Entitäten mit Wikipedia-Daten")
        
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
                
            # Verarbeite alle Kontexte mit dem WikipediaService
            await self._link_contexts(contexts)
            
            # Aktualisiere die ursprünglichen Entity-Objekte
            for context in contexts:
                if hasattr(context, 'entity') and context.entity and hasattr(context, 'wikipedia_data'):
                    entity = context.entity
                    if 'url' in context.wikipedia_data:
                        entity.wikipedia_url = context.wikipedia_data['url']
                    
                    # Wichtig: Registriere Wikipedia als Quelle in der sources-Liste der Entität
                    entity.add_source('wikipedia', context.wikipedia_data)
            
        except Exception as e:
            self.logger.error(f"Fehler bei der Anreicherung der Entitäten: {str(e)}", 
                           exc_info=self.config.get('DEBUG', False))
            
            # Im Fehlerfall versuchen, so viele Entitäten wie möglich zu retten
            if 'contexts' in locals():
                for context in contexts:
                    if hasattr(context, 'entity') and context.entity and not hasattr(context, 'wikipedia_data'):
                        # Markiere die Entität als Fehler
                        context.entity.add_source('wikipedia', {
                            'status': 'error',
                            'error': str(e)
                        })
        
        return entities
    
    async def _link_contexts(self, contexts: List[EntityProcessingContext]) -> None:
        """
        Verarbeitet eine Liste von EntityProcessingContext-Objekten mit dem WikipediaService.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        # Batch-Verarbeitung mit dem WikipediaService
        batch_size = self.config.get('WIKIPEDIA_BATCH_SIZE', 10)
        
        # Verarbeite die Kontexte in Batches
        for i in range(0, len(contexts), batch_size):
            batch = contexts[i:i+batch_size]
            await self.wikipedia_service.process_batch(batch)
    
    async def process_entity(self, context: EntityProcessingContext) -> None:
        """
        Verarbeitet eine einzelne Entität.
        
        Args:
            context: Der Verarbeitungskontext der Entität
        """
        # Verarbeite den Kontext mit dem WikipediaService
        await self.wikipedia_service.process_entity(context)
        
        # Aktualisiere die ursprüngliche Entity, falls vorhanden
        if hasattr(context, 'entity') and context.entity and hasattr(context, 'wikipedia_data'):
            entity = context.entity
            if 'url' in context.wikipedia_data:
                entity.wikipedia_url = context.wikipedia_data['url']
            
            # Wichtig: Registriere Wikipedia als Quelle in der sources-Liste der Entität
            entity.add_source('wikipedia', context.wikipedia_data)


# Kompatibilitätsfunktion für die alte API
async def batch_get_wikipedia_pages(search_terms, config=None):
    """
    Verarbeitet eine Liste von Suchbegriffen mit dem WikipediaService.
    
    Diese Funktion dient als Kompatibilitätsschicht für bestehenden Code,
    der die alte batch_get_wikipedia_pages-Funktion verwendet.
    
    Args:
        search_terms: Liste oder Dictionary von Suchbegriffen
        config: Optionale Konfiguration
        
    Returns:
        Dictionary mit den Ergebnissen
    """
    service = BatchWikipediaService(config)
    
    # Erstelle Kontexte für jeden Suchbegriff
    contexts = []
    if isinstance(search_terms, dict):
        for key, term in search_terms.items():
            context = EntityProcessingContext(entity_name=term)
            context.set_processing_info('search_key', key)
            contexts.append(context)
    else:
        for term in search_terms:
            context = EntityProcessingContext(entity_name=term)
            contexts.append(context)
    
    # Verarbeite die Kontexte
    await service._link_contexts(contexts)
    
    # Erstelle das Ergebnis-Dictionary
    results = {}
    for context in contexts:
        key = context.get_processing_info('search_key', context.entity_name)
        if hasattr(context, 'wikipedia_data'):
            results[key] = context.wikipedia_data
        else:
            results[key] = {'status': 'error', 'error': 'Keine Wikipedia-Daten gefunden'}
    
    return results
