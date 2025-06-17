#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BatchWikidataService - Direkte Implementierung für die Batch-Verarbeitung mit dem Wikidata-Service.

Diese Klasse ersetzt den alten Wrapper (batch_wikidata_service_enhanced.py) und bietet
die gleiche Funktionalität direkt im Wikidata-Service-Modul.
"""

import logging
from typing import List, Dict, Any, Optional

from entityextractor.models.entity import Entity
from entityextractor.core.context import EntityProcessingContext
from entityextractor.config.settings import get_config
from entityextractor.utils.logging_utils import get_service_logger
from entityextractor.services.wikidata.service import WikidataService, process_entities_strict_pipeline_wikidata

# Logger konfigurieren
logger = get_service_logger(__name__, 'wikidata')

class BatchWikidataService:
    """
    BatchWikidataService - Service für die Batch-Verarbeitung von Entitäten mit Wikidata.
    
    Diese Klasse dient als Schnittstelle für die Batch-Verarbeitung und nutzt
    intern den WikidataService für die eigentliche Verarbeitung.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialisiert den BatchWikidataService.
        
        Args:
            config: Optionale Konfiguration, die an den Hauptservice weitergegeben wird.
        """
        self.config = config or get_config()
        self.logger = logger
        self._wikidata_service = None
        self._openai_service = None
        self.user_agent = self.config.get('USER_AGENT', 'EntityExtractor/1.0')
        
    @property
    def wikidata_service(self):
        """Lazy-loading für den WikidataService."""
        if self._wikidata_service is None:
            self._wikidata_service = WikidataService(self.config)
            # Wenn ein OpenAI-Service gesetzt wurde, übertrage ihn an den Wikidata-Service
            if self._openai_service is not None and hasattr(self._wikidata_service, 'set_openai_service'):
                self._wikidata_service.set_openai_service(self._openai_service)
        return self._wikidata_service
    
    def set_openai_service(self, openai_service):
        """
        Setzt den OpenAI-Service für Übersetzungen und Synonyme.
        
        Args:
            openai_service: Die OpenAIService-Instanz
        """
        self._openai_service = openai_service
        # Wenn der Wikidata-Service bereits initialisiert wurde, übertrage den Service
        if self._wikidata_service is not None and hasattr(self._wikidata_service, 'set_openai_service'):
            self._wikidata_service.set_openai_service(openai_service)
    
    def get_stats(self) -> Dict[str, int]:
        """
        Gibt die aktuellen Verarbeitungsstatistiken zurück.
        
        Returns:
            Ein Dictionary mit den Statistiken
        """
        if hasattr(self, '_wikidata_service') and self._wikidata_service is not None:
            return getattr(self._wikidata_service, 'stats', {})
        return {}
    
    async def enrich_entities(self, entities: List[Entity]) -> List[Entity]:
        """
        Reichert eine Liste von Entitäten mit Wikidata-Daten an.
        
        Delegiert die Verarbeitung an die Hauptverarbeitungspipeline.
        
        Args:
            entities: Liste von Entity-Objekten, die angereichert werden sollen.
            
        Returns:
            Die gleiche Liste von Entitäten, nun mit Wikidata-Daten angereichert.
        """
        if not entities:
            self.logger.warning("Keine Entitäten zur Anreicherung übergeben")
            return entities
            
        self.logger.debug(f"Starte Anreicherung von {len(entities)} Entitäten mit Wikidata-Daten")
        
        try:
            # Erstelle Verarbeitungskontexte für jede Entität
            contexts = []
            for entity in entities:
                config = self.config or {}
                context = EntityProcessingContext(
                    entity_name=entity.name,
                    entity_id=getattr(entity, 'id', None),
                    entity_type=getattr(entity, 'type', None),
                    original_text=getattr(entity, 'original_text', None)
                )
                # Set the language from config or default to 'de'
                context.language = config.get('LANGUAGE', 'de')
                # Copy the label from entity if it exists
                if hasattr(entity, 'label'):
                    context.label = entity.label  # Copy the LocalizedText object
                else:
                    # Fallback: Create a simple label from the entity name
                    context.label = {context.language: entity.name}
                # Wichtig: Verknüpfe die ursprüngliche Entität mit dem Kontext
                context.entity = entity
                contexts.append(context)
                
                # Debug-Ausgabe
                self.logger.debug(f"Erstelle Kontext für Entität: {entity.name} (ID: {getattr(entity, 'id', 'keine')})")
            
            # Verarbeite alle Kontexte mit der strikten Pipeline
            contexts, wikidata_service_instance = await process_entities_strict_pipeline_wikidata(
                contexts,
                config=self.config,
                openai_service=self._openai_service
            )
            
            # Speichere die WikidataService-Instanz, um auf die Statistiken zuzugreifen
            self._wikidata_service = wikidata_service_instance
            
            # Aktualisiere die ursprünglichen Entity-Objekte
            for context in contexts:
                if hasattr(context, 'entity') and context.entity and hasattr(context, 'wikidata_data'):
                    entity = context.entity
                    entity.wikidata_id = context.wikidata_data.get('id')
                    
                    # Wichtig: Registriere Wikidata als Quelle in der sources-Liste der Entität
                    entity.add_source('wikidata', context.wikidata_data)
            
            # Protokolliere die Ergebnisse
            stats = {}
            if hasattr(self.wikidata_service, 'get_stats'):
                stats = self.wikidata_service.get_stats()
            
            total_entities = len(entities)
            
            # Verwende die korrekten Statistiken für erfolgreiche und teilweise Verlinkungen
            successful = getattr(self.wikidata_service, 'successful_entities', 0)
            partial = getattr(self.wikidata_service, 'partial_entities', 0)
            
            # Erstelle eine Zusammenfassung der Fallback-Ergebnisse
            self.logger.info("\n" + "="*80)
            self.logger.info("ZUSAMMENFASSUNG DER WIKIDATA-ANREICHERUNG")
            self.logger.info("="*80)
            self.logger.info(f"Gesamtzahl der verarbeiteten Entitäten: {total_entities}")
            self.logger.info(f"Vollständig verlinkt: {successful} ({(successful/max(total_entities, 1))*100:.1f}%)")
            self.logger.info(f"Nicht verlinkt: {total_entities - successful} ({((total_entities - successful)/max(total_entities, 1))*100:.1f}%)\n")
            
        except Exception as e:
            self.logger.error(f"Fehler bei der Anreicherung der Entitäten: {str(e)}", 
                           exc_info=self.config.get('DEBUG', False))
            
            # Im Fehlerfall versuchen, so viele Entitäten wie möglich zu retten
            if 'contexts' in locals():
                for context in contexts:
                    if hasattr(context, 'entity') and context.entity and not hasattr(context, 'wikidata_data'):
                        # Markiere die Entität als Fehler
                        context.entity.add_source('wikidata', {
                            'status': 'error',
                            'error': str(e)
                        })
        
        return entities
            
    async def process_entity(self, context: EntityProcessingContext) -> None:
        """
        Verarbeitet eine einzelne Entität.
        
        Args:
            context: Der Verarbeitungskontext der Entität
        """
        # Erstelle eine Liste mit einem einzelnen Kontext
        contexts = [context]
        
        # Verwende die strikte Pipeline für die Verarbeitung
        contexts, wikidata_service_instance = await process_entities_strict_pipeline_wikidata(
            contexts,
            config=self.config,
            openai_service=self._openai_service
        )
        
        # Speichere die WikidataService-Instanz, um auf die Statistiken zuzugreifen
        self._wikidata_service = wikidata_service_instance
        
        # Aktualisiere die ursprüngliche Entity, falls vorhanden
        if hasattr(context, 'entity') and context.entity and hasattr(context, 'wikidata_data'):
            entity = context.entity
            entity.wikidata_id = context.wikidata_data.get('id')
            
            # Wichtig: Registriere Wikidata als Quelle in der sources-Liste der Entität
            entity.add_source('wikidata', context.wikidata_data)
