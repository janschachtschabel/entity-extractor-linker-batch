#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wikidata-Service mit klaren Datenstrukturen

Diese Service-Implementierung nutzt den EntityProcessingContext für strukturierte
Datenübergabe und Schema-Validierung. Sie unterstützt Batch-Verarbeitung, asynchrone
Anfragen und verschiedene Fallback-Mechanismen zur Maximierung der Verlinkungsquote.
"""

import os
import logging
import asyncio
from typing import Dict, List, Any, Optional, Tuple

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.services.wikidata.async_fetchers import async_fetch_wikidata_batch, async_search_wikidata, async_fetch_entity_labels
from entityextractor.services.wikidata.fallbacks import apply_all_fallbacks, apply_direct_search, apply_language_fallback, apply_synonym_fallback
from entityextractor.services.wikidata.formatters import format_wikidata_entity, has_required_fields, enrich_flat_entity_references
from entityextractor.utils.context_cache import load_service_data_from_cache, cache_service_data
from entityextractor.utils.api_request_utils import extract_wikidata_id_from_wikipedia

class WikidataService:
    """
    Service für die Anreicherung von Entitäten mit Wikidata-Daten.
    
    Unterstützt verschiedene Fallback-Mechanismen, um die Erfolgsrate zu maximieren:
    1. Primär: Extraktion der Wikidata-ID aus der Wikipedia-Seite
    2. Sekundär: Direkte Suche in Wikidata nach dem Entitätsnamen
    3. Tertiär: Sprachfallback (Übersetzung ins Englische) und Synonym-Generierung
    """
    
    def __init__(self, config=None):
        """
        Initialisiert den Wikidata-Service mit Konfigurationsoptionen.
        
        Args:
            config: Optionale Konfiguration (wird aus settings.py geladen, falls nicht angegeben)
        """
        self.config = config or get_config()
        self.logger = logging.getLogger(__name__)
        
        # Fallback-Konfiguration
        self.use_fallbacks = self.config.get("WIKIDATA_USE_FALLBACKS", True)
        self.enable_translation_fallback = self.config.get("WIKIDATA_ENABLE_TRANSLATION_FALLBACK", True)
        self.enable_synonym_fallback = self.config.get("WIKIDATA_ENABLE_SYNONYM_FALLBACK", True)
        
        # Statistik
        self.successful_entities = 0
        self.partial_entities = 0
        self.failed_entities = 0
        self.api_calls = {"search": 0, "entity": 0, "labels": 0}
        self.fallback_usage = {"direct_search": 0, "language": 0, "synonym": 0}
        
        self.logger.info(f"Wikidata-Service initialisiert (Fallbacks: {self.use_fallbacks})")
    
    async def process_entity(self, context: EntityProcessingContext) -> EntityProcessingContext:
        """
        Verarbeitet eine einzelne Entität und reichert sie mit Wikidata-Daten an.
        
        Args:
            context: EntityProcessingContext-Objekt
            
        Returns:
            Verarbeiteter Kontext
        """
        # Verarbeite die Entität als Teil eines Batches mit nur einem Element
        await self._process_batch_async([context])
        return context
        
    async def process_entities(self, contexts: List[EntityProcessingContext]) -> List[EntityProcessingContext]:
        """
        Verarbeitet eine Liste von Entitäten und reichert sie mit Wikidata-Daten an.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
            
        Returns:
            Liste der verarbeiteten Kontexte
        """
        if not contexts:
            return contexts
            
        self.logger.info(f"Verarbeite {len(contexts)} Entitäten mit Wikidata-Service")
        
        # Batch-Verarbeitung für bessere Performance
        await self._process_batch_async(contexts)
        
        return contexts
    
    async def _process_batch_async(self, contexts: List[EntityProcessingContext]):
        """
        Verarbeitet einen Batch von Entitäten asynchron mit Wikidata-Daten und aktualisiert die Kontexte.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        # 1. Lade zunächst eventuell vorhandene Cachedaten, um unnötige API-Aufrufe zu vermeiden
        contexts_with_ids: List[Tuple[EntityProcessingContext, str]] = []
        contexts_without_ids: List[EntityProcessingContext] = []

        for context in contexts:
            # Versuche, gecachte Daten zu laden
            cached_data = load_service_data_from_cache(context.entity_name, "wikidata")
            if cached_data and has_required_fields(cached_data):
                # Cache-Treffer – Kontext direkt anreichern und Statistik aktualisieren
                context.set_processing_info("wikidata_data", cached_data)
                context.set_processing_info("wikidata_status", "success")
                context.add_service_data("wikidata", cached_data)
                context.processed_by_services.add("wikidata")
                self.successful_entities += 1
                self._update_statistics(cached_data, "cache")
                continue  # Keine weitere Verarbeitung nötig

            # Kein Cache oder unvollständige Daten
            wikidata_id = context.get_processing_info("wikidata_id")
            if wikidata_id:
                contexts_with_ids.append((context, wikidata_id))
            else:
                contexts_without_ids.append(context)
        
        # 2. Für Kontexte ohne IDs den dreistufigen Fallback-Mechanismus anwenden
        if contexts_without_ids:
            # Stufe 1: Wikipedia-Extraktion (primär)
            await self._extract_from_wikipedia(contexts_without_ids)
                
            # Kontexte neu filtern nach erfolgreicher Wikipedia-Extraktion
            remaining_contexts = []
            for context in contexts_without_ids:
                wikidata_id = context.get_processing_info("wikidata_id")
                if wikidata_id:
                    contexts_with_ids.append((context, wikidata_id))
                else:
                    remaining_contexts.append(context)
            contexts_without_ids = remaining_contexts
            
            # Stufe 2: Direkte Suche (sekundär)
            if contexts_without_ids and self.use_fallbacks:
                await self._apply_direct_search(contexts_without_ids)
                
                # Kontexte neu filtern nach erfolgreicher direkter Suche
                remaining_contexts = []
                for context in contexts_without_ids:
                    wikidata_id = context.get_processing_info("wikidata_id")
                    if wikidata_id:
                        contexts_with_ids.append((context, wikidata_id))
                    else:
                        remaining_contexts.append(context)
                contexts_without_ids = remaining_contexts
            
            # Stufe 3a: Sprachfallback (tertiär)
            if contexts_without_ids and self.enable_translation_fallback and self.use_fallbacks:
                await self._apply_language_fallback(contexts_without_ids)
                
                # Kontexte neu filtern nach erfolgreicher Sprachübersetzung
                remaining_contexts = []
                for context in contexts_without_ids:
                    wikidata_id = context.get_processing_info("wikidata_id")
                    if wikidata_id:
                        contexts_with_ids.append((context, wikidata_id))
                    else:
                        remaining_contexts.append(context)
                contexts_without_ids = remaining_contexts
            
            # Stufe 3b: Synonym-Fallback (tertiär)
            if contexts_without_ids and self.enable_synonym_fallback and self.use_fallbacks:
                await self._apply_synonym_fallback(contexts_without_ids)
                
                # Kontexte neu filtern nach erfolgreichem Synonym-Fallback
                for context in contexts_without_ids:
                    wikidata_id = context.get_processing_info("wikidata_id")
                    if wikidata_id:
                        contexts_with_ids.append((context, wikidata_id))
        
        # 3. Wikidata-Daten für Kontexte mit IDs abrufen
        if not contexts_with_ids:
            # Für die verbleibenden Kontexte ohne ID konnten keine Daten gefunden werden
            self.failed_entities += len(contexts_without_ids)
            for context in contexts_without_ids:
                context.set_processing_info("wikidata_status", "not_found")
            return
        
        # IDs und zugehörige Kontexte sammeln
        wikidata_ids = [wid for _, wid in contexts_with_ids]
        context_by_id = {wid: ctx for ctx, wid in contexts_with_ids}
        
        try:
            # Batch-Abruf der Wikidata-Entitäten
            entities_data_list = await async_fetch_wikidata_batch(wikidata_ids)
            
            # Konvertiere die Liste in ein Dictionary für einfacheren Zugriff
            # Wir nehmen an, dass jedes Element in der Liste ein Dictionary mit einem 'id'-Feld ist
            entities_data = {}
            for i, entity_data in enumerate(entities_data_list):
                if entity_data and 'id' in entity_data:
                    entities_data[entity_data['id']] = entity_data
                elif i < len(wikidata_ids):
                    # Wenn keine ID im Ergebnis vorhanden ist, verwenden wir die angeforderte ID
                    entities_data[wikidata_ids[i]] = entity_data if entity_data else {}
            
            self.logger.debug(f"Erhaltene Wikidata-Daten für {len(entities_data)} von {len(wikidata_ids)} angeforderten IDs")
            
            # Sammle alle Entitäts-IDs für Batch-Label-Abruf
            entity_ids_for_labels = set()
            temp_formatted_entities = {}
            
            # Erste Formatierung ohne Labels
            for wikidata_id, entity_data in entities_data.items():
                if wikidata_id in context_by_id and entity_data:
                    # Formatiere die Entität zunächst ohne Label-Anreicherung
                    formatted_data = format_wikidata_entity(entity_data, context_by_id[wikidata_id].entity_name)
                    temp_formatted_entities[wikidata_id] = formatted_data
                    
                    # Sammle referenzierte Entitäts-IDs für Label-Anreicherung
                    for prop in ['instance_of', 'subclass_of', 'part_of', 'has_part']:
                        if prop in formatted_data:
                            for ref in formatted_data[prop]:
                                if 'id' in ref:
                                    entity_ids_for_labels.add(ref['id'])
            
            # Batch-Abruf der Labels für referenzierte Entitäten
            entity_labels = {}
            if entity_ids_for_labels:
                self.logger.debug(f"Rufe Labels für {len(entity_ids_for_labels)} referenzierte Entitäten ab...")
                entity_labels = await async_fetch_entity_labels(list(entity_ids_for_labels))
            
            # Entitäten mit Wikidata-Daten und Labels anreichern
            for wikidata_id, formatted_data in temp_formatted_entities.items():
                if wikidata_id in context_by_id:
                    context = context_by_id[wikidata_id]
                    
                    # Labels für referenzierte Entitäten hinzufügen mit der neuen Funktion
                    enrich_flat_entity_references(formatted_data, lambda ids, lang: entity_labels, "de")
                    
                    # Kontext aktualisieren
                    context.set_processing_info("wikidata_data", formatted_data)
                    context.set_processing_info("wikidata_status", "success")
                    
                    # WICHTIG: Die formatierten Daten in ein korrektes Format für add_service_data bringen
                    # Die Methode add_service_data erwartet ein Dictionary mit dem Service-Namen als Schlüssel
                    # und den formatierten Daten als Wert
                    self.logger.info(f"Wikidata-Daten für '{context.entity_name}' vor add_service_data: {formatted_data.keys() if formatted_data else 'None'}")
                    
                    # KORREKTUR: Die Daten müssen direkt übergeben werden, nicht in einem verschachtelten Dictionary
                    context.add_service_data("wikidata", formatted_data)
                    
                    # KRITISCH: Explizit 'wikidata' zu processed_by_services hinzufügen
                    # Dies ist entscheidend, damit die Daten in der finalen Ausgabe erscheinen
                    context.processed_by_services.add("wikidata")

                    # Nach erfolgreicher Anreicherung: Daten im Cache speichern, falls vollständig
                    try:
                        if has_required_fields(formatted_data):
                            cache_service_data(context.entity_name, "wikidata", formatted_data)
                            self.logger.debug(f"Wikidata-Daten für '{context.entity_name}' im Cache gespeichert")
                    except Exception as cache_exc:
                        self.logger.debug(f"Konnte Wikidata-Daten für '{context.entity_name}' nicht cachen: {cache_exc}")

                    self.logger.info(f"processed_by_services für '{context.entity_name}' nach Hinzufügen: {context.processed_by_services}")
                    
                    # Überprüfen, ob die Daten korrekt hinzugefügt wurden
                    wikidata_in_sources = "wikidata" in context.output_data["sources"]
                    self.logger.info(f"Wikidata in sources für '{context.entity_name}': {wikidata_in_sources}")
                    if wikidata_in_sources:
                        self.logger.info(f"Wikidata-Daten in sources für '{context.entity_name}': {context.output_data['sources']['wikidata'].keys() if 'wikidata' in context.output_data['sources'] else 'None'}")
                    else:
                        self.logger.warning(f"Wikidata-Daten wurden nicht zu den sources für '{context.entity_name}' hinzugefügt!")
                    
                    self.successful_entities += 1
                    self._update_statistics(formatted_data, "api")
            
            # Prüfen, ob alle IDs erfolgreich abgerufen wurden
            for wikidata_id, context in context_by_id.items():
                if wikidata_id not in entities_data:
                    context.set_processing_info("wikidata_status", "not_found")
                    self.partial_entities += 1
        
        except Exception as e:
            self.logger.error(f"Fehler bei der Batch-Verarbeitung von Wikidata-Daten: {str(e)}")
            for context in contexts:
                context.set_processing_info("wikidata_status", "error")
                context.set_processing_info("wikidata_error", str(e))
            self.failed_entities += len(contexts_with_ids)
            
    async def _extract_from_wikipedia(self, contexts: List[EntityProcessingContext]):
        """
        Extrahiert Wikidata-IDs aus Wikipedia-Seiten für eine Liste von Kontexten.
        Dies ist der primäre Mechanismus zur Wikidata-ID-Extraktion.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        for context in contexts:
            entity_name = context.entity_name
            language = context.get_processing_info("language", "de")
            
            # Wikidata-ID aus Wikipedia extrahieren
            wikidata_id = None
            try:
                wikipedia_url = f"https://{language}.wikipedia.org/wiki/{entity_name.replace(' ', '_')}"
                wikidata_id = extract_wikidata_id_from_wikipedia(wikipedia_url, self.config)
            except Exception as e:
                self.logger.debug(f"Fehler bei der Extraktion der Wikidata-ID aus Wikipedia für '{entity_name}': {str(e)}")
            
            if wikidata_id:
                context.set_processing_info("wikidata_id", wikidata_id)
                context.set_processing_info("wikidata_id_source", "wikipedia_extraction")
                
                # Statistik aktualisieren
                self.logger.debug(f"Wikidata-ID für '{entity_name}' aus Wikipedia extrahiert: {wikidata_id}")
    
    async def _apply_direct_search(self, contexts: List[EntityProcessingContext]):
        """
        Wendet die direkte Suche in Wikidata an, um IDs für Entitäten zu finden.
        Dies ist der sekundäre Fallback-Mechanismus.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        for context in contexts:
            entity_name = context.entity_name
            language = context.get_processing_info("language", "de")
            
            # Direkte Suche in Wikidata
            try:
                search_results = await async_search_wikidata(entity_name, language)
                if search_results and len(search_results) > 0:
                    # Beste Übereinstimmung verwenden
                    wikidata_id = search_results[0]["id"]
                    context.set_processing_info("wikidata_id", wikidata_id)
                    context.set_processing_info("wikidata_id_source", "direct_search")
                    
                    # Statistik aktualisieren
                    self.fallback_usage["direct_search"] += 1
                    self.logger.debug(f"Wikidata-ID für '{entity_name}' durch direkte Suche gefunden: {wikidata_id}")
            except Exception as e:
                self.logger.debug(f"Fehler bei der direkten Suche für '{entity_name}': {str(e)}")
    
    async def _apply_language_fallback(self, contexts: List[EntityProcessingContext]):
        """
        Wendet den Sprachfallback an, um IDs für Entitäten zu finden.
        Dies ist Teil des tertiären Fallback-Mechanismus.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        # Prepare OpenAI service, API URL, user agent from config
        openai_service = getattr(self, 'openai_service', None)
        api_url = self.config.get('WIKIDATA_API_URL', 'https://www.wikidata.org/w/api.php')
        user_agent = self.config.get('USER_AGENT', 'EntityExtractor/1.0')

        # Call fallback for each context (batch fallback expects per-entity args)
        for context in contexts:
            entity_name = context.entity_name
            wikidata_result = context.get_processing_info('wikidata_data')
            # Call the fallback and update context if new data is found
            result, _ = await apply_language_fallback(
                entity_name,
                wikidata_result,
                openai_service,
                api_url,
                user_agent,
                self.config
            )
            if result and result.get('id'):
                context.set_processing_info('wikidata_id', result['id'])
                context.set_processing_info('wikidata_data', result)
                context.set_processing_info('wikidata_id_source', 'language_fallback')
        
        # Statistik aktualisieren
        for context in contexts:
            if context.get_processing_info('wikidata_id') and context.get_processing_info('wikidata_id_source') == 'language_fallback':
                self.fallback_usage['language'] += 1
    
    async def _apply_synonym_fallback(self, contexts: List[EntityProcessingContext]):
        """
        Wendet den Synonym-Fallback an, um IDs für Entitäten zu finden.
        Dies ist Teil des tertiären Fallback-Mechanismus.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        openai_service = getattr(self, 'openai_service', None)
        api_url = self.config.get('WIKIDATA_API_URL', 'https://www.wikidata.org/w/api.php')
        user_agent = self.config.get('USER_AGENT', 'EntityExtractor/1.0')
        max_fallback_attempts = self.config.get('WIKIDATA_MAX_SYNONYM_FALLBACK_ATTEMPTS', 3)

        for context in contexts:
            entity_name = context.entity_name
            wikidata_result = context.get_processing_info('wikidata_data')
            current_fallback_attempts = context.get_processing_info('synonym_fallback_attempts', 0)
            result, _ = await apply_synonym_fallback(
                entity_name,
                wikidata_result,
                openai_service,
                api_url,
                user_agent,
                self.config,
                current_fallback_attempts,
                max_fallback_attempts
            )
            if result and result.get('id'):
                context.set_processing_info('wikidata_id', result['id'])
                context.set_processing_info('wikidata_data', result)
                context.set_processing_info('wikidata_id_source', 'synonym_fallback')
        
        # Statistik aktualisieren
        for context in contexts:
            if context.get_processing_info('wikidata_id') and context.get_processing_info('wikidata_id_source') == 'synonym_fallback':
                self.fallback_usage['synonym'] += 1
    
    def _update_statistics(self, entity_data: Dict[str, Any], source: str):
        """
        Aktualisiert die Statistik für eine verarbeitete Entität.
        
        Args:
            entity_data: Die Entitätsdaten
            source: Quelle der Daten (api, cache)
        """
        # API-Aufrufe zählen
        if source == "api":
            self.api_calls["entity"] += 1
            
    def get_statistics(self):
        """
        Gibt die Statistik des Services zurück.
        
        Returns:
            Dictionary mit Statistikdaten
        """
        return {
            "successful_entities": self.successful_entities,
            "partial_entities": self.partial_entities,
            "failed_entities": self.failed_entities,
            "api_calls": self.api_calls,
            "fallback_usage": self.fallback_usage
        }
        
    async def close_session(self) -> None:
        """
        Schließt die aiohttp.ClientSession, falls vorhanden.
        Diese Methode ist ein Stub, da WikidataService keine eigene Session verwaltet,
        aber sie wird benötigt, um die Schnittstelle mit anderen Services konsistent zu halten.
        """
        # WikidataService verwaltet keine eigene Session, daher ist keine Aktion erforderlich
        # Diese Methode existiert nur für die Kompatibilität mit dem Orchestrator
        self.logger.debug("WikidataService: Keine Session zu schließen")
        pass

# Hilfsfunktion für die strikte Pipeline
async def process_entities_strict_pipeline_wikidata(contexts: List[EntityProcessingContext], config=None, openai_service=None):
    """
    Verarbeitet Entitäten mit dem Wikidata-Service in einer strikten Pipeline.
    
    Args:
        contexts: Liste von EntityProcessingContext-Objekten
        config: Optionale Konfiguration
        openai_service: Optionaler OpenAI-Service für Sprachfallbacks
        
    Returns:
        Tuple aus Liste der verarbeiteten Kontexte und WikidataService-Instanz
    """
    service = WikidataService(config)
    processed_contexts = await service.process_entities(contexts)
    return processed_contexts, service

# Singleton-Instanz für den Import in anderen Modulen
wikidata_service = WikidataService()
