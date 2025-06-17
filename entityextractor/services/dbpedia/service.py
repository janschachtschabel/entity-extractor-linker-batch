#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DBpedia Service

This service processes DBpedia data and provides it in a structured form.
It uses Pydantic for data validation and Loguru for improved logging.
The implementation uses asynchronous processing and batch operations for optimal performance.
"""

import os
import json
import asyncio
import time
import re
from typing import Dict, List, Any, Optional, Set, Tuple
from loguru import logger

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.services.base_service import BaseService
from entityextractor.models.data_models import EntityData
from entityextractor.services.dbpedia.fetchers import fetch_dbpedia_sparql, fetch_dbpedia_lookup
from entityextractor.services.translation_service import translate_term_to_en

class DBpediaService(BaseService):
    """
    Service for processing DBpedia requests with Pydantic models.
    
    This service processes entities in batches and uses a multi-level fallback strategy
    to achieve the highest possible linking rate. It ensures that only entities
    with URI, English label, and English abstract are marked as "linked".
    
    This class implements a singleton pattern to ensure only one instance exists.
    """
    
    # Singleton instance
    _instance = None
    
    def __new__(cls, config: Optional[Dict[str, Any]] = None):
        """
        Ensure singleton pattern - only one instance of DBpediaService exists.
        
        Args:
            config: Optional configuration dictionary
            
        Returns:
            The singleton instance of DBpediaService
        """
        if cls._instance is None:
            instance = super(DBpediaService, cls).__new__(cls)
            return instance
        return cls._instance
        
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DBpediaService.
        
        Args:
            config: Optional configuration dictionary
        """
        # If this instance has already been initialized, don't reinitialize
        if hasattr(self, 'initialized') and self.initialized:
            if config is not None:
                self.config.update(config)
                # Update configuration-dependent attributes
                self.use_de = self.config.get('DBPEDIA_USE_DE', False)
                self.timeout = self.config.get('TIMEOUT_THIRD_PARTY', 30)
                self.endpoints = self._get_endpoints()
            return
        
        logger.debug("Creating new DBpediaService instance")
        super().__init__(config)
        
        # Load configuration
        self.use_de = self.config.get('DBPEDIA_USE_DE', False)
        self.timeout = self.config.get('TIMEOUT_THIRD_PARTY', 30)  # Use global timeout setting
        self.batch_size = self.config.get('DBPEDIA_BATCH_SIZE', 10)
        self.max_lookup_results = self.config.get('DBPEDIA_MAX_LOOKUP_RESULTS', 5)
        
        # Configure endpoints
        self.endpoints = self._get_endpoints()
        
        # Create cache directory
        self.cache_dir = os.path.join(self.config.get('CACHE_DIR', 'entityextractor_cache'), "dbpedia")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Mark as initialized
        self.initialized = True
        
        # Set this instance as the singleton
        DBpediaService._instance = self
        
        # Statistics
        self.successful_entities = 0
        self.failed_entities = 0
        self.cache_hits = 0
        self.processed_entities = 0
        self.service_name = "dbpedia"
        
        logger.info(f"DBpediaService initialized: use_de={self.use_de}, timeout={self.timeout}s")
        logger.info(f"Primary endpoint: {self.endpoints[0]}")
        logger.debug(f"Cache directory: {self.cache_dir}")
        logger.debug(f"Batch size: {self.batch_size}")
        logger.debug(f"Max lookup results: {self.max_lookup_results}")
        
    @classmethod
    async def close_all_sessions(cls) -> None:
        """Static method to close the session on the singleton instance."""
        if cls._instance is not None and hasattr(cls._instance, 'session') and cls._instance.session is not None:
            await cls._instance.close_session()
            logger.info("Closed DBpediaService singleton session")
            
    @classmethod
    def get_instance(cls, config: Optional[Dict[str, Any]] = None) -> 'DBpediaService':
        """Get the singleton instance of DBpediaService."""
        if cls._instance is None:
            return cls(config)
        return cls._instance
        
        # Already processed URIs (to avoid duplicates)
        self.processed_uris = set()
        
    def _get_endpoints(self) -> List[str]:
        """
        Bestimmt die zu verwendenden DBpedia-Endpoints basierend auf der Konfiguration.
        
        Returns:
            Liste von DBpedia-Endpoints, sortiert nach Priorität
        """
        # Standard-Endpoint ist immer der internationale DBpedia-Endpoint
        endpoints = ["http://dbpedia.org/sparql"]
        
        # Wenn deutsche Endpoints aktiviert sind, füge sie hinzu
        if self.use_de:
            endpoints.append("http://de.dbpedia.org/sparql")
        
        # Alternative Endpoints als Fallback
        endpoints.extend([
            "http://live.dbpedia.org/sparql",
            "http://dbpedia-live.openlinksw.com/sparql"
        ])
        
        logger.debug(f"Konfigurierte DBpedia-Endpoints: {endpoints}")
        return endpoints
    
    def _get_primary_endpoint(self) -> str:
        """
        Gibt den primären DBpedia-Endpoint zurück.
        
        Returns:
            URL des primären DBpedia-Endpoints
        """
        if not hasattr(self, 'endpoints') or not self.endpoints:
            self.endpoints = self._get_endpoints()
        
        return self.endpoints[0] if self.endpoints else "http://dbpedia.org/sparql"
        
    def _create_dbpedia_uri_from_label(self, label: str) -> str:
        """
        Erstellt einen DBpedia-URI aus einem Label.
        
        Args:
            label: Das Label, aus dem der URI erstellt werden soll
            
        Returns:
            Der erstellte DBpedia-URI
        """
        if not label:
            return ""

        # Konvertiere Label in ein gültiges DBpedia-Resource-Format
        # 1. Ersetze Leerzeichen durch Unterstriche (wie bei DBpedia üblich)
        # 2. Verwende urllib.parse.quote, um Sonderzeichen korrekt zu encodieren,
        #    lasse jedoch gängige Zeichen wie '(', ')' und '_' unangetastet,
        #    da DBpedia-Ressourcen diese enthalten können (z. B. Prism_(optics)).
        from urllib.parse import quote
            
        # Entferne Sonderzeichen und formatiere das Label für einen DBpedia-URI
        # 1. Ersetze Leerzeichen durch Unterstriche
        # 2. Entferne Sonderzeichen, die in URIs problematisch sein könnten
        import re
        
        formatted_label = label.replace(' ', '_')
        # Entferne einfache Steuerzeichen wie Tabs/Zeilenumbrüche
        formatted_label = formatted_label.replace('\n', '').replace('\r', '')
        # Stelle sicher, dass erster Buchstabe groß ist (DBpedia-Konvention)
        if formatted_label:
            formatted_label = formatted_label[0].upper() + formatted_label[1:]
        # URL-encode – Klammern und Unterstrich bleiben erhalten
        encoded = quote(formatted_label, safe='()_')
        return f"http://dbpedia.org/resource/{encoded}"
        
    def _get_endpoints(self) -> List[str]:
        """
        Determines the SPARQL endpoints to use based on the configuration.
        Respects DBPEDIA_USE_DE=False to completely disable German endpoints.
        
        Returns:
            List of SPARQL endpoint URLs
        """
        # Standard endpoints - always use the international DBpedia endpoint
        endpoints = [
            "http://dbpedia.org/sparql"  # Main DBpedia endpoint (HTTP)
        ]
        
        # If German DBpedia is enabled, add these endpoints
        if self.use_de:
            endpoints.append("http://de.dbpedia.org/sparql")  # German DBpedia endpoint
            logger.info("German DBpedia endpoints enabled")
        else:
            logger.info("German DBpedia endpoints disabled")
        
        # Wenn benutzerdefinierte Endpoints in der Konfiguration angegeben sind, verwende diese
        custom_endpoints = self.config.get('DBPEDIA_ENDPOINTS', None)
        if custom_endpoints:
            # Filtere deutsche Endpoints heraus, wenn DBPEDIA_USE_DE=False
            if not self.use_de:
                custom_endpoints = [ep for ep in custom_endpoints if 'de.dbpedia.org' not in ep]
            
            if custom_endpoints:  # Nur verwenden, wenn nach dem Filtern noch Endpoints übrig sind
                endpoints = custom_endpoints
                logger.info(f"Verwende benutzerdefinierte DBpedia-Endpoints: {endpoints}")
        
        # Stelle sicher, dass wir mindestens einen Endpoint haben
        if not endpoints:
            logger.warning("Keine DBpedia-Endpoints konfiguriert, verwende Standard")
            endpoints = ["http://dbpedia.org/sparql"]
        
        # Logge die endgültige Liste der Endpoints
        logger.debug(f"DBpedia-Endpoints (in Reihenfolge der Präferenz): {endpoints}")
        
        return endpoints
        
    async def process_entity(self, entity: EntityData) -> EntityData:
        """
        Verarbeitet eine einzelne Entität und reichert sie mit DBpedia-Daten an.
        
        Diese Methode wird von der BaseService-Klasse aufgerufen und implementiert die
        spezifische Logik für die DBpedia-Verarbeitung. Sie prüft zunächst den Cache,
        führt dann SPARQL-Abfragen durch und verwendet Fallback-Mechanismen, wenn nötig.
        
        Args:
            entity: Die zu verarbeitende Entität
            
        Returns:
            Die angereicherte Entität mit DBpedia-Daten
        """
        # Ensure we have an active aiohttp session for downstream fetcher calls
        if self.session is None or getattr(self.session, "closed", True):
            await self.create_session()

        self.processed_entities += 1
        logger.info(f"Verarbeite Entität '{entity.entity_name}' (ID: {entity.entity_id})")
        
        # Prüfe, ob die Entität bereits eine DBpedia-URI hat
        if hasattr(entity, 'dbpedia_data') and entity.dbpedia_data and entity.dbpedia_data.uri:
            logger.debug(f"Entität '{entity.entity_name}' hat bereits eine DBpedia-URI: {entity.dbpedia_data.uri}")
            return entity
        
        # Cache-Check
        cache_path = os.path.join(self.cache_dir, f"dbpedia_{entity.entity_name.lower()}.json")
        logger.debug(f"Prüfe Cache für '{entity.entity_name}' unter {cache_path}")
        cached_data = None
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                logger.info(f"Cache-Treffer für '{entity.entity_name}': {cached_data.get('uri', 'Kein URI im Cache')}")
                has_label = isinstance(cached_data.get('label'), dict) and ('en' in cached_data['label'] or any(cached_data['label'].values()))
                has_abstract = isinstance(cached_data.get('abstract'), dict) and ('en' in cached_data['abstract'] or any(cached_data['abstract'].values()))
                if cached_data.get('uri') and has_label and has_abstract:
                    logger.info(f"Gecachte Daten für '{entity.entity_name}' sind vollständig (URI, Label und Abstract vorhanden, bevorzugt Englisch). Markiere als 'linked'.")
                    cached_data['status'] = 'linked'
                    self.successful_entities += 1
                else:
                    logger.warning(f"Gecachte Daten für '{entity.entity_name}' sind unvollständig (fehlendes URI, Label oder Abstract).")
                # Don't convert to DBpediaData here to avoid import issues. Let downstream code handle raw dict.
                entity.output_data[self.service_name] = cached_data
                return entity
            except Exception as e:
                logger.error(f"Fehler beim Laden des Caches für '{entity.entity_name}': {str(e)}")
                cached_data = None
        
        logger.info(f"Kein Cache-Treffer für '{entity.entity_name}'. Starte DBpedia-Abfrage.")
        
        # Versuche zuerst SPARQL-Abfrage mit dem englischen Label, falls verfügbar
        try:
            # Bestimme den zu verwendenden Endpunkt
            endpoint = self._get_primary_endpoint()
            logger.debug(f"Verwende SPARQL-Endpunkt: {endpoint}")
            
            # Extrahiere das englische Label aus Wikipedia-Daten, wenn vorhanden
            english_label = None
            english_uri = None
            dbpedia_uri = None
            query_label = entity.entity_name
                   
            # Prüfe zuerst, ob wir ein spezielles Mapping für diese Entität haben
            special_mappings = {}
            if entity.entity_name in special_mappings:
                special_uri = special_mappings[entity.entity_name]
                logger.info(f"Spezial-Mapping für '{entity.entity_name}': {special_uri}")
                english_uri = special_uri
                # Extrahiere den englischen Begriff aus der URI
                english_term = special_uri.split('/')[-1].replace('_', ' ')
                english_label = english_term
                query_label = english_term
            
            # Wenn kein spezielles Mapping, prüfe auf wikipedia_multilang (EntityData) oder processing_data['wikipedia_multilang'] (EntityProcessingContext)
            elif (
                (hasattr(entity, 'wikipedia_multilang') and entity.wikipedia_multilang and getattr(entity.wikipedia_multilang, 'en', None))
                or (hasattr(entity, 'processing_data') and isinstance(entity.processing_data, dict) and entity.processing_data.get('wikipedia_multilang'))
            ):
                # Versuche zuerst das strukturierte WikipediaMultilangData-Objekt
                if hasattr(entity, 'wikipedia_multilang') and entity.wikipedia_multilang and getattr(entity.wikipedia_multilang, 'en', None):
                    english_label = entity.wikipedia_multilang.en.label
                else:
                    # Fallback: dict-Struktur aus processing_data
                    wiki_ml = entity.processing_data.get('wikipedia_multilang', {}) if hasattr(entity, 'processing_data') else {}
                    if isinstance(wiki_ml, dict):
                        english_label = wiki_ml.get('en', {}).get('label')
                if english_label:
                    # Verwerfe potenziell falsches "englisches" Label, wenn es identisch zum deutschen Namen ist
                    # oder offensichtlich deutsche Sonderzeichen enthält (ä, ö, ü, ß).
                    if english_label.lower() == entity.entity_name.lower() or re.search(r"[äöüßÄÖÜ]", english_label):
                        logger.debug(f"Ignoriere vermeintliches englisches Label '{english_label}' für '{entity.entity_name}' – entspricht deutschem Namen oder enthält deutsche Umlaute")
                        english_label = None
                    else:
                        query_label = english_label
                        english_uri = self._create_dbpedia_uri_from_label(english_label)
                        logger.info(f"Generierter DBpedia-URI aus englischem Label (multilang) '{english_label}': {english_uri}")
            
            # Fallback: Prüfe auf wikipedia_data (ältere Struktur)
            elif hasattr(entity, 'wikipedia_data') and entity.wikipedia_data:
                if isinstance(entity.wikipedia_data, dict):
                    # First try explicit English label
                    english_label = entity.wikipedia_data.get("label_en")
                    labels_dict = entity.wikipedia_data.get("labels", {}) if isinstance(entity.wikipedia_data.get("labels"), dict) else {}
                    if not english_label and labels_dict:
                        english_label = labels_dict.get("en")
                    if not english_label and entity.wikipedia_data.get("fallback_title"):
                        english_label = entity.wikipedia_data.get("fallback_title")
                    if english_label:
                        dbpedia_uri = self._create_dbpedia_uri_from_label(english_label)
            
            # Wenn kein englisches Label gefunden wurde, versuche Übersetzung per OpenAI-Prompt
            if not english_label:
                translated_label = translate_term_to_en(entity.entity_name, self.config)
                if translated_label and translated_label.lower() != entity.entity_name.lower():
                    english_label = translated_label
                    query_label = english_label
                    english_uri = self._create_dbpedia_uri_from_label(english_label)
                    logger.info(f"Übersetzung via OpenAI für '{entity.entity_name}': '{english_label}' -> URI {english_uri}")
                    direct_uris = [english_uri] if english_uri else []
                else:
                    # Letzter Fallback: Generiere URI aus deutschem Namen
                    dbpedia_uri = self._create_dbpedia_uri_from_label(entity.entity_name)
                    logger.warning(
                        f"Kein englisches Label gefunden. Fallback: Generierter DBpedia-URI aus Entitätsnamen '{query_label}': {dbpedia_uri}"
                    )
                    direct_uris = [dbpedia_uri] if dbpedia_uri else []
            else:
                # Verwende den englischen URI für die SPARQL-Abfrage
                direct_uris = [english_uri] if english_uri else []
                logger.debug(f"Englisches Label gefunden: '{english_label}', generierter URI: {english_uri}")
        
            # Debug-Ausgabe
            logger.debug(f"Führe SPARQL-Abfrage für '{entity.entity_name}' mit Query-Label '{query_label}' durch")
            logger.info(f"Direkte URIs für SPARQL: {direct_uris}")
            
            # Führe SPARQL-Abfrage durch
            logger.info(f"Führe SPARQL-Abfrage für '{entity.entity_name}' mit Query-Label '{query_label}' und Endpunkt {endpoint} durch")
            dbpedia_results = await fetch_dbpedia_sparql(
                session=self.session,
                labels=[query_label],
                endpoint=endpoint,
                language="en",
                timeout=self.timeout,
                direct_uris=direct_uris
            )
            
            # Prüfe, ob wir Daten für die Entität haben
            if dbpedia_results and query_label in dbpedia_results:
                dbpedia_data = dbpedia_results[query_label]
                logger.debug(f"SPARQL-Abfrage erfolgreich für '{entity.entity_name}', URI: {dbpedia_data.uri if dbpedia_data.uri else 'N/A'}")
                
                # Entity is only linked if URI, label, and abstract are present
                if dbpedia_data.uri and dbpedia_data.label and dbpedia_data.abstract:
                    dbpedia_data.status = 'linked'
                    logger.info(f"Entität '{entity.entity_name}' als 'linked' markiert, da URI, Label und Abstract vorhanden sind.")
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump(dbpedia_data.dict(), f, ensure_ascii=False)
                else:
                    dbpedia_data.status = 'not_linked'
                    logger.warning(f"Entität '{entity.entity_name}' nicht als 'linked' markiert, da URI, Label oder Abstract fehlen.")
                entity.output_data[self.service_name] = dbpedia_data.dict()
                return entity
            else:
                logger.warning(f"SPARQL-Abfrage lieferte keine Daten für '{entity.entity_name}' mit Endpunkt {endpoint}")
        except Exception as e:
            logger.warning(f"SPARQL-Abfrage fehlgeschlagen für '{entity.entity_name}': {str(e)}")
    
        # Wenn SPARQL fehlschlägt oder unvollständige Daten liefert, versuche DBpedia Lookup API
        try:
            query = english_label if english_label else entity.entity_name
            logger.debug(f"Versuche DBpedia Lookup API für '{entity.entity_name}' mit Query: '{query}'")
            logger.info(f"Lookup API Query: '{query}' (Englisches Label: {bool(english_label)})")
            
            dbpedia_data = await fetch_dbpedia_lookup(
                session=self.session,
                query=query,
                language="en",
                timeout=self.timeout,
                max_results=self.max_lookup_results,
                force_english=True
            )
            
            if dbpedia_data:
                # Entity is only linked if URI, label, and abstract are present
                if dbpedia_data.uri and dbpedia_data.label and dbpedia_data.abstract:
                    dbpedia_data.status = 'linked'
                    logger.info(f"Entität '{entity.entity_name}' als 'linked' markiert (Lookup API), da URI, Label und Abstract vorhanden sind.")
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump(dbpedia_data.dict(), f, ensure_ascii=False)
                else:
                    dbpedia_data.status = 'not_linked'
                    logger.warning(f"Entität '{entity.entity_name}' nicht als 'linked' markiert (Lookup API), da URI, Label oder Abstract fehlen.")
                entity.output_data[self.service_name] = dbpedia_data.dict()
                return entity
            else:
                logger.warning(f"Lookup API lieferte keine Daten für '{entity.entity_name}'")
        except Exception as e:
            logger.warning(f"DBpedia Lookup API fehlgeschlagen für '{entity.entity_name}': {str(e)}")
    
        # Wenn wir hier ankommen, konnten wir die Entität nicht erfolgreich verknüpfen
        self.failed_entities += 1
        entity.output_data[self.service_name] = {"status": "not_found"}
        logger.warning(f"Entität '{entity.entity_name}' konnte nicht verknüpft werden")
        return entity
        
    async def process_batch(self, batch: List[EntityData]) -> List[EntityData]:
        """
        Verarbeitet einen Batch von Entitäten parallel.
        
        Diese Methode überschreibt die process_batch-Methode der BaseService-Klasse,
        um zusätzliche Logik für die Batch-Verarbeitung zu implementieren.
        
        Args:
            batch: Liste von Entitäten, die verarbeitet werden sollen
            
        Returns:
            Liste der verarbeiteten Entitäten
        """
        start_time = time.time()
        logger.info(f"Starte Batch-Verarbeitung für {len(batch)} Entitäten")
        
        # Vor der Batch-Verarbeitung: Zeige die verwendeten URIs für jede Entität an
        logger.info("=== Verwendete URIs für DBpedia-Abfragen ===")
        for entity in batch:
            # Versuche, ein englisches Label aus Wikipedia zu bekommen
            english_label = None
            english_uri = None
            dbpedia_uri = None
            
            if hasattr(entity, 'wikipedia_data') and entity.wikipedia_data:
                if entity.wikipedia_data.labels and "en" in entity.wikipedia_data.labels:
                    english_label = entity.wikipedia_data.labels["en"]
                    dbpedia_uri = self._create_dbpedia_uri_from_label(english_label)
                elif hasattr(entity.wikipedia_data, 'fallback_title') and entity.wikipedia_data.fallback_title:
                    english_label = entity.wikipedia_data.fallback_title
                    dbpedia_uri = self._create_dbpedia_uri_from_label(english_label)
            
            # Wenn kein englisches Label verfügbar ist, verwende den Entitätsnamen direkt
            if not english_label:
                dbpedia_uri = self._create_dbpedia_uri_from_label(entity.entity_name)
                logger.info(f"Entität: '{entity.entity_name}' -> Kein englisches Label -> URI: {dbpedia_uri}")
            else:
                logger.info(f"Entität: '{entity.entity_name}' -> Englisches Label: '{english_label}' -> URI: {dbpedia_uri}")
        
        # Verwende die process_batch-Methode der Basisklasse für die parallele Verarbeitung
        processed_entities = await super().process_batch(batch)
        
        # Logge Statistiken
        duration = time.time() - start_time
        logger.info(f"Batch-Verarbeitung abgeschlossen in {duration:.2f}s")
        logger.info(f"Statistik: {self.successful_entities} erfolgreich, {self.failed_entities} fehlgeschlagen, {self.cache_hits} Cache-Treffer")
        
        return processed_entities


async def process_entities(entities: List[EntityData], config: Optional[Dict[str, Any]] = None) -> List[EntityData]:
    """
    Verarbeitet eine Liste von Entitäten mit dem DBpediaService.
    
    Diese Funktion erstellt einen DBpediaService und verarbeitet die übergebenen Entitäten.
    Sie stellt sicher, dass die Session nach der Verarbeitung ordnungsgemäß geschlossen wird.
    
    Args:
        entities: Liste von Entitäten, die verarbeitet werden sollen
        config: Optionales Konfigurationswörterbuch
        
    Returns:
        Liste der verarbeiteten Entitäten
    """
    service = DBpediaService(config)
    async with service:
        processed_entities = await service.process_batch(entities)
        return processed_entities


async def _test_service_pipeline():
    """
    End-to-End Test pipeline für den DBpediaService.
    """
    logger.info("Starte DBpediaService Test-Pipeline...")
    
    # Konfiguration laden
    config = get_config()
    config['DBPEDIA_BATCH_SIZE'] = 5  # Kleinere Batch-Größe für Tests
    
    # Erstelle Test-Entitäten - entfernt, um keine Beispieldaten im Code zu haben
    test_entities = []
    logger.warning("Test-Entitäten wurden entfernt, um keine Beispieldaten im Code zu haben. Bitte fügen Sie bei Bedarf eigene Testdaten hinzu.")
    
    # Initialisiere den Service
    async with DBpediaService(config=config) as service:
        # Verarbeite die Test-Entitäten
        processed_entities = await service.process_batch(test_entities)
        
        # Ausgabe der Ergebnisse
        logger.info("Test-Pipeline Ergebnisse:")
        for entity in processed_entities:
            if hasattr(entity, 'output_data') and entity.output_data and service.service_name in entity.output_data:
                logger.info(f"Entität: {entity.entity_name} -> Status: {entity.output_data[service.service_name].get('status', 'N/A')}")
                if entity.output_data[service.service_name].get('status') == "linked":
                    logger.info(f"  URI: {entity.output_data[service.service_name].get('uri', 'N/A')}")
                    logger.info(f"  Label (en): {entity.output_data[service.service_name].get('label', {}).get('en', 'N/A')}")
                    logger.info(f"  Abstract (en): {entity.output_data[service.service_name].get('abstract', {}).get('en', 'N/A')[:100]}...")
            else:
                logger.warning(f"Entität: {entity.entity_name} -> Keine DBpedia-Daten gefunden")
        
        # Statistik
        logger.info(f"Statistik: {service.successful_entities} erfolgreich, {service.failed_entities} fehlgeschlagen, {service.cache_hits} Cache-Treffer")

if __name__ == '__main__':
    # Führe die Test-Pipeline aus
    asyncio.run(_test_service_pipeline())
