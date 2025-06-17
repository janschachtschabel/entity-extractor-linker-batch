#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Neuer Wikipedia-Service mit klaren Datenstrukturen

Diese neue Service-Implementierung nutzt den EntityProcessingContext für strukturierte
Datenübergabe und Schema-Validierung. Sie setzt auf die bewährten Batch-Funktionen
des bestehenden Services auf, aber mit verbesserter Ausgabestruktur und ausgelagerten 
Fallback-Mechanismen.
"""

import os
import logging
import asyncio
import aiohttp
from typing import Dict, List, Any, Optional, Tuple

from entityextractor.config.settings import get_config
from entityextractor.core.context import EntityProcessingContext
from entityextractor.schemas.service_schemas import validate_wikipedia_data
from entityextractor.services.wikipedia.async_fetchers import async_fetch_wikipedia_data, async_fetch_multilang_wikipedia_data
from entityextractor.services.wikipedia.fallbacks import apply_all_fallbacks
from entityextractor.utils.cache_utils import load_cache, save_cache
from entityextractor.utils.category_utils import filter_category_counts
from entityextractor.utils.logging_utils import get_service_logger

# Configure logger using loguru
from loguru import logger

# --- NEW: external libs
import itertools
import requests
logger = get_service_logger(__name__, 'wikipedia')

class WikipediaService:
    """
    Neuer Service für die Verarbeitung von Wikipedia-Anfragen mit verbesserter Datenstruktur.
    Nutzt den EntityProcessingContext für strukturierte Datenübergabe und separate Fallback-Module.
    """

    async def _lookup_en_title(self, de_title: str) -> Optional[str]:
        """Try to map a German Wikipedia title to its English counterpart via langlinks,
        falling back to Wikidata sitelinks. Returns the English title or ``None``.
        Uses the existing aiohttp session.
        """
        if not de_title:
            return None
        await self.create_session()
        try:
            params = {
                "action": "query",
                "titles": de_title,
                "redirects": 1,
                "prop": "langlinks|pageprops",
                "lllang": "en",
                "lllimit": "max",
                "ppprop": "wikibase_item",
                "format": "json"
            }
            async with self.session.get("https://de.wikipedia.org/w/api.php", params=params) as resp:
                data = await resp.json()
            page = next(iter(data.get("query", {}).get("pages", {}).values()), {})
            langlinks = page.get("langlinks", [])
            if langlinks:
                return langlinks[0].get("*")
            qid = page.get("pageprops", {}).get("wikibase_item")
            if not qid:
                return None
            # Wikidata fallback
            wd_params = {
                "action": "wbgetentities",
                "ids": qid,
                "props": "sitelinks",
                "sitefilter": "enwiki",
                "format": "json"
            }
            async with self.session.get("https://www.wikidata.org/w/api.php", params=wd_params) as resp:
                wd = await resp.json()
            ent = wd.get("entities", {}).get(qid, {})
            sitelink = ent.get("sitelinks", {}).get("enwiki", {})
            return sitelink.get("title")
        except Exception as exc:
            self.logger.warning(f"[LookupEN] Fehler beim Nachschlagen des englischen Titels für '{de_title}': {exc}")
            return None

    async def fetch_multilang_batch(self, urls: List[str]) -> Dict[str, Dict[str, Dict]]:
        """
        Fetch both German and English labels/metadata for a list of Wikipedia URLs.
        Returns: {url: {'de': {...}, 'en': {...}}}
        """
        self.logger.info(f"Starte Multi-Language Wikipedia-Batch für {len(urls)} URLs.")
        result = await async_fetch_multilang_wikipedia_data(urls, self.user_agent, self.config)
        self.logger.info(f"Multi-Language Wikipedia-Batch abgeschlossen.")
        return result
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialisiert den WikipediaService.
        
        Args:
            config: Optionale Konfiguration (verwendet Default-Konfiguration, falls nicht angegeben)
        """
        self.config = config or get_config()
        self.logger = logger
        
        # API-Konfiguration
        self.batch_size = self.config.get('WIKIPEDIA_BATCH_SIZE', 20)
        
        # API-URL basierend auf der Spracheinstellung dynamisch setzen
        language = self.config.get('LANGUAGE', 'en')
        default_api_url = f"https://{language}.wikipedia.org/w/api.php"
        self.api_url = self.config.get('WIKIPEDIA_API_URL', default_api_url)
        self.logger.info(f"WikipediaService verwendet API-URL: {self.api_url} (Sprache: {language})")
        
        self.user_agent = self.config.get('USER_AGENT', 'EntityExtractor/1.0')
        
        # Cache-Konfiguration
        self.cache_dir = os.path.join(self.config.get('CACHE_DIR', 'entityextractor_cache'), "wikipedia")
        os.makedirs(self.cache_dir, exist_ok=True)
        self.logger.debug(f"WikipediaService verwendet Cache-Verzeichnis: {self.cache_dir}")
        
        # Debug-Flags
        self.debug_mode = self.config.get('DEBUG_WIKIPEDIA', False)
        self.logger.debug(f"WikipediaService Debug-Modus: {self.debug_mode}")
        
        # Fallback-Konfiguration
        self.use_fallbacks = self.config.get('WIKIPEDIA_USE_FALLBACKS', True)
        self.max_fallback_attempts = self.config.get('WIKIPEDIA_MAX_FALLBACK_ATTEMPTS', 3)
        self.always_run_fallbacks = self.config.get('WIKIPEDIA_ALWAYS_RUN_FALLBACKS', False)
        self.logger.debug(f"Fallback-Mechanismen aktiviert: {self.use_fallbacks}, Max Versuche: {self.max_fallback_attempts}, Immer Fallbacks: {self.always_run_fallbacks}")
        
        # HTTP Session
        self.session = None
        
        # Statistikzähler
        self.successful_entities = 0
        self.partial_entities = 0
        self.failed_entities = 0
        self.fallback_successes = 0
        self.processed_contexts = 0

    # -------------------------------------------------------------
    # Helper: parse a Wikipedia URL into (language, title)
    def _parse_wikipedia_url(self, url: str) -> Tuple[str, str]:
        """Return (lang, title) derived from a full Wikipedia URL or ("", "") on failure."""
        try:
            import urllib.parse
            p = urllib.parse.urlparse(url)
            lang = p.netloc.split('.')[0]
            if '/wiki/' not in p.path:
                return "", ""
            title = urllib.parse.unquote(p.path.split('/wiki/')[1]).replace('_', ' ')
            return lang, title
        except Exception:
            return "", ""
    
    # -------------------------------------------------------------
    # Helper: map a title to the configured language using langlinks
    async def _map_title_to_language(self, title: str, target_lang: str) -> Optional[str]:
        """Return the page title in target_lang via Wikipedia langlinks, or None."""
        import urllib.parse
        if target_lang == "en":
            return None
        params = {
            "action": "query",
            "titles": title,
            "prop": "langlinks",
            "lllang": target_lang,
            "format": "json",
            "formatversion": "2"
        }
        url = f"https://en.wikipedia.org/w/api.php?{urllib.parse.urlencode(params)}"
        try:
            if not self.session:
                await self.create_session()
            async with self.session.get(url, timeout=self.config.get("HTTP_TIMEOUT", 10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                pages = data.get("query", {}).get("pages", [])
                if pages and pages[0].get("langlinks"):
                    return pages[0]["langlinks"][0]["title"]
        except Exception as exc:
            self.logger.debug(f"[langlinks] mapping failed: {exc}")
        return None

    async def process_entity(self, context: EntityProcessingContext) -> None:
        """
        Verarbeitet eine Entität mit Wikipedia-Daten und aktualisiert den Kontext.
        
        Args:
            context: Verarbeitungskontext der Entität
        """
        entity_name = context.entity_name
        
        # ---------------------------------------------------------
        # Wenn bereits eine Wikipedia-URL vorhanden ist (im Entity-Namen oder in additional_data),
        # extrahiere Titel + Sprache und nutze diese für den ersten API-Call.
        api_title = entity_name  # Default
        candidate_url: Optional[str] = None
        if isinstance(entity_name, str) and entity_name.startswith("http"):
            candidate_url = entity_name
        elif isinstance(getattr(context, "additional_data", {}), dict):
            candidate_url = context.additional_data.get("wikipedia_url") or context.additional_data.get("url")
        
        if candidate_url and "wikipedia.org" in candidate_url:
            lang_from_url, title_from_url = self._parse_wikipedia_url(candidate_url)
            configured_lang = self.config.get("LANGUAGE", "de")
            if lang_from_url and title_from_url and lang_from_url == configured_lang:
                self.logger.debug(f"[URL-Primary] Nutze vorhandene Wikipedia-URL '{candidate_url}' → Titel '{title_from_url}' für ersten API-Call")
                api_title = title_from_url
                # Stelle sicher, dass die API-URL ebenfalls auf diese Sprache zeigt
                self.api_url = f"https://{lang_from_url}.wikipedia.org/w/api.php"
        else:
            # Kein URL-Hinweis – versuche, falls entity_name vermutlich Englisch ist, den deutschen Titel via langlinks zu ermitteln
            configured_lang = self.config.get("LANGUAGE", "de")
            if configured_lang != "en":
                mapped = await self._map_title_to_language(entity_name, configured_lang)
                if mapped:
                    self.logger.debug(f"[langlinks] Ersetze '{entity_name}' durch '{mapped}' für den Primäraufruf")
                    api_title = mapped
        if not entity_name:
            self.logger.warning(f"EntityProcessingContext ohne Entitätsnamen übersprungen")
            self.failed_entities += 1
            return
            
        self.logger.info(f"Verarbeite Entität '{entity_name}' mit Wikipedia-Service")
        
        # 1. Cache überprüfen
        cache_path = os.path.join(self.cache_dir, f"{api_title.lower().replace(' ', '_')}.json")
        cached_result = None
        
        if self.config.get('CACHE_WIKIPEDIA_ENABLED', True):
            cached_result = load_cache(cache_path)
        
        # Variablen für die Wikipedia-Daten initialisieren
        wikipedia_data = None
        wiki_result = None
            
        if cached_result:
            # Wenn es ein Treffer ist und das Ergebnis bereits das richtige Format hat,
            # verwenden wir es direkt
            if isinstance(cached_result, dict) and 'wikipedia' in cached_result:
                wikipedia_data = cached_result
                extract_length = len(cached_result.get('wikipedia', {}).get('extract', ''))
                wikidata_id = cached_result.get('wikipedia', {}).get('wikidata_id', 'keine')
                self.logger.info(f"[Cache] Vorformatierte Daten für '{entity_name}' gefunden. Extract: {extract_length} Zeichen, Wikidata-ID: {wikidata_id}")
                self.logger.debug(f"Cache-Inhalt vorformatiert: {list(cached_result['wikipedia'].keys())}")
            else:
                # Andernfalls behandeln wir es als Roh-API-Ergebnis
                wiki_result = cached_result
                extract_length = len(wiki_result.get('extract', ''))
                self.logger.info(f"[Cache] Roh-API-Daten für '{entity_name}' gefunden. Extract: {extract_length} Zeichen")
                self.logger.debug(f"Cache-Inhalt roh: {list(wiki_result.keys()) if isinstance(wiki_result, dict) else 'Kein Dictionary'}")
        
        # Wikipedia-API abfragen, wenn nötig
        fallback_attempts = 0
        if not cached_result:
            self.logger.info(f"[API] Frage Wikipedia-API für '{entity_name}' ab")
            try:
                # API-Abfrage für eine einzelne Entität
                api_results = await async_fetch_wikipedia_data(
                    [api_title], 
                    self.api_url, 
                    self.user_agent, 
                    self.config
                )
                
                # Extrahiere das Ergebnis für diese Entität aus dem Dictionary
                # Die API gibt ein Dictionary zurück, wobei die Schlüssel die Titel sind
                wiki_result = api_results.get(api_title, {})
                
                if wiki_result:
                    extract_length = len(wiki_result.get('extract', ''))
                    wikidata_id = wiki_result.get('wikidata_id', 'keine')
                    status = "gefunden (mit Extract)" if wiki_result.get('extract') else "teilweise (ohne Extract)"
                    
                    self.logger.info(f"[API] Ergebnis für '{entity_name}' erhalten. Status: {status}, Extract: {extract_length} Zeichen, Wikidata-ID: {wikidata_id}")
                    self.logger.debug(f"API-Antwort-Keys: {list(wiki_result.keys())}")
                    if wiki_result.get('extract') and self.debug_mode:
                        self.logger.debug(f"Extract-Anfang: '{wiki_result['extract'][:100]}...'")
                    
                    # Speichere das Roh-Ergebnis im Cache, wenn aktiviert
                    if self.config.get('CACHE_WIKIPEDIA_ENABLED', True):
                        save_cache(cache_path, wiki_result)
                        self.logger.debug(f"[Cache] Wikipedia-Ergebnis für '{entity_name}' gespeichert")
                else:
                    self.logger.warning(f"[API] Kein Ergebnis von Wikipedia-API für '{entity_name}'")
                    wiki_result = None
            except Exception as e:
                self.logger.error(f"[API] Fehler bei Wikipedia-API-Abfrage für '{entity_name}': {str(e)}")
                wiki_result = None
        
        # Setze needs_fallback Flag - dies ist unabhängig davon, ob Fallbacks aktiviert sind
        # Fallback is needed only when no extract is present at all (or result is missing).
        # Previously, we also triggered a fallback when the extract was deemed "too short" (<100 chars),
        # which caused the service to replace valid extracts from the configured language (e.g. German)
        # with English ones. To respect the LANGUAGE setting, we now skip this length-based check.
        needs_fallback = (wiki_result is None or not wiki_result.get('extract'))
        fallback_attempts = 0  # Default, wird ggf. von apply_all_fallbacks überschrieben
        fallback_source = None
        
        # Entscheide, ob Fallbacks immer ausgeführt werden sollen (Debugging/Analyse) oder nur bei Bedarf
        always_run_fallbacks = self.config.get('WIKIPEDIA_ALWAYS_RUN_FALLBACKS', False)
        # Fallbacks werden ausgeführt, wenn entweder needs_fallback ODER always_run_fallbacks aktiv ist (und use_fallbacks aktiviert ist)
        run_fallbacks = self.use_fallbacks and (needs_fallback or always_run_fallbacks)

        primary_url = wiki_result.get('url') if wiki_result else None
        primary_lang = wiki_result.get('language') if wiki_result else None

        if run_fallbacks:
          # 3. Ergebnis verarbeiten und ggf. Fallbacks ausführen (für Logging)
            if needs_fallback:
                if wiki_result is None:
                    reason = "nicht gefunden"
                elif not wiki_result.get('extract'):
                    reason = "kein Extract"
                # Wenn ein Fallback-Ergebnis vorliegt, aber die Sprache nicht der Konfiguration entspricht,
                # behalten wir die URL der ursprünglichen Sprache
                if wiki_result and primary_url and wiki_result.get('language') != self.config.get("LANGUAGE", "de"):
                    wiki_result.setdefault('alternate_urls', {})[self.config.get("LANGUAGE", "de")] = primary_url
                    wiki_result['url'] = primary_url
                else:
                    if wiki_result:
                        extract_length = len(wiki_result.get('extract', ''))
                        reason = f"Extract zu kurz ({extract_length} Zeichen < 100)"
                    else:
                        reason = "kein Ergebnis"
            else:
                reason = "Debug/Analyse: Fallbacks werden immer ausgeführt (Konfigurations-Flag)"
            self.logger.info(f"[Fallbacks] Starte Fallback-Strategien für '{entity_name}' (Grund: {reason})")

            # Fallback-Logik ist in das Modul fallbacks.py ausgelagert
            wiki_result, fallback_attempts = await apply_all_fallbacks(
                entity_name,
                wiki_result,
                self.api_url,
                self.user_agent,
                self.config,
                self.max_fallback_attempts
            )
            # Fallback-Quelle ggf. extrahieren
            fallback_source = wiki_result.get('fallback_source') if wiki_result else None
            # Erfolg des Fallbacks protokollieren
            if wiki_result and wiki_result.get('extract'):
                extract_length = len(wiki_result.get('extract', ''))
                wikidata_id = wiki_result.get('wikidata_id', 'keine')
                fallback_source = wiki_result.get('fallback_source', 'unbekannt')
                self.logger.info(f"[Fallbacks] Strategie '{fallback_source}' erfolgreich für '{entity_name}' nach {fallback_attempts} Versuchen. Extract: {extract_length} Zeichen, Wikidata-ID: {wikidata_id}")
                self.fallback_successes += 1
                if self.config.get('CACHE_WIKIPEDIA_ENABLED', True) and fallback_attempts > 0:
                    save_cache(cache_path, wiki_result)
                    self.logger.debug(f"[Cache] Fallback-Ergebnis für '{entity_name}' gespeichert")
            else:
                self.logger.warning(f"[Fallbacks] Alle Strategien für '{entity_name}' fehlgeschlagen nach {fallback_attempts} Versuchen")
        elif needs_fallback:
            # needs_fallback=True, aber Fallbacks sind deaktiviert
            self.logger.info(f"[Fallbacks] Fallbacks für '{entity_name}' wären nötig, sind aber deaktiviert. Kein Extract gefunden/zu kurz.")
        
        # Fallback-Metadaten IMMER im wiki_result setzen, auch im Fehlerfall
        if wiki_result is None:
            wiki_result = {}
        wiki_result['needs_fallback'] = needs_fallback
        wiki_result['fallback_attempts'] = fallback_attempts
        if fallback_source:
            wiki_result['fallback_source'] = fallback_source
        
        # Formatiere das Ergebnis für den Kontext
        if wiki_result:
            # Validiere das Ergebnis vor der Formatierung
            validation_result = validate_wikipedia_data(wiki_result)
            if isinstance(validation_result, dict):  # Fehler bei der Validierung
                self.logger.warning(f"Schema-Validierung fehlgeschlagen für '{entity_name}': {validation_result.get('error')}")
                wikipedia_data = self._format_wikipedia_result(wiki_result, entity_name, needs_fallback)
            elif validation_result is True:  # Erfolgreiche Validierung
                wikipedia_data = self._format_wikipedia_result(wiki_result, entity_name, needs_fallback)
            else:  # Bei anderen Rückgabewerten vorsichtshalber formatieren
                self.logger.warning(f"Unerwartetes Validierungsergebnis für '{entity_name}': {validation_result}")
                wikipedia_data = self._format_wikipedia_result(wiki_result, entity_name, needs_fallback)
        else:
            # Kein Ergebnis - leere Struktur erstellen mit needs_fallback=True
            wikipedia_data = self._format_wikipedia_result(None, entity_name, True)
        
        # Aktualisiere den Kontext mit den Wikipedia-Daten
        if 'wikipedia' in wikipedia_data:
            # Übergib nur den inneren 'wikipedia'-Block, um doppelte Verschachtelung zu vermeiden
            context.add_service_data('wikipedia', wikipedia_data.get('wikipedia', {}))
            
            # Wikidata-ID für andere Services verfügbar machen
            wikidata_id = wikipedia_data.get("wikipedia", {}).get("wikidata_id")
            if wikidata_id:
                context.set_processing_info("wikidata_id", wikidata_id)
                self.logger.debug(f"Wikidata-ID für '{entity_name}' gefunden: {wikidata_id}")
        else:
            # Fallback für unerwartete Strukturen
            self.logger.warning(f"Unerwartetes Datenformat für '{entity_name}', erstelle minimale Struktur")
            context.add_service_data('wikipedia', {
                'wikipedia': {
                    'label': entity_name,
                    'status': 'not_found',
                    'source': 'format_error'
                }
            })
        # --- Multilang-Block: Schreibe wikipedia_multilang ins context.processing_data ---
        wiki_data = wikipedia_data.get('wikipedia', {})
        multilang_entry = {}
        lang = self.config.get('LANGUAGE', 'de')
        if wiki_data:
            multilang_entry[lang] = {
                'label': wiki_data.get('label'),
                'description': wiki_data.get('extract') or wiki_data.get('description'),
                'url': wiki_data.get('url'),
            }
        wiki_url = wiki_data.get('url') or getattr(context, 'wikipedia_url', None)
        if wiki_url and 'wikipedia_multilang' not in context.processing_data:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Falls wir in einer laufenden Event-Loop sind (z.B. Jupyter), fetch_multilang_batch als Task
                    ml_batch = await self.fetch_multilang_batch([wiki_url])
                else:
                    ml_batch = loop.run_until_complete(self.fetch_multilang_batch([wiki_url]))
                ml = ml_batch.get(wiki_url, {})
                # Merge fetched multilang data first
                for k in ('en', 'de'):
                    if k in ml and ml[k]:
                        multilang_entry[k] = {
                            'label': ml[k].get('label'),
                            'description': ml[k].get('description'),
                            'url': ml[k].get('url'),
                        }
                # If no English entry came back but we already have an English label
                if 'en' not in multilang_entry:
                    label_en = None
                    if isinstance(wiki_data.get('labels'), dict):
                        label_en = wiki_data.get('labels', {}).get('en')
                    label_en = label_en or wiki_data.get('label_en')
                    if label_en:
                        multilang_entry['en'] = {
                            'label': label_en,
                            'description': wiki_data.get('extract') or wiki_data.get('description'),
                            'url': wiki_data.get('url'),
                        }
                        self.logger.debug(f"[Multilang] Ergänze English label from primary result for '{entity_name}': {label_en}")
            except Exception as e:
                self.logger.warning(f"Fehler beim Nachholen von Multilang-Daten für '{entity_name}': {e}")
        if multilang_entry:
            context.processing_data['wikipedia_multilang'] = multilang_entry
            en_label = multilang_entry.get('en', {}).get('label')
            self.logger.info(f"[DEBUG] Nach Wikipedia: {entity_name} hat wikipedia_multilang: {context.processing_data.get('wikipedia_multilang')}")
            self.logger.info(f"[DEBUG] Kontext-ID nach Multilang-Setzen: {id(context)} für '{entity_name}'")
            if en_label:
                self.logger.info(f"[Wikipedia-Multilang] Entity '{entity_name}': Englisches Label = '{en_label}'")
            else:
                self.logger.warning(f"[Wikipedia-Multilang] Entity '{entity_name}': Kein englisches Label gefunden!")
        else:
            self.logger.warning(f"[Wikipedia-Multilang] Entity '{entity_name}': Keine Multilang-Daten verfügbar!")
        
        # Aktualisiere Statistik basierend auf dem Status
        status = wikipedia_data.get('wikipedia', {}).get('status', 'not_found')
        
        # Zähler aktualisieren - kein 'partial' Status mehr
        if status == 'found':
            self.successful_entities += 1
        else:
            self.failed_entities += 1
        
        self.processed_contexts += 1
        
        # Datenquelle bestimmen (Cache, API oder Fallback)
        source = wikipedia_data.get('wikipedia', {}).get('source', 'unbekannt')
        fallback_source = wikipedia_data.get('wikipedia', {}).get('fallback_source', '')
        
        if fallback_source:
            source_text = f"Fallback ({fallback_source})"
        elif cached_result:
            source_text = "Cache"
        else:
            source_text = "API"
            
        extract_length = len(wikipedia_data.get('wikipedia', {}).get('extract', ''))
        wikidata_id = wikipedia_data.get('wikipedia', {}).get('wikidata_id', 'keine')
            
        self.logger.info(f"[Ergebnis] Entity '{entity_name}' verarbeitet. Status: {status}, Quelle: {source_text}, Extract: {extract_length} Zeichen, Wikidata-ID: {wikidata_id}")
        
        # Debug-Ausgabe der im Kontext gespeicherten Daten
        if self.debug_mode:
            self.logger.debug(f"Gespeicherte Service-Daten im Kontext für '{entity_name}':")
            self.logger.debug(f"  - Service-Daten-Keys: {list(context.service_data.keys()) if hasattr(context, 'service_data') else 'keine'}")
            if hasattr(context, 'service_data') and 'wikipedia' in context.service_data:
                wiki_data = context.service_data['wikipedia']
                self.logger.debug(f"  - Wikipedia-Daten-Keys: {list(wiki_data.keys()) if wiki_data else 'keine'}")
                if 'wikipedia' in wiki_data:
                    wiki_source = wiki_data['wikipedia']
                    self.logger.debug(f"  - Wikipedia-Source-Keys: {list(wiki_source.keys()) if wiki_source else 'keine'}")
                    self.logger.debug(f"  - Status: {wiki_source.get('status', 'unbekannt')}")
                    self.logger.debug(f"  - Extract vorhanden: {bool(wiki_source.get('extract', ''))}")
                    self.logger.debug(f"  - Wikidata-ID: {wiki_source.get('wikidata_id', 'keine')}")
                    self.logger.debug(f"  - URL: {wiki_source.get('url', 'keine')}")
                else:
                    self.logger.debug("  - Keine 'wikipedia' in wiki_data")
            else:
                self.logger.debug("  - Keine 'wikipedia' in service_data")
                
            # Zusätzliche detaillierte Debug-Ausgabe für die aktuellen Daten
            if 'wikipedia' in wikipedia_data:
                self.logger.debug(f"Wikipedia-Daten für '{entity_name}':")
                for key, value in wikipedia_data['wikipedia'].items():
                    if key in ['extract', 'categories', 'internal_links']:
                        # Für große Felder nur die Länge anzeigen
                        length = len(value) if value else 0
                        self.logger.debug(f"  - {key}: {length} Einträge/Zeichen")
                    else:
                        self.logger.debug(f"  - {key}: {value}")
    
    async def fetch_multilang_batch(self, wiki_urls: List[str]) -> Dict[str, Dict[str, Dict[str, str]]]:
        """
        Holt mehrsprachige Daten für eine Liste von Wikipedia-URLs.
        
        Args:
            wiki_urls: Liste von Wikipedia-URLs
            
        Returns:
            Dictionary mit URL als Schlüssel und mehrsprachigen Daten als Wert
        """
        if not wiki_urls:
            return {}
            
        # Stelle sicher, dass eine Session existiert
        await self.create_session()
            
        self.logger.info(f"[Multilang] Hole mehrsprachige Daten für {len(wiki_urls)} URLs")
        results = {}
        
        for url in wiki_urls:
            try:
                # URL analysieren, um Sprache und Titel zu extrahieren
                import urllib.parse
                lang = 'de'  # Standard
                if '/en.wikipedia.org/' in url:
                    lang = 'en'
                elif '/de.wikipedia.org/' in url:
                    lang = 'de'
                else:
                    # Versuche die Sprache aus der URL zu extrahieren
                    url_parts = urllib.parse.urlparse(url)
                    domain_parts = url_parts.netloc.split('.')
                    if len(domain_parts) > 0 and domain_parts[0] != 'www':
                        lang = domain_parts[0]
                        
                # Titel extrahieren
                title = url.split("/wiki/")[-1]
                title = urllib.parse.unquote(title)
                title_readable = title.replace('_', ' ')
                
                # Erstelle Basis-Eintrag für die Originalsprache
                results[url] = {
                    lang: {
                        'label': title_readable,
                        'url': url,
                        'description': None
                    }
                }
                
                # Hole Langlinks für die URL
                langlinks = await self._fetch_langlinks(url)
                
                # Füge Langlinks hinzu
                for ll_lang, ll_data in langlinks.items():
                    if ll_lang != lang:  # Vermeide Duplikate
                        ll_title = ll_data.get('url', '').split("/wiki/")[-1]
                        ll_title = urllib.parse.unquote(ll_title)
                        ll_title_readable = ll_title.replace('_', ' ')
                        
                        results[url][ll_lang] = {
                            'label': ll_title_readable,
                            'url': ll_data.get('url'),
                            'description': None
                        }
                
                # Stelle sicher, dass wir englische Daten haben (wichtig für DBpedia)
                if 'en' not in results[url] and lang != 'en':
                    # Versuche, englische Daten zu konstruieren
                    en_url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title)}"
                    try:
                        # Prüfe, ob die englische URL existiert
                        async with self.session.head(en_url, allow_redirects=True, timeout=5) as response:
                            if response.status == 200:
                                results[url]['en'] = {
                                    'label': title_readable,  # Verwende den Originaltitel als Fallback
                                    'url': en_url,
                                    'description': None
                                }
                                self.logger.info(f"[Multilang] Englische URL für '{title_readable}' konstruiert: {en_url}")
                    except Exception as e:
                        self.logger.warning(f"[Multilang] Fehler beim Konstruieren der englischen URL für '{title_readable}': {e}")
                        
            except Exception as e:
                self.logger.error(f"[Multilang] Fehler beim Verarbeiten von URL '{url}': {e}")
                results[url] = {}
                
        self.logger.info(f"[Multilang] Mehrsprachige Daten für {len(results)} URLs geholt")
        return results
        
    async def _fetch_langlinks(self, wiki_url: str) -> Dict[str, Dict[str, str]]:
        """
        Holt Langlinks für eine Wikipedia-URL.
        
        Args:
            wiki_url: Wikipedia-URL
            
        Returns:
            Dictionary mit Sprachcode als Schlüssel und Link-Daten als Wert
        """
        # Stelle sicher, dass eine Session existiert
        await self.create_session()
        
        try:
            # Extrahiere Titel und Sprache aus der URL
            import urllib.parse
            lang = 'de'  # Standard
            if '/en.wikipedia.org/' in wiki_url:
                lang = 'en'
            elif '/de.wikipedia.org/' in wiki_url:
                lang = 'de'
            else:
                # Versuche die Sprache aus der URL zu extrahieren
                url_parts = urllib.parse.urlparse(wiki_url)
                domain_parts = url_parts.netloc.split('.')
                if len(domain_parts) > 0 and domain_parts[0] != 'www':
                    lang = domain_parts[0]
                    
            title = wiki_url.split("/wiki/")[-1]
            title = urllib.parse.unquote(title)
            
            # Erstelle API-URL für Langlinks
            api_url = f"https://{lang}.wikipedia.org/w/api.php"
            params = {
                "action": "query",
                "format": "json",
                "prop": "langlinks",
                "titles": title,
                "lllimit": "500"
            }
            
            # Hole Langlinks
            async with self.session.get(api_url, params=params, timeout=10) as response:
                data = await response.json()
                pages = data.get("query", {}).get("pages", {})
                
                # Extrahiere Langlinks
                langlinks = {}
                for page_id, page_data in pages.items():
                    if "langlinks" in page_data:
                        for ll in page_data["langlinks"]:
                            ll_lang = ll.get("lang")
                            ll_title = ll.get("*")
                            if ll_lang and ll_title:
                                # Erstelle URL für diese Sprache
                                ll_url = f"https://{ll_lang}.wikipedia.org/wiki/{urllib.parse.quote(ll_title)}"
                                langlinks[ll_lang] = {
                                    "url": ll_url,
                                    "title": ll_title
                                }
                
                return langlinks
                
        except Exception as e:
            self.logger.error(f"[Langlinks] Fehler beim Holen von Langlinks für '{wiki_url}': {e}")
            return {}
    
        # -------------------------------------------------------------
    # Helper: Batch mapping between DE and EN titles via MediaWiki/Wikidata
    # -------------------------------------------------------------
    def _batched(self, seq, n=50):
        it = iter(seq)
        while True:
            chunk = list(itertools.islice(it, n))
            if not chunk:
                break
            yield chunk

    def _fetch_labels(self, titles, src_api, target_lang):
        """Fetch langlinks + wikidata sitelinks to map titles across languages.
        Returns dict {original_title: {'src_title': str|None, 'tgt_title': str|None}}
        Uses synchronous requests; lightweight and only for small batches when labels are missing.
        """
        results, need_qid = {}, set()
        session = requests.Session()
        for block in self._batched(titles, 50):
            params = {
                "action": "query",
                "redirects": 1,
                "titles": "|".join(block),
                "prop": "langlinks|pageprops",
                "lllang": target_lang,
                "lllimit": "max",
                "ppprop": "wikibase_item",
                "format": "json"
            }
            data = session.get(src_api, params=params, timeout=10).json()
            redirects = {rd["from"]: rd["to"] for rd in data.get("query", {}).get("redirects", [])}
            for page in data["query"]["pages"].values():
                src_title = None if page.get("missing") == "" else page.get("title")
                original = next((t for t in block if redirects.get(t, t) == src_title), None)
                ll = page.get("langlinks", [])
                tgt_title = ll[0]["*"] if ll else None
                qid = page.get("pageprops", {}).get("wikibase_item")
                if tgt_title is None and qid:
                    need_qid.add(qid)
                results[original] = {"src_title": src_title, "tgt_title": tgt_title, "qid": qid}
        # Wikidata fallback
        if need_qid:
            wd_params = {
                "action": "wbgetentities",
                "ids": "|".join(need_qid),
                "props": "sitelinks",
                "sitefilter": f"{target_lang}wiki",
                "format": "json"
            }
            wd = session.get("https://www.wikidata.org/w/api.php", params=wd_params, timeout=10).json()
            for ent in wd.get("entities", {}).values():
                qid = ent["id"]
                sitelink = ent.get("sitelinks", {}).get(f"{target_lang}wiki", {})
                title = sitelink.get("title")
                for rec in results.values():
                    if rec["qid"] == qid and rec["tgt_title"] is None:
                        rec["tgt_title"] = title
        return results

    async def _resolve_bilingual_labels_batch(self, contexts):
        """Ensure each context has both German and English labels/URLs before API calls."""
        # Gather titles needing mapping
        missing_en, missing_de = [], []
        for c in contexts:
            name = c.entity_name
            details = c.output_data.get("details", {})
            lang = self.config.get("LANGUAGE", "de")
            # Determine if english label missing
            if lang == "de":
                if not details.get("label_en") and not c.get_processing_info("label_en"):
                    missing_en.append(name)
            else:
                if not details.get("label_de") and not c.get_processing_info("label_de"):
                    missing_de.append(name)
        if not missing_en and not missing_de:
            return
        self.logger.info(f"[LabelBatch] Resolving missing labels: {len(missing_de)} de, {len(missing_en)} en")
        try:
            if missing_en:
                maps = self._fetch_labels(missing_en, "https://de.wikipedia.org/w/api.php", "en")
                for c in contexts:
                    if c.entity_name in maps:
                        tgt = maps[c.entity_name]["tgt_title"]
                        if tgt:
                            c.set_processing_info("label_en", tgt)
            if missing_de:
                maps = self._fetch_labels(missing_de, "https://en.wikipedia.org/w/api.php", "de")
                for c in contexts:
                    if c.entity_name in maps:
                        tgt = maps[c.entity_name]["tgt_title"]
                        if tgt:
                            c.set_processing_info("label_de", tgt)
        except Exception as exc:
            self.logger.warning(f"[LabelBatch] Fehler beim Auflösen fehlender Labels: {exc}")

    # EXISTING METHODS CONTINUE BELOW

    async def process_entity_batch(self, contexts: List[EntityProcessingContext]) -> None:
        """
        Verarbeitet einen Batch von Entitäten parallel und gibt detaillierte Fallback- und Batch-Statistiken aus.
        
        Args:
            contexts: Liste von EntityProcessingContext-Objekten
        """
        if not contexts:
            self.logger.warning("Leerer Kontext-Batch übergeben")
            return

        self.logger.info(f"[Batch] Starte Verarbeitung von {len(contexts)} Entitäten.")

        # Stelle sicher, dass eine Session existiert
        await self.create_session()

        # Statistik-Zwischenspeicher für Fallback-Analyse
        fallback_type_counter = {}
        fallback_success_entities = []
        not_found_entities = []
        cache_hit_count = 0
        api_fetch_count = 0
        fallback_attempt_count = 0
        fallback_attempts_per_type = {}

        # Sorge dafür, dass fehlende bilingual Labels aufgelöst werden, bevor wir API-Aufrufe starten
        await self._resolve_bilingual_labels_batch(contexts)

        # Vorherige Zählerstände sichern
        prev_success = self.successful_entities
        prev_partial = self.partial_entities
        prev_failed = self.failed_entities
        prev_fallback_success = self.fallback_successes

        # Parallele Verarbeitung
        tasks = [self.process_entity(context) for context in contexts]
        await asyncio.gather(*tasks)

        # Nach Verarbeitung: Detaillierte Analyse
        for context in contexts:
            wiki_data = context.get_service_data("wikipedia") or {}
            entity_name = wiki_data.get("label") or getattr(context, "entity_name", None)
            status = wiki_data.get("status", "not_found")
            needs_fallback = wiki_data.get("needs_fallback", False)
            fallback_attempts = wiki_data.get("fallback_attempts", 0)
            fallback_source = wiki_data.get("fallback_source", None)
            source = wiki_data.get("source", "unbekannt")

            # --- Multilang-Block: Schreibe wikipedia_multilang ins context.processing_data ---
            # Starte mit bestehendem Eintrag, um Überschreiben zu vermeiden
            multilang_entry = dict(context.processing_data.get('wikipedia_multilang', {}))
            lang = self.config.get('LANGUAGE', 'de')
            if wiki_data and lang not in multilang_entry:
                multilang_entry[lang] = {
                    'label': wiki_data.get('label'),
                    'description': wiki_data.get('extract') or wiki_data.get('description'),
                    'url': wiki_data.get('url'),
                }
            wiki_url = wiki_data.get('url') or getattr(context, 'wikipedia_url', None)
            if wiki_url and 'wikipedia_multilang' not in context.processing_data:
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        ml_batch = await self.fetch_multilang_batch([wiki_url])
                    else:
                        ml_batch = loop.run_until_complete(self.fetch_multilang_batch([wiki_url]))
                    ml = ml_batch.get(wiki_url, {})
                    # Merge fetched multilang data
                    for k in ('en', 'de'):
                        if k in ml and ml[k]:
                            multilang_entry[k] = {
                                'label': ml[k].get('label'),
                                'description': ml[k].get('description'),
                                'url': ml[k].get('url'),
                            }
                    # If the English entry is still missing, try to supplement it from the primary result
                    if 'en' not in multilang_entry:
                        label_en = None
                        if isinstance(wiki_data.get('labels'), dict):
                            label_en = wiki_data['labels'].get('en')
                        label_en = label_en or wiki_data.get('label_en')
                        if label_en:
                            multilang_entry['en'] = {
                                'label': label_en,
                                'description': wiki_data.get('extract') or wiki_data.get('description'),
                                'url': wiki_data.get('url'),
                            }
                            self.logger.debug(f"[Multilang] Ergänze English label from primary result for '{entity_name}': {label_en}")
                    # Final attempt: active lookup via langlinks/Wikidata
                    needs_en_lookup = (
                        'en' not in multilang_entry or
                        not multilang_entry.get('en', {}).get('description')
                    )
                    if needs_en_lookup:
                        en_title = await self._lookup_en_title(wiki_data.get('label') or entity_name)
                        if en_title:
                            multilang_entry['en'] = {
                                'label': en_title,
                                'description': wiki_data.get('extract') or wiki_data.get('description'),
                                'url': f"https://en.wikipedia.org/wiki/{en_title.replace(' ', '_')}",
                            }
                            self.logger.debug(f"[Multilang] LookupEN erfolgreich für '{entity_name}': {en_title}")
                except Exception as e:
                    self.logger.warning(f"Fehler beim Nachholen von Multilang-Daten für '{entity_name}': {e}")
            if multilang_entry:
                context.processing_data['wikipedia_multilang'] = multilang_entry
                en_label = multilang_entry.get('en', {}).get('label')
                self.logger.info(f"[DEBUG] Nach Wikipedia: {entity_name} hat wikipedia_multilang: {context.processing_data.get('wikipedia_multilang')}")
                self.logger.info(f"[DEBUG] Kontext-ID nach Multilang-Setzen: {id(context)} für '{entity_name}'")
                if en_label:
                    self.logger.info(f"[Wikipedia-Multilang] Entity '{entity_name}': Englisches Label = '{en_label}'")
                else:
                    self.logger.warning(f"[Wikipedia-Multilang] Entity '{entity_name}': Kein englisches Label gefunden!")
            else:
                self.logger.warning(f"[Wikipedia-Multilang] Entity '{entity_name}': Keine Multilang-Daten verfügbar!")

            # Cache Hit/Fetch Statistik
            if source == "Cache":
                cache_hit_count += 1
            elif source == "API":
                api_fetch_count += 1
            # Fallback-Statistik
            if fallback_source:
                fallback_type_counter.setdefault(fallback_source, 0)
                fallback_type_counter[fallback_source] += 1
                fallback_success_entities.append(entity_name)
                fallback_attempt_count += fallback_attempts
                fallback_attempts_per_type.setdefault(fallback_source, 0)
                fallback_attempts_per_type[fallback_source] += fallback_attempts
            if status == "not_found":
                not_found_entities.append(entity_name)

        # Gesamtsummen
        total = len(contexts)
        batch_success = self.successful_entities - prev_success
        batch_partial = self.partial_entities - prev_partial
        batch_failed = self.failed_entities - prev_failed
        batch_fallback_success = self.fallback_successes - prev_fallback_success
        success_rate = (batch_success / total * 100) if total > 0 else 0
        fallback_rate = (batch_fallback_success / total * 100) if total > 0 else 0

        self.logger.info(f"[Batch-Statistik] Verarbeitung abgeschlossen:")
        self.logger.info(f"[Batch-Statistik] - Verarbeitete Entitäten: {total}")
        self.logger.info(f"[Batch-Statistik] - Erfolgreich mit Extract: {batch_success} ({success_rate:.1f}%)")
        self.logger.info(f"[Batch-Statistik] - Teilweise (nur URL): {batch_partial}")
        self.logger.info(f"[Batch-Statistik] - Fehlgeschlagen: {batch_failed}")
        self.logger.info(f"[Batch-Statistik] - Fallback-Erfolge: {batch_fallback_success} ({fallback_rate:.1f}%)")
        self.logger.info(f"[Batch-Statistik] - Cache-Treffer: {cache_hit_count}")
        self.logger.info(f"[Batch-Statistik] - API-Fetches: {api_fetch_count}")
        self.logger.info(f"[Batch-Statistik] - Fallback-Versuche gesamt: {fallback_attempt_count}")
        if fallback_type_counter:
            fallback_types_str = ", ".join([f"{k}: {v}" for k, v in fallback_type_counter.items()])
            self.logger.info(f"[Batch-Statistik] - Fallback-Typen: {fallback_types_str}")
        if fallback_attempts_per_type:
            attempts_types_str = ", ".join([f"{k}: {v}" for k, v in fallback_attempts_per_type.items()])
            self.logger.info(f"[Batch-Statistik] - Fallback-Versuche pro Typ: {attempts_types_str}")
        if fallback_success_entities:
            self.logger.info(f"[Batch-Statistik] - Durch Fallback gerettete Entitäten: {', '.join([str(e) for e in fallback_success_entities])}")
        if not_found_entities:
            self.logger.info(f"[Batch-Statistik] - Nicht gefunden nach allen Stufen: {', '.join([str(e) for e in not_found_entities])}")
        if self.debug_mode:
            self.logger.debug(f"[Statistik-Debug] Batch-Größe: {self.batch_size}, API-URL: {self.api_url}")
            self.logger.debug(f"[Statistik-Debug] Cache-Verzeichnis: {self.cache_dir}")
    
    def _format_wikipedia_result(self, result: Optional[Dict[str, Any]], entity_name: str, needs_fallback: bool = False) -> Dict[str, Any]:
        """
        Formatiert das Wikipedia-API-Ergebnis in das standardisierte Format.
        
        Args:
            result: Ergebnis der Wikipedia-API
            entity_name: Name der Entität
            needs_fallback: Flag ob Fallback benötigt wird, überschreibt automatische Erkennung
            
        Returns:
            Standardisiertes Wikipedia-Datenwörterbuch
        """
        # Minimale Struktur für den Fall, dass kein Ergebnis vorliegt
        if not result:
            self.logger.debug(f"Kein Wikipedia-Ergebnis für '{entity_name}'")
            return {
                "wikipedia": {
                    "label": entity_name,
                    "url": "",
                    "status": "not_found",
                    "source": "api_no_result",
                    "needs_fallback": True,  # Immer TRUE für nicht gefundene Entitäten
                    "fallback_attempts": 0
                }
            }
        
        # Relevante Informationen extrahieren
        title = result.get("title", entity_name)
        url = result.get("url", "")
        extract = result.get("extract", "")
        categories = result.get("categories", [])
        filtered_categories = categories  # Keine Filterung nötig, direkt übernehmen
        internal_links = result.get("internal_links", [])
        wikidata_id = result.get("wikidata_id", "")
        pageid = result.get("pageid")
        thumbnail = result.get("thumbnail", "")
        lang = result.get("language", "de")
        # Enforce language consistency: only keep the URL as primary if its language
        # matches the configured LANGUAGE. Otherwise, move it to a separate field so
        # that downstream consumers (e.g. compendium generation) do not treat it as
        # the canonical reference.
        cfg_lang = self.config.get("LANGUAGE", "de").split("-")[0].lower()
        lang_mismatch = lang != cfg_lang
        if lang_mismatch:
            url_alt = url  # Preserve the original URL for debugging/inspection
            url = ""
        else:
            url_alt = None
        redirected_from = result.get("redirected_from", "")
        source = result.get("source", "api")
        fallback_source = result.get("fallback_source", "")
        fallback_title = result.get("fallback_title", "")
        original_title = result.get("original_title", "")
        fallback_attempts = result.get("fallback_attempts", 0)
        coordinates = result.get("coordinates")
        # Extract multilingual label data (if present) and build a simple labels dictionary
        multilang = result.get("multilang", {})
        labels: Dict[str, str] = {lang: title} if lang else {}
        for ml_code, ml_data in multilang.items():
            label_val = ml_data.get("label") if isinstance(ml_data, dict) else None
            if label_val:
                labels[ml_code] = label_val
        english_label = labels.get("en")
        
        # Status bestimmen
        status = "found" if extract else "not_found"
        
        # Needs fallback berechnen, falls nicht explizit übergeben
        # Use configurable minimal extract length to decide if fallbacks are required
        min_extract_len = self.config.get('WIKIPEDIA_MIN_EXTRACT_LEN', 100)
        computed_needs_fallback = needs_fallback or lang_mismatch or (not extract or len(extract) < min_extract_len)
        
        # Standardisiertes Format erstellen
        result_dict = {
            "wikipedia": {
                "label": title,
                "url": url,
                "extract": extract,
                "categories": filtered_categories,
                "internal_links": internal_links,
                "wikidata_id": wikidata_id,
                "pageid": pageid,
                "thumbnail": thumbnail,
                "language": lang,
                "redirected_from": redirected_from,
                "status": status,
                "source": source,
                "needs_fallback": computed_needs_fallback,
                "fallback_attempts": fallback_attempts
            }
        }
        
        # Koordinaten hinzufügen, wenn vorhanden
        if coordinates:
            result_dict["wikipedia"]["coordinates"] = coordinates
            self.logger.debug(f"Koordinaten für '{entity_name}' hinzugefügt: {coordinates['lat']}, {coordinates['lon']}")
            
        # Fallback-Informationen hinzufügen, wenn vorhanden
        if fallback_source:
            result_dict["wikipedia"]["fallback_source"] = fallback_source
        if fallback_title:
            result_dict["wikipedia"]["fallback_title"] = fallback_title
        if original_title:
            result_dict["wikipedia"]["original_title"] = original_title
        # Attach the alternate URL if present (language mismatch scenario)
        if url_alt:
            result_dict["wikipedia"]["url_alt"] = url_alt
        
        # Expose the collected labels so that downstream services (e.g. DBpediaService)
        # can reliably access English and other language variants.
        result_dict["wikipedia"]["labels"] = labels
        if english_label:
            result_dict["wikipedia"]["label_en"] = english_label
        return result_dict
    
    async def process_contexts(self, contexts: List[EntityProcessingContext]) -> None:
        """Compatibility wrapper expected by downstream code.
        Delegates to :py:meth:`process_entity_batch`.
        Args:
            contexts: List of EntityProcessingContext objects to process
        """
        if not contexts:
            return
        await self.process_entity_batch(contexts)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Gibt Statistiken über die Verarbeitung von Entitäten zurück.
        
        Returns:
            Statistik-Dictionary
        """
        return {
            "successful_entities": self.successful_entities,
            "failed_entities": self.failed_entities,
            "fallback_successes": self.fallback_successes,
            "processed_contexts": self.processed_contexts,
            "total_entities": self.successful_entities + self.failed_entities,
            "batch_size": self.batch_size,
            "api_url": self.api_url,
            "use_fallbacks": self.use_fallbacks
        }


# ---------------------------------------------------------------------------
# Lazy global instance for backward compatibility
# ---------------------------------------------------------------------------
from typing import Optional

_wikipedia_service_instance: Optional["WikipediaService"] = None

def get_wikipedia_service(config: Optional[Dict[str, Any]] = None) -> "WikipediaService":
    """Return a singleton WikipediaService that always matches the LANGUAGE.
    A new instance is created if none exists yet or the language changed."""
    global _wikipedia_service_instance
    cfg = config or get_config()
    if (
        _wikipedia_service_instance is None
        or _wikipedia_service_instance.config.get("LANGUAGE") != cfg.get("LANGUAGE")
    ):
        _wikipedia_service_instance = WikipediaService(cfg)
    return _wikipedia_service_instance

class _WikipediaServiceProxy:
    """Attribute proxy delegating to the actual singleton instance."""

    def __getattr__(self, item):
        return getattr(get_wikipedia_service(), item)

# Public alias expected by existing import sites
wikipedia_service = _WikipediaServiceProxy()

# Methoden zur Session-Verwaltung für die WikipediaService-Klasse
async def create_session(self):
    """
    Erstellt eine aiohttp.ClientSession für HTTP-Anfragen.
    """
    if self.session is None or self.session.closed:
        timeout = aiohttp.ClientTimeout(total=30)  # 30 Sekunden Timeout
        headers = {'User-Agent': self.user_agent}
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        self.logger.debug("Neue aiohttp.ClientSession für WikipediaService erstellt")
    return self.session

async def close_session(self):
    """
    Schließt die aiohttp.ClientSession.
    """
    if self.session and not self.session.closed:
        await self.session.close()
        self.logger.debug("aiohttp.ClientSession für WikipediaService geschlossen")
        self.session = None

async def __aenter__(self):
    """
    Async context manager entry.
    """
    await self.create_session()
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    """
    Async context manager exit.
    """
    await self.close_session()

# Füge die Methoden zur WikipediaService-Klasse hinzu
WikipediaService.create_session = create_session
WikipediaService.close_session = close_session
WikipediaService.__aenter__ = __aenter__
WikipediaService.__aexit__ = __aexit__



import asyncio
from entityextractor.services.wikipedia.fallbacks import (
    apply_language_fallback,
    apply_opensearch_fallback,
    apply_synonym_fallback,
    apply_beautifulsoup_fallback
)

async def process_entities_strict_pipeline(contexts):
    """
    Strikte Pipeline: Jede Entität durchläuft alle Stufen (Cache, API, Sprachfallback, Opensearch, Synonym, BS4),
    unabhängig davon, ob vorher schon ein Extract gefunden wurde. Nach jedem Schritt wird geloggt, wie viele gelöst/ungelöst sind.
    Args:
        contexts: List[EntityProcessingContext]
    """
    logger = logging.getLogger(__name__)
    total = len(contexts)
    logger.info(f"[Pipeline] Starte Batch-Verarbeitung für {total} Entitäten.")

    # Helper: Filter Contexts nach Extract
    def split_by_extract(contexts):
        resolved, unresolved = [], []
        for c in contexts:
            extract = None
            wiki = c.get_service_data('wikipedia') if hasattr(c, 'get_service_data') else None
            if wiki and wiki.get('extract') and len(wiki.get('extract')) > 0:
                resolved.append(c)
            else:
                unresolved.append(c)
        return resolved, unresolved

    # 1. Cache-Check (angenommen: Contexts enthalten evtl. schon Cache-Daten)
    resolved, unresolved = split_by_extract(contexts)
    logger.info(f"[Cache] {total} geprüft, {len(resolved)} gelöst, {len(unresolved)} ungelöst.")

    # 2. Batch-API für ungelöste
    if unresolved:
        names = [c.entity_name for c in unresolved]
        api_results = await async_fetch_wikipedia_data(names, wikipedia_service.api_url, wikipedia_service.user_agent, wikipedia_service.config)
        for c in unresolved:
            result = api_results.get(c.entity_name)
            if result:
                c.add_service_data('wikipedia', result)
        resolved_api, unresolved = split_by_extract(unresolved)
        logger.info(f"[API] {len(resolved_api)} neu gelöst, {len(unresolved)} ungelöst.")

    # 3. Sprachfallback für ungelöste
    async def batch_language_fallback(contexts):
        tasks = []
        for c in contexts:
            wiki = c.get_service_data('wikipedia') if hasattr(c, 'get_service_data') else None
            tasks.append(apply_language_fallback(c.entity_name, wiki, wikipedia_service.user_agent, wikipedia_service.config))
        results = await asyncio.gather(*tasks)
        for c, (result, _) in zip(contexts, results):
            if result:
                c.add_service_data('wikipedia', result)
        return split_by_extract(contexts)

    if unresolved:
        resolved_lang, unresolved = await batch_language_fallback(unresolved)
        logger.info(f"[Sprachfallback] {len(resolved_lang)} neu gelöst, {len(unresolved)} ungelöst.")

    # 4. Opensearch-Fallback für ungelöste
    async def batch_opensearch_fallback(contexts):
        tasks = []
        for c in contexts:
            wiki = c.get_service_data('wikipedia') if hasattr(c, 'get_service_data') else None
            tasks.append(apply_opensearch_fallback(c.entity_name, wiki, wikipedia_service.api_url, wikipedia_service.user_agent, wikipedia_service.config, 0))
        results = await asyncio.gather(*tasks)
        for c, (result, _) in zip(contexts, results):
            if result:
                c.add_service_data('wikipedia', result)
        return split_by_extract(contexts)

    if unresolved:
        resolved_open, unresolved = await batch_opensearch_fallback(unresolved)
        logger.info(f"[Opensearch] {len(resolved_open)} neu gelöst, {len(unresolved)} ungelöst.")

    # 5. Synonym-Fallback für ungelöste
    async def batch_synonym_fallback(contexts):
        tasks = []
        for c in contexts:
            wiki = c.get_service_data('wikipedia') if hasattr(c, 'get_service_data') else None
            tasks.append(apply_synonym_fallback(c.entity_name, wiki, wikipedia_service.api_url, wikipedia_service.user_agent, wikipedia_service.config, 0, wikipedia_service.max_fallback_attempts))
        results = await asyncio.gather(*tasks)
        for c, (result, _) in zip(contexts, results):
            if result:
                c.add_service_data('wikipedia', result)
        return split_by_extract(contexts)

    if unresolved:
        resolved_syn, unresolved = await batch_synonym_fallback(unresolved)
        logger.info(f"[Synonym] {len(resolved_syn)} neu gelöst, {len(unresolved)} ungelöst.")

    # 6. BeautifulSoup-Fallback für ungelöste
    async def batch_bs4_fallback(contexts):
        tasks = []
        for c in contexts:
            wiki = c.get_service_data('wikipedia') if hasattr(c, 'get_service_data') else None
            tasks.append(apply_beautifulsoup_fallback(c.entity_name, wiki, wikipedia_service.user_agent, 0, wikipedia_service.max_fallback_attempts))
        results = await asyncio.gather(*tasks)
        for c, (result, _) in zip(contexts, results):
            if result:
                c.add_service_data('wikipedia', result)
        return split_by_extract(contexts)

    if unresolved:
        resolved_bs4, unresolved = await batch_bs4_fallback(unresolved)
        logger.info(f"[BeautifulSoup] {len(resolved_bs4)} neu gelöst, {len(unresolved)} ungelöst.")

    # Abschluss-Log
    logger.info(f"[Pipeline] Verarbeitung abgeschlossen. Gesamt: {total}, Erfolgreich: {total-len(unresolved)}, Nicht gefunden: {len(unresolved)}")
