#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch Wikipedia service module for the Entity Extractor.

This module provides optimized batch-processing functions for interacting with
the Wikipedia API and extracting information from Wikipedia pages, with the goal
of minimizing the number of API requests through batching, caching, and smart fallbacks.
"""

import logging
logger = logging.getLogger('entityextractor.services.batch_wikipedia_service')
if not logger.hasHandlers():
    handler = logging.FileHandler('entity_extractor_debug.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
logger.info('TEST: Logger in batch_wikipedia_service funktioniert')
import re
import urllib.parse
import time
import json
import hashlib

from entityextractor.utils.cache_utils import get_cache_path, load_cache, save_cache
from entityextractor.utils.api_utils import limited_get, create_standard_headers, batch_processor
from entityextractor.utils.language_utils import detect_language, clean_title
from entityextractor.utils.translation_utils import translate_to_english
from entityextractor.utils.synonym_utils import generate_entity_synonyms
from entityextractor.config.settings import get_config, DEFAULT_CONFIG
from entityextractor.utils.text_utils import is_valid_wikipedia_url
from entityextractor.utils.wiki_url_utils import sanitize_wikipedia_url
from entityextractor.utils.html_scrape_utils import scrape_wikipedia_extract

_config = get_config()

# Verwende die gemeinsame limited_get Funktion aus api_utils

def batch_get_wikipedia_info(entity_names, lang="de", config=None):
    """
    Holt Wikipedia-Informationen für mehrere Entitäten in einem Batch.
    
    Args:
        entity_names: Liste von Entitätsnamen
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    # Dictionary für die Ergebnisse vorbereiten
    results = {}
    
    # 1. Cache prüfen - bereits vorhandene Daten identifizieren
    cached_entities = {}
    entities_to_fetch = []
    
    for entity in entity_names:
        cache_key = f"{lang}:{entity}"
        cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikipedia", cache_key)
        cached = load_cache(cache_path)
        
        if cached is not None:
            # Prüfe ob der Cache-Eintrag ein gültiges Extract enthält
            if cached.get("status") == "found" and cached.get("extract"):
                cached_entities[entity] = cached
                results[entity] = cached
            else:
                # Cache-Eintrag hat kein Extract, nicht verwenden
                logger.info(f"Cache-Eintrag für '{entity}' hat kein Extract oder ist nicht 'found', ignoriere.")
                entities_to_fetch.append(entity)
        else:
            entities_to_fetch.append(entity)
    
    # Wenn alles aus dem Cache geladen werden konnte, früh zurückkehren
    if not entities_to_fetch:
        return results
    
    # 2. Aufteilung in Batches von je 50 Entitäten
    batch_size = 50
    batches = [entities_to_fetch[i:i+batch_size] for i in range(0, len(entities_to_fetch), batch_size)]
    
    # 3. Batch-Abfragen durchführen
    for batch in batches:
        batch_results = _fetch_wikipedia_batch(batch, lang, config)
        
        # Differenziertes Caching: Speichere nur erfolgreiche Ergebnisse mit Mindestdaten
        for entity, data in batch_results.items():
            # Prüfe, ob der Status erfolgreich ist und Mindestdatenfelder vorhanden sind
            if data.get("status") == "found":
                # Prüfe Mindestdatenfelder für Wikipedia
                # Hinweis: Im Wikipedia-Service heißt das Label-Feld "title", wir prüfen daher beide Varianten
                has_label = ("label" in data and data["label"]) or ("title" in data and data["title"])
                has_url = "url" in data and data["url"]
                has_extract = "extract" in data and data["extract"]
                
                if has_label and has_url and has_extract:
                    results[entity] = data
                    cache_key = f"{lang}:{entity}"
                    cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikipedia", cache_key)
                    logger.info(f"Cache vollständige Wikipedia-Daten für {entity}")
                    save_cache(cache_path, data)
                elif has_label and has_url:  # Teilweise Daten vorhanden
                    # Wenn wenigstens Titel und URL vorhanden sind, trotzdem speichern
                    results[entity] = data
                    cache_key = f"{lang}:{entity}"
                    cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikipedia", cache_key)
                    logger.info(f"Entität {entity} hat nicht alle Mindestdatenfelder für Wikipedia, wird aber trotzdem gecacht")
                    save_cache(cache_path, data)
                
                # Zusätzlich: Wenn Redirect erkannt wurde, auch den aufgelösten Titel cachen
                if data.get("redirect_from") and data.get("redirect_from") != entity:
                    redirect_cache_key = f"{lang}:{data.get('redirect_from')}"
                    redirect_cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikipedia", redirect_cache_key)
                    save_cache(redirect_cache_path, data)
            # Hat keinen extract trotz found status? -> Braucht Fallback
            elif data.get("status") == "found" and not data.get("extract"):
                logger.warning(f"Wikipedia-API: '{entity}' hat status 'found', aber kein Extract. Starte Fallback.")
            # Auch negative Ergebnisse cachen (missing/error)
            elif data.get("status") in ["missing", "error"]:
                cache_key = f"{lang}:{entity}"
                cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikipedia", cache_key)
                # Mit TTL (Time-To-Live) von 1 Tag für negative Ergebnisse
                if config.get("CACHE_ENABLED") and config.get("CACHE_WIKIPEDIA_ENABLED"):
                    data["ttl"] = 86400  # 24 Stunden
                    save_cache(cache_path, data)
    
    # 4. Fallback für Entitäten, für die KEIN Extract geladen werden konnte
    # (unabhängig davon, ob ein Status "found" vorliegt oder nicht)
    entities_for_fallback = [e for e in entities_to_fetch if not (e in results and results.get(e, {}).get("extract"))]
    if entities_for_fallback:
        fallback_results = _perform_fallback_strategies(entities_for_fallback, lang, config)
        results.update(fallback_results)
    
    return results

def _fetch_wikipedia_batch(entity_names, lang="de", config=None):
    """
    Führt eine Batch-Abfrage für mehrere Entitäten durch.
    
    Args:
        entity_names: Liste von Entitätsnamen
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    results = {entity: {"status": "pending"} for entity in entity_names}
    
    # API-Endpunkt und Parameter
    endpoint = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'extracts|categories|pageprops|langlinks|info',
        'ppprop': 'wikibase_item',
        'titles': '|'.join(entity_names),  # Titel mit Pipe-Symbol verbinden
        'redirects': 'true',  # Redirects automatisch folgen
        'exintro': 1,
        'explaintext': 1,
        'cllimit': 'max',
        'clshow': '!hidden',
        'lllang': 'en' if lang == 'de' else 'de',  # Immer die andere Hauptsprache anfordern
        'inprop': 'url',
        'maxlag': config.get("WIKIPEDIA_MAXLAG")
    }
    
    headers = {"User-Agent": config.get("USER_AGENT")}
    
    try:
        logging.info(f"Batch-Abfrage für {len(entity_names)} Entitäten in {lang}.wikipedia.org")
        r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
        r.raise_for_status()
        data = r.json()
        
        # Redirects verarbeiten
        redirects_map = {}
        if 'redirects' in data.get('query', {}):
            for redirect in data['query']['redirects']:
                from_title = redirect.get('from', '')
                to_title = redirect.get('to', '')
                redirects_map[from_title] = to_title
        
        # Normalisierungen verarbeiten
        normalized_map = {}
        if 'normalized' in data.get('query', {}):
            for norm in data['query']['normalized']:
                from_title = norm.get('from', '')
                to_title = norm.get('to', '')
                normalized_map[from_title] = to_title
        
        # Seiteninformationen verarbeiten
        pages = data.get('query', {}).get('pages', {})
        
        # Zuerst die Zuordnung von normalisierten/umgeleiteten Titeln zu Page-IDs erstellen
        title_to_pageid = {}
        pageid_to_original = {}
        
        for entity in entity_names:
            normalized = normalized_map.get(entity, entity)
            redirected = redirects_map.get(normalized, normalized)
            
            for pageid, page in pages.items():
                if page.get('title') == redirected:
                    title_to_pageid[entity] = pageid
                    pageid_to_original[pageid] = entity
                    break
        
        # Dann die Seiteninformationen verarbeiten
        for pageid, page in pages.items():
            original_entity = pageid_to_original.get(pageid)
            
            if not original_entity:
                continue
                
            if 'missing' in page:
                results[original_entity] = {
                    "status": "missing",
                    "title": page.get('title', original_entity),
                    "timestamp": int(time.time())
                }
                continue
                
            # Informationen aus der Seite extrahieren
            normalized = normalized_map.get(original_entity, original_entity)
            redirected = redirects_map.get(normalized, normalized)
            
            langlinks = {ll.get('lang'): ll.get('*') for ll in page.get('langlinks', [])}
            
            results[original_entity] = {
                "status": "found",
                "title": page.get('title'),
                "original_query": original_entity,
                "redirect_from": original_entity if redirected != original_entity else None,
                "extract": page.get('extract', ''),
                "url": page.get('fullurl', f"https://{lang}.wikipedia.org/wiki/{urllib.parse.quote(page.get('title', '').replace(' ', '_'))}"),
                "categories": [c.get('title', '').split('Category:', 1)[-1] for c in page.get('categories', [])],
                "wikidata_id": page.get('pageprops', {}).get('wikibase_item'),
                "langlinks": langlinks,
                "de_url": f"https://de.wikipedia.org/wiki/{urllib.parse.quote(langlinks.get('de', page.get('title')).replace(' ', '_'))}" if lang != 'de' else page.get('fullurl'),
                "en_url": f"https://en.wikipedia.org/wiki/{urllib.parse.quote(langlinks.get('en', page.get('title')).replace(' ', '_'))}" if lang != 'en' else page.get('fullurl'),
                "timestamp": int(time.time())
            }
            
            # Wikidata-URL hinzufügen wenn ID vorhanden
            wid = results[original_entity]["wikidata_id"]
            if wid:
                results[original_entity]["wikidata_url"] = f"https://www.wikidata.org/wiki/{wid}"
        
        # Prüfen, ob es Entitäten gibt, die nicht gefunden wurden
        for entity in entity_names:
            if results[entity]["status"] == "pending":
                results[entity] = {
                    "status": "missing",
                    "title": entity,
                    "timestamp": int(time.time())
                }
        
        return results
        
    except Exception as e:
        logging.error(f"Fehler bei Batch-Abfrage: {e}")
        # Bei einem Fehler alle pendenten Entitäten als error markieren
        for entity in entity_names:
            if results[entity]["status"] == "pending":
                results[entity] = {
                    "status": "error",
                    "title": entity,
                    "error": str(e),
                    "timestamp": int(time.time())
                }
        return results

def _perform_fallback_strategies(entity_names, lang="de", config=None):
    """
    Führt Fallback-Strategien für nicht gefundene Entitäten durch.
    Optimierte Reihenfolge: Alternativsprache, OpenSearch, BeautifulSoup, Synonym-Generierung.
    Nach jedem erfolgreichen Schritt verbleibende Entitäten reduzieren. Synonym-Mapping wird gecacht.
    """
    import urllib.parse
    import time
    from entityextractor.utils.cache_utils import get_cache_path, save_cache
    start_time = time.time()
    if config is None:
        from entityextractor.config.settings import DEFAULT_CONFIG
        config = DEFAULT_CONFIG
    logging.info(f"Wikipedia-Fallback: Starte Fallback-Strategien für {len(entity_names)} Entitäten: {', '.join(entity_names)}")
    results = {}
    remaining_entities = list(entity_names)
    alt_lang = "en" if lang == "de" else "de"
    # 1a. Softredirects explizit prüfen
    if remaining_entities:
        softredirect_results = _fetch_wikipedia_batch(remaining_entities, lang, config)
        found = []
        for entity, data in softredirect_results.items():
            # Softredirect: Wenn 'redirect_from' gesetzt und extract vorhanden
            if data.get("status") == "found" and data.get("extract") and data.get("redirect_from"):
                logging.info(f"Softredirect: '{entity}' ist ein Softredirect auf '{data.get('title')}'. Extract gefunden.")
                results[entity] = data
                found.append(entity)
            elif data.get("status") == "found" and data.get("extract") and not data.get("redirect_from"):
                logging.info(f"Softredirect: '{entity}' ist KEIN Softredirect. (Direkter Treffer)")
            else:
                logging.info(f"Softredirect: '{entity}' kein Treffer oder kein Extract.")
        for entity in found:
            remaining_entities.remove(entity)

    # 1. Alternativsprache
    if remaining_entities:
        alt_lang_results = _fetch_wikipedia_batch(remaining_entities, alt_lang, config)
        found = []
        for entity, data in alt_lang_results.items():
            if data.get("status") == "found" and data.get("extract"):
                results[entity] = data
                found.append(entity)
        for entity in found:
            remaining_entities.remove(entity)
    # 2. OpenSearch
    if remaining_entities:
        opensearch_results = _perform_opensearch(remaining_entities, lang, config)
        found = []
        for entity in remaining_entities:
            result = opensearch_results.get(entity)
            if result and result.get("status") == "found" and result.get("extract"):
                results[entity] = result
                found.append(entity)
        for entity in found:
            remaining_entities.remove(entity)
    # 3. BeautifulSoup Scraping (vor Synonym)
    if remaining_entities:
        from entityextractor.utils.html_scrape_utils import scrape_wikipedia_extract
        found = []
        for entity in remaining_entities:
            sanitized_entity = entity.replace(' ', '_')
            url = f"https://{lang}.wikipedia.org/wiki/{urllib.parse.quote(sanitized_entity)}"
            scraped = scrape_wikipedia_extract(url, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
            # Alternative Schreibweise probieren falls nötig
            if not scraped or not scraped.get("extract"):
                alt_entity = entity.replace('_', ' ') if '_' in entity else entity.replace(' ', '_')
                alt_url = f"https://{lang}.wikipedia.org/wiki/{urllib.parse.quote(alt_entity)}"
                scraped = scrape_wikipedia_extract(alt_url, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
            if scraped and scraped.get("extract"):
                results[entity] = {
                    "status": "found",
                    "title": scraped.get("title", entity),
                    "extract": scraped.get("extract", ""),
                    "categories": scraped.get("categories", []),
                    "url": url,
                    "scraped": True,
                    "timestamp": int(time.time())
                }
                found.append(entity)
        for entity in found:
            remaining_entities.remove(entity)
    # 4. Synonym-Generierung (mit Caching)
    if remaining_entities and config.get("USE_SYNONYM_FALLBACK", True):
        synonym_results = _perform_synonym_search(remaining_entities, lang, config)
        for entity in remaining_entities:
            result = synonym_results.get(entity)
            if result and result.get("status") == "found" and result.get("extract"):
                # Synonym-Mapping cachen
                if result.get("match_term") and result.get("match_term") != entity:
                    synonym_cache_key = f"synonym_map:{lang}:{entity}"
                    synonym_cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "synonym_mappings", synonym_cache_key)
                    synonym_mapping = {
                        "original": entity,
                        "match_term": result.get("match_term"),
                        "title": result.get("title"),
                        "url": result.get("url"),
                        "timestamp": int(time.time())
                    }
                    save_cache(synonym_cache_path, synonym_mapping)
                results[entity] = result
    # Not found
    for entity in entity_names:
        if entity not in results:
            results[entity] = {
                "status": "not_found",
                "title": entity,
                "timestamp": int(time.time())
            }
    logging.info(f"Wikipedia-Fallback: Fallback-Strategien in {time.time() - start_time:.2f} Sekunden abgeschlossen. Ergebnis: {sum(1 for r in results.values() if r.get('status') == 'found')} von {len(entity_names)} erfolgreich.")
    return results

    """
    Führt Fallback-Strategien für nicht gefundene Entitäten durch.
    
    Args:
        entity_names: Liste von Entitätsnamen
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Informationen als Werte
    """
    start_time = time.time()
    if config is None:
        config = DEFAULT_CONFIG
        
    logging.info(f"Wikipedia-Fallback: Starte Fallback-Strategien für {len(entity_names)} Entitäten: {', '.join(entity_names)}")
    results = {}
    
    # Strategie 1: Suche in Alternativsprache (wenn lang="de", dann in "en" suchen, sonst in "de")
    alt_lang = "en" if lang == "de" else "de"
    alt_lang_results = _fetch_wikipedia_batch(entity_names, alt_lang, config)
    for entity, data in alt_lang_results.items():
        if data.get("status") == "found" and data.get("extract"):
            logging.info(f"Wikipedia-Fallback: '{entity}' in Alternativsprache '{alt_lang}' gefunden: {data.get('title')}")
            results[entity] = data
        else:
            logging.info(f"Wikipedia-Fallback: '{entity}' in Alternativsprache '{alt_lang}' NICHT gefunden.")
    
    # Strategie 2: OpenSearch für verbleibende Entitäten
    remaining_entities = [e for e in entity_names if e not in results]
    if remaining_entities:
        opensearch_results = _perform_opensearch(remaining_entities, lang, config)
        for entity in remaining_entities:
            result = opensearch_results.get(entity)
            if result and result.get("status") == "found" and result.get("extract"):
                logging.info(f"Wikipedia-Fallback: OpenSearch für '{entity}' erfolgreich: {result.get('title')}")
                results[entity] = result
            else:
                logging.info(f"Wikipedia-Fallback: OpenSearch für '{entity}' NICHT erfolgreich.")
    
    # Strategie 3: Synonym-Generierung für noch verbleibende Entitäten
    remaining_entities = [e for e in entity_names if e not in results]
    if remaining_entities and config.get("USE_SYNONYM_FALLBACK", True):
        synonym_results = _perform_synonym_search(remaining_entities, lang, config)
        for entity in remaining_entities:
            result = synonym_results.get(entity)
            if result and result.get("status") == "found" and result.get("extract"):
                logging.info(f"Wikipedia-Fallback: Synonymsuche für '{entity}' erfolgreich: {result.get('title')}")
                results[entity] = result
            else:
                logging.info(f"Wikipedia-Fallback: Synonymsuche für '{entity}' NICHT erfolgreich.")

    # Strategie 4: BeautifulSoup Scraping als letzter Versuch
    remaining_entities = [e for e in entity_names if e not in results]
    if remaining_entities:
        for entity in remaining_entities:
            # Generiere eine vermutete URL für das Scraping
            sanitized_entity = entity.replace(' ', '_')
            url = f"https://{lang}.wikipedia.org/wiki/{urllib.parse.quote(sanitized_entity)}"
            logging.info(f"Wikipedia-Fallback: BeautifulSoup-Scraping für '{entity}' probieren ({url})")
            
            scraped_data = scrape_wikipedia_extract(url, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
            if scraped_data and scraped_data.get("extract"):
                # Erfolgreicher Scrape
                logging.info(f"Wikipedia-Fallback: BeautifulSoup-Scraping für '{entity}' ERFOLGREICH")
                results[entity] = {
                    "status": "found",
                    "title": scraped_data.get("title", entity),
                    "extract": scraped_data.get("extract", ""),
                    "categories": scraped_data.get("categories", []),
                    "url": url,
                    "scraped": True,
                    "timestamp": int(time.time())
                }
            else:
                logging.info(f"Wikipedia-Fallback: BeautifulSoup-Scraping für '{entity}' NICHT erfolgreich.")
    
    # Markiere letztendlich nicht gefundene Entitäten als "not_found"
    for entity in entity_names:
        if entity not in results:
            logging.warning(f"Wikipedia-Fallback: Für '{entity}' konnte KEINE Wikipedia-Seite gefunden werden (alle Fallbacks erschöpft).")
            results[entity] = {
                "status": "not_found",
                "title": entity,
                "timestamp": int(time.time())
            }
            
    logging.info(f"Wikipedia-Fallback: Fallback-Strategien in {time.time() - start_time:.2f} Sekunden abgeschlossen. "
               f"Ergebnis: {sum(1 for r in results.values() if r.get('status') == 'found')} von {len(entity_names)} erfolgreich.")
    
    return results

def _perform_opensearch(entity_names, lang="de", config=None):
    """
    Verwendet WikiMedia OpenSearch API für Entitätssuche.
    
    Args:
        entity_names: Liste von Entitätsnamen
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    results = {}
    
    for entity in entity_names:
        try:
            endpoint = f"https://{lang}.wikipedia.org/w/api.php"
            params = {
                'action': 'opensearch',
                'search': entity,
                'limit': 1,
                'namespace': 0,
                'format': 'json',
                "maxlag": config.get("WIKIPEDIA_MAXLAG")
            }
            
            headers = {"User-Agent": config.get("USER_AGENT")}
            
            logging.info(f"OpenSearch für '{entity}' in {lang}.wikipedia.org")
            r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
            r.raise_for_status()
            data = r.json()
            
            if data and len(data) >= 4 and data[1] and data[3]:
                # OpenSearch zurückgeben: [Suchbegriff, [Titelliste], [Beschreibungsliste], [URLliste]]
                best_match_title = data[1][0]
                best_match_url = data[3][0]
                
                # Nun die vollständigen Informationen holen
                full_info = _fetch_wikipedia_batch([best_match_title], lang, config).get(best_match_title, {})
                
                if full_info.get("status") == "found":
                    # Original-Entitätsnamen als Schlüssel beibehalten
                    full_info["original_query"] = entity
                    full_info["opensearch_match"] = best_match_title
                    results[entity] = full_info
                
            else:
                logging.info(f"Keine OpenSearch-Ergebnisse für '{entity}'")
                
        except Exception as e:
            logging.error(f"Fehler bei OpenSearch für '{entity}': {e}")
    
    return results

def _perform_synonym_search(entity_names, lang="de", config=None):
    """
    Generiert Synonyme für Entitätsnamen und sucht danach mit direkter API-Abfrage und OpenSearch.
    
    Args:
        entity_names: Liste von Entitätsnamen
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    results = {}
    start_time = time.time()
    
    for entity in entity_names:
        entity_start = time.time()
        logging.info(f"Wikipedia-Fallback: Starte erweiterte Synonymsuche für '{entity}'")
        
        # Synonyme generieren (aus bestehender Funktion)
        synonyms = generate_entity_synonyms(entity, language=lang)
        
        if not synonyms:
            logging.info(f"Wikipedia-Fallback: Keine Synonyme für '{entity}' gefunden.")
            continue
            
        logging.info(f"Wikipedia-Fallback: Für '{entity}' {len(synonyms)} Synonyme generiert: {synonyms}")
        
        # Für die Suche original_entity + alle Synonyme verwenden
        all_search_terms = [entity] + synonyms
        
        found = False
        
        # STRATEGIE 1: Direktes API-Matching mit ALLEN Synonymen in einer Batch-Abfrage
        # (Original-Entity wurde bereits in der Hauptsuche versucht)
        if synonyms:
            start_batch = time.time()
            logging.info(f"Wikipedia-Fallback: Starte Batch-API-Abfrage für {len(synonyms)} Synonyme von '{entity}'")
            
            # Alle Synonyme auf einmal abfragen in einer Batch-Anfrage
            batch_results = _fetch_wikipedia_batch(synonyms, lang, config)
            
            # Prüfen, ob eines der Synonyme einen Treffer hat
            for synonym, data in batch_results.items():
                if data.get("status") == "found" and data.get("extract"):
                    # Erfolgreich! Ergebnis speichern und mit dem nächsten Entity fortfahren
                    data["original_query"] = entity
                    data["match_term"] = synonym
                    data["match_type"] = "batch_api"
                    results[entity] = data
                    found = True
                    logging.info(f"Wikipedia-Fallback: Erfolgreich! Batch-API-Abfrage fand '{synonym}' für '{entity}' in {time.time() - start_batch:.2f}s")
                    break
            
            if not found:
                logging.info(f"Wikipedia-Fallback: Batch-API-Abfrage für alle Synonyme von '{entity}' ergab keine Treffer in {time.time() - start_batch:.2f}s")

        
        # STRATEGIE 2: Wenn direktes Matching fehlschlägt, versuche OpenSearch mit allen Termen
        # (Original + Synonyme, da OpenSearch anders funktioniert als direktes API-Matching)
        if not found:
            logging.info(f"Wikipedia-Fallback: Direktes API-Matching erfolglos, versuche OpenSearch für '{entity}' und alle Synonyme")
            for i, synonym in enumerate(all_search_terms, 0):
                term_type = "Original-Entity" if i == 0 else f"Synonym {i}/{len(synonyms)}"
                
                try:
                    # OpenSearch API-Aufruf
                    logging.info(f"Wikipedia-Fallback: OpenSearch für {term_type} '{synonym}' ({entity})")
                    
                    endpoint = f"https://{lang}.wikipedia.org/w/api.php"
                    params = {
                        'action': 'opensearch',
                        'search': synonym,
                        'limit': 1,
                        'namespace': 0,
                        'format': 'json',
                        "maxlag": config.get("WIKIPEDIA_MAXLAG")
                    }
                    
                    headers = {"User-Agent": config.get("USER_AGENT")}
                    r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
                    r.raise_for_status()
                    data = r.json()
                    
                    if data and len(data) >= 4 and data[1] and data[3]:
                        # OpenSearch-Ergebnisse verarbeiten
                        best_match_title = data[1][0]
                        best_match_url = data[3][0]
                        
                        logging.info(f"Wikipedia-Fallback: OpenSearch für '{synonym}' liefert Treffer: '{best_match_title}'")
                        
                        # Vollständige Informationen für den Treffer holen
                        full_info = _fetch_wikipedia_batch([best_match_title], lang, config).get(best_match_title, {})
                        
                        if full_info.get("status") == "found" and full_info.get("extract"):
                            # Erfolgreicher Treffer mit Extract
                            full_info["original_query"] = entity
                            full_info["match_term"] = synonym
                            full_info["opensearch_match"] = best_match_title
                            full_info["match_type"] = "opensearch"
                            results[entity] = full_info
                            found = True
                            logging.info(f"Wikipedia-Fallback: Erfolgreich! OpenSearch mit '{synonym}' für '{entity}' findet '{best_match_title}'")
                            break
                        else:
                            logging.info(f"Wikipedia-Fallback: OpenSearch-Treffer '{best_match_title}' für '{synonym}' hat kein Extract oder ist ungültig")
                    else:
                        logging.info(f"Wikipedia-Fallback: Keine OpenSearch-Ergebnisse für '{synonym}'")
                        
                except Exception as e:
                    logging.error(f"Wikipedia-Fallback: Fehler bei OpenSearch für '{synonym}': {e}")
        
        # STRATEGIE 3: Englische Übersetzung als letzter Versuch
        if not found and lang == "de":
            # Versuche, ins Englische zu übersetzen und dort zu suchen
            english_term = translate_to_english(entity)
            if english_term and english_term != entity:
                logging.info(f"Wikipedia-Fallback: Übersetzung für '{entity}' ins Englische: '{english_term}'")
                
                # Direktes API-Matching mit englischem Begriff
                en_results = _fetch_wikipedia_batch([english_term], "en", config)
                en_data = en_results.get(english_term, {})
                
                if en_data.get("status") == "found" and en_data.get("extract"):
                    logging.info(f"Wikipedia-Fallback: Englische Übersetzung '{english_term}' für '{entity}' erfolgreich!")
                    en_data["original_query"] = entity
                    en_data["english_translation"] = english_term
                    en_data["match_type"] = "english_translation"
                    results[entity] = en_data
                    found = True
                else:
                    # Versuche OpenSearch mit englischem Begriff als letzten Versuch
                    logging.info(f"Wikipedia-Fallback: Direkte Suche mit englischer Übersetzung fehlgeschlagen, versuche OpenSearch für '{english_term}'")
                    
                    try:
                        endpoint = f"https://en.wikipedia.org/w/api.php"
                        params = {
                            'action': 'opensearch',
                            'search': english_term,
                            'limit': 1,
                            'namespace': 0,
                            'format': 'json',
                            "maxlag": config.get("WIKIPEDIA_MAXLAG")
                        }
                        
                        headers = {"User-Agent": config.get("USER_AGENT")}
                        r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
                        r.raise_for_status()
                        data = r.json()
                        
                        if data and len(data) >= 4 and data[1] and data[3]:
                            best_match_title = data[1][0]
                            logging.info(f"Wikipedia-Fallback: OpenSearch (EN) für '{english_term}' liefert Treffer: '{best_match_title}'")
                            
                            full_info = _fetch_wikipedia_batch([best_match_title], "en", config).get(best_match_title, {})
                            
                            if full_info.get("status") == "found" and full_info.get("extract"):
                                full_info["original_query"] = entity
                                full_info["english_translation"] = english_term
                                full_info["opensearch_match"] = best_match_title
                                full_info["match_type"] = "english_opensearch"
                                results[entity] = full_info
                                found = True
                                logging.info(f"Wikipedia-Fallback: Erfolgreich! OpenSearch (EN) mit '{english_term}' findet '{best_match_title}'")
                            else:
                                logging.info(f"Wikipedia-Fallback: OpenSearch (EN) Treffer '{best_match_title}' hat kein Extract oder ist ungültig")
                    except Exception as e:
                        logging.error(f"Wikipedia-Fallback: Fehler bei OpenSearch (EN) für '{english_term}': {e}")
        
        entity_duration = time.time() - entity_start
        if found:
            result_info = results[entity]
            match_type = result_info.get("match_type", "unknown")
            match_term = result_info.get("match_term", result_info.get("english_translation", entity))
            found_title = result_info.get("title", "unknown")
            
            logging.info(f"Wikipedia-Fallback: Synonymsuche für '{entity}' erfolgreich nach {entity_duration:.2f}s. "   
                       f"Methode: {match_type}, Begriff: '{match_term}', Gefundener Titel: '{found_title}'")
        else:
            logging.info(f"Wikipedia-Fallback: Synonymsuche für '{entity}' fehlgeschlagen nach {entity_duration:.2f}s - "  
                       f"alle {len(synonyms)} Synonyme mit direkter API und OpenSearch geprüft")
    
    total_duration = time.time() - start_time
    success_count = sum(1 for e in entity_names if e in results)
    logging.info(f"Wikipedia-Fallback: Gesamte Synonymsuche abgeschlossen in {total_duration:.2f}s. "  
               f"Erfolgreich: {success_count}/{len(entity_names)} Entitäten ({success_count/max(1,len(entity_names))*100:.1f}%)")
    logging.info(f"Wikipedia-Fallback: Hinweis: Für zukünftige Optimierung könnte erwogen werden, die gefundenen erfolgreichen Synonyme zu cachen.")
    
    return results

def get_wikipedia_info(entity_name, lang="de", config=None):
    """
    Einzelne Entitätsabfrage - Wrapper um die Batch-Funktion.
    
    Args:
        entity_name: Name der Entität
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Informationen zur Entität
    """
    results = batch_get_wikipedia_info([entity_name], lang, config)
    return results.get(entity_name, {"status": "error", "error": "Unknown error"})

# Wrapper-Funktionen für Kompatibilität mit der bestehenden API

def get_wikipedia_extract(wikipedia_url_or_title, config=None):
    """
    Kompatibilitätsfunktion für die bestehende API.
    
    Args:
        wikipedia_url_or_title: Wikipedia-URL oder Titel
        config: Konfigurationswörterbuch
        
    Returns:
        Wikipedia-Extrakt
    """
    # Wenn eine URL übergeben wurde, extrahiere den Titel
    if is_valid_wikipedia_url(wikipedia_url_or_title):
        wikipedia_url = sanitize_wikipedia_url(wikipedia_url_or_title)
        parts = wikipedia_url.split('/wiki/')
        if len(parts) < 2:
            logging.warning("Invalid Wikipedia URL: %s", wikipedia_url)
            return ""
        title = parts[1].split('#')[0]
        lang = wikipedia_url.split('://')[1].split('.')[0]
    else:
        # Sonst direkt als Titel verwenden
        title = wikipedia_url_or_title
        lang = "de"  # Standard-Sprache
        
    info = get_wikipedia_info(title, lang, config)
    
    if info.get("status") == "found":
        return info.get("extract", "")
    return ""

def get_wikipedia_categories(wikipedia_url_or_title, config=None):
    """
    Kompatibilitätsfunktion für die bestehende API.
    
    Args:
        wikipedia_url_or_title: Wikipedia-URL oder Titel
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von Wikipedia-Kategorien
    """
    # Wenn eine URL übergeben wurde, extrahiere den Titel
    if is_valid_wikipedia_url(wikipedia_url_or_title):
        wikipedia_url = sanitize_wikipedia_url(wikipedia_url_or_title)
        parts = wikipedia_url.split('/wiki/')
        if len(parts) < 2:
            logging.warning("Invalid Wikipedia URL: %s", wikipedia_url)
            return []
        title = parts[1].split('#')[0]
        lang = wikipedia_url.split('://')[1].split('.')[0]
    else:
        # Sonst direkt als Titel verwenden
        title = wikipedia_url_or_title
        lang = "de"  # Standard-Sprache
        
    info = get_wikipedia_info(title, lang, config)
    
    if info.get("status") == "found":
        return info.get("categories", [])
    return []

def get_wikipedia_details(wikipedia_url_or_title, config=None):
    """
    Kompatibilitätsfunktion für die bestehende API.
    
    Args:
        wikipedia_url_or_title: Wikipedia-URL oder Titel
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Details zur Wikipedia-Seite
    """
    # Wenn eine URL übergeben wurde, extrahiere den Titel
    if is_valid_wikipedia_url(wikipedia_url_or_title):
        wikipedia_url = sanitize_wikipedia_url(wikipedia_url_or_title)
        parts = wikipedia_url.split('/wiki/')
        if len(parts) < 2:
            logging.warning("Invalid Wikipedia URL: %s", wikipedia_url)
            return {}
        title = parts[1].split('#')[0]
        lang = wikipedia_url.split('://')[1].split('.')[0]
    else:
        # Sonst direkt als Titel verwenden
        title = wikipedia_url_or_title
        lang = "de"  # Standard-Sprache
        
    info = get_wikipedia_info(title, lang, config)
    
    if info.get("status") == "found":
        return {
            "title": info.get("title"),
            "extract": info.get("extract", ""),
            "url": info.get("url"),
            "categories": info.get("categories", []),
            "wikidata_id": info.get("wikidata_id"),
            "wikidata_url": info.get("wikidata_url"),
            "de_url": info.get("de_url"),
            "en_url": info.get("en_url"),
            "langlinks": info.get("langlinks", {}),
        }
    return {}

def fallback_wikipedia_url(query, langs=None, language="de", config=None):
    """
    Kompatibilitätsfunktion für die bestehende API.
    
    Args:
        query: Suchbegriff
        langs: Liste von Sprachen (prioritätsgeordnet)
        language: Standardsprache
        config: Konfigurationswörterbuch
        
    Returns:
        Wikipedia-URL oder None
    """
    if langs is None:
        langs = [language, "en"] if language != "en" else ["en", "de"]
        
    for lang in langs:
        info = get_wikipedia_info(query, lang, config)
        if info.get("status") == "found":
            return info.get("url")
            
    # Wenn nichts gefunden wurde, versuche OpenSearch
    for lang in langs:
        results = _perform_opensearch([query], lang, config)
        if query in results and results[query].get("status") == "found":
            return results[query].get("url")
            
    return None

def get_wikipedia_summary_and_categories_props(wikipedia_url, config=None):
    """
    Kompatibilitätsfunktion für die bestehende API.
    
    Args:
        wikipedia_url: Wikipedia-URL
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Titel, Extrakt, Kategorien und Wikidata-ID/URL
    """
    if not is_valid_wikipedia_url(wikipedia_url):
        logging.warning("Invalid Wikipedia URL: %s", wikipedia_url)
        return {}
        
    wikipedia_url = sanitize_wikipedia_url(wikipedia_url)
    parts = wikipedia_url.split('/wiki/')
