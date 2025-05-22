#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch Wikidata service module for the Entity Extractor.

This module provides optimized batch-processing functions for interacting with
the Wikidata API, minimizing the number of API requests through batching, 
caching, and smart fallbacks.
"""

import logging
logger = logging.getLogger('entityextractor.services.batch_wikidata_service')
if not logger.hasHandlers():
    handler = logging.FileHandler('entity_extractor_debug.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
logger.info('TEST: Logger in batch_wikidata_service funktioniert')
import re
import urllib.parse
import time
import json
import hashlib
from typing import Dict, List, Any, Optional, Set, Tuple

from entityextractor.utils.cache_utils import get_cache_path, load_cache, save_cache
from entityextractor.utils.api_utils import limited_get, create_standard_headers
from entityextractor.utils.language_utils import detect_language, clean_title
from entityextractor.utils.translation_utils import get_wikipedia_title_in_language, translate_to_english
from entityextractor.utils.synonym_utils import generate_entity_synonyms
from entityextractor.config.settings import get_config, DEFAULT_CONFIG

_config = get_config()

# Verwende die gemeinsame API-Funktionalität aus api_utils

# Verwende get_wikipedia_title_in_language aus translation_utils


# Verwende die entsprechenden Funktionen aus den Utility-Modulen:
# - get_language_map aus language_utils
# - translate_to_english aus translation_utils
# - generate_entity_synonyms aus synonym_utils

def batch_get_wikidata_ids(entities: Dict[str, Dict[str, Any]], config=None):
    """
    Holt Wikidata-IDs für mehrere Entitäten in einem Batch.
    
    Args:
        entities: Dict mit Entitätsnamen als Schlüssel und Entitätsinformationen als Werte.
                 Jede Entität sollte entweder 'wikipedia_url' oder 'wikidata_id' enthalten.
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Wikidata-IDs als Werte
    """
    logger.info(f"Starte Wikidata-ID-Lookup für {len(entities)} Entitäten")
    
    if config is None:
        config = DEFAULT_CONFIG
        
    # Dictionary für die Ergebnisse vorbereiten
    results = {}
    
    # 1. Cache prüfen - bereits vorhandene Daten identifizieren
    # Erstelle ein Dict mit Wikipedia-URLs als Schlüssel und Entitätsnamen als Werte
    wikipedia_urls = {}
    results = {}
    
    logger.debug("Verarbeite Entitäten:")
    for name, entity in entities.items():
        logger.debug(f"- {name}: {json.dumps(entity, ensure_ascii=False, indent=2)}")
        if 'wikipedia_url' in entity and entity['wikipedia_url']:
            wikipedia_urls[entity['wikipedia_url']] = name
            logger.debug(f"  - Füge Wikipedia-URL hinzu: {entity['wikipedia_url']}")
        elif 'wikidata_id' in entity and entity['wikidata_id']:
            # Wenn bereits eine Wikidata-ID vorhanden ist, direkt zurückgeben
            results[name] = {
                'status': 'found',
                'wikidata_id': entity['wikidata_id'],
                'source': 'existing'
            }
            logger.info(f"Verwende vorhandene Wikidata-ID für {name}: {entity['wikidata_id']}")
            continue
            
        # Sonst Cache prüfen
        cache_key = f"wikidata_id:{name.lower()}"
        cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikidata", cache_key)
        cached = load_cache(cache_path)
        
        if cached is not None:
            results[name] = cached
            logger.info(f"  - Verwende gecachten Wert für {name}: {cached}")
        else:
            logger.info(f"  - Kein Cache-Eintrag für {name} gefunden")
    
    # Wenn alles aus dem Cache geladen werden konnte, früh zurückkehren
    if len(results) == len(entities):
        return results
    
    logger.info(f"Extrahiere Wikidata-IDs aus {len(wikipedia_urls)} Wikipedia-URLs (Batch)")
    found_wikipedia = []
    notfound_wikipedia = []
    for url, name in wikipedia_urls.items():
        logger.info(f"Verarbeite Wikipedia-URL für {name}: {url}")
        try:
            wikidata_id = _get_wikidata_id_from_wikipedia(url, config)
            if wikidata_id:
                results[name] = {
                    'status': 'found',
                    'wikidata_id': wikidata_id,
                    'source': 'wikipedia',
                    'wikipedia_url': url
                }
                found_wikipedia.append(name)
                logger.info(f"Erfolgreich Wikidata-ID für {name} gefunden: {wikidata_id}")
            else:
                notfound_wikipedia.append(name)
                logger.warning(f"Keine Wikidata-ID für {name} in Wikipedia-URL gefunden: {url}")
        except Exception as e:
            notfound_wikipedia.append(name)
            logger.error(f"Fehler beim Extrahieren der Wikidata-ID aus {url}: {str(e)}", exc_info=True)
    logger.info(f"Wikidata-Batch: {len(found_wikipedia)} Entitäten über Wikipedia-URL gefunden, {len(notfound_wikipedia)} nicht gefunden: {notfound_wikipedia}")

    # 3. Für verbleibende Entitäten direkt in Wikidata suchen
    missing_entities = {name: entities[name] for name in entities if name not in results}
    if missing_entities:
        logger.info(f"Starte direkte Wikidata-Suche für {len(missing_entities)} Entitäten (Batch)")
        search_results = _perform_wikidata_search(missing_entities, config)
        found_direct = [name for name, result in search_results.items() if result and 'wikidata_id' in result]
        notfound_direct = [name for name in missing_entities if name not in found_direct]
        logger.info(f"Wikidata-Batch: {len(found_direct)} Entitäten durch direkte Suche gefunden, {len(notfound_direct)} nicht gefunden: {notfound_direct}")
        for name, result in search_results.items():
            if result and 'wikidata_id' in result:
                results[name] = result
                logger.info(f"Erfolgreich Wikidata-ID für {name} durch direkte Suche gefunden: {result['wikidata_id']}")

    # 4. Für weiterhin nicht gefundene Entitäten Synonym-Suche durchführen
    still_missing = {name: entities[name] for name in entities if name not in results}
    if still_missing and config and config.get('USE_SYNONYMS', True):
        logger.info(f"Starte Synonym-Suche für {len(still_missing)} Entitäten (Batch)")
        synonym_results = _perform_synonym_wikidata_search(still_missing, config)
        found_synonym = [name for name, result in synonym_results.items() if result and 'wikidata_id' in result]
        notfound_synonym = [name for name in still_missing if name not in found_synonym]
        logger.info(f"Wikidata-Batch: {len(found_synonym)} Entitäten durch Synonym-Suche gefunden, {len(notfound_synonym)} nicht gefunden: {notfound_synonym}")
        for name, result in synonym_results.items():
            if result and 'wikidata_id' in result:
                results[name] = result
                logger.info(f"Erfolgreich Wikidata-ID für {name} durch Synonym-Suche gefunden: {result['wikidata_id']}")

    # 5. Für weiterhin nicht gefundene Entitäten Übersetzung ins Englische versuchen
    still_missing = {name: entities[name] for name in entities if name not in results}
    if still_missing and config and config.get('TRANSLATE_TO_ENGLISH', True):
        logger.info(f"Starte Übersetzungs-Suche für {len(still_missing)} Entitäten (Batch)")
        translation_results = _perform_translation_wikidata_search(still_missing, config)
        found_translation = [name for name, result in translation_results.items() if result and 'wikidata_id' in result]
        notfound_translation = [name for name in still_missing if name not in found_translation]
        logger.info(f"Wikidata-Batch: {len(found_translation)} Entitäten durch Übersetzungs-Suche gefunden, {len(notfound_translation)} nicht gefunden: {notfound_translation}")
        for name, result in translation_results.items():
            if result and 'wikidata_id' in result:
                results[name] = result
                logger.info(f"Erfolgreich Wikidata-ID für {name} durch Übersetzungssuche gefunden: {result['wikidata_id']}")
    
    # Füge Fehlermeldungen für nicht gefundene Entitäten hinzu
    not_found_count = 0
    for name in entities:
        if name not in results:
            results[name] = {
                'status': 'not_found',
                'error': 'Keine Wikidata-ID gefunden',
                'source': 'none',
                'tried_methods': ['wikipedia_lookup', 'direct_search', 'synonym_search', 'translation_search']
            }
            not_found_count += 1
            logger.warning(f"Keine Wikidata-ID für Entität gefunden: {name}")
    
    if not_found_count > 0:
        logger.error(f"Konnte für {not_found_count} von {len(entities)} Entitäten keine Wikidata-ID finden")
    
    # Zusammenfassung der Ergebnisse
    found_count = len([r for r in results.values() if r.get('status') == 'found'])
    logger.info(f"Zusammenfassung: {found_count} von {len(entities)} Entitäten erfolgreich mit Wikidata verknüpft")
    
    # Debug-Ausgabe der Ergebnisse
    logger.info("Detaillierte Wikidata-Ergebnisse:")
    for name, result in results.items():
        logger.info(f"- {name}: {json.dumps(result, ensure_ascii=False)}")
    
    return results

def batch_get_wikidata_entities(wikidata_ids: Dict[str, str], config=None):
    """
    Holt detaillierte Wikidata-Informationen für mehrere Entitäten in einem Batch.
    
    Args:
        wikidata_ids: Dict mit Entitätsnamen als Schlüssel und Wikidata-IDs als Werte
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Wikidata-Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    # Dictionary für die Ergebnisse vorbereiten
    results = {}
    
    # 1. Cache prüfen - bereits vorhandene Daten identifizieren
    cached_entities = {}
    ids_to_fetch = {}
    
    for entity_name, wikidata_id in wikidata_ids.items():
        cache_key = f"wikidata:{wikidata_id}"
        cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikidata", cache_key)
        cached = load_cache(cache_path)
        
        if cached is not None:
            cached_entities[entity_name] = cached
            results[entity_name] = cached
        else:
            ids_to_fetch[entity_name] = wikidata_id
    
    # Wenn alles aus dem Cache geladen werden konnte, früh zurückkehren
    if not ids_to_fetch:
        return results
    
    # 2. Aufteilung in Batches von je 50 IDs
    batch_size = 50
    entity_items = list(ids_to_fetch.items())
    batches = [dict(entity_items[i:i+batch_size]) for i in range(0, len(entity_items), batch_size)]
    
    # 3. Batch-Abfragen durchführen
    for batch in batches:
        batch_results = _fetch_wikidata_batch(batch, config)
        results.update(batch_results)
        
        # Differenziertes Caching: Speichere nur erfolgreiche Ergebnisse mit Mindestdaten
        for name, data in batch_results.items():
            if data.get("status") == "found":
                # Prüfe, ob Wikidata-Mindestdatenfelder vorhanden sind
                required_fields = ["wikidata_id", "url", "label", "description"]
                has_required_fields = all(key in data and data[key] for key in required_fields)
                has_types = "types" in data and isinstance(data["types"], list) and len(data["types"]) > 0
                
                if has_required_fields and has_types:
                    # Vollständige Daten - cache speichern
                    cache_key = f"wikidata:{data['wikidata_id']}"
                    cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikidata", cache_key)
                    logger.info(f"Cache vollständige Wikidata-Daten für {name}: {data['wikidata_id']}")
                    save_cache(cache_path, data)
                elif has_required_fields:
                    # Teilweise Daten - cache speichern, aber loggen
                    cache_key = f"wikidata:{data['wikidata_id']}"
                    cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "wikidata", cache_key)
                    logger.info(f"Entität {name} hat nicht alle Mindestdatenfelder für Wikidata, wird aber trotzdem gecacht")
                    save_cache(cache_path, data)
    
    return results

def _get_wikidata_id_from_wikipedia(wikipedia_url, config=None):
    """
    Extrahiert die Wikidata-ID aus einer Wikipedia-URL.
    
    Args:
        wikipedia_url: Wikipedia-URL
        config: Konfigurationswörterbuch
        
    Returns:
        Wikidata-ID oder None
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    try:
        # Extrahiere Sprache und Titel aus der URL
        parts = wikipedia_url.split('/wiki/')
        if len(parts) < 2:
            return None
        
        title = parts[1].split('#')[0]
        title = urllib.parse.unquote(title)  # URL-Decode
        title = title.replace('_', ' ')  # Unterstriche durch Leerzeichen ersetzen
        
        lang = wikipedia_url.split('://')[1].split('.')[0]
        
        # API-Anfrage
        endpoint = f"https://{lang}.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'prop': 'pageprops',
            'ppprop': 'wikibase_item',
            'titles': title,
            'format': 'json',
            "maxlag": config.get("WIKIPEDIA_MAXLAG")
        }
        
        headers = {"User-Agent": config.get("USER_AGENT")}
        r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
        r.raise_for_status()
        
        data = r.json()
        pages = data.get('query', {}).get('pages', {})
        
        for page_id, page in pages.items():
            if 'pageprops' in page and 'wikibase_item' in page['pageprops']:
                return page['pageprops']['wikibase_item']
        
        return None
    
    except Exception as e:
        logging.error(f"Fehler beim Extrahieren der Wikidata-ID aus {wikipedia_url}: {e}")
        return None

def _perform_wikidata_search(entities, config=None):
    """
    Direkte Suche nach Entitäten in Wikidata.
    
    Args:
        entities: Dict mit Entitätsnamen als Schlüssel und Entitätsinformationen als Werte
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Suchergebnissen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    results = {}
    
    for entity_name, entity_info in entities.items():
        language = entity_info.get("language", "de")
        
        try:
            endpoint = "https://www.wikidata.org/w/api.php"
            params = {
                'action': 'wbsearchentities',
                'search': entity_name,
                'language': language,
                'format': 'json',
                'limit': 1
            }
            
            headers = {"User-Agent": config.get("USER_AGENT")}
            r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
            r.raise_for_status()
            
            data = r.json()
            search_results = data.get('search', [])
            
            if search_results:
                results[entity_name] = {
                    "status": "found",
                    "wikidata_id": search_results[0]['id'],
                    "source": "direct_search",
                    "match_text": search_results[0].get('label', entity_name),
                    "timestamp": int(time.time())
                }
            else:
                results[entity_name] = {
                    "status": "not_found_direct",
                    "entity": entity_name,
                    "timestamp": int(time.time())
                }
        
        except Exception as e:
            logging.error(f"Fehler bei der Wikidata-Suche für '{entity_name}': {e}")
            results[entity_name] = {
                "status": "error",
                "entity": entity_name,
                "error": str(e),
                "timestamp": int(time.time())
            }
    
    return results

def _perform_synonym_wikidata_search(entities, config=None):
    """
    Suche nach Synonymen für Entitäten in Wikidata.
    
    Args:
        entities: Dict mit Entitätsnamen als Schlüssel und Entitätsinformationen als Werte
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Suchergebnissen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    results = {}
    
    for entity_name, entity_info in entities.items():
        language = entity_info.get("language", "de")
        
        # Synonyme generieren
        synonyms = generate_entity_synonyms(entity_name, language=language)
        
        if not synonyms:
            continue
        
        # Synonyme durchsuchen
        found = False
        for synonym in synonyms:
            if synonym == entity_name:
                continue  # Überspringe das Original
            
            try:
                endpoint = "https://www.wikidata.org/w/api.php"
                params = {
                    'action': 'wbsearchentities',
                    'search': synonym,
                    'language': language,
                    'format': 'json',
                    'limit': 1
                }
                
                headers = {"User-Agent": config.get("USER_AGENT")}
                r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
                r.raise_for_status()
                
                data = r.json()
                search_results = data.get('search', [])
                
                if search_results:
                    results[entity_name] = {
                        "status": "found",
                        "wikidata_id": search_results[0]['id'],
                        "source": "synonym_search",
                        "synonym": synonym,
                        "match_text": search_results[0].get('label', synonym),
                        "timestamp": int(time.time())
                    }
                    found = True
                    break
            
            except Exception as e:
                logging.error(f"Fehler bei der Synonym-Wikidata-Suche für '{entity_name}' mit Synonym '{synonym}': {e}")
                continue
        
        if not found:
            results[entity_name] = {
                "status": "not_found_synonym",
                "entity": entity_name,
                "timestamp": int(time.time())
            }
    
    return results

def _perform_translation_wikidata_search(entities, config=None):
    """
    Übersetzung und Suche für deutsche Entitäten in Wikidata.
    
    Args:
        entities: Dict mit Entitätsnamen als Schlüssel und Entitätsinformationen als Werte
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Suchergebnissen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    results = {}
    
    for entity_name, entity_info in entities.items():
        # Ins Englische übersetzen
        english_term = translate_to_english(entity_name)
        
        if not english_term or english_term == entity_name:
            continue
        
        try:
            # In englischer Wikidata suchen
            endpoint = "https://www.wikidata.org/w/api.php"
            params = {
                'action': 'wbsearchentities',
                'search': english_term,
                'language': 'en',
                'format': 'json',
                'limit': 1
            }
            
            headers = {"User-Agent": config.get("USER_AGENT")}
            r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 15))
            r.raise_for_status()
            
            data = r.json()
            search_results = data.get('search', [])
            
            if search_results:
                results[entity_name] = {
                    "status": "found",
                    "wikidata_id": search_results[0]['id'],
                    "source": "translation_search",
                    "original": entity_name,
                    "translation": english_term,
                    "match_text": search_results[0].get('label', english_term),
                    "timestamp": int(time.time())
                }
            else:
                results[entity_name] = {
                    "status": "not_found_translation",
                    "entity": entity_name,
                    "translation": english_term,
                    "timestamp": int(time.time())
                }
        
        except Exception as e:
            logging.error(f"Fehler bei der Übersetzungs-Wikidata-Suche für '{entity_name}' -> '{english_term}': {e}")
            results[entity_name] = {
                "status": "error",
                "entity": entity_name,
                "error": str(e),
                "timestamp": int(time.time())
            }
    
    return results

def _fetch_wikidata_batch(entities_with_ids, config=None):
    """
    Batch-Abfrage für Wikidata-Entitäten.
    
    Args:
        entities_with_ids: Dict mit Entitätsnamen als Schlüssel und Wikidata-IDs als Werte
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Wikidata-Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    results = {entity_name: {"status": "pending"} for entity_name in entities_with_ids}
    
    try:
        # API-Endpunkt und Parameter
        endpoint = "https://www.wikidata.org/w/api.php"
        ids = list(entities_with_ids.values())
        
        params = {
            'action': 'wbgetentities',
            'ids': '|'.join(ids),
            'format': 'json',
            'languages': 'de|en',
            'props': 'labels|descriptions|claims|aliases|sitelinks'
        }
        
        headers = {"User-Agent": config.get("USER_AGENT", "Wikidata Entity Extractor/1.0")}
        
        logging.info(f"Wikidata-Batch-Abfrage für {len(ids)} Entitäten")
        r = limited_get(endpoint, params=params, headers=headers, timeout=config.get('TIMEOUT_THIRD_PARTY', 30))
        r.raise_for_status()
        data = r.json()
        
        # Mapping von Wikidata-IDs zu Entitätsnamen erstellen
        id_to_entity = {wikidata_id: entity_name for entity_name, wikidata_id in entities_with_ids.items()}
        
        # Entitäten aus der Antwort extrahieren
        entities = data.get('entities', {})
        
        # 1. Sammle alle Q-IDs aus allen wichtigen Properties für Batch-Label-Lookup
        # Wir sammeln nicht nur instance_of und subclass_of, sondern auch part_of, has_parts etc.
        all_reference_qids = set()
        property_ids = ['P31', 'P279', 'P361', 'P527', 'P106', 'P27', 'P19', 'P20', 'P463']
        
        for wikidata_id, entity_data in entities.items():
            claims = entity_data.get('claims', {})
            for prop_id in property_ids:
                all_reference_qids.update(_extract_claim_values(claims, prop_id))
        
        # Filtern auf nur Q-IDs
        all_reference_qids = {qid for qid in all_reference_qids if isinstance(qid, str) and qid.startswith('Q')}
        
        # 2. Hole Labels für alle Q-IDs in einem Batch (für deutlich bessere Qualität)
        reference_labels = {}
        if all_reference_qids:
            # Batche die Q-IDs in Gruppen von 50, um API-Limits zu vermeiden
            batch_size = 50
            qid_batches = [list(all_reference_qids)[i:i+batch_size] for i in range(0, len(all_reference_qids), batch_size)]
            logging.info(f"Starte Batch-Lookup für {len(all_reference_qids)} Referenz-Entitäten in {len(qid_batches)} Batches")
            
            for batch_idx, qid_batch in enumerate(qid_batches):
                batch_endpoint = "https://www.wikidata.org/w/api.php"
                batch_params = {
                    'action': 'wbgetentities',
                    'ids': '|'.join(qid_batch),
                    'format': 'json',
                    'languages': 'de|en',
                    'props': 'labels|descriptions'
                }
                batch_headers = {"User-Agent": config.get("USER_AGENT", "Wikidata Entity Extractor/1.0")}
                try:
                    r_batch = limited_get(batch_endpoint, params=batch_params, headers=batch_headers, 
                                          timeout=config.get('TIMEOUT_THIRD_PARTY', 30))
                    r_batch.raise_for_status()
                    label_data = r_batch.json().get('entities', {})
                    for qid, data in label_data.items():
                        # Für jede Q-ID speichern wir die Labels in allen verfügbaren Sprachen
                        de_label = data.get('labels', {}).get('de', {}).get('value')
                        en_label = data.get('labels', {}).get('en', {}).get('value')
                        
                        # Bevorzuge deutsches Label, mit Fallback auf englisches Label
                        primary_label = de_label or en_label or qid
                        
                        description = (data.get('descriptions', {}).get('de', {}).get('value') or 
                                      data.get('descriptions', {}).get('en', {}).get('value'))
                        
                        if de_label or en_label or description:
                            reference_labels[qid] = {
                                "label": primary_label,  # Haupt-Label (bevorzugt deutsch)
                                "de": de_label,          # Explizites deutsches Label
                                "en": en_label,          # Explizites englisches Label
                                "description": description or ""
                            }
                    logging.info(f"Batch {batch_idx+1}/{len(qid_batches)}: {len(reference_labels)} Labels/Beschreibungen abgerufen")
                except Exception as e:
                    logging.error(f"Fehler beim Batch-Lookup der Referenz-Labels (Batch {batch_idx+1}): {e}")
        
        # 3. Ergebnisse verarbeiten und mit den detaillierten Labels anreichern
        for wikidata_id, entity_data in entities.items():
            entity_name = id_to_entity.get(wikidata_id)
            if not entity_name:
                continue
            
            # Behandlung fehlender Entitäten
            if 'missing' in entity_data:
                results[entity_name] = {
                    "status": "missing",
                    "wikidata_id": wikidata_id,
                    "timestamp": int(time.time())
                }
                continue
                
            # Labels extrahieren (vollständig)
            labels = {}
            for lang, label_data in entity_data.get('labels', {}).items():
                labels[lang] = label_data.get('value')
                
            # Beschreibungen extrahieren (vollständig)
            descriptions = {}
            for lang, desc_data in entity_data.get('descriptions', {}).items():
                descriptions[lang] = desc_data.get('value')
                
            # Aliase extrahieren (vollständig)
            aliases = {}
            for lang, alias_list in entity_data.get('aliases', {}).items():
                aliases[lang] = [alias.get('value') for alias in alias_list]
                
            # Claims extrahieren
            claims = entity_data.get('claims', {})
            
            # Wichtige Eigenschaften mit Labels extrahieren
            # Die Extraktion von Claims in Funktionen bündeln
            instance_of_qids = _extract_claim_values(claims, 'P31')  # instance of
            subclass_of_qids = _extract_claim_values(claims, 'P279')  # subclass of
            part_of_qids = _extract_claim_values(claims, 'P361')  # part of
            has_parts_qids = _extract_claim_values(claims, 'P527')  # has part
            occupations_qids = _extract_claim_values(claims, 'P106')  # occupation
            citizenships_qids = _extract_claim_values(claims, 'P27')  # country of citizenship
            birthplace_qids = _extract_claim_values(claims, 'P19')  # place of birth
            deathplace_qids = _extract_claim_values(claims, 'P20')  # place of death
            member_of_qids = _extract_claim_values(claims, 'P463')  # member of
            
            # Weitere wichtige Eigenschaften extrahieren
            gnd_ids = _extract_claim_values(claims, 'P227')  # GND ID
            gnd_id = gnd_ids[0] if gnd_ids else None
            
            # Bild extrahieren
            image_url = _extract_image_url(claims)
            
            # Wikipedia-URLs extrahieren
            sitelinks = entity_data.get('sitelinks', {})
            wikipedia_urls = {}
            for site, sitelink in sitelinks.items():
                if site.endswith('wiki'):
                    lang = site.replace('wiki', '')
                    title = sitelink.get('title', '')
                    url = f"https://{lang}.wikipedia.org/wiki/{urllib.parse.quote(title.replace(' ', '_'))}"
                    wikipedia_urls[lang] = url
            
            # Für jede Property, generiere Label-Informationen mit zusätzlichen IDs
            def create_labeled_list(qid_list):
                """Erstellt eine Liste mit Labels und IDs"""
                result = []
                result_with_ids = []
                for qid in qid_list:
                    ref_data = reference_labels.get(qid, {})
                    label = ref_data.get("label", qid)  # Fallback zur QID wenn kein Label
                    result.append(label)
                    # Dict mit ID und Label für verbesserte Ausgabe
                    result_with_ids.append({
                        "id": qid,
                        "label": label,
                        "de": ref_data.get("de", label) or label,  # Deutsches Label bevorzugen
                        "en": ref_data.get("en", label) or label   # Englisches Label als Fallback
                    })
                return result, result_with_ids
            
            # Typen und Subklassen als lesbare Labels und mit IDs konvertieren
            instance_of_labels, instance_of_with_ids = create_labeled_list(instance_of_qids)
            subclass_of_labels, subclass_of_with_ids = create_labeled_list(subclass_of_qids)
            part_of_labels, part_of_with_ids = create_labeled_list(part_of_qids)
            has_parts_labels, has_parts_with_ids = create_labeled_list(has_parts_qids)
            occupations_labels, occupations_with_ids = create_labeled_list(occupations_qids)
            citizenships_labels, citizenships_with_ids = create_labeled_list(citizenships_qids)
            
            # Einzelwerte für Geburts- und Sterbeort
            birthplace_labels, birthplace_with_ids = create_labeled_list(birthplace_qids)
            deathplace_labels, deathplace_with_ids = create_labeled_list(deathplace_qids)
            birthplace_label = birthplace_labels[0] if birthplace_labels else ""
            deathplace_label = deathplace_labels[0] if deathplace_labels else ""
            birthplace_with_id = birthplace_with_ids[0] if birthplace_with_ids else None
            deathplace_with_id = deathplace_with_ids[0] if deathplace_with_ids else None
            
            member_of_labels, member_of_with_ids = create_labeled_list(member_of_qids)
            
            # Label/Description extrahieren (de, dann en)
            label = labels.get("de") or labels.get("en") or ""
            description = descriptions.get("de") or descriptions.get("en") or ""
            
            # Ergebnis zusammenstellen mit allen verfügbaren Informationen
            results[entity_name] = {
                "status": "found",
                "wikidata_id": wikidata_id,
                "labels": labels,
                "descriptions": descriptions,
                "label": label,
                "description": description,
                "aliases": aliases,
                "instance_of": instance_of_qids,
                "subclass_of": subclass_of_qids,
                # Sowohl menschenlesbare Labels als auch strukturierte Daten mit IDs
                "types": instance_of_labels,
                "types_with_ids": instance_of_with_ids,
                "subclasses": subclass_of_labels,
                "subclasses_with_ids": subclass_of_with_ids,
                "part_of": part_of_labels,
                "part_of_with_ids": part_of_with_ids,
                "has_parts": has_parts_labels,
                "has_parts_with_ids": has_parts_with_ids,
                "gnd_id": gnd_id,
                "image_url": image_url,
                "wikipedia_urls": wikipedia_urls,
                # Zusätzliche Felder für mehr Kontext
                "occupations": occupations_labels,
                "occupations_with_ids": occupations_with_ids,
                "citizenships": citizenships_labels,
                "citizenships_with_ids": citizenships_with_ids,
                "birth_place": birthplace_label,
                "birth_place_with_id": birthplace_with_id,
                "death_place": deathplace_label,
                "death_place_with_id": deathplace_with_id,
                "member_of": member_of_labels,
                "member_of_with_ids": member_of_with_ids,
                "timestamp": int(time.time())
            }
        
        # Prüfen, ob es Entitäten gibt, die nicht gefunden wurden
        for entity_name, result in results.items():
            if result["status"] == "pending":
                results[entity_name] = {
                    "status": "error",
                    "wikidata_id": entities_with_ids[entity_name],
                    "error": "Entity not found in response",
                    "timestamp": int(time.time())
                }
        
        return results
        
    except Exception as e:
        logging.error(f"Fehler bei Wikidata-Batch-Abfrage: {e}")
        # Bei einem Fehler alle pendenten Entitäten als error markieren
        for entity_name, result in results.items():
            if result["status"] == "pending":
                results[entity_name] = {
                    "status": "error",
                    "wikidata_id": entities_with_ids[entity_name],
                    "error": str(e),
                    "timestamp": int(time.time())
                }
        return results

def _extract_claim_values(claims, property_id):
    """
    Extrahiert Werte aus einem Wikidata-Claim.
    
    Args:
        claims: Dict mit Claims aus der Wikidata-API
        property_id: Eigenschafts-ID (z.B. 'P31' für 'instance of')
        
    Returns:
        Liste von Werten
    """
    values = []
    
    if property_id not in claims:
        return values
        
    for claim in claims[property_id]:
        mainsnak = claim.get('mainsnak', {})
        if mainsnak.get('snaktype') != 'value':
            continue
            
        datavalue = mainsnak.get('datavalue', {})
        if datavalue.get('type') == 'wikibase-entityid':
            entity_id = datavalue.get('value', {}).get('id')
            if entity_id:
                values.append(entity_id)
        elif datavalue.get('type') == 'string':
            string_value = datavalue.get('value')
            if string_value:
                values.append(string_value)
        elif datavalue.get('type') == 'time':
            time_value = datavalue.get('value', {}).get('time')
            if time_value:
                values.append(time_value)
        elif datavalue.get('type') == 'monolingualtext':
            text_value = datavalue.get('value', {}).get('text')
            if text_value:
                values.append(text_value)
    
    return values

def _extract_image_url(claims):
    """
    Extrahiert die Bild-URL aus Wikidata-Claims.
    
    Args:
        claims: Dict mit Claims aus der Wikidata-API
        
    Returns:
        Bild-URL oder None
    """
    # P18 ist die Eigenschaft für Bilder
    image_values = _extract_claim_values(claims, 'P18')
    
    if not image_values:
        return None
        
    image_filename = image_values[0]
    
    # MD5-Hash des Dateinamens berechnen (Wikimedia-Konvention)
    image_filename = image_filename.replace(' ', '_')
    md5_hash = hashlib.md5(image_filename.encode('utf-8')).hexdigest()
    
    # URL nach Wikimedia-Commons-Konvention generieren
    url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{urllib.parse.quote(image_filename)}"
    
    return url

def get_wikidata_entity(wikidata_id, config=None):
    """
    Einzelne Entitätsabfrage - Wrapper um die Batch-Funktion.
    
    Args:
        wikidata_id: Wikidata-ID
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Wikidata-Informationen zur Entität
    """
    results = batch_get_wikidata_entities({"entity": wikidata_id}, config)
    return results.get("entity", {"status": "error", "error": "Unknown error"})
