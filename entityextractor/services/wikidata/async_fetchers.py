#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Asynchrone API-Interaktionsmodule für den Wikidata-Service.

Dieses Modul stellt asynchrone Funktionen für den Abruf von Daten über die Wikidata-API bereit.
Es unterstützt Batch-Verarbeitung und effiziente Datenabfragen.
"""

import logging
import json
import aiohttp
import asyncio
from typing import List, Dict, Any, Optional, Set, Tuple

from entityextractor.utils.api_request_utils import create_standard_headers
from entityextractor.utils.rate_limiter import RateLimiter
from entityextractor.utils.logging_utils import get_service_logger

# Logger konfigurieren
logger = get_service_logger(__name__, 'wikidata')

# Konstanten
WIKIDATA_API_URL = 'https://www.wikidata.org/w/api.php'

# Asynchroner Rate-Limiter für API-Anfragen
_async_rate_limiter = RateLimiter(5, 1.0)  # 5 Anfragen pro Sekunde für Wikidata

@_async_rate_limiter
async def async_limited_get(url, headers=None, params=None, timeout=None, config=None):
    """
    Führt einen asynchronen GET-Request mit Rate-Limiting durch.
    
    Args:
        url: URL für den Request
        headers: Optional, HTTP-Header
        params: Optional, URL-Parameter
        timeout: Optional, Timeout in Sekunden
        config: Optional, Konfiguration
        
    Returns:
        JSON-Antwort oder None bei Fehler
    """
    if not config:
        config = {}
        
    if not headers:
        headers = create_standard_headers()
        
    if not timeout:
        timeout = config.get("TIMEOUT_THIRD_PARTY", 15)
    
    # Detailliertes Logging der Anfrageparameter für Diagnose
    logger.debug(f"Wikidata API: URL={url}, Params={params}")
    
    try:
        # API-Anfrage mit aiohttp und erweiterter Fehlerbehandlung
        async with aiohttp.ClientSession() as session:
            try:
                logger.debug(f"HTTP-Request: URL={url}, Timeout={timeout}s")
                response = await session.get(url, params=params, headers=headers, timeout=timeout)
                logger.debug(f"API Status: {response.status}")
                
                if response.status == 200:
                    try:
                        json_data = await response.json()
                        logger.debug(f"JSON-Antwort: {list(json_data.keys()) if json_data else 'Keine'} Keys")
                        return json_data
                    except Exception as json_error:
                        logger.error(f"Fehler beim Parsen der JSON-Antwort: {str(json_error)}")
                        text = await response.text()
                        logger.debug(f"Rohantwort: {text[:100]}..." if len(text) > 100 else text)
                        return None
                else:
                    logger.error(f"HTTP-Fehler {response.status} bei {url}")
                    try:
                        error_text = await response.text()
                        logger.error(f"Fehlerantwort: {error_text[:200]}..." if len(error_text) > 200 else error_text)
                    except:
                        pass
                    return None
            except aiohttp.ClientError as e:
                logger.error(f"aiohttp ClientError bei Wikidata API-Anfrage: {str(e)}")
                raise
            except asyncio.TimeoutError:
                logger.error(f"Timeout bei Wikidata API-Anfrage nach {timeout} Sekunden")
                raise
    except Exception as e:
        logger.error(f"Unbehandelte Exception bei API-Anfrage an {url}: {str(e)}", exc_info=True)
        return None

async def async_fetch_wikidata_batch(entity_ids: List[str], config: Dict[str, Any] = None) -> List[Dict]:
    """
    Ruft Daten für mehrere Wikidata-Entitäten in einem Batch ab.
    
    Args:
        entity_ids: Liste von Wikidata-IDs oder Entitätsnamen
        config: Konfiguration (optional)
        
    Returns:
        Liste mit Wikidata-Daten für jede Entität
    """
    logger.debug(f"async_fetch_wikidata_batch: {len(entity_ids)} Entitäten")
    if not entity_ids:
        return []
    
    # Konfiguration laden
    if not config:
        config = {}
        
    user_agent = config.get('USER_AGENT', 'EntityExtractor/1.0')
    batch_size = config.get('WIKIDATA_BATCH_SIZE', 50)
    languages = config.get('LANGUAGES', ['de', 'en'])
    if isinstance(languages, str):
        languages = [languages]
    
    # Ergebnisse vorbereiten
    results = []
    
    try:
        # Verarbeite Entitäten in Batches
        for i in range(0, len(entity_ids), batch_size):
            batch = entity_ids[i:i+batch_size]
            logger.debug(f"Verarbeite Batch {i//batch_size + 1} mit {len(batch)} Entitäten")
            
            # Prüfe, ob es sich um Wikidata-IDs oder Entitätsnamen handelt
            is_wikidata_ids = all(eid.startswith('Q') and eid[1:].isdigit() for eid in batch)
            
            if is_wikidata_ids:
                # Wenn es Wikidata-IDs sind, verwende wbgetentities
                batch_results = await _fetch_wikidata_entities(batch, WIKIDATA_API_URL, user_agent, languages, config)
            else:
                # Wenn es Entitätsnamen sind, verwende wbsearchentities
                batch_results = await _search_wikidata_entities(batch, WIKIDATA_API_URL, user_agent, languages[0], config)
            
            results.extend(batch_results)
    
    except Exception as e:
        logger.error(f"Fehler beim Batch-Abruf von Wikidata: {str(e)}", exc_info=True)
    
    return results

async def _fetch_wikidata_entities(entity_ids: List[str], api_url: str, user_agent: str, 
                                  languages: List[str], config: Dict[str, Any]) -> List[Dict]:
    """
    Ruft detaillierte Informationen für Wikidata-Entitäten ab.
    
    Args:
        entity_ids: Liste von Wikidata-IDs
        api_url: URL der Wikidata-API
        user_agent: User-Agent für die API-Anfrage
        languages: Liste der Sprachen für Labels, Beschreibungen, etc.
        config: Konfiguration
        
    Returns:
        Liste mit Wikidata-Daten für jede Entität
    """
    results = []
    
    try:
        # Parameter für wbgetentities
        params = {
            'action': 'wbgetentities',
            'format': 'json',
            'ids': '|'.join(entity_ids),
            'props': 'labels|descriptions|aliases|claims',  # Explizit nur diese Properties abrufen, keine Sitelinks
            'languages': '|'.join(languages),
            'normalize': '1'
        }
        
        # API-Anfrage senden
        headers = create_standard_headers(user_agent)
        json_response = await async_limited_get(
            api_url,
            headers=headers,
            params=params,
            timeout=config.get('TIMEOUT_THIRD_PARTY', 15),
            config=config
        )
        
        if json_response and 'entities' in json_response:
            entities = json_response['entities']
            
            # Verarbeite jede Entität
            for entity_id in entity_ids:
                if entity_id in entities:
                    entity_data = entities[entity_id]
                    
                    # Formatiere das Ergebnis
                    formatted_result = {
                        'id': entity_id,
                        'type': entity_data.get('type', ''),
                        'labels': entity_data.get('labels', {}),
                        'descriptions': entity_data.get('descriptions', {}),
                        'aliases': entity_data.get('aliases', {}),
                        'claims': entity_data.get('claims', {}),
                        # Sitelinks komplett entfernt
                        'status': 'found',
                        'source': 'wikidata_api'
                    }
                    
                    results.append(formatted_result)
                else:
                    # Entität nicht gefunden
                    results.append({
                        'id': entity_id,
                        'status': 'not_found',
                        'source': 'wikidata_api'
                    })
        else:
            # API-Fehler
            for entity_id in entity_ids:
                results.append({
                    'id': entity_id,
                    'status': 'error',
                    'source': 'wikidata_api',
                    'error': 'API-Fehler oder keine Antwort'
                })
    
    except Exception as e:
        logger.error(f"Fehler beim Abruf von Wikidata-Entitäten: {str(e)}", exc_info=True)
        # Bei Fehler leere Ergebnisse für alle Entitäten
        for entity_id in entity_ids:
            results.append({
                'id': entity_id,
                'status': 'error',
                'source': 'wikidata_api',
                'error': str(e)
            })
    
    return results

async def _search_wikidata_entities(entity_names: List[str], api_url: str, user_agent: str, 
                                   language: str, config: Dict[str, Any]) -> List[Dict]:
    """
    Sucht nach Wikidata-Entitäten basierend auf Namen/Bezeichnungen.
    
    Args:
        entity_names: Liste von Entitätsnamen
        api_url: URL der Wikidata-API
        user_agent: User-Agent für die API-Anfrage
        language: Sprache für die Suche
        config: Konfiguration
        
    Returns:
        Liste mit Wikidata-Daten für jede Entität
    """
    results = []
    entity_ids = []
    
    try:
        # Zuerst nach Entitäten suchen
        for entity_name in entity_names:
            # Parameter für wbsearchentities
            params = {
                'action': 'wbsearchentities',
                'format': 'json',
                'search': entity_name,
                'language': language,
                'limit': 5,  # Mehr Ergebnisse für bessere Trefferquote
                'strictlanguage': '0'  # Auch andere Sprachen durchsuchen (als String '0' statt Boolean False)
            }
            
            # API-Anfrage senden
            headers = create_standard_headers(user_agent)
            json_response = await async_limited_get(
                api_url,
                headers=headers,
                params=params,
                timeout=config.get('TIMEOUT_THIRD_PARTY', 15),
                config=config
            )
            
            if json_response and 'search' in json_response and json_response['search']:
                # Beste Übereinstimmung nehmen
                best_match = json_response['search'][0]
                entity_ids.append(best_match['id'])
                
                # Vorläufiges Ergebnis speichern
                results.append({
                    'id': best_match['id'],
                    'entity_name': entity_name,
                    'label': best_match.get('label', entity_name),
                    'description': best_match.get('description', ''),
                    'status': 'found',
                    'source': 'wikidata_search'
                })
            else:
                # Keine Übereinstimmung gefunden
                results.append({
                    'entity_name': entity_name,
                    'status': 'not_found',
                    'source': 'wikidata_search'
                })
        
        # Wenn Entitäten gefunden wurden, detaillierte Informationen abrufen
        if entity_ids:
            detailed_results = await _fetch_wikidata_entities(entity_ids, api_url, user_agent, [language], config)
            
            # Ergebnisse mit den detaillierten Informationen aktualisieren
            for i, result in enumerate(results):
                if result['status'] == 'found' and i < len(detailed_results):
                    # Detaillierte Informationen hinzufügen
                    result.update(detailed_results[i])
    
    except Exception as e:
        logger.error(f"Fehler bei der Wikidata-Suche: {str(e)}", exc_info=True)
        # Bei Fehler leere Ergebnisse für alle Entitäten
        results = []
        for entity_name in entity_names:
            results.append({
                'entity_name': entity_name,
                'status': 'error',
                'source': 'wikidata_search',
                'error': str(e)
            })
    
    return results

async def async_fetch_entity_labels(entity_ids: List[str], language: str = 'de') -> Dict[str, str]:
    """
    Ruft nur die Labels für eine Liste von Wikidata-Entitäts-IDs im Batch ab.
    
    Args:
        entity_ids: Liste von Wikidata-Entitäts-IDs (z.B. ['Q123', 'Q456'])
        language: Bevorzugte Sprache für Labels
        
    Returns:
        Dictionary mit Entitäts-IDs als Schlüssel und Labels als Werte
    """
    # Maximal 50 Entitäten pro Anfrage
    languages = [language]
    if language != 'en':
        languages.append('en')  # Englisch als Fallback
    
    results = {}
    
    # Batch in Gruppen von maximal 50 aufteilen
    for i in range(0, len(entity_ids), 50):
        batch = entity_ids[i:i+50]
        try:
            # Nur Labels abrufen für optimierte Anfrage
            params = {
                'action': 'wbgetentities',
                'format': 'json',
                'ids': '|'.join(batch),
                'props': 'labels',  # Nur Labels abrufen
                'languages': '|'.join(languages),
                'normalize': '1'
            }
            
            # API-Anfrage senden
            headers = create_standard_headers()
            async with aiohttp.ClientSession() as session:
                async with session.get(WIKIDATA_API_URL, params=params) as response:
                    if response.status == 200:
                        batch_data = await response.json()
                        
                        # Labels extrahieren
                        if 'entities' in batch_data:
                            for entity_id, entity_data in batch_data['entities'].items():
                                # Bevorzugte Sprache oder Fallback
                                if 'labels' in entity_data:
                                    if language in entity_data['labels']:
                                        results[entity_id] = entity_data['labels'][language]['value']
                                    elif 'en' in entity_data['labels']:
                                        results[entity_id] = entity_data['labels']['en']['value']
                                    elif entity_data['labels']:
                                        # Erste verfügbare Sprache als Fallback
                                        first_lang = next(iter(entity_data['labels']))
                                        results[entity_id] = entity_data['labels'][first_lang]['value']
                                    else:
                                        results[entity_id] = ''  # Kein Label verfügbar
                                else:
                                    results[entity_id] = ''  # Keine Labels vorhanden
        except Exception as e:
            logger.error(f"Fehler beim Batch-Abruf von Wikidata-Labels: {str(e)}")
    
    return results


async def async_search_wikidata(query: str, language: str = 'de', limit: int = 10, 
                               config: Dict[str, Any] = None) -> List[Dict]:
    """
    Sucht nach Wikidata-Entitäten mit einer Suchanfrage.
    
    Args:
        query: Suchanfrage
        language: Sprache für die Suche
        limit: Maximale Anzahl von Ergebnissen
        config: Konfiguration
        
    Returns:
        Liste mit Suchergebnissen
    """
    if not config:
        config = {}
    
    api_url = config.get('WIKIDATA_API_URL', 'https://www.wikidata.org/w/api.php')
    user_agent = config.get('USER_AGENT', 'EntityExtractor/1.0')
    
    results = []
    
    try:
        # Parameter für wbsearchentities
        params = {
            'action': 'wbsearchentities',
            'format': 'json',
            'search': query,
            'language': language,
            'limit': limit,
            'strictlanguage': '0'  # Auch andere Sprachen durchsuchen (als String '0' statt Boolean False)
        }
        
        # API-Anfrage senden
        headers = create_standard_headers(user_agent)
        json_response = await async_limited_get(
            api_url,
            headers=headers,
            params=params,
            timeout=config.get('TIMEOUT_THIRD_PARTY', 15),
            config=config
        )
        
        if json_response and 'search' in json_response:
            results = json_response['search']
    
    except Exception as e:
        logger.error(f"Fehler bei der Wikidata-Suche für '{query}': {str(e)}", exc_info=True)
    
    return results
