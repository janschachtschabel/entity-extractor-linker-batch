#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wikipedia-Service Fallback-Mechanismen

Dieses Modul enthält die Fallback-Strategien für den Wikipedia-Service, um die
Verlinkungsquote zu maximieren. Es verwendet verschiedene Ansätze, um Entitäten
zu finden, wenn die primäre Suche fehlschlägt.
"""

import logging
import aiohttp
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional, Tuple

from entityextractor.services.wikipedia.async_fetchers import async_fetch_wikipedia_data
from entityextractor.utils.synonym_utils import generate_entity_synonyms
from entityextractor.utils.logging_utils import get_service_logger

# Configure logger using loguru
from loguru import logger
logger = get_service_logger(__name__, 'wikipedia')


async def apply_language_fallback(
    entity_name: str,
    wiki_result: Optional[Dict[str, Any]],
    user_agent: str,
    config: Dict[str, Any]
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Versucht, Daten aus der englischen Wikipedia zu holen, wenn die primäre
    Wikipedia keine ausreichenden Ergebnisse liefert. Funktioniert für jede
    Sprachkombination, auch wenn kein Ergebnis vorhanden ist.
    
    Args:
        entity_name: Name der Entität
        wiki_result: Aktuelles Ergebnis aus der primären Wikipedia-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        
    Returns:
        Tuple mit (aktualisiertes Wiki-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = 0
    
    # Bestimmen der aktuellen Sprache (aus dem Ergebnis oder der Konfiguration)
    current_language = wiki_result.get('language') if wiki_result else config.get('LANGUAGE', 'de')
    
    # Wenn wir kein Ergebnis haben ODER wenn wir ein Ergebnis ohne Extract haben
    # ODER wenn wir auf Englisch sind und trotzdem kein vollständiges Ergebnis haben
    if not wiki_result or not wiki_result.get('extract'):
        try:
            target_language = 'en' if current_language != 'en' else 'de'  # Fallback zu einer anderen Sprache
            logger.info(f"Versuche Sprach-Fallback {current_language} -> {target_language} für '{entity_name}'")
            logger.info(f"[Fallback 1/4] Sprach-Fallback {current_language} -> {target_language} für '{entity_name}'")
            
            # Versuche es mit alternativer Sprache
            fallback_api_url = f'https://{target_language}.wikipedia.org/w/api.php'
            fallback_results = await async_fetch_wikipedia_data(
                [entity_name], 
                fallback_api_url, 
                user_agent, 
                config
            )
            
            if entity_name in fallback_results and fallback_results[entity_name].get('extract'):
                extract_length = len(fallback_results[entity_name].get('extract', ''))
                logger.info(f"[Erfolg] {target_language.upper()}-Sprach-Fallback für '{entity_name}' lieferte {extract_length} Zeichen und Wikidata-ID: {fallback_results[entity_name].get('wikidata_id', 'keine')}")
                wiki_result = fallback_results[entity_name]
                wiki_result['fallback_source'] = f'{target_language}_wikipedia'
                wiki_result['fallback_attempts'] = 1
                fallback_attempts = 1
                logger.debug(f"{target_language.upper()}-API-Antwort für '{entity_name}': {list(fallback_results[entity_name].keys())}")
        except Exception as e:
            logger.error(f"Fehler beim Sprach-Fallback für '{entity_name}': {str(e)}")
    
    return wiki_result, fallback_attempts


async def apply_opensearch_fallback(
    entity_name: str,
    wiki_result: Optional[Dict[str, Any]],
    api_url: str,
    user_agent: str,
    config: Dict[str, Any],
    current_fallback_attempts: int
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Verwendet die OpenSearch-API von Wikipedia, um alternative Titel für die
    Entität zu finden.
    
    Args:
        entity_name: Name der Entität
        wiki_result: Aktuelles Ergebnis aus der primären Wikipedia-API
        api_url: URL der Wikipedia-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        current_fallback_attempts: Anzahl der bisherigen Fallback-Versuche
        
    Returns:
        Tuple mit (aktualisiertes Wiki-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = current_fallback_attempts
    
    if not wiki_result or not wiki_result.get('extract'):
        try:
            logger.info(f"[Fallback 2/4] OpenSearch-Fallback für '{entity_name}'")
            
            # OpenSearch API-Anfrage
            params = {
                "action": "opensearch",
                "search": entity_name,
                "limit": 5,  # Erhöht auf 5 für mehr Optionen
                "namespace": 0,
                "format": "json"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, params=params, headers={"User-Agent": user_agent}) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data and len(data) >= 2 and data[1]:
                            # Log vorgeschlagene Titel für den User
                            suggestions = ", ".join([f"'{t}'" for t in data[1][:5]])
                            logger.info(f"OpenSearch-Vorschläge für '{entity_name}': {suggestions}")
                            logger.debug(f"Vollständige OpenSearch-Antwort: {data}")
                            
                            # Versuche jeden Vorschlag, bis einer funktioniert
                            for i, suggested_title in enumerate(data[1]):
                                logger.info(f"Teste OpenSearch-Vorschlag [{i+1}/{len(data[1])}] '{suggested_title}' für '{entity_name}'")
                                suggested_results = await async_fetch_wikipedia_data(
                                    [suggested_title], 
                                    api_url, 
                                    user_agent, 
                                    config
                                )
                                
                                if suggested_title in suggested_results and \
                                   suggested_results[suggested_title].get('extract'):
                                    # Ergebnis gefunden
                                    extract_length = len(suggested_results[suggested_title].get('extract', ''))
                                    wikidata_id = suggested_results[suggested_title].get('wikidata_id', 'keine')
                                    logger.info(f"[Erfolg] OpenSearch-Fallback mit '{suggested_title}' für '{entity_name}' lieferte {extract_length} Zeichen und Wikidata-ID: {wikidata_id}")
                                    wiki_result = suggested_results[suggested_title]
                                    wiki_result['fallback_source'] = 'opensearch'
                                    wiki_result['fallback_title'] = suggested_title
                                    wiki_result['original_title'] = entity_name
                                    wiki_result['fallback_attempts'] = fallback_attempts + 1
                                    fallback_attempts += 1
                                    break
        except Exception as e:
            logger.error(f"Fehler beim OpenSearch-Fallback für '{entity_name}': {str(e)}")
    
    return wiki_result, fallback_attempts


async def apply_synonym_fallback(
    entity_name: str,
    wiki_result: Optional[Dict[str, Any]],
    api_url: str,
    user_agent: str,
    config: Dict[str, Any],
    current_fallback_attempts: int,
    max_fallback_attempts: int
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Generiert Synonyme für die Entität und versucht, mit diesen Wikipedia-Daten zu finden.
    
    Args:
        entity_name: Name der Entität
        wiki_result: Aktuelles Ergebnis aus der primären Wikipedia-API
        api_url: URL der Wikipedia-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        current_fallback_attempts: Anzahl der bisherigen Fallback-Versuche
        max_fallback_attempts: Maximale Anzahl von Fallback-Versuchen
        
    Returns:
        Tuple mit (aktualisiertes Wiki-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = current_fallback_attempts
    
    if (not wiki_result or not wiki_result.get('extract')) and fallback_attempts < max_fallback_attempts:
        try:
            logger.info(f"[Fallback 3/4] Synonym-Fallback für '{entity_name}'")
            
            # Generiere Synonyme für die Entität
            language = 'en' if api_url and 'en.wikipedia.org' in api_url else 'de'
            synonyms = generate_entity_synonyms(entity_name, language=language, config=config)
            
            # Zeige generierte Synonyme an
            if synonyms:
                synonym_list = ", ".join([f"'{s}'" for s in synonyms[:10]])
                logger.info(f"Generierte Synonyme für '{entity_name}': {synonym_list}" + 
                          (" (gekürzt)" if len(synonyms) > 10 else ""))
                logger.debug(f"Alle generierten Synonyme ({len(synonyms)}): {synonyms}")
            else:
                logger.info(f"Keine Synonyme für '{entity_name}' gefunden")
                
            # Versuche jedes Synonym, bis eines funktioniert
            for i, synonym in enumerate(synonyms):
                if synonym.lower() == entity_name.lower():
                    continue  # Überspringe das Original
                    
                logger.info(f"Teste Synonym [{i+1}/{len(synonyms)}] '{synonym}' für '{entity_name}'")
                synonym_results = await async_fetch_wikipedia_data(
                    [synonym], 
                    api_url, 
                    user_agent, 
                    config
                )
                
                if synonym in synonym_results and \
                   synonym_results[synonym].get('extract'):
                    # Ergebnis gefunden
                    extract_length = len(synonym_results[synonym].get('extract', ''))
                    wikidata_id = synonym_results[synonym].get('wikidata_id', 'keine')
                    logger.info(f"[Erfolg] Synonym-Fallback mit '{synonym}' für '{entity_name}' lieferte {extract_length} Zeichen und Wikidata-ID: {wikidata_id}")
                    wiki_result = synonym_results[synonym]
                    wiki_result['fallback_source'] = 'synonym'
                    wiki_result['fallback_title'] = synonym
                    wiki_result['original_title'] = entity_name
                    wiki_result['fallback_attempts'] = fallback_attempts + 1
                    fallback_attempts += 1
                    break
        except Exception as e:
            logger.error(f"Fehler beim Synonym-Fallback für '{entity_name}': {str(e)}")
    
    return wiki_result, fallback_attempts


async def apply_beautifulsoup_fallback(
    entity_name: str,
    wiki_result: Optional[Dict[str, Any]],
    user_agent: str,
    current_fallback_attempts: int,
    max_fallback_attempts: int
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Versucht, Daten direkt von der Wikipedia-Seite zu extrahieren, wenn
    andere Methoden fehlgeschlagen sind.
    
    Args:
        entity_name: Name der Entität
        wiki_result: Aktuelles Ergebnis aus der primären Wikipedia-API
        user_agent: User-Agent-String für API-Anfragen
        current_fallback_attempts: Anzahl der bisherigen Fallback-Versuche
        max_fallback_attempts: Maximale Anzahl von Fallback-Versuchen
        
    Returns:
        Tuple mit (aktualisiertes Wiki-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = current_fallback_attempts
    
    if (not wiki_result or not wiki_result.get('extract')) and fallback_attempts < max_fallback_attempts:
        try:
            # Nur wenn wir eine URL haben, können wir scrapen
            if wiki_result and wiki_result.get('url'):
                url = wiki_result.get('url')
                logger.info(f"[Fallback 4/4] BeautifulSoup-Fallback für '{entity_name}' mit URL {url}")
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers={"User-Agent": user_agent}) as response:
                        if response.status == 200:
                            html = await response.text()
                            soup = BeautifulSoup(html, 'html.parser')
                            logger.debug(f"HTML-Länge für '{entity_name}': {len(html)} Zeichen")
                            
                            # Suche nach dem ersten Absatz im Hauptinhalt
                            main_content = soup.select_one('#mw-content-text > .mw-parser-output')
                            content = None
                            
                            if main_content:
                                paragraphs = []
                                for p in main_content.find_all('p'):
                                    if p.text.strip() and not p.find_parent(class_='infobox'):
                                        paragraphs.append(p.text.strip())
                                if paragraphs:
                                    content = ' '.join(paragraphs[:3])
                                    logger.debug(f"Gefundene Absatzanzahl: {len(paragraphs)}")
                            
                            if content:
                                extract_length = len(content)
                                wikidata_id = wiki_result.get('wikidata_id', 'keine')
                                logger.info(f"[Erfolg] BeautifulSoup-Fallback für '{entity_name}' lieferte {extract_length} Zeichen und Wikidata-ID: {wikidata_id}")
                                if not wiki_result:
                                    wiki_result = {
                                        'title': entity_name,
                                        'url': url,
                                        'language': 'de' if 'de.wikipedia.org' in url else 'en'
                                    }
                                wiki_result['extract'] = content
                                wiki_result['fallback_source'] = 'beautifulsoup'
                                wiki_result['fallback_attempts'] = fallback_attempts + 1
                                fallback_attempts += 1
        except Exception as e:
            logger.error(f"Fehler beim BeautifulSoup-Fallback für '{entity_name}': {str(e)}")
    
    return wiki_result, fallback_attempts


async def apply_all_fallbacks(
    entity_name: str,
    wiki_result: Optional[Dict[str, Any]],
    api_url: str,
    user_agent: str,
    config: Dict[str, Any],
    max_fallback_attempts: int = 3
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Wendet alle verfügbaren Fallback-Strategien der Reihe nach an,
    um Wikipedia-Daten für eine Entität zu finden.
    
    Args:
        entity_name: Name der Entität
        wiki_result: Aktuelles Ergebnis aus der primären Wikipedia-API
        api_url: URL der Wikipedia-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        max_fallback_attempts: Maximale Anzahl von Fallback-Versuchen
        
    Returns:
        Tuple mit (aktualisiertes Wiki-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = 0
    fallback_success = False
    
    # Keine Fallbacks, wenn wir bereits ein gültiges Ergebnis haben
    # Wenn bereits ein Extract vorhanden ist (unabhängig von der Länge), sind keine Fallbacks nötig
    if wiki_result and wiki_result.get('extract'):
        extract_length = len(wiki_result.get('extract', ''))
        logger.info(f"[Fallback] Keine Fallbacks nötig für '{entity_name}', vorhandener Extract ({extract_length} Zeichen)")
        return wiki_result, fallback_attempts
    
    extract_length = len(wiki_result.get('extract', '')) if wiki_result and wiki_result.get('extract') else 0
    logger.info(f"[Fallback] Starte Fallback-Algorithmen für '{entity_name}'"+
               f"{f', aktueller Extract zu kurz: {extract_length} Zeichen' if extract_length > 0 else ', kein Extract vorhanden'}")
    
    
    logger.info(f"Starte Fallback-Sequenz für '{entity_name}' - Initialer Status: {wiki_result.get('status', 'Kein Ergebnis') if wiki_result else 'Kein Ergebnis'}")
    
    # 1. Sprach-Fallback
    if not fallback_success:
        wiki_result, language_fallback_attempts = await apply_language_fallback(
            entity_name, wiki_result, user_agent, config
        )
        fallback_attempts += language_fallback_attempts
        fallback_success = wiki_result and wiki_result.get('extract')
        logger.debug(f"Nach Sprach-Fallback: Erfolg={fallback_success}, Versuche={fallback_attempts}")
    
    # 2. OpenSearch-Fallback
    if not fallback_success:
        wiki_result, opensearch_fallback_attempts = await apply_opensearch_fallback(
            entity_name, wiki_result, api_url, user_agent, config, fallback_attempts
        )
        fallback_attempts += opensearch_fallback_attempts
        fallback_success = wiki_result and wiki_result.get('extract')
        logger.debug(f"Nach OpenSearch-Fallback: Erfolg={fallback_success}, Versuche={fallback_attempts}")
    
    # 3. Synonym-Fallback
    if not fallback_success:
        wiki_result, synonym_fallback_attempts = await apply_synonym_fallback(
            entity_name, wiki_result, api_url, user_agent, config, 
            fallback_attempts, max_fallback_attempts
        )
        fallback_attempts += synonym_fallback_attempts
        fallback_success = wiki_result and wiki_result.get('extract')
        logger.debug(f"Nach Synonym-Fallback: Erfolg={fallback_success}, Versuche={fallback_attempts}")
    
    # 4. BeautifulSoup-Fallback (letzter Versuch)
    if not fallback_success:
        wiki_result, bs_fallback_attempts = await apply_beautifulsoup_fallback(
            entity_name, wiki_result, user_agent, 
            fallback_attempts, max_fallback_attempts
        )
        fallback_attempts += bs_fallback_attempts
        fallback_success = wiki_result and wiki_result.get('extract')
        logger.debug(f"Nach BeautifulSoup-Fallback: Erfolg={fallback_success}, Versuche={fallback_attempts}")
    
    # Zusammenfassung des Fallback-Ergebnisses
    if wiki_result and wiki_result.get('extract'):
        extract_length = len(wiki_result.get('extract', ''))
        wikidata_id = wiki_result.get('wikidata_id', 'keine')
        fallback_source = wiki_result.get('fallback_source', 'unbekannt')
        logger.info(f"[Zusammenfassung] Fallback für '{entity_name}' erfolgreich nach {fallback_attempts} Versuchen. Quelle: {fallback_source}, Extract: {extract_length} Zeichen, Wikidata-ID: {wikidata_id}")
    else:
        logger.info(f"[Zusammenfassung] Alle Fallback-Versuche ({fallback_attempts}) für '{entity_name}' fehlgeschlagen.")
    
    return wiki_result, fallback_attempts
