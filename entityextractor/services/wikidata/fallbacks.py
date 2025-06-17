#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wikidata-Service Fallback-Mechanismen

Dieses Modul enthält die Fallback-Strategien für den Wikidata-Service, um die
Verlinkungsquote zu maximieren. Es verwendet verschiedene Ansätze, um Entitäten
zu finden, wenn die primäre Suche fehlschlägt.
"""

import logging
import aiohttp
import asyncio
from typing import Dict, List, Any, Optional, Tuple

from entityextractor.services.wikidata.async_fetchers import async_fetch_wikidata_batch, async_search_wikidata
from entityextractor.utils.logging_utils import get_service_logger
from entityextractor.services.openai_service import OpenAIService

# Logger konfigurieren
logger = get_service_logger(__name__, 'wikidata')

async def apply_direct_search(
    entity_name: str,
    wikidata_result: Optional[Dict[str, Any]],
    api_url: str,
    user_agent: str,
    config: Dict[str, Any]
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Führt eine direkte Suche nach der Entität in Wikidata durch.
    
    Args:
        entity_name: Name der Entität
        wikidata_result: Aktuelles Ergebnis (falls vorhanden)
        api_url: URL der Wikidata-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        
    Returns:
        Tuple mit (aktualisiertes Wikidata-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = 0
    
    # Wenn wir bereits ein vollständiges Ergebnis haben, nichts tun
    if wikidata_result and wikidata_result.get('id'):
        return wikidata_result, fallback_attempts
    
    try:
        logger.info(f"Direkte Suche in Wikidata für '{entity_name}'")
        
        # Direkte Suche in Wikidata
        results = await async_fetch_wikidata_batch([entity_name], config)
        
        if results and results[0] and results[0].get('status') == 'found':
            # Erfolgreiche Suche
            wikidata_result = results[0]
            wikidata_result['source'] = 'direct_search'
            fallback_attempts = 1
            logger.info(f"Direkte Suche erfolgreich für '{entity_name}', ID: {wikidata_result.get('id')}")
        else:
            logger.info(f"Keine direkten Treffer für '{entity_name}' in Wikidata")
    
    except Exception as e:
        logger.error(f"Fehler bei direkter Wikidata-Suche für '{entity_name}': {str(e)}")
    
    return wikidata_result, fallback_attempts

async def apply_language_fallback(
    entity_name: str,
    wikidata_result: Optional[Dict[str, Any]],
    openai_service: Optional[OpenAIService],
    api_url: str,
    user_agent: str,
    config: Dict[str, Any]
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Versucht, die Entität in einer anderen Sprache zu finden.
    Übersetzt deutsche Begriffe ins Englische und umgekehrt.
    
    Args:
        entity_name: Name der Entität
        wikidata_result: Aktuelles Ergebnis (falls vorhanden)
        openai_service: OpenAI-Service für Übersetzungen
        api_url: URL der Wikidata-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        
    Returns:
        Tuple mit (aktualisiertes Wikidata-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = 0
    
    # Wenn wir bereits ein vollständiges Ergebnis haben, nichts tun
    if wikidata_result and wikidata_result.get('id'):
        return wikidata_result, fallback_attempts
    
    # Bestimmen der aktuellen Sprache (aus dem Ergebnis oder der Konfiguration)
    current_language = wikidata_result.get('language') if wikidata_result else config.get('LANGUAGE', 'de')
    
    try:
        # Zielsprache bestimmen (wenn aktuell Deutsch, dann Englisch und umgekehrt)
        target_language = 'en' if current_language != 'en' else 'de'
        logger.info(f"Versuche Sprach-Fallback {current_language} -> {target_language} für '{entity_name}'")
        
        # Übersetzung mit OpenAI, falls verfügbar
        translated_name = entity_name
        if openai_service:
            try:
                # Spezifischen Prompt für Wikidata-kompatible Übersetzungen verwenden
                if current_language == 'de':
                    prompt = (
                        "Übersetze den folgenden deutschen Fachbegriff präzise ins Englische. "
                        "Verwende die offizielle englische Fachterminologie, die in Wikidata-Einträgen "
                        "verwendet wird. Gib nur die Übersetzung zurück, ohne zusätzlichen Text: "
                    )
                else:
                    prompt = (
                        "Translate the following English technical term precisely into German. "
                        "Use the official German terminology that is used in Wikidata entries. "
                        "Return only the translation, without any additional text: "
                    )
                
                # Übersetzung durchführen
                translation_result = await openai_service.translate_text(
                    entity_name, 
                    source_lang=current_language, 
                    target_lang=target_language,
                    custom_prompt=prompt,
                    temperature=0.1  # Niedrige Temperatur für präzise Übersetzungen
                )
                
                if translation_result:
                    translated_name = translation_result.strip()
                    logger.info(f"Übersetzung: '{entity_name}' -> '{translated_name}'")
            except Exception as e:
                logger.error(f"Fehler bei der Übersetzung von '{entity_name}': {str(e)}")
        
        # Suche mit übersetztem Namen
        if translated_name != entity_name:
            results = await async_fetch_wikidata_batch([translated_name], config)
            
            if results and results[0] and results[0].get('status') == 'found':
                # Erfolgreiche Suche mit Übersetzung
                wikidata_result = results[0]
                wikidata_result['source'] = 'language_fallback'
                wikidata_result['fallback_used'] = True
                wikidata_result['fallback_source'] = f'translation_{current_language}_to_{target_language}'
                wikidata_result['original_name'] = entity_name
                wikidata_result['translated_name'] = translated_name
                fallback_attempts = 1
                logger.info(f"Sprach-Fallback erfolgreich für '{entity_name}' -> '{translated_name}', ID: {wikidata_result.get('id')}")
            else:
                logger.info(f"Keine Treffer für übersetzte Entität '{translated_name}' in Wikidata")
    
    except Exception as e:
        logger.error(f"Fehler beim Sprach-Fallback für '{entity_name}': {str(e)}")
    
    return wikidata_result, fallback_attempts

async def apply_synonym_fallback(
    entity_name: str,
    wikidata_result: Optional[Dict[str, Any]],
    openai_service: Optional[OpenAIService],
    api_url: str,
    user_agent: str,
    config: Dict[str, Any],
    current_fallback_attempts: int,
    max_fallback_attempts: int = 3
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Generiert Synonyme für die Entität und versucht, mit diesen Wikidata-Daten zu finden.
    
    Args:
        entity_name: Name der Entität
        wikidata_result: Aktuelles Ergebnis (falls vorhanden)
        openai_service: OpenAI-Service für Synonymgenerierung
        api_url: URL der Wikidata-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        current_fallback_attempts: Aktuelle Anzahl von Fallback-Versuchen
        max_fallback_attempts: Maximale Anzahl von Fallback-Versuchen
        
    Returns:
        Tuple mit (aktualisiertes Wikidata-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = 0
    
    # Wenn wir bereits ein vollständiges Ergebnis haben, nichts tun
    if wikidata_result and wikidata_result.get('id'):
        return wikidata_result, fallback_attempts
    
    # Wenn wir das Limit für Fallback-Versuche erreicht haben, nichts tun
    if current_fallback_attempts >= max_fallback_attempts:
        logger.info(f"Maximale Anzahl von Fallback-Versuchen ({max_fallback_attempts}) für '{entity_name}' erreicht")
        return wikidata_result, fallback_attempts
    
    # Wenn kein OpenAI-Service verfügbar ist, nichts tun
    if not openai_service:
        logger.info(f"Kein OpenAI-Service verfügbar für Synonym-Fallback für '{entity_name}'")
        return wikidata_result, fallback_attempts
    
    try:
        logger.info(f"Versuche Synonym-Fallback für '{entity_name}'")
        
        # Bestimmen der aktuellen Sprache (aus dem Ergebnis oder der Konfiguration)
        current_language = wikidata_result.get('language') if wikidata_result else config.get('LANGUAGE', 'de')
        
        # Spezifischen Prompt für Wikidata-kompatible Synonyme verwenden
        if current_language == 'de':
            prompt = (
                "Generiere Synonyme, die den Namenskonventionen von Wikidata entsprechen würden. "
                "Konzentriere dich auf die offizielle Terminologie, die in Wikidata-Einträgen verwendet wird. "
                "Berücksichtige verschiedene Schreibweisen, Abkürzungen und alternative Bezeichnungen. "
                "Gib die Synonyme als Liste zurück."
            )
        else:
            prompt = (
                "Generate synonyms that would match Wikidata's naming conventions. "
                "Focus on official terminology used in Wikidata entries. "
                "Consider different spellings, abbreviations, and alternative designations. "
                "Return the synonyms as a list."
            )
        
        # Synonyme generieren
        synonyms_result = openai_service.generate_synonyms(entity_name, custom_prompt=prompt)
        
        # Koroutinen-Ergebnis abwarten, falls nötig
        if asyncio.iscoroutine(synonyms_result):
            synonyms = await synonyms_result
        else:
            synonyms = synonyms_result
        
        if not synonyms or not isinstance(synonyms, list):
            logger.info(f"Keine Synonyme gefunden für '{entity_name}'")
            return wikidata_result, fallback_attempts
        
        logger.info(f"Generierte Synonyme für '{entity_name}': {synonyms}")
        
        # Für jedes Synonym Wikidata-Suche durchführen
        for synonym in synonyms[:5]:  # Begrenze die Anzahl der Versuche
            try:
                results = await async_fetch_wikidata_batch([synonym], config)
                
                if results and results[0] and results[0].get('status') == 'found':
                    # Erfolgreiche Suche mit Synonym
                    wikidata_result = results[0]
                    wikidata_result['source'] = 'synonym_fallback'
                    wikidata_result['fallback_used'] = True
                    wikidata_result['fallback_source'] = f'synonym_fallback_{synonym[:20]}'
                    wikidata_result['original_name'] = entity_name
                    wikidata_result['synonym'] = synonym
                    fallback_attempts = 1
                    logger.info(f"Synonym-Fallback erfolgreich für '{entity_name}' mit Synonym '{synonym}', ID: {wikidata_result.get('id')}")
                    break  # Erfolg, weitere Synonyme nicht mehr prüfen
                else:
                    logger.info(f"Keine Treffer für Synonym '{synonym}' in Wikidata")
            
            except Exception as e:
                logger.error(f"Fehler bei der Wikidata-Suche für Synonym '{synonym}': {str(e)}")
                fallback_attempts += 1
    
    except Exception as e:
        logger.error(f"Fehler beim Synonym-Fallback für '{entity_name}': {str(e)}")
    
    return wikidata_result, fallback_attempts

async def apply_all_fallbacks(
    entity_name: str,
    wikidata_result: Optional[Dict[str, Any]],
    openai_service: Optional[OpenAIService],
    api_url: str,
    user_agent: str,
    config: Dict[str, Any],
    max_fallback_attempts: int = 3
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Wendet alle verfügbaren Fallback-Strategien der Reihe nach an,
    um Wikidata-Daten für eine Entität zu finden.
    
    Args:
        entity_name: Name der Entität
        wikidata_result: Aktuelles Ergebnis (falls vorhanden)
        openai_service: OpenAI-Service für Übersetzungen und Synonyme
        api_url: URL der Wikidata-API
        user_agent: User-Agent-String für API-Anfragen
        config: Konfiguration für API-Anfragen
        max_fallback_attempts: Maximale Anzahl von Fallback-Versuchen
        
    Returns:
        Tuple mit (aktualisiertes Wikidata-Ergebnis, Anzahl der Fallback-Versuche)
    """
    fallback_attempts = 0
    fallback_success = False
    
    # Keine Fallbacks, wenn wir bereits ein vollständiges Ergebnis haben
    # Vollständig bedeutet: ID, Label und Beschreibung sind vorhanden
    if wikidata_result and wikidata_result.get('id') and wikidata_result.get('label'):
        # Prüfen, ob eine Beschreibung vorhanden ist (entweder unter 'description' oder 'descriptions')
        has_description = wikidata_result.get('description') or wikidata_result.get('descriptions')
        if has_description:
            logger.info(f"[link_with_wikidata] [fallback] Keine Fallbacks nötig für '{entity_name}', bereits vollständiges Ergebnis")
            return wikidata_result, fallback_attempts
    
    logger.info(f"[link_with_wikidata] [fallback] Starte Fallback-Sequenz für '{entity_name}'")
    if not wikidata_result:
        logger.info(f"[link_with_wikidata] [fallback] Keine bisherigen Daten für '{entity_name}'")
    elif wikidata_result.get('id'):
        logger.info(f"[link_with_wikidata] [fallback] Unvollständige Daten für '{entity_name}': ID={wikidata_result.get('id')}, Label={bool(wikidata_result.get('label'))}, Beschreibung={bool(wikidata_result.get('description') or wikidata_result.get('descriptions'))}")
    else:
        logger.info(f"[link_with_wikidata] [fallback] Keine ID gefunden für '{entity_name}'")
    
    # 1. Direkte Suche
    if not fallback_success:
        logger.info(f"[link_with_wikidata] [fallback_direct] Starte direkte Suche für '{entity_name}'")
        wikidata_result, direct_fallback_attempts = await apply_direct_search(
            entity_name, wikidata_result, api_url, user_agent, config
        )
        fallback_attempts += direct_fallback_attempts
        # Vollständiges Ergebnis erfordert ID, Label und Beschreibung
        fallback_success = (wikidata_result and wikidata_result.get('id') and 
                          wikidata_result.get('label') and 
                          (wikidata_result.get('description') or wikidata_result.get('descriptions')))
        
        if fallback_success:
            logger.info(f"[link_with_wikidata] [fallback_direct] Erfolg für '{entity_name}': ID={wikidata_result.get('id')}")
        elif wikidata_result and wikidata_result.get('id'):
            logger.info(f"[link_with_wikidata] [fallback_direct] Teilweise Daten für '{entity_name}': ID={wikidata_result.get('id')}, Label={bool(wikidata_result.get('label'))}, Beschreibung={bool(wikidata_result.get('description') or wikidata_result.get('descriptions'))}")
        else:
            logger.info(f"[link_with_wikidata] [fallback_direct] Keine Ergebnisse für '{entity_name}' nach {direct_fallback_attempts} Versuchen")
    
    # 2. Sprach-Fallback
    if not fallback_success:
        logger.info(f"[link_with_wikidata] [fallback_language] Starte Sprachfallback für '{entity_name}'")
        wikidata_result, language_fallback_attempts = await apply_language_fallback(
            entity_name, wikidata_result, openai_service, api_url, user_agent, config
        )
        fallback_attempts += language_fallback_attempts
        # Vollständiges Ergebnis erfordert ID, Label und Beschreibung
        fallback_success = (wikidata_result and wikidata_result.get('id') and 
                          wikidata_result.get('label') and 
                          (wikidata_result.get('description') or wikidata_result.get('descriptions')))
        
        if fallback_success:
            logger.info(f"[link_with_wikidata] [fallback_language] Erfolg für '{entity_name}': ID={wikidata_result.get('id')}")
        elif wikidata_result and wikidata_result.get('id'):
            logger.info(f"[link_with_wikidata] [fallback_language] Teilweise Daten für '{entity_name}': ID={wikidata_result.get('id')}, Label={bool(wikidata_result.get('label'))}, Beschreibung={bool(wikidata_result.get('description') or wikidata_result.get('descriptions'))}")
        else:
            logger.info(f"[link_with_wikidata] [fallback_language] Keine Ergebnisse für '{entity_name}' nach {language_fallback_attempts} Versuchen")
    
    # 3. Synonym-Fallback
    if not fallback_success:
        logger.info(f"[link_with_wikidata] [fallback_synonym] Starte Synonym-Fallback für '{entity_name}'")
        wikidata_result, synonym_fallback_attempts = await apply_synonym_fallback(
            entity_name, wikidata_result, openai_service, api_url, user_agent, config,
            fallback_attempts, max_fallback_attempts
        )
        fallback_attempts += synonym_fallback_attempts
        # Vollständiges Ergebnis erfordert ID, Label und Beschreibung
        fallback_success = (wikidata_result and wikidata_result.get('id') and 
                          wikidata_result.get('label') and 
                          (wikidata_result.get('description') or wikidata_result.get('descriptions')))
        
        if fallback_success:
            logger.info(f"[link_with_wikidata] [fallback_synonym] Erfolg für '{entity_name}': ID={wikidata_result.get('id')}")
        elif wikidata_result and wikidata_result.get('id'):
            logger.info(f"[link_with_wikidata] [fallback_synonym] Teilweise Daten für '{entity_name}': ID={wikidata_result.get('id')}, Label={bool(wikidata_result.get('label'))}, Beschreibung={bool(wikidata_result.get('description') or wikidata_result.get('descriptions'))}")
        else:
            logger.info(f"[link_with_wikidata] [fallback_synonym] Keine Ergebnisse für '{entity_name}' nach {synonym_fallback_attempts} Versuchen")
    
    # Zusammenfassung des Fallback-Ergebnisses
    # Vollständiges Ergebnis erfordert ID, Label und Beschreibung
    has_complete_result = (wikidata_result and wikidata_result.get('id') and 
                         wikidata_result.get('label') and 
                         (wikidata_result.get('description') or wikidata_result.get('descriptions')))
    
    if has_complete_result:
        wikidata_id = wikidata_result.get('id', 'keine')
        fallback_source = wikidata_result.get('source', 'unbekannt')
        logger.info(f"[link_with_wikidata] [fallback_summary] Entität '{entity_name}' erfolgreich verlinkt nach {fallback_attempts} Fallback-Versuchen")
        logger.info(f"[link_with_wikidata] [fallback_summary] Details: ID={wikidata_id}, Quelle={fallback_source}, Label={wikidata_result.get('label', 'N/A')}")
    else:
        # Wenn wir zumindest eine ID haben, aber nicht alle erforderlichen Felder
        if wikidata_result and wikidata_result.get('id'):
            logger.info(f"[link_with_wikidata] [fallback_summary] Entität '{entity_name}' teilweise verlinkt, aber unvollständige Daten. ID={wikidata_result.get('id')}")
        else:
            logger.info(f"[link_with_wikidata] [fallback_summary] Alle Fallback-Versuche ({fallback_attempts}) für '{entity_name}' fehlgeschlagen.")
    
    return wikidata_result, fallback_attempts
