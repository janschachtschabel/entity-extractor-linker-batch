#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Synonym-Hilfsfunktionen für den Entity Extractor.

Dieses Modul stellt Funktionen zur Generierung von Synonymen und alternativen 
Bezeichnungen für Entitäten zur Verfügung, mit Unterstützung für verschiedene
Quellen (lokale Mappings, OpenAI API).
"""

import logging
import hashlib
import re
import time
import os

from entityextractor.utils.cache_utils import get_cache_path, load_cache, save_cache
from entityextractor.utils.language_utils import get_language_map, clean_title
from entityextractor.config.settings import get_config, DEFAULT_CONFIG

# Statische Synonymmap für häufige Entitäten (Fallback ohne API-Aufruf)
_COMMON_SYNONYMS = {
    "Deutschland": ["Bundesrepublik Deutschland", "BRD", "German", "Germany"],
    "England": ["Great Britain", "United Kingdom", "UK", "Britain"],
    "USA": ["United States", "United States of America", "America", "US"],
    "Steve Jobs": ["Steven Paul Jobs", "Apple CEO", "Apple founder"],
    "Bill Gates": ["William Henry Gates III", "Microsoft founder", "Microsoft CEO"],
    "Albert Einstein": ["Einstein", "Nobel physicist"],
    "Isaac Newton": ["Sir Isaac Newton", "Newton"],
    "Berlin": ["German capital", "Berlin, Germany"],
    "Paris": ["French capital", "Paris, France"],
    "Eiffel Tower": ["Tour Eiffel", "Eiffelturm"],
    "Artificial Intelligence": ["AI", "Machine Intelligence", "Künstliche Intelligenz"],
    "Machine Learning": ["ML", "Maschinelles Lernen"],
    "Natural Language Processing": ["NLP", "Sprachverarbeitung"],
    "Computer Science": ["CS", "Informatik"],
    "The Beatles": ["Beatles", "Fab Four"],
    "Microsoft": ["Microsoft Corporation", "MSFT"],
    "Apple": ["Apple Inc.", "AAPL"]
}


def generate_synonyms_with_openai(entity, language="German", config=None):
    """
    Generiert Synonyme für eine Entität mit der OpenAI API.
    
    Args:
        entity: Die Entität für die Synonyme generiert werden sollen
        language: Vollständiger Sprachname (z.B. "German", "English")
        config: Konfigurationsobjekt mit API-Schlüssel und Modelleinstellungen
        
    Returns:
        Liste von Synonymen oder leere Liste bei Fehler
    """
    # Statische Antworten zurückgeben, wenn verfügbar (für bessere Performance)
    if entity in _COMMON_SYNONYMS:
        logging.info(f"Synonyme für '{entity}' aus statischer Liste: {_COMMON_SYNONYMS[entity]}")
        return _COMMON_SYNONYMS[entity]

    try:
        # Import hier zur Vermeidung zirkulärer Abhängigkeiten
        from entityextractor.utils.openai_utils import call_openai_api
        
        if config is None:
            from entityextractor.config.settings import get_config
            config = get_config()
        
        model = config.get("MODEL", "gpt-4.1-mini")
        temperature = 0.2  # Niedrige Temperatur für konsistente Ergebnisse

        # System-Prompt
        system_prompt = f"You are an assistant specialized in finding existing Wikipedia articles when direct searches have failed. Your expertise lies in identifying alternative terms and closely related concepts that definitely exist in {language} Wikipedia."
        
        # User-Prompt
        user_prompt = f"A search for the term '{entity}' in {language} Wikipedia was unsuccessful. I need 3 alternative terms that:"
        user_prompt += "\n1. DEFINITELY exist as Wikipedia articles or redirects"
        user_prompt += "\n2. Are logically related to or synonymous with the original term"
        user_prompt += "\n3. Would provide relevant information about the original concept"
        user_prompt += f"\n\nPrefer these types of alternatives for {language} Wikipedia:"
        user_prompt += "\n- Broader categorizations (e.g., 'Pedagogical method' instead of 'Specific teaching technique')"
        user_prompt += "\n- Official terminology used in academic or professional contexts"
        user_prompt += "\n- Well-established conventional terms or historical concepts"
        user_prompt += "\n- Different grammatical forms or standard compounds that may have dedicated articles"
        user_prompt += "\n\nImportant: Only suggest terms that are very likely to exist in Wikipedia. Respond ONLY with these 3 alternatives, one per line. NO explanations or other text."

        # Log Start der Synonym-Generierung
        logging.info(f"Starte OpenAI-Synonym-Generierung für: '{entity}' in {language}")
        start_time = time.time()

        # API-Anfrage
        response = call_openai_api(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=temperature,
            config=config
        )

        if response and "choices" in response and response["choices"]:
            content = response["choices"][0]["message"]["content"]
            # Extrahiere Synonyme (ein Synonym pro Zeile)
            synonyms = [line.strip() for line in content.strip().split("\n") if line.strip()]
            
            # Aufräumen: Entferne Nummerierungen, Punkte, etc.
            cleaned_synonyms = []
            for syn in synonyms:
                # Entferne Nummerierung wie "1. " oder "- "
                clean_syn = re.sub(r'^\d+\.\s*|^-\s*|^\*\s*', '', syn).strip()
                if clean_syn:
                    cleaned_synonyms.append(clean_syn)
            
            logging.info(f"OpenAI-Synonym-Generierung für '{entity}' erfolgreich in {time.time() - start_time:.2f}s: {cleaned_synonyms}")
            return cleaned_synonyms

        logging.warning(f"OpenAI-Synonym-Generierung für '{entity}' fehlgeschlagen: Keine Antwort vom API-Call")
        return []
        
    except Exception as e:
        logging.error(f"Fehler bei der OpenAI-Synonymgenerierung für '{entity}': {str(e)}")
        return []


def generate_entity_synonyms(entity, language="en", config=None):
    """
    Generiert Synonyme für eine Entität mit OpenAI-Unterstützung und Caching.
    
    Args:
        entity: Name der Entität für die Synonyme generiert werden sollen
        language: Zielsprache für die Synonyme als ISO-Code (de, en, fr, etc.)
        config: Konfigurationsobjekt mit LLM-Einstellungen
        
    Returns:
        Liste von Synonymen (kann leer sein bei Fehler)
    """
    if not entity or len(entity) < 2:
        return []
    
    if config is None:
        config = DEFAULT_CONFIG
    
    # Entferne Klammerzusätze für besseres Matching
    clean_entity = clean_title(entity)
    
    # Cache-Schlüssel generieren
    cache_key = f"synonyms_{language}_{hashlib.sha256(clean_entity.encode()).hexdigest()}"
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
    cache_file = get_cache_path(cache_dir, "synonyms", cache_key)
    cache_ttl = 86400 * 30  # 30 Tage Cache
    
    # Cache überprüfen
    cached = load_cache(cache_file)
    cache_valid = cached and (("timestamp" not in cached) or (time.time() - cached.get("timestamp", 0) < cache_ttl))
    if cache_valid and "synonyms" in cached and cached["synonyms"]:
        return cached["synonyms"]
    
    try:
        # Sprache-zu-Vollname-Mapping
        lang_map = get_language_map()
        lang_name = lang_map.get(language, "English")
        
        # Synonyme mit OpenAI generieren
        synonyms = generate_synonyms_with_openai(clean_entity, lang_name, config=config)
        
        if synonyms:
            # Entferne Duplikate und den Original-Begriff
            synonyms = [s for s in synonyms if s.lower() != clean_entity.lower()]
            unique_synonyms = []
            for syn in synonyms:
                if syn and syn.lower() not in [s.lower() for s in unique_synonyms]:
                    unique_synonyms.append(syn)
            
            # Cache setzen
            save_cache(cache_file, {"synonyms": unique_synonyms, "timestamp": time.time()})
            return unique_synonyms
    
    except Exception as e:
        logging.error(f"Fehler beim Generieren von Synonymen für '{clean_entity}': {e}")
    
    # Bei Fehler leere Liste zurückgeben und cachen
    save_cache(cache_file, {"synonyms": [], "timestamp": time.time()})
    return []


def get_entity_variations(entity, language="en", include_original=True, config=None):
    """
    Erweiterte Funktion, die alle möglichen Variationen einer Entität zurückgibt.
    
    Kombiniert Synonyme und andere Varianten wie Akronyme, usw.
    
    Args:
        entity: Name der Entität
        language: Zielsprache als ISO-Code (de, en, fr, etc.)
        include_original: Ob der Originalname in der Ergebnisliste enthalten sein soll
        config: Konfigurationsobjekt
        
    Returns:
        Liste aller Variationen der Entität
    """
    if not entity:
        return []
        
    # Original
    variations = [entity] if include_original else []
    
    # Finde Synonyme
    synonyms = generate_entity_synonyms(entity, language, config)
    variations.extend(synonyms)
    
    # Akronyme für mehrgliedrige Namen hinzufügen (z.B. "Europäische Union" -> "EU")
    words = entity.split()
    if len(words) > 1:
        # Nur Wörter mit Großbuchstaben, ohne Artikel
        stop_words = {'der', 'die', 'das', 'the', 'a', 'an', 'of', 'for', 'and', 'oder', 'und', 'von', 'zu'}
        acronym_parts = [word[0].upper() for word in words if word.lower() not in stop_words and word[0].isalpha()]
        if len(acronym_parts) > 1:
            acronym = ''.join(acronym_parts)
            if acronym not in variations:
                variations.append(acronym)
    
    # Duplikate entfernen
    seen = set()
    unique_variations = []
    for var in variations:
        if var and var.lower() not in seen:
            seen.add(var.lower())
            unique_variations.append(var)
    
    return unique_variations
