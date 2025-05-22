#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sprachbezogene Hilfsfunktionen für den Entity Extractor.

Dieses Modul stellt Hilfsfunktionen für Spracherkennung, Sprachmappings
und andere sprachbezogene Operationen zur Verfügung.
"""

import logging
import re


def detect_language(text):
    """
    Einfache Spracherkennung basierend auf häufigen Wörtern in verschiedenen Sprachen.
    
    Args:
        text: Der zu analysierende Text
        
    Returns:
        Der erkannte Sprachcode (de, en, fr, ...) oder None wenn nicht erkannt
    """
    if not text or len(text) < 3:
        return None
        
    # Häufige Wörter in verschiedenen Sprachen
    language_markers = {
        'de': ['der', 'die', 'das', 'und', 'ist', 'in', 'von', 'zu', 'mit', 'den', 'für', 'auf', 'ein', 'eine'],
        'en': ['the', 'and', 'is', 'in', 'to', 'of', 'for', 'with', 'on', 'at', 'from', 'by', 'an', 'as'],
        'fr': ['le', 'la', 'les', 'et', 'est', 'en', 'de', 'du', 'dans', 'pour', 'avec', 'sur', 'un', 'une'],
        'es': ['el', 'la', 'los', 'las', 'y', 'es', 'en', 'de', 'para', 'con', 'por', 'un', 'una', 'su']
    }
    
    # Text in Kleinbuchstaben umwandeln und in Wörter aufteilen
    words = text.lower().split()
    
    # Zählen der Treffer pro Sprache
    matches = {lang: 0 for lang in language_markers}
    
    for word in words:
        clean_word = word.strip('.,;:!?()[]{}"\'')
        for lang, markers in language_markers.items():
            if clean_word in markers:
                matches[lang] += 1
    
    # Beste Übereinstimmung finden
    best_lang = None
    best_count = 0
    
    for lang, count in matches.items():
        if count > best_count:
            best_count = count
            best_lang = lang
    
    # Mindestanzahl an Übereinstimmungen für eine zuverlässige Erkennung
    if best_count >= 2:
        return best_lang
        
    # Fallback: Überprüfe auf Umlaute für Deutsch
    if any(char in text.lower() for char in 'äöüß'):
        return 'de'
        
    # Standardeinstellung: Englisch
    return 'en'


def get_language_map():
    """
    Liefert ein Mapping von Sprachcodes zu vollständigen Sprachnamen.
    
    Returns:
        Dict mit Sprach-Codes als Schlüssel und vollständigen Sprachnamen als Werte
    """
    return {
        "de": "German",
        "en": "English",
        "fr": "French",
        "es": "Spanish",
        "it": "Italian",
        "nl": "Dutch",
        "pl": "Polish",
        "ru": "Russian",
        "ja": "Japanese",
        "zh": "Chinese",
        "pt": "Portuguese",
        "sv": "Swedish",
        "da": "Danish",
        "no": "Norwegian",
        "fi": "Finnish",
        "cs": "Czech",
        "hu": "Hungarian",
        "tr": "Turkish",
        "ar": "Arabic",
        "ko": "Korean"
    }


def clean_title(title):
    """
    Bereinigt einen Titel von Klammerzusätzen und anderen unerwünschten Formatierungen.
    
    Args:
        title: Der zu reinigende Titel
        
    Returns:
        Bereinigter Titel
    """
    if not title:
        return title
        
    # Entferne Klammerzusätze wie "(Film)" oder "(Politiker)"
    clean = re.sub(r'\s+\([^)]*\)$', '', title)
    
    # Normalisiere Leerzeichen
    clean = ' '.join(clean.split())
    
    return clean
