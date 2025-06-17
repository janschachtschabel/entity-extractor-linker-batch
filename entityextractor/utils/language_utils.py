#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Language-related utilities for the Entity Extractor.

This module provides helper functions for language detection, language mappings,
and other language-related operations.
"""

import re
from loguru import logger


def detect_language(text):
    """
    Simple language detection based on common words in different languages.
    
    Args:
        text: The text to analyze
        
    Returns:
        The detected language code (de, en, fr, ...) or None if not recognized
    """
    if not text or len(text) < 3:
        return None
        
    # Common words in different languages
    language_markers = {
        'de': ['der', 'die', 'das', 'und', 'ist', 'in', 'von', 'zu', 'mit', 'den', 'für', 'auf', 'ein', 'eine'],
        'en': ['the', 'and', 'is', 'in', 'to', 'of', 'for', 'with', 'on', 'at', 'from', 'by', 'an', 'as'],
        'fr': ['le', 'la', 'les', 'et', 'est', 'en', 'de', 'du', 'dans', 'pour', 'avec', 'sur', 'un', 'une'],
        'es': ['el', 'la', 'los', 'las', 'y', 'es', 'en', 'de', 'para', 'con', 'por', 'un', 'una', 'su']
    }
    
    # Convert text to lowercase and split into words
    words = text.lower().split()
    
    # Count matches per language
    matches = {lang: 0 for lang in language_markers}
    
    for word in words:
        clean_word = word.strip('.,;:!?()[]{}"\'')
        for lang, markers in language_markers.items():
            if clean_word in markers:
                matches[lang] += 1
    
    # Find best match
    best_lang = None
    best_count = 0
    
    for lang, count in matches.items():
        if count > best_count:
            best_count = count
            best_lang = lang
    
    # Minimum number of matches for reliable detection
    if best_count >= 2:
        return best_lang
        
    # Fallback: Check for German umlauts
    if any(char in text.lower() for char in 'äöüß'):
        return 'de'
        
    # Default: English
    return 'en'


def get_language_map():
    """
    Provides a mapping of language codes to full language names.
    
    Returns:
        Dict with language codes as keys and full language names as values
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
    Cleans a title from parenthetical additions and other unwanted formatting.
    
    Args:
        title: The title to clean
        
    Returns:
        Cleaned title
    """
    if not title:
        return title
        
    # Remove parenthetical additions like "(Film)" or "(Politician)"
    clean = re.sub(r'\s+\([^)]*\)$', '', title)
    
    # Normalize whitespace
    clean = ' '.join(clean.split())
    
    return clean
