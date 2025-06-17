#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pydantic-Modelle für die Datenstrukturen im Entity Extractor.

Diese Modelle definieren die Struktur der Daten, die zwischen den Services ausgetauscht werden.
Sie stellen sicher, dass die Daten validiert werden und eine konsistente Struktur haben.
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class WikipediaLanguageData(BaseModel):
    """Daten für eine einzelne Sprache in Wikipedia."""
    label: str
    url: str
    description: Optional[str] = None


class WikipediaMultilangData(BaseModel):
    """Mehrsprachige Daten aus Wikipedia."""
    de: Optional[WikipediaLanguageData] = None
    en: Optional[WikipediaLanguageData] = None
    fr: Optional[WikipediaLanguageData] = None
    es: Optional[WikipediaLanguageData] = None
    it: Optional[WikipediaLanguageData] = None
    nl: Optional[WikipediaLanguageData] = None
    pl: Optional[WikipediaLanguageData] = None
    ru: Optional[WikipediaLanguageData] = None
    ja: Optional[WikipediaLanguageData] = None
    zh: Optional[WikipediaLanguageData] = None


class WikidataProperty(BaseModel):
    """Ein einzelnes Wikidata-Property mit Wert und Metadaten."""
    value: Any
    type: str
    qualifiers: Optional[Dict[str, List[Any]]] = None


class WikidataData(BaseModel):
    """Strukturierte Wikidata-Daten für eine Entität."""
    entity_id: str
    label: Optional[Dict[str, str]] = None
    description: Optional[Dict[str, str]] = None
    aliases: Optional[Dict[str, List[str]]] = None
    claims: Optional[Dict[str, List[WikidataProperty]]] = None


class DBpediaData(BaseModel):
    """Strukturierte DBpedia-Daten für eine Entität."""
    uri: Optional[str] = None
    label: Optional[Dict[str, str]] = None
    abstract: Optional[Dict[str, str]] = None
    types: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    has_part: Optional[List[str]] = None
    part_of: Optional[List[str]] = None
    geo: Optional[Dict[str, float]] = None
    wiki_url: Optional[str] = None
    image_url: Optional[str] = None
    status: str = "not_processed"
    error: Optional[str] = None
    message: Optional[str] = None


class EntityData(BaseModel):
    """Hauptdatenstruktur für eine Entität mit Daten aus allen Services."""
    entity_id: str
    entity_name: str
    entity_type: Optional[str] = None
    language: str = "de"
    wikipedia_url: Optional[str] = None
    wikipedia_data: Optional[Dict[str, Any]] = None
    wikipedia_multilang: Optional[WikipediaMultilangData] = None
    wikidata_id: Optional[str] = None
    wikidata_data: Optional[WikidataData] = None
    dbpedia_data: Optional[DBpediaData] = None
    
    class Config:
        arbitrary_types_allowed = True
