#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Neuimplementierung des Batch DBpedia Service Moduls für den Entity Extractor.

Optimiert für konsistente Datenabfrage mit lokalem Fallback-System für 
Ausfallsicherheit. Unterstützt Batch-Verarbeitung und Cache für optimale Leistung.
"""

import logging
logger = logging.getLogger('entityextractor.services.batch_dbpedia_service')
if not logger.hasHandlers():
    handler = logging.FileHandler('entity_extractor_debug.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = True

import re
import requests
import urllib.parse
import time
import json
import hashlib
import os
import SPARQLWrapper
from typing import Dict, List, Any, Optional, Tuple

from entityextractor.services.batch_wikipedia_service import batch_get_wikipedia_info
from entityextractor.utils.cache_utils import get_cache_path, load_cache, save_cache
from entityextractor.utils.rate_limiter import RateLimiter
from entityextractor.config.settings import get_config, DEFAULT_CONFIG
from entityextractor.services.batch_wikidata_service import get_wikipedia_title_in_language

# Lokales Fallback-System
FALLBACK_DATA = {
    # Personen
    "albert_einstein": {
        "status": "fallback",
        "label": "Albert Einstein",
        "abstract": "Albert Einstein war ein theoretischer Physiker mit Schweizer und US-amerikanischer Staatsbürgerschaft.",
        "resource_uri": "http://de.dbpedia.org/resource/Albert_Einstein",
        "types": ["Person", "Physiker", "Wissenschaftler"],
        "subjects": ["Physik", "Relativitätstheorie"],
        "categories": ["Kategorie:Nobelpreisträger für Physik", "Kategorie:Person (20. Jahrhundert)"],
        "gnd_id": "118529579",
        "thumbnail": "http://commons.wikimedia.org/wiki/Special:FilePath/Albert_Einstein_Head.jpg?width=300",
        "coordinates": None
    },
    
    # Orte
    "berlin": {
        "status": "fallback",
        "label": "Berlin",
        "abstract": "Berlin ist die Hauptstadt und ein Land der Bundesrepublik Deutschland.",
        "resource_uri": "http://de.dbpedia.org/resource/Berlin",
        "types": ["Stadt", "Hauptstadt", "Bundesland"],
        "subjects": ["Deutschland", "Geographie"],
        "categories": ["Kategorie:Hauptstadt in Europa", "Kategorie:Ort in Deutschland"],
        "part_of": ["Deutschland"],
        "has_parts": ["Mitte", "Kreuzberg", "Charlottenburg"],
        "gnd_id": "4005728-8",
        "homepage": "https://www.berlin.de/",
        "thumbnail": "http://commons.wikimedia.org/wiki/Special:FilePath/Aerial_view_of_Berlin_(32881394467).jpg?width=300",
        "coordinates": {"lat": 52.52, "long": 13.405}
    },
    
    # Konzepte/Theorien
    "quantenphysik": {
        "status": "fallback",
        "label": "Quantenphysik",
        "abstract": "Die Quantenphysik ist eine physikalische Theorie, welche die Wechselwirkungen der Materie sowie von Materie und Licht auf atomarer und subatomarer Ebene beschreibt.",
        "resource_uri": "http://de.dbpedia.org/resource/Quantenphysik",
        "types": ["Theorie", "Teilgebiet der Physik"],
        "subjects": ["Physik", "Theoretische Physik"],
        "categories": ["Kategorie:Quantenphysik", "Kategorie:Physikalisches Fachgebiet"],
        "gnd_id": "4047794-0",
        "homepage": None,
        "thumbnail": None,
        "coordinates": None
    },
    
    # Organisationen
    "vereinte_nationen": {
        "status": "fallback",
        "label": "Vereinte Nationen",
        "abstract": "Die Vereinten Nationen (VN), englisch United Nations (UN), sind ein zwischenstaatlicher Zusammenschluss von 193 Staaten.",
        "resource_uri": "http://de.dbpedia.org/resource/Vereinte_Nationen",
        "types": ["Organisation", "Internationale Organisation"],
        "subjects": ["Politik", "Internationale Beziehungen"],
        "categories": ["Kategorie:Vereinte Nationen", "Kategorie:Internationale Organisation"],
        "has_parts": ["Generalversammlung", "Sicherheitsrat", "Wirtschafts- und Sozialrat"],
        "gnd_id": "1007824-1",
        "homepage": "https://www.un.org/",
        "thumbnail": "http://commons.wikimedia.org/wiki/Special:FilePath/UN_emblem_blue.svg?width=300",
        "coordinates": {"lat": 40.75, "long": -73.97},
        "foundingDate": "1945-10-24"
    },
    
    # Bildungskonzepte
    "projektbasiertes_lernen": {
        "status": "fallback",
        "label": "Projektbasiertes Lernen",
        "abstract": "Projektbasiertes Lernen (PBL) ist eine schülerzentrierte Pädagogik, die einen dynamischen Klassenraumansatz beinhaltet, bei dem Schüler aktiv Wissen und Fähigkeiten erwerben, indem sie über einen längeren Zeitraum an der Untersuchung und Reaktion auf eine authentische, ansprechende und komplexe Frage, ein Problem oder eine Herausforderung arbeiten.",
        "resource_uri": "http://de.dbpedia.org/resource/Projektbasiertes_Lernen",
        "types": ["Lernmethode", "Pädagogisches Konzept"],
        "subjects": ["Bildung", "Pädagogik"],
        "categories": ["Kategorie:Pädagogische Methode/Lehre", "Kategorie:Lernen"],
        "part_of": ["Konstruktivistische Didaktik"],
        "has_parts": [],
        "gnd_id": None,
        "homepage": None,
        "thumbnail": None,
        "coordinates": None
    },
    
    "konstruktivistische_didaktik": {
        "status": "fallback",
        "label": "Konstruktivistische Didaktik",
        "abstract": "Die Konstruktivistische Didaktik ist ein Ansatz der Didaktik, der auf dem erkenntnistheoretischen Konstruktivismus basiert. Sie betont, dass Lernende ihr Wissen aktiv konstruieren, statt es passiv zu empfangen.",
        "resource_uri": "http://de.dbpedia.org/resource/Konstruktivistische_Didaktik",
        "types": ["Didaktik", "Pädagogisches Konzept"],
        "subjects": ["Bildung", "Pädagogik"],
        "categories": ["Kategorie:Pädagogische Methode/Lehre", "Kategorie:Lernen", "Kategorie:Didaktik"],
        "has_parts": ["Projektbasiertes Lernen", "Problembasiertes Lernen"],
        "gnd_id": None,
        "homepage": None,
        "thumbnail": None,
        "coordinates": None
    }
}

# Einrichtung des Rate-Limiters
_config = get_config()
_rate_limiter = RateLimiter(
    _config.get("RATE_LIMIT_MAX_CALLS", 10), 
    _config.get("RATE_LIMIT_PERIOD", 1), 
    _config.get("RATE_LIMIT_BACKOFF_BASE", 2), 
    _config.get("RATE_LIMIT_BACKOFF_MAX", 60)
)

@_rate_limiter
def _limited_get(url: str, **kwargs) -> requests.Response:
    """
    Führt einen GET-Request mit Rate-Limiting durch.
    
    Args:
        url: URL für den Request
        **kwargs: Zusätzliche Parameter für requests.get
        
    Returns:
        Response-Objekt
    """
    return requests.get(url, **kwargs)

def get_local_fallback(wikipedia_url: str, lang: str = "de") -> Optional[Dict[str, Any]]:
    """
    Versucht, lokale Fallback-Daten für eine Wikipedia-URL zu finden.
    
    Args:
        wikipedia_url: URL der Wikipedia-Entität
        lang: Sprachcode (default: "de")
        
    Returns:
        Fallback-Daten oder None, wenn keine gefunden wurden
    """
    if not wikipedia_url:
        return None
    
    # Extrahiere den Titel aus der URL
    try:
        parts = wikipedia_url.split('/')
        title = parts[-1].lower()  # Normalisiere zu Kleinbuchstaben
        
        # Versuche direkten Lookup
        if title in FALLBACK_DATA:
            logger.info(f"Lokaler Fallback gefunden für {title}")
            return FALLBACK_DATA[title]
        
        # Versuche normalisierte Version (Unterstriche durch Leerzeichen)
        normalized = title.replace('_', ' ').lower()
        normalized_key = normalized.replace(' ', '_')
        if normalized_key in FALLBACK_DATA:
            logger.info(f"Lokaler Fallback gefunden für {normalized}")
            return FALLBACK_DATA[normalized_key]
        
        logger.info(f"Kein lokaler Fallback gefunden für {title}")
        return None
    except Exception as e:
        logger.error(f"Fehler beim Abrufen des lokalen Fallbacks: {e}")
        return None


def get_batch_local_fallbacks(entity_wikipedia_urls: Dict[str, str]) -> Dict[str, Any]:
    """
    Ruft Fallback-Daten für mehrere Entitäten ab.
    
    Args:
        entity_wikipedia_urls: Dict mit Entitätsnamen als Schlüssel und Wikipedia-URLs als Werte
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Fallback-Daten als Werte
    """
    results = {}
    
    for entity_name, wikipedia_url in entity_wikipedia_urls.items():
        fallback_data = get_local_fallback(wikipedia_url)
        if fallback_data:
            results[entity_name] = fallback_data
    
    return results


def _fetch_dbpedia_batch(entity_titles: Dict[str, str], lang: str = "de", config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Führt eine Batch-Abfrage für DBpedia durch.
    
    Args:
        entity_titles: Dict mit Entitätsnamen als Schlüssel und Titeln als Werte
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    # Keine Entitäten, leeres Ergebnis zurückgeben
    if not entity_titles:
        return {}
    
    results = {}
    
    # Definiere Standard-Ergebnis für Fehlerfälle
    def create_error_result(entity_name, title, error_msg):
        return {
            "status": "error",
            "label": title,
            "abstract": "",
            "resource_uri": f"http://{lang}.dbpedia.org/resource/{urllib.parse.quote(title.replace(' ', '_'))}",
            "types": [],
            "subjects": [],
            "categories": [],
            "part_of": [],
            "has_parts": [],
            "gnd_id": None,
            "homepage": None,
            "thumbnail": None,
            "coordinates": None,
            "error": error_msg
        }
    
    # Verarbeite die Entitäten in Batches (maximal 25 pro Anfrage)
    batch_size = 25
    batches = [list(entity_titles.items())[i:i+batch_size] for i in range(0, len(entity_titles), batch_size)]
    
    for batch in batches:
        # Erzeuge eine VALUES-Klausel für den aktuellen Batch
        values_clause = "\n".join([f'<http://{lang}.dbpedia.org/resource/{urllib.parse.quote(title.replace(" ", "_"))}>'
                                  for _, title in batch])
        
        # Standardisierte SPARQL-Abfrage mit allen wichtigen Informationen
        sparql_query = f"""
            PREFIX dbo: <http://dbpedia.org/ontology/>
            PREFIX dbp: <http://dbpedia.org/property/>
            PREFIX dbr: <http://dbpedia.org/resource/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>

            SELECT ?entity ?label ?abstract ?type ?typeLabel ?category ?categoryLabel 
                   ?partOf ?partOfLabel ?hasPart ?hasPartLabel 
                   ?homepage ?thumbnail ?gndId ?lat ?long ?birthDate ?deathDate ?foundingDate ?population
            WHERE {{
                VALUES ?entity {{ {values_clause} }}

                # Grundlegende Informationen plus Fallbacks
                OPTIONAL {{ ?entity rdfs:label ?label . FILTER(LANG(?label) = "{lang}") }}
                OPTIONAL {{ ?entity dbo:abstract ?abstract . FILTER(LANG(?abstract) = "{lang}") }}
                OPTIONAL {{ ?entity rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
                OPTIONAL {{ ?entity dbo:abstract ?abstractEn . FILTER(LANG(?abstractEn) = "en") }}
                
                # Erweiterte Typ-Informationen mit Labels
                OPTIONAL {{ 
                    ?entity rdf:type ?type . 
                    OPTIONAL {{ ?type rdfs:label ?typeLabel . FILTER(LANG(?typeLabel) = "{lang}") }}
                }}
                
                # Kategorien und Subjects mit Labels
                OPTIONAL {{ 
                    ?entity dct:subject ?category .
                    OPTIONAL {{ ?category rdfs:label ?categoryLabel . FILTER(LANG(?categoryLabel) = "{lang}") }}
                }}
                
                # Teil-von und Hat-Teil Beziehungen mit Labels
                OPTIONAL {{ 
                    ?entity dbo:isPartOf ?partOf .
                    OPTIONAL {{ ?partOf rdfs:label ?partOfLabel . FILTER(LANG(?partOfLabel) = "{lang}") }}
                }}
                OPTIONAL {{ 
                    ?entity dbp:subdivisions ?hasPart .
                    OPTIONAL {{ ?hasPart rdfs:label ?hasPartLabel . FILTER(LANG(?hasPartLabel) = "{lang}") }}
                }}
                
                # Verschiedene Metadaten
                OPTIONAL {{ ?entity foaf:homepage ?homepage . }}
                OPTIONAL {{ ?entity dbo:thumbnail ?thumbnail . }}
                OPTIONAL {{ ?entity dbo:gndId ?gndId . }}
                OPTIONAL {{ ?entity geo:lat ?lat ; geo:long ?long . }}
                
                # Datums- und Zeitangaben
                OPTIONAL {{ ?entity dbo:birthDate ?birthDate . }}
                OPTIONAL {{ ?entity dbo:deathDate ?deathDate . }}
                OPTIONAL {{ ?entity dbo:foundingDate ?foundingDate . }}
                
                # Demografische Daten
                OPTIONAL {{ ?entity dbo:populationTotal ?population . }}
            }}
        """
        
        # Wähle den passenden Endpunkt aus, basierend auf der Konfiguration
        if config.get("DBPEDIA_USE_DE", False) or lang == "de":
            endpoints = [
                f"http://{lang}.dbpedia.org/sparql",
                "http://dbpedia.org/sparql"
            ]
        else:
            endpoints = [
                "http://dbpedia.org/sparql",
                f"http://{lang}.dbpedia.org/sparql"
            ]
        
        # Versuche jeden Endpunkt der Reihe nach
        query_success = False
        response_bindings = []
        
        for endpoint_url in endpoints:
            try:
                logger.info(f"Versuche SPARQL-Endpoint: {endpoint_url}")
                sparql = SPARQLWrapper.SPARQLWrapper(endpoint_url)
                sparql.setQuery(sparql_query)
                sparql.setReturnFormat(SPARQLWrapper.JSON)
                # Timeout aus der Konfiguration verwenden
                sparql.setTimeout(config.get("TIMEOUT_THIRD_PARTY", 15))
                
                # Query ausführen
                response = sparql.query().convert()
                response_bindings = response.get("results", {}).get("bindings", [])
                
                if response_bindings:
                    query_success = True
                    logger.info(f"Erfolgreiche SPARQL-Abfrage an {endpoint_url}")
                    break
                else:
                    logger.warning(f"Leere Antwort von {endpoint_url}")
            except Exception as e:
                logger.error(f"Fehler bei DBpedia-Batch-Abfrage: {e}")
        
        if not query_success:
            # Wenn keiner der Endpunkte funktioniert hat, füge Fehlerinformationen hinzu
            for entity_name, title in batch:
                results[entity_name] = create_error_result(
                    entity_name, title, "SPARQL endpoints nicht erreichbar"
                )
            continue
        
        # Verarbeite die SPARQL-Antwort und erstelle ein strukturiertes Ergebnis für jede Entität
        entities_found = set()
        entity_uri_to_name = {
            f"http://{lang}.dbpedia.org/resource/{urllib.parse.quote(title.replace(' ', '_'))}": entity_name
            for entity_name, title in batch
        }
        
        for binding in response_bindings:
            entity_uri = binding.get("entity", {}).get("value", "")
            
            # Finde den zugehörigen Entitätsnamen
            entity_name = None
            for uri, name in entity_uri_to_name.items():
                if uri in entity_uri:
                    entity_name = name
                    break
            
            if not entity_name:
                continue
            
            # Initialisiere ein Ergebnis für diese Entität, wenn es noch nicht existiert
            if entity_name not in results:
                _, title = next((name, title) for name, title in batch if name == entity_name)
                results[entity_name] = {
                    "status": "found",
                    "label": title,  # Wird später überschrieben, falls vorhanden
                    "abstract": "",
                    "resource_uri": entity_uri,
                    "types": [],
                    "subjects": [],
                    "categories": [],
                    "part_of": [],
                    "has_parts": [],
                    "part_of_labels": {},
                    "has_parts_labels": {},
                    "gnd_id": None,
                    "homepage": None,
                    "thumbnail": None,
                    "coordinates": None,
                    "birth_date": None,
                    "death_date": None,
                    "founding_date": None,
                    "population": None
                }
                entities_found.add(entity_name)
            
            # Extrahiere Werte aus dem Binding
            if "label" in binding and "value" in binding["label"]:
                results[entity_name]["label"] = binding["label"]["value"]
            
            if "abstract" in binding and "value" in binding["abstract"]:
                results[entity_name]["abstract"] = binding["abstract"]["value"]
            
            if "type" in binding and "value" in binding["type"]:
                type_uri = binding["type"]["value"]
                type_label = binding.get("typeLabel", {}).get("value", "")
                
                # Extrahiere nur den Namen aus der URI
                if "/" in type_uri:
                    type_name = type_uri.split("/")[-1]
                    if type_name and type_name not in results[entity_name]["types"]:
                        results[entity_name]["types"].append(type_name)
            
            if "category" in binding and "value" in binding["category"]:
                category_uri = binding["category"]["value"]
                category_label = binding.get("categoryLabel", {}).get("value", "")
                
                if category_label and category_label.startswith("Kategorie:"):
                    if category_label not in results[entity_name]["categories"]:
                        results[entity_name]["categories"].append(category_label)
                elif "/" in category_uri:
                    category_name = category_uri.split("/")[-1]
                    if category_name and f"Kategorie:{category_name}" not in results[entity_name]["categories"]:
                        results[entity_name]["categories"].append(f"Kategorie:{category_name}")
            
            if "partOf" in binding and "value" in binding["partOf"]:
                part_of_uri = binding["partOf"]["value"]
                part_of_label = binding.get("partOfLabel", {}).get("value", "")
                
                if "/" in part_of_uri:
                    part_of_name = part_of_uri.split("/")[-1]
                    if part_of_name:
                        if part_of_name not in results[entity_name]["part_of"]:
                            results[entity_name]["part_of"].append(part_of_name)
                        if part_of_label:
                            results[entity_name]["part_of_labels"][part_of_name] = part_of_label
            
            if "hasPart" in binding and "value" in binding["hasPart"]:
                has_part_uri = binding["hasPart"]["value"]
                has_part_label = binding.get("hasPartLabel", {}).get("value", "")
                
                if "/" in has_part_uri:
                    has_part_name = has_part_uri.split("/")[-1]
                    if has_part_name:
                        if has_part_name not in results[entity_name]["has_parts"]:
                            results[entity_name]["has_parts"].append(has_part_name)
                        if has_part_label:
                            results[entity_name]["has_parts_labels"][has_part_name] = has_part_label
            
            if "homepage" in binding and "value" in binding["homepage"]:
                results[entity_name]["homepage"] = binding["homepage"]["value"]
            
            if "thumbnail" in binding and "value" in binding["thumbnail"]:
                results[entity_name]["thumbnail"] = binding["thumbnail"]["value"]
            
            if "gndId" in binding and "value" in binding["gndId"]:
                results[entity_name]["gnd_id"] = binding["gndId"]["value"]
            
            if "lat" in binding and "value" in binding["lat"] and "long" in binding and "value" in binding["long"]:
                results[entity_name]["coordinates"] = {
                    "lat": float(binding["lat"]["value"]),
                    "long": float(binding["long"]["value"])
                }
            
            if "birthDate" in binding and "value" in binding["birthDate"]:
                results[entity_name]["birth_date"] = binding["birthDate"]["value"]
            
            if "deathDate" in binding and "value" in binding["deathDate"]:
                results[entity_name]["death_date"] = binding["deathDate"]["value"]
            
            if "foundingDate" in binding and "value" in binding["foundingDate"]:
                results[entity_name]["founding_date"] = binding["foundingDate"]["value"]
            
            if "population" in binding and "value" in binding["population"]:
                try:
                    results[entity_name]["population"] = int(binding["population"]["value"])
                except ValueError:
                    results[entity_name]["population"] = binding["population"]["value"]
        
        # Für Entitäten, die nicht gefunden wurden, einen leeren Datensatz einfügen
        for entity_name, title in batch:
            if entity_name not in entities_found and entity_name not in results:
                results[entity_name] = create_error_result(
                    entity_name, title, "Entität nicht gefunden"
                )
    
    # Bereinige die Labels, entferne leere Helper-Dictionaries
    for entity_name, data in results.items():
        if "part_of_labels" in data:
            del data["part_of_labels"]
        if "has_parts_labels" in data:
            del data["has_parts_labels"]
    
    return results


def _fallback_to_dbpedia_lookup(entity_title: str, lang: str = "de", config: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Versucht, Informationen über die DBpedia Lookup API abzurufen, wenn SPARQL fehlschlägt.
    
    Args:
        entity_title: Titel der Entität
        lang: Sprache (default: "de")
        config: Konfigurationswörterbuch
        
    Returns:
        DBpedia-Lookup-Ergebnis oder None, wenn nichts gefunden wurde
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    # Wenn DBpedia Lookup API nicht aktiviert ist, abbrechen
    if not config.get("DBPEDIA_LOOKUP_API", True):
        return None
    
    # Bestimme passende Lookup-Endpunkte basierend auf der Konfiguration
    lookup_endpoints = []
    if lang == "de" or config.get("DBPEDIA_USE_DE", False):
        lookup_endpoints.append(f"https://lookup.dbpedia.org/api/search?query={urllib.parse.quote(entity_title)}&format=json&maxResults=1&language={lang}")
        lookup_endpoints.append(f"https://lookup.dbpedia.org/api/search?query={urllib.parse.quote(entity_title)}&format=json&maxResults=1")
    else:
        lookup_endpoints.append(f"https://lookup.dbpedia.org/api/search?query={urllib.parse.quote(entity_title)}&format=json&maxResults=1")
        lookup_endpoints.append(f"https://lookup.dbpedia.org/api/search?query={urllib.parse.quote(entity_title)}&format=json&maxResults=1&language={lang}")
    
    for endpoint_url in lookup_endpoints:
        try:
            logger.info(f"Versuche DBpedia Lookup API: {endpoint_url}")
            response = _limited_get(endpoint_url, timeout=5)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if "docs" in data and len(data["docs"]) > 0:
                        first_result = data["docs"][0]
                        
                        # Extrahiere relevante Daten
                        label = first_result.get("label", entity_title)
                        abstract = first_result.get("comment", "")
                        resource_uri = first_result.get("resource", [f"http://{lang}.dbpedia.org/resource/{urllib.parse.quote(entity_title.replace(' ', '_'))}"])[0]
                        categories = first_result.get("category", [])
                        
                        # Erzeuge strukturierte Antwort
                        lookup_result = {
                            "status": "lookup",
                            "label": label,
                            "abstract": abstract,
                            "resource_uri": resource_uri,
                            "types": first_result.get("type", []),
                            "subjects": [],
                            "categories": [f"Kategorie:{category}" for category in categories],
                            "part_of": [],
                            "has_parts": [],
                            "gnd_id": None,
                            "homepage": None,
                            "thumbnail": None,
                            "coordinates": None
                        }
                        
                        logger.info(f"Erfolgreiche DBpedia-Lookup-Abfrage für {entity_title}")
                        return lookup_result
                    else:
                        logger.warning(f"Leeres Ergebnis von DBpedia Lookup für {entity_title}")
                except Exception as lookup_parse_error:
                    logger.error(f"Fehler beim Parsing der DBpedia-Lookup-Antwort: {lookup_parse_error}")
            else:
                logger.warning(f"DBpedia Lookup API lieferte Status {response.status_code}")
        except Exception as lookup_error:
            logger.error(f"Fehler bei der DBpedia-Lookup-Abfrage: {lookup_error}")
    
    logger.info(f"Keine Ergebnisse von DBpedia Lookup API für {entity_title}")
    return None


def _get_wikipedia_titles(entity_wikipedia_urls: Dict[str, str], lang: str = "de") -> Dict[str, str]:
    """
    Extrahiert Titel aus Wikipedia-URLs.
    
    Args:
        entity_wikipedia_urls: Dict mit Entitätsnamen als Schlüssel und Wikipedia-URLs als Werte
        lang: Sprache (default: "de")
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und extrahierten Titeln als Werte
    """
    titles = {}
    for entity_name, url in entity_wikipedia_urls.items():
        if not url:
            continue
        
        # Extrahiere den Titel aus der URL
        parts = url.split('/')
        if len(parts) > 0:
            title = parts[-1]
            title = urllib.parse.unquote(title)
            title = title.replace('_', ' ')
            titles[entity_name] = title
    
    return titles


def batch_get_dbpedia_info(entity_wikipedia_urls: Dict[str, str], config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Ruft DBpedia-Informationen für mehrere Entitäten ab.
    
    Diese Funktion ruft zunächst den Cache ab und lädt dann fehlende Daten von DBpedia.
    Wenn ein Fehler auftritt, wird automatisch lokales Fallback oder DBpedia Lookup verwendet.
    Unterstützt Sprach-Fallbacks zwischen Deutsch und Englisch, wenn nötig.
    
    Args:
        entity_wikipedia_urls: Dict mit Entitätsnamen als Schlüssel und Wikipedia-URLs als Werte
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Dict mit Entitätsnamen als Schlüssel und erweiterten Informationen als Werte
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    # Wenn DBpedia deaktiviert ist, leeres Ergebnis zurückgeben
    if not config.get("USE_DBPEDIA", False):
        return {}
    
    # Keine Entitäten, leeres Ergebnis zurückgeben
    if not entity_wikipedia_urls:
        return {}
    
    # Bestimme die Sprache basierend auf der Konfiguration
    lang = "de" if config.get("DBPEDIA_USE_DE", False) else "en"
    
    # Cache-Schlüssel erstellen und prüfen
    cache_key = "batch_dbpedia_" + lang + "_" + hashlib.md5(str(sorted(entity_wikipedia_urls.items())).encode()).hexdigest()
    cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "dbpedia", cache_key)
    cached_data = load_cache(cache_path)
    
    if cached_data is not None:
        logger.info(f"Cache-Treffer für DBpedia-Batch: {cache_key}")
        return cached_data
    
    logger.info(f"Cache-Fehltreffer für DBpedia-Batch: {cache_key}")
    
    # 1. Versuche lokale Fallbacks für wichtige/häufige Entitäten
    results = get_batch_local_fallbacks(entity_wikipedia_urls)
    logger.info(f"Fallback-Ergebnisse gefunden: {len(results)}")
    
    # Entitäten erfassen, die noch nicht im Ergebnis sind
    missing_entities = {entity_name: url for entity_name, url in entity_wikipedia_urls.items() if entity_name not in results}
    
    # 2. Extrahiere Titel aus den URLs für die fehlenden Entitäten
    entity_titles = _get_wikipedia_titles(missing_entities, lang)
    
    # 3. SPARQL-Abfrage durchführen, wenn SPARQL nicht deaktiviert ist
    if not config.get("DBPEDIA_SKIP_SPARQL", False):
        try:
            # Führe SPARQL-Abfrage durch
            sparql_results = _fetch_dbpedia_batch(entity_titles, lang, config)
            
            # Füge die Ergebnisse zum Gesamtergebnis hinzu
            results.update(sparql_results)
            
            # Aktualisiere die Liste der fehlenden Entitäten
            missing_entities = {entity_name: url for entity_name, url in missing_entities.items() 
                               if entity_name not in results or results[entity_name].get("status") == "error"}
        except Exception as sparql_error:
            logger.error(f"Fehler bei SPARQL-Batch-Abfrage: {sparql_error}")
            # Behandele fehlende Entitäten im nächsten Schritt
    
    # 4. Für alle verbleibenden fehlenden Entitäten: Versuche DBpedia Lookup API als letzten Ausweg
    if missing_entities and config.get("DBPEDIA_LOOKUP_API", True):
        for entity_name, wikipedia_url in missing_entities.items():
            # Überspringe Entitäten, die bereits erfolgreich abgerufen wurden
            if entity_name in results and results[entity_name].get("status") != "error":
                continue
            
            title = entity_titles.get(entity_name, "")
            if not title and wikipedia_url:
                # Extrahiere den Titel aus der URL
                parts = wikipedia_url.split('/')
                if len(parts) > 0:
                    title = parts[-1]
                    title = urllib.parse.unquote(title)
                    title = title.replace('_', ' ')
            
            if title:
                lookup_result = _fallback_to_dbpedia_lookup(title, lang, config)
                if lookup_result:
                    results[entity_name] = lookup_result
    
    # Differenziertes Caching: Speichere nur erfolgreiche Ergebnisse mit Mindestdaten im Cache
    cacheable_results = {}
    for entity_name, data in results.items():
        # Prüfe, ob der Status erfolgreich ist
        if data.get("status") in ["found", "fallback", "lookup"]:
            # Prüfe Mindestdatenfelder für DBpedia
            if all(key in data and data[key] for key in ["label", "resource_uri", "abstract"]):
                cacheable_results[entity_name] = data
            elif "label" in data and "resource_uri" in data:  # Teilweise Daten vorliegen
                # Wenn wenigstens Label und URI vorhanden sind, aber kein Abstract, trotzdem cachen
                # (hilft bei Entitäten, die wenig Informationen haben)
                cacheable_results[entity_name] = data
                logger.info(f"Entität {entity_name} hat nicht alle Mindestdatenfelder, wird aber trotzdem gecacht")
    
    # Nur cachen, wenn gültige Ergebnisse vorhanden sind
    if cacheable_results:
        logger.info(f"Cache {len(cacheable_results)} Entitäten mit vollständigen Daten")
        cache_path = get_cache_path(config.get("CACHE_DIR", "cache"), "dbpedia", cache_key)
        save_cache(cache_path, cacheable_results)
    else:
        logger.warning(f"Keine cachelbare Ergebnisse gefunden - kein Cache-Update durchgeführt")
    
    return results
