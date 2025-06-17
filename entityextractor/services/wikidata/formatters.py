#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Formatierungsfunktionen für Wikidata-Daten.

Dieses Modul enthält Funktionen zum Formatieren und Transformieren von Wikidata-API-Antworten
in einheitliche Datenstrukturen für die Weiterverarbeitung.

Optimierter Output mit reduzierten Feldern:
- Id
- Label
- Aliases
- Description
- type
- Instance Of
- Subclass Of
- Part Of
- Has Part
- Geo Koordinaten
- Img URL
- Date Birth, Date Founded, Date of Death, End Time
- official Website
- gnd
- isni
"""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple

from entityextractor.utils.logging_utils import get_service_logger

# Logger konfigurieren
logger = get_service_logger(__name__, 'wikidata')

# Wikidata Property IDs
PROPERTY_IDS = {
    'instance_of': 'P31',      # Instance of
    'subclass_of': 'P279',     # Subclass of
    'part_of': 'P361',         # Part of
    'has_part': 'P527',        # Has part
    'coordinates': 'P625',     # Coordinate location
    'image': 'P18',            # Image
    'date_of_birth': 'P569',   # Date of birth
    'date_founded': 'P571',    # Inception
    'date_of_death': 'P570',   # Date of death
    'end_time': 'P582',        # End time
    'official_website': 'P856', # Official website
    'gnd_id': 'P227',          # GND ID
    'isni_id': 'P213'          # ISNI ID
}

def format_wikidata_entity(entity_data: Dict[str, Any], entity_name: str = None, 
                          needs_fallback: bool = False, language: str = 'de',
                          batch_label_fetcher=None) -> Dict[str, Any]:
    """
    Formatiert Wikidata-Entitätsdaten in ein einheitliches, kompaktes Format.
    
    Args:
        entity_data: Rohdaten der Wikidata-Entität
        entity_name: Ursprünglicher Name der Entität (optional)
        needs_fallback: Flag, ob Fallback benötigt wird
        language: Bevorzugte Sprache für Labels und Beschreibungen
        batch_label_fetcher: Optionale Funktion zum Abrufen von Labels im Batch
        
    Returns:
        Formatierte Wikidata-Daten in kompakter Form
    """
    # Wenn keine Daten vorhanden sind, leeres Ergebnis zurückgeben
    if not entity_data:
        return {
            'status': 'not_found',
            'source': 'wikidata_api',
            'entity_name': entity_name
        }
    
    # Wenn es sich um eine Fehlermeldung handelt
    if 'error' in entity_data:
        return {
            'status': 'error',
            'source': 'wikidata_api',
            'error': entity_data.get('error'),
            'entity_name': entity_name
        }
    
    # Wikidata-URL hinzufügen
    entity_id = entity_data.get('id', '')
    
    # Kompaktes Ergebnis-Format
    result = {
        'id': entity_id,
        'url': f"https://www.wikidata.org/wiki/{entity_id}"
    }
    
    # Sitelinks sofort aus den Eingabedaten entfernen, falls vorhanden
    if 'sitelinks' in entity_data:
        del entity_data['sitelinks']
    
    # Label in der bevorzugten Sprache
    labels = entity_data.get('labels', {})
    if labels:
        if language in labels:
            result['label'] = labels[language].get('value', '')
        elif 'en' in labels:
            result['label'] = labels['en'].get('value', '')
        elif labels:
            first_lang = next(iter(labels))
            result['label'] = labels[first_lang].get('value', '')
    
    # Beschreibung in der bevorzugten Sprache
    descriptions = entity_data.get('descriptions', {})
    if descriptions:
        if language in descriptions:
            result['description'] = descriptions[language].get('value', '')
        elif 'en' in descriptions:
            result['description'] = descriptions['en'].get('value', '')
        elif descriptions:
            first_lang = next(iter(descriptions))
            result['description'] = descriptions[first_lang].get('value', '')
    
    # Aliase in der bevorzugten Sprache (optional)
    aliases = entity_data.get('aliases', {})
    if aliases:
        if language in aliases:
            result['aliases'] = [alias.get('value', '') for alias in aliases[language]]
        elif 'en' in aliases:
            result['aliases'] = [alias.get('value', '') for alias in aliases['en']]
    
    # Typen aus instance_of extrahieren
    types = []
    if 'claims' in entity_data and PROPERTY_IDS['instance_of'] in entity_data['claims']:
        instance_refs = extract_entity_references(entity_data['claims'], PROPERTY_IDS['instance_of'])
        if instance_refs:
            types = [ref.get('label', '') for ref in instance_refs if 'label' in ref and ref.get('label')]
    
    if types:
        result['types'] = types
    
    # Claims/Statements verarbeiten - nur spezifische Properties
    extract_flat_properties(result, entity_data.get('claims', {}))
    
    # Fallback-Informationen hinzufügen (optional)
    if needs_fallback or entity_data.get('fallback_used', False):
        result['fallback_used'] = True
        if 'fallback_source' in entity_data:
            result['fallback_source'] = entity_data.get('fallback_source')
    
    # Labels für referenzierte Entitäten anreichern, wenn ein Batch-Fetcher verfügbar ist
    if batch_label_fetcher:
        enrich_flat_entity_references(result, batch_label_fetcher, language)
    
    return result


def extract_flat_properties(result: Dict[str, Any], claims: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Extrahiert Properties aus den Wikidata-Claims in einem flachen Format.
    Alle Properties werden auf der gleichen Ebene wie label und description angeordnet.
    
    Args:
        result: Das Ergebnis-Dictionary, das aktualisiert wird
        claims: Die Wikidata-Claims
    """
    if not claims:
        return
    
    # Instance of (P31) - Flaches Format
    if PROPERTY_IDS['instance_of'] in claims:
        instance_of_refs = extract_entity_references(claims, PROPERTY_IDS['instance_of'])
        if instance_of_refs:
            result['instance_of'] = [{'id': ref.get('id', ''), 'label': ref.get('label', '')} 
                                     for ref in instance_of_refs]

    # Subclass of (P279) - Flaches Format
    if PROPERTY_IDS['subclass_of'] in claims:
        subclass_refs = extract_entity_references(claims, PROPERTY_IDS['subclass_of'])
        if subclass_refs:
            result['subclass_of'] = [{'id': ref.get('id', ''), 'label': ref.get('label', '')} 
                                    for ref in subclass_refs]
    
    # Part of (P361) - Flaches Format
    if PROPERTY_IDS['part_of'] in claims:
        part_of_refs = extract_entity_references(claims, PROPERTY_IDS['part_of'])
        if part_of_refs:
            result['part_of'] = [{'id': ref.get('id', ''), 'label': ref.get('label', '')} 
                                for ref in part_of_refs]
    
    # Has part (P527) - Flaches Format
    if PROPERTY_IDS['has_part'] in claims:
        has_part_refs = extract_entity_references(claims, PROPERTY_IDS['has_part'])
        if has_part_refs:
            result['has_part'] = [{'id': ref.get('id', ''), 'label': ref.get('label', '')} 
                                 for ref in has_part_refs]
    
    # Coordinates (P625) - Einfaches Format
    if PROPERTY_IDS['coordinates'] in claims:
        coords = extract_coordinates(claims, PROPERTY_IDS['coordinates'])
        if coords and coords[0]:
            result['coordinates'] = {
                'latitude': coords[0].get('latitude'), 
                'longitude': coords[0].get('longitude')
            }
    
    # Image (P18) - Nur die URL
    if PROPERTY_IDS['image'] in claims:
        image_urls = extract_image_urls(claims, PROPERTY_IDS['image'])
        if image_urls:
            result['image_url'] = image_urls[0]  # Nur die erste URL
    
    # Date of birth (P569) - Nur das Datum
    if PROPERTY_IDS['date_of_birth'] in claims:
        dates = extract_time_values(claims, PROPERTY_IDS['date_of_birth'])
        if dates:
            result['date_of_birth'] = dates[0].get('time')
    
    # Date founded (P571) - Nur das Datum
    if PROPERTY_IDS['date_founded'] in claims:
        dates = extract_time_values(claims, PROPERTY_IDS['date_founded'])
        if dates:
            result['date_founded'] = dates[0].get('time')
    
    # Date of death (P570) - Nur das Datum
    if PROPERTY_IDS['date_of_death'] in claims:
        dates = extract_time_values(claims, PROPERTY_IDS['date_of_death'])
        if dates:
            result['date_of_death'] = dates[0].get('time')
    
    # End time (P582) - Nur das Datum
    if PROPERTY_IDS['end_time'] in claims:
        dates = extract_time_values(claims, PROPERTY_IDS['end_time'])
        if dates:
            result['end_time'] = dates[0].get('time')
    
    # Official website (P856) - Nur die URL
    if PROPERTY_IDS['official_website'] in claims:
        urls = extract_url_values(claims, PROPERTY_IDS['official_website'])
        if urls:
            result['official_website'] = urls[0]
    
    # GND ID (P227) - Nur die ID
    if PROPERTY_IDS['gnd_id'] in claims:
        ids = extract_string_values(claims, PROPERTY_IDS['gnd_id'])
        if ids:
            result['gnd_id'] = ids[0]
    
    # ISNI ID (P213) - Nur die ID
    if PROPERTY_IDS['isni_id'] in claims:
        ids = extract_string_values(claims, PROPERTY_IDS['isni_id'])
        if ids:
            result['isni_id'] = ids[0]


def extract_specific_properties(result: Dict[str, Any], claims: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Extrahiert nur die spezifischen Properties aus den Wikidata-Claims in kompakter Form.
    Diese Funktion wird aus Kompatibilitätsgründen beibehalten.
    
    Args:
        result: Das Ergebnis-Dictionary, das aktualisiert wird
        claims: Die Wikidata-Claims
    """
    # Verwende stattdessen die neue flache Extraktion
    extract_flat_properties(result, claims)


def extract_entity_references(claims: Dict[str, List[Dict[str, Any]]], property_id: str) -> List[Dict[str, Any]]:
    """
    Extrahiert Entitätsreferenzen (Q-IDs) aus Claims.
    
    Args:
        claims: Die Wikidata-Claims
        property_id: Die Property-ID (z.B. 'P31')
        
    Returns:
        Liste von Dictionaries mit ID und Typ
    """
    references = []
    
    for claim in claims.get(property_id, []):
        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        
        if not datavalue:
            continue
            
        datatype = mainsnak.get('datatype')
        if datatype == 'wikibase-item':
            value = datavalue.get('value', {})
            qid = value.get('id')
            if qid:
                # Hier nur die ID speichern, Labels werden später angereichert
                references.append({
                    'id': qid,
                    'label': '',  # Leeres Label, wird später angereichert
                    'type': 'entity'
                })
    
    return references


def extract_coordinates(claims: Dict[str, List[Dict[str, Any]]], property_id: str) -> List[Dict[str, Any]]:
    """
    Extrahiert Geo-Koordinaten aus Claims.
    
    Args:
        claims: Die Wikidata-Claims
        property_id: Die Property-ID für Koordinaten
        
    Returns:
        Liste von Koordinaten-Dictionaries
    """
    coordinates = []
    
    for claim in claims.get(property_id, []):
        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        
        if not datavalue:
            continue
            
        datatype = mainsnak.get('datatype')
        if datatype == 'globecoordinate':
            value = datavalue.get('value', {})
            if isinstance(value, dict):
                coordinates.append({
                    'latitude': value.get('latitude', 0),
                    'longitude': value.get('longitude', 0),
                    'precision': value.get('precision', 0),
                    'globe': value.get('globe', 'http://www.wikidata.org/entity/Q2')
                })
    
    return coordinates


def extract_image_urls(claims: Dict[str, List[Dict[str, Any]]], property_id: str) -> List[str]:
    """
    Extrahiert Bild-URLs aus Claims und formatiert sie als Commons-URLs.
    
    Args:
        claims: Die Wikidata-Claims
        property_id: Die Property-ID für Bilder
        
    Returns:
        Liste von Bild-URLs
    """
    image_urls = []
    
    for claim in claims.get(property_id, []):
        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        
        if not datavalue:
            continue
            
        datatype = mainsnak.get('datatype')
        if datatype == 'string':
            value = datavalue.get('value')
            if value:
                # Commons-URL formatieren
                file_name = value.replace(' ', '_')
                image_url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{file_name}"
                image_urls.append(image_url)
    
    return image_urls


def extract_time_values(claims: Dict[str, List[Dict[str, Any]]], property_id: str) -> List[Dict[str, Any]]:
    """
    Extrahiert Zeitangaben aus Claims.
    
    Args:
        claims: Die Wikidata-Claims
        property_id: Die Property-ID für Zeitangaben
        
    Returns:
        Liste von Zeitangaben-Dictionaries
    """
    time_values = []
    
    for claim in claims.get(property_id, []):
        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        
        if not datavalue:
            continue
            
        datatype = mainsnak.get('datatype')
        if datatype == 'time':
            value = datavalue.get('value', {})
            if isinstance(value, dict):
                time_values.append({
                    'time': value.get('time', ''),
                    'precision': value.get('precision', 0),
                    'calendar': value.get('calendarmodel', '')
                })
    
    return time_values


def extract_url_values(claims: Dict[str, List[Dict[str, Any]]], property_id: str) -> List[str]:
    """
    Extrahiert URLs aus Claims.
    
    Args:
        claims: Die Wikidata-Claims
        property_id: Die Property-ID für URLs
        
    Returns:
        Liste von URLs
    """
    urls = []
    
    for claim in claims.get(property_id, []):
        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        
        if not datavalue:
            continue
            
        datatype = mainsnak.get('datatype')
        if datatype == 'url':
            value = datavalue.get('value')
            if value:
                urls.append(value)
    
    return urls


def extract_string_values(claims: Dict[str, List[Dict[str, Any]]], property_id: str) -> List[str]:
    """
    Extrahiert String-Werte aus Claims.
    
    Args:
        claims: Die Wikidata-Claims
        property_id: Die Property-ID für String-Werte
        
    Returns:
        Liste von String-Werten
    """
    strings = []
    
    for claim in claims.get(property_id, []):
        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        
        if not datavalue:
            continue
            
        datatype = mainsnak.get('datatype')
        if datatype == 'string' or datatype == 'external-id':
            value = datavalue.get('value')
            if value:
                strings.append(value)
    
    return strings


def enrich_flat_entity_references(result: Dict[str, Any], label_fetcher, language: str = 'de') -> None:
    """
    Reichert Entitätsreferenzen in einem flachen Format mit Labels an.
    
    Args:
        result: Das Ergebnis-Dictionary mit Entitätsreferenzen
        label_fetcher: Funktion zum Abrufen von Labels
        language: Bevorzugte Sprache für Labels
    """
    try:
        # Sammle alle Entitäts-IDs
        entity_ids = set()
        
        # Durchsuche alle Properties nach Entitätsreferenzen
        for prop_name in ['instance_of', 'subclass_of', 'part_of', 'has_part']:
            if prop_name in result:
                for ref in result[prop_name]:
                    if 'id' in ref and not ref.get('label'):
                        entity_ids.add(ref['id'])
        
        if not entity_ids:
            return
        
        # Labels im Batch abrufen
        entity_labels = label_fetcher(list(entity_ids), language)
        
        # Labels zu Referenzen hinzufügen
        for prop_name in ['instance_of', 'subclass_of', 'part_of', 'has_part']:
            if prop_name in result:
                for ref in result[prop_name]:
                    if 'id' in ref and ref['id'] in entity_labels:
                        ref['label'] = entity_labels[ref['id']]
    except Exception as e:
        logger.error(f"Fehler beim Anreichern von Labels für Referenzen: {str(e)}")


def enrich_entity_references_with_labels(result: Dict[str, Any], label_fetcher, language: str = 'de') -> None:
    """
    Reichert Entitätsreferenzen mit Labels an.
    Diese Funktion wird aus Kompatibilitätsgründen beibehalten.
    
    Args:
        result: Das Ergebnis-Dictionary mit Entitätsreferenzen
        label_fetcher: Funktion zum Abrufen von Labels
        language: Bevorzugte Sprache für Labels
    """
    # Verwende stattdessen die neue flache Anreicherung
    enrich_flat_entity_references(result, label_fetcher, language)


def format_wikidata_search_results(search_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Formatiert Wikidata-Suchergebnisse in ein einheitliches Format.
    
    Args:
        search_results: Suchergebnisse der Wikidata-API
        
    Returns:
        Formatierte Suchergebnisse
    """
    formatted_results = []
    
    for item in search_results:
        formatted_item = {
            'id': item.get('id', ''),
            'label': item.get('label', ''),
            'description': item.get('description', ''),
            'url': f"https://www.wikidata.org/entity/{item.get('id', '')}",
            'match': {
                'type': item.get('match', {}).get('type', ''),
                'language': item.get('match', {}).get('language', '')
            },
            'status': 'found',
            'source': 'wikidata_search',
            'aliases': [item.get('label', '')] if item.get('label') else []
        }
        formatted_results.append(formatted_item)
    
    return formatted_results


def has_required_fields(entity_data: Dict[str, Any], required_fields: List[str] = None) -> bool:
    """
    Prüft, ob die Entitätsdaten alle erforderlichen Felder enthalten.
    
    Args:
        entity_data: Wikidata-Entitätsdaten
        required_fields: Liste der zu überprüfenden Felder (optional)
        
    Returns:
        True, wenn alle erforderlichen Felder vorhanden sind
    """
    # Grundlegende Prüfung
    if not entity_data:
        return False
    
    # Mindestanforderungen für das neue flache Format: ID, URL, Label und Beschreibung
    if not entity_data.get('id'):
        return False
    
    if not entity_data.get('url'):
        return False
    
    if not entity_data.get('label'):
        return False
    
    if not entity_data.get('description'):
        return False
    
    # Wenn zusätzliche Felder angegeben wurden, diese überprüfen
    if required_fields:
        for field in required_fields:
            if field not in entity_data or not entity_data.get(field):
                return False
    
    # Im neuen Format sind die Mindestanforderungen immer erfüllt
    return True


def enrich_entity_references(entities: List[Dict[str, Any]], reference_entities: Dict[str, Dict[str, Any]]) -> None:
    """
    Reichert Entitätsreferenzen mit Labels aus den Referenzentitäten an.
    
    Args:
        entities: Liste von Entitäten mit Referenzen
        reference_entities: Dictionary mit Referenzentitäten (ID -> Entität)
    """
    for entity in entities:
        # Durchlaufe alle Felder, die Referenzen enthalten könnten
        for field in ['instance_of', 'subclass_of', 'part_of', 'has_part']:
            if field in entity and isinstance(entity[field], list):
                for ref in entity[field]:
                    if 'id' in ref and ref['id'] in reference_entities:
                        ref_entity = reference_entities[ref['id']]
                        # Labels hinzufÃ¼gen
                        if 'labels' in ref_entity:
                            ref['labels'] = ref_entity['labels']
                        if 'label' in ref_entity:
                            ref['label'] = ref_entity['label']
