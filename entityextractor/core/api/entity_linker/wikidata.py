"""
entity_linker/wikidata.py

Enthält Funktionen für die Verknüpfung von Entitäten mit Wikidata-Daten.
"""

import logging
from typing import List, Dict, Any, Optional

from entityextractor.models.entity import Entity
from entityextractor.services.wikidata import get_batch_wikidata_service
from entityextractor.utils.source_utils import safe_get, safe_source_access

async def extract_wikidata_ids(entities: List[Dict[str, Any]], config: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Extrahiert Wikidata-IDs für Entitäten mit Wikipedia-URLs.
    
    Args:
        entities: Liste von Entity-Dictionaries
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Dictionary mit Wikipedia-URLs als Schlüssel und Wikidata-Informationen als Werte
    """
    # Mapping Wikipedia-URL zu Entität
    entities_for_wikidata = {e.get("wikipedia_url"): e for e in entities if e.get("wikipedia_url")}
    
    if not entities_for_wikidata:
        logging.info("[extract_wikidata_ids] Keine Entitäten für Wikidata-Lookup verfügbar (keine Wikipedia-URLs).")
        return {}
    
    logging.info(f"[extract_wikidata_ids] Extrahiere Wikidata-IDs für {len(entities_for_wikidata)} Entitäten mit Wikipedia-URLs")
    
    try:
        # Initialisiere den Wikidata-Service
        wikidata_service = get_batch_wikidata_service(config)
        
        # Erstelle Dummy-Entitäten für die Verarbeitung
        dummy_entities = [
            Entity(name=entity.get("name", ""), sources={"wikipedia": {"url": url}})
            for url, entity in entities_for_wikidata.items()
        ]
        
        # Führe die Anreicherung durch
        enriched_entities = await wikidata_service.enrich_entities(dummy_entities)
        
        # Erstelle das Ergebnis-Dictionary
        result = {}
        for entity in enriched_entities:
            if entity.sources.get("wikipedia", {}).get("wikidata_id"):
                url = entity.sources["wikipedia"]["url"]
                result[url] = {
                    "wikidata_id": entity.sources["wikipedia"]["wikidata_id"],
                    "status": "found"
                }
        
        logging.info(f"[extract_wikidata_ids] {len(result)} Wikidata-IDs erfolgreich extrahiert")
        return result
    except Exception as e:
        logging.error(f"[extract_wikidata_ids] Fehler bei der Extraktion der Wikidata-IDs: {str(e)}")
        return {}

async def fetch_wikidata_details(wikidata_ids: List[str], config: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Holt detaillierte Informationen für eine Liste von Wikidata-IDs.
    
    Args:
        wikidata_ids: Liste von Wikidata-IDs
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Dictionary mit Wikidata-IDs als Schlüssel und Detailinformationen als Werte
    """
    if not wikidata_ids:
        return {}
        
    # Initialisiere den Wikidata-Service
    wikidata_service = get_batch_wikidata_service(config)
    
    # Erstelle Dummy-Entitäten mit den Wikidata-IDs
    entities = [Entity(wikidata_id=wikidata_id) for wikidata_id in wikidata_ids]
    
    # Führe die Anreicherung durch
    try:
        enriched_entities = await wikidata_service.enrich_entities(entities)
        logging.info(f"[fetch_wikidata_details] Erfolgreich {len(enriched_entities)} von {len(entities)} Entitäten mit Wikidata angereichert")
        
        # Erstelle das Ergebnis-Dictionary
        return {e.wikidata_id: e.sources.get("wikidata", {}) for e in enriched_entities if e.wikidata_id}
    except Exception as e:
        logging.error(f"[fetch_wikidata_details] Fehler bei der Anreicherung mit Wikidata: {str(e)}")
        return {}

async def link_with_wikidata(entities: List[Entity], config: Optional[Dict[str, Any]] = None) -> List[Entity]:
    """
    Verknüpft eine Liste von Entitäten mit Wikidata-Daten basierend auf ihren Wikipedia-URLs.
    
    Args:
        entities: Liste von Entity-Objekten
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Angereicherte Liste von Entity-Objekten
    """
    if not entities:
        return []
        
    # Initialisiere den Wikidata-Service
    wikidata_service = get_batch_wikidata_service(config)
    
    # Filtere Entitäten mit Wikipedia-URLs für die Verknüpfung
    entities_with_wikipedia = [e for e in entities if e.has_source("wikipedia")]
    
    # Logging für Diagnose
    found_no_extract = sum(1 for e in entities_with_wikipedia 
                         if e.sources.get("wikipedia", {}).get("status") == "found" 
                         and not e.sources.get("wikipedia", {}).get("extract"))
    partial_count = sum(1 for e in entities_with_wikipedia 
                      if e.sources.get("wikipedia", {}).get("status") == "partial")
    
    logging.info(f"[link_with_wikidata] Verarbeite {len(entities_with_wikipedia)} von {len(entities)} Entitäten mit Wikipedia-URLs")
    logging.info(f"[link_with_wikidata] Status-Verteilung: {len(entities_with_wikipedia)} mit Wikipedia-Quelle, "
                f"davon {found_no_extract} 'found' ohne Extract, {partial_count} 'partial'")
    
    if not entities_with_wikipedia:
        logging.info("[link_with_wikidata] Keine Entitäten mit Wikipedia-URLs gefunden")
        return entities
    
    # Führe die Anreicherung durch
    try:
        enriched_entities = await wikidata_service.enrich_entities(entities_with_wikipedia)
        logging.info(f"[link_with_wikidata] Erfolgreich {len(enriched_entities)} Entitäten mit Wikidata angereichert")
        return enriched_entities
    except Exception as e:
        logging.error(f"[link_with_wikidata] Fehler bei der Anreicherung mit Wikidata: {str(e)}")
        return entities
    
    # Dictionary erstellen für die Suche
    wiki_url_to_entity = {}
    for entity in entities_with_wikipedia:
        if entity.has_source("wikipedia"):
            source = entity.sources.get("wikipedia")
            url = safe_get(source, "url")
            if url:
                wiki_url_to_entity[url] = entity
    
    # Wikidata-IDs extrahieren
    wikidata_id_results = await get_wikidata_ids_for_entities(wiki_url_to_entity, config)
    
    # IDs zu Entitäten hinzufügen und für Detailabfrage sammeln
    wikidata_ids = []
    for url, wikidata_info in wikidata_id_results.items():
        if wikidata_info and wikidata_info.get('status') == 'found':
            entity = wiki_url_to_entity.get(url)
            if entity:
                wid = wikidata_info.get('id', wikidata_info.get('wikidata_id', ''))
                if wid:
                    entity.wikidata_id = wid
                    wikidata_ids.append(wid)
    
    # Detaillierte Wikidata-Informationen holen, wenn IDs vorhanden sind
    if wikidata_ids:
        wikidata_details = await get_wikidata_entities(wikidata_ids, config)
        
        # Wikidata-Details zu Entitäten hinzufügen
        for entity in entities:
            if entity.wikidata_id and entity.wikidata_id in wikidata_details:
                details = wikidata_details[entity.wikidata_id]
                
                # Wikidata als Quelle hinzufügen
                entity.add_source('wikidata', {
                    'id': entity.wikidata_id,
                    'url': details.get('url', f'https://www.wikidata.org/entity/{entity.wikidata_id}'),
                    'labels': details.get('labels', {}),
                    'descriptions': details.get('descriptions', {}),
                    'aliases': details.get('aliases', {}),
                    'claims': details.get('claims', {}),
                    'sitelinks': details.get('sitelinks', {}),
                    
                    # Ontologische Informationen
                    'ontology': details.get('ontology', {
                        'instance_of': [],
                        'subclass_of': [],
                        'part_of': [],
                        'has_part': [],
                        'facet_of': []
                    }),
                    
                    # Semantische Informationen
                    'semantics': details.get('semantics', {
                        'main_subject': [],
                        'main_subject_of': [],
                        'field_of_work': [],
                        'applies_to': []
                    }),
                    
                    # Medien-Informationen
                    'media': details.get('media', {
                        'image': None,
                        'image_url': None
                    }),
                    
                    'status': 'found'
                })
                
                # Aktualisiere auch die Metadaten der Entität
                if 'media' in details and details['media'].get('image_url'):
                    entity.metadata['image_url'] = details['media']['image_url']
                
                entity.metadata['wikidata_url'] = details.get('url', f'https://www.wikidata.org/entity/{entity.wikidata_id}')
                
                # Aktualisiere den Entitätstyp, wenn er aus Wikidata bestimmt werden kann
                if 'ontology' in details and details['ontology'].get('instance_of'):
                    instance_of = details['ontology']['instance_of']
                    if instance_of and not entity.type:
                        # Hier könnte die determine_entity_type Funktion aufgerufen werden
                        # Für jetzt übernehmen wir einfach den ersten Typ als Beispiel
                        entity.type = 'THING'
    
    # Detaillierte Erfolgsrate mit mehr Diagnostik loggen
    entities_with_wikidata_id = [e for e in entities if e.wikidata_id]
    entities_with_wikidata_source = [e for e in entities if e.has_source('wikidata')]
    
    success_count = len(entities_with_wikidata_source)
    id_only_count = len(entities_with_wikidata_id) - success_count
    total_initial_entities = len(entities)
    had_wikipedia_count = sum(1 for e in entities if e.has_source('wikipedia'))
    had_valid_wikipedia = sum(1 for e in entities if e.has_source('wikipedia') and 
                             e.sources.get('wikipedia', {}).get('status') == 'found' and
                             e.sources.get('wikipedia', {}).get('extract'))
    
    # Erfolgsrate loggen
    success_count = sum(1 for e in entities if e.wikidata_id)
    logging.info(f"[link_entities] Wikidata-Verknüpfung: {success_count}/{len(entities)} Entitäten erfolgreich verknüpft")
    
    return entities
