"""
entity_linker/wikipedia.py

Enthält Funktionen für die Verknüpfung von Entitäten mit Wikipedia-Daten.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from uuid import UUID

from entityextractor.models.base import LanguageCode
from entityextractor.models.entity import Entity
# Verwende den neuen WikipediaService statt des alten BatchWikipediaService
from entityextractor.services.wikipedia.service import WikipediaService
from entityextractor.utils.source_utils import safe_get, safe_source_access

async def link_with_wikipedia(entities: List[Entity], config: Optional[Dict[str, Any]] = None) -> List[Entity]:
    """
    Verknüpft eine Liste von Entitäten mit Wikipedia-Daten.
    
    Args:
        entities: Liste von Entity-Objekten
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Angereicherte Liste von Entity-Objekten
    """
    if not entities:
        return []
    
    logging.info(f"Starte Wikipedia-Anreicherung für {len(entities)} Entitäten")
    
    # Verwende den neuen WikipediaService
    wiki_service = WikipediaService(config)
    
    # Debug-Logging aktivieren
    logging.info(f"[link_with_wikipedia] Verwendung des neuen WikipediaService")
    logging.debug(f"[link_with_wikipedia] Debug-Modus aktiviert: {config.get('DEBUG_WIKIPEDIA', False)}")
    logging.debug(f"[link_with_wikipedia] Entitäten: {[e.name for e in entities]}")
    
    # Einfacherer direkter Ansatz ohne EntityProcessingContext
    # Direkt die Wikipedia-API aufrufen und die Daten verarbeiten
    enriched_entities = []
    
    # In Batches von 10 verarbeiten
    batch_size = 10
    for i in range(0, len(entities), batch_size):
        batch = entities[i:i+batch_size]
        entity_names = [entity.name for entity in batch]
        
        logging.info(f"Verarbeite Batch mit {len(batch)} Entitäten: {entity_names}")
        
        # Wikipedia-API direkt aufrufen
        from entityextractor.services.wikipedia.async_fetchers import async_fetch_wikipedia_data
        api_results = await async_fetch_wikipedia_data(
            entity_names, 
            wiki_service.api_url, 
            wiki_service.user_agent, 
            wiki_service.config
        )
        
        # Für jede Entität im Batch
        for entity in batch:
            entity_name = entity.name
            logging.info(f"Verarbeite Ergebnis für '{entity_name}'")
            
            # API-Ergebnis extrahieren
            result = api_results.get(entity_name, {})
            
            if result:
                logging.info(f"Wikipedia-Daten für '{entity_name}' gefunden")
                
                # Status festlegen - WICHTIG: Nur als "found" markieren, wenn sowohl URL als auch Extract vorhanden sind
                if result.get("url") and result.get("extract") and len(result.get("extract", "").strip()) > 50:
                    status = "found"
                    logging.debug(f"Wikipedia-Status für '{entity_name}': found (mit URL und ausreichendem Extract)")
                else:
                    status = "not_found"
                    reason = ""
                    if not result.get("url"):
                        reason = "keine URL"
                    elif not result.get("extract"):
                        reason = "kein Extract"
                    else:
                        reason = f"Extract zu kurz ({len(result.get('extract', '').strip())}) Zeichen"
                    logging.debug(f"Wikipedia-Status für '{entity_name}': not_found ({reason})")
                
                # Wikipedia-Daten erstellen
                wiki_data = {
                    "label": entity_name,
                    "url": result.get("url", ""),
                    "extract": result.get("extract", ""),
                    "status": status,
                    "wikidata_id": result.get("wikidata_id", "")
                }
                
                # Weitere Felder hinzufügen, falls vorhanden
                for field in ["thumbnail", "categories", "langlinks", "internal_links", "coordinates"]:
                    if field in result:
                        wiki_data[field] = result[field]
                
                # Daten zur Entity hinzufügen
                from entityextractor.models.base import SourceData
                
                # Erstelle ein SourceData-Objekt mit korrekt gesetzten Feldern
                wiki_source = SourceData(id="wikipedia")
                wiki_source.status = status  # "found" oder "partial"
                wiki_source.url = result.get("url", "")
                
                # Daten direkt in die SourceData setzen
                if "extract" in result and result["extract"]:
                    wiki_source["extract"] = result["extract"]
                if "wikidata_id" in result and result["wikidata_id"]:
                    wiki_source["wikidata_id"] = result["wikidata_id"]
                if "title" in result:
                    wiki_source["label"] = result["title"]
                else:
                    wiki_source["label"] = entity_name
                    
                # Weitere Datenfelder
                for field in ["thumbnail", "categories", "langlinks", "internal_links", "pageid", "coordinates"]:
                    if field in result and result[field]:
                        wiki_source[field] = result[field]
                
                # Quelle zur Entity hinzufügen
                entity.sources["wikipedia"] = wiki_source
                
                # Wikidata-ID übernehmen
                if "wikidata_id" in result and result["wikidata_id"]:
                    entity.wikidata_id = result["wikidata_id"]
                    logging.info(f"Wikidata-ID {result['wikidata_id']} für '{entity_name}' gesetzt")
                
                # Weitere Metadaten übernehmen
                if "extract" in result and result["extract"]:
                    entity.metadata["description"] = entity.metadata.get("description") or result["extract"]
                if "url" in result and result["url"]:
                    entity.wikipedia_url = result["url"]  # Legacy-Feld
                    entity.metadata["wikipedia_url"] = result["url"]
                if "thumbnail" in result and result["thumbnail"]:
                    entity.metadata["image_url"] = entity.metadata.get("image_url") or result["thumbnail"]
                
                logging.info(f"Wikipedia-Daten für '{entity_name}' zur Entity hinzugefügt")
            else:
                logging.warning(f"Keine Wikipedia-Daten für '{entity_name}' gefunden")
            
            # Entität zu den angereicherten Entitäten hinzufügen
            enriched_entities.append(entity)
    
    logging.info(f"Wikipedia-Anreicherung abgeschlossen für {len(enriched_entities)} Entitäten")
    
    # Überprüfe und KORRIGIERE inkonsistente Status direkt hier
    # Die Formatierung in der formatters.py sollte eigentlich korrekt sein, aber wir müssen sicherstellen
    found_with_extract = 0
    found_without_extract = 0
    partial_entities = 0
    other_status = 0
    
    for entity in enriched_entities:
        if entity.has_source('wikipedia'):
            source = entity.sources.get('wikipedia', {})
            current_status = source.get('status', 'unknown')
            has_extract = bool(source.get('extract', ''))
            
            # Zähle Status-Typen
            if current_status == 'found' and has_extract:
                found_with_extract += 1
            elif current_status == 'found' and not has_extract:
                found_without_extract += 1
                # KORRIGIERE den Status hier, wenn er inkonsistent ist
                logging.warning(f"[link_with_wikipedia] Inkonsistenz bei '{entity.name}': Status 'found', aber kein Extract")
                
                # Dank der erweiterten SourceData-Klasse können wir den Status direkt aktualisieren
                source['status'] = 'not_found'
                logging.warning(f"[link_with_wikipedia] Inkonsistenz bei '{entity.name}': Status 'found', aber kein/kurzer Extract -> Status auf 'not_found' gesetzt")
            # 'partial' Status gibt es nicht mehr
            elif current_status == 'not_found':
                partial_entities += 1
            else:
                other_status += 1
    
    # Detaillierte Erfolgsstatistik nach der Statuskorrektur - nur Entitäten mit Extract zählen als Erfolg
    wikipedia_total = sum(1 for e in enriched_entities if e.has_source('wikipedia'))
    
    # Zähle nach Status-Typ
    found_with_extract = sum(1 for e in enriched_entities if e.has_source('wikipedia') and 
                           e.sources.get('wikipedia', {}).get('status') == 'found' and
                           e.sources.get('wikipedia', {}).get('extract'))
    found_without_extract = sum(1 for e in enriched_entities if e.has_source('wikipedia') and 
                              e.sources.get('wikipedia', {}).get('status') == 'found' and
                              not e.sources.get('wikipedia', {}).get('extract'))
    # partial_count nicht mehr benötigt, da kein 'partial' Status mehr existiert
    partial_count = 0
    not_found_count = sum(1 for e in enriched_entities if e.has_source('wikipedia') and 
                        e.sources.get('wikipedia', {}).get('status') == 'not_found')
    missing_count = sum(1 for e in enriched_entities if e.has_source('wikipedia') and 
                      e.sources.get('wikipedia', {}).get('status') == 'missing')
    
    # Wir überspringen hier die detaillierte Status-Auflistung, da wir sie später in konsolidierter Form ausgeben
    
    # Nur Entitäten mit Status 'found' und vorhandenem Extract zählen als echter Erfolg
    success_count = found_with_extract
    partial_count = sum(1 for e in enriched_entities if e.has_source('wikipedia') and 
                     e.sources.get('wikipedia', {}).get('status') == 'partial')
    missing_count = sum(1 for e in enriched_entities if e.has_source('wikipedia') and 
                     e.sources.get('wikipedia', {}).get('status') == 'missing')
    not_found_count = sum(1 for e in enriched_entities if e.has_source('wikipedia') and 
                      e.sources.get('wikipedia', {}).get('status') == 'not_found')
    
    # Zusammenfassung und Detailliertes Logging
    total_wikipedia = sum(1 for e in enriched_entities if e.has_source('wikipedia'))
    
    # Kritischere Warnung, wenn Entitäten ohne Extract als "found" markiert wurden
    if found_without_extract > 0:
        logging.warning(f"[link_with_wikipedia] KRITISCH: {found_without_extract} Entitäten wurden mit Status 'found' markiert, obwohl kein Extract vorhanden ist!")
        logging.warning(f"[link_with_wikipedia] Diese Entitäten gelten NICHT als erfolgreich verknüpft und werden von nachfolgenden Verarbeitungsschritten ignoriert.")
    
    # Klarere Erfolgsmeldung - NUR Entitäten mit Extract zählen als Erfolg
    logging.info(f"[link_with_wikipedia] Anreicherung abgeschlossen: {success_count}/{len(entities)} Entitäten ERFOLGREICH verknüpft (mit Status 'found' UND Extract)")
    logging.info(f"[link_with_wikipedia] Verteilung: {total_wikipedia} mit Wikipedia-Quelle, davon {success_count} vollständig")
    if missing_count > 0:
        logging.warning(f"[link_with_wikipedia] {missing_count} Entitäten haben Status 'missing' (existieren nicht in Wikipedia)")
    if not_found_count > 0:
        logging.warning(f"[link_with_wikipedia] {not_found_count} Entitäten haben Status 'not_found' (nicht gefunden)")
    
    # Beispiel-Entitäten für Diagnose loggen
    for i, entity in enumerate(enriched_entities[:3]):
        if entity.has_source('wikipedia'):
            source = entity.sources.get('wikipedia', {})
            status = source.get('status', 'unbekannt')
            has_extract = bool(source.get('extract', ''))
            logging.info(f"[link_with_wikipedia] Beispiel-Entität {i+1}: '{entity.name}', Status={status}, Hat Extract={has_extract}")
            # Überprüfe auf Inkonsistenzen und logge Warnungen
            if status == 'found' and not has_extract:
                logging.warning(f"[link_with_wikipedia] Inkonsistenz bei '{entity.name}': Status 'found', aber kein Extract vorhanden!")
            # 'partial' Status existiert nicht mehr
    
    # Wikidata-IDs aus Wikipedia-Quellen extrahieren und setzen, aber nur für Entitäten mit gültigen Daten
    for entity in enriched_entities:
        if entity.has_source('wikipedia'):
            source = entity.sources.get('wikipedia')
            status = safe_get(source, 'status')
            
            # Extrakt korrekt aus dem SourceData.data Dictionary holen
            has_extract = False
            if hasattr(source, 'data') and isinstance(source.data, dict) and 'extract' in source.data:
                has_extract = bool(source.data['extract'])
            else:
                # Fallback für direkten Zugriff
                has_extract = bool(safe_get(source, 'extract'))
            
            # Überprüfe auf Inkonsistenzen für Logging
            if status == 'found' and not has_extract:
                logging.warning(f"[link_with_wikipedia] Inkonsistenz bei '{entity.name}': Status 'found', aber kein Extract (überprüft via data-Dict)")
                
            # Wikidata-ID setzen, falls vorhanden und nicht bereits gesetzt
            wikidata_id = safe_get(source, 'wikidata_id')
            if wikidata_id and not entity.wikidata_id:
                entity.wikidata_id = wikidata_id
                logging.info(f"[link_with_wikipedia] Wikidata-ID für '{entity.name}' gesetzt: {wikidata_id}")
                
            # Metadaten aktualisieren, wenn Status 'found' und Extrakt vorhanden
            if status == 'found' and has_extract:
                # Beschreibung aus dem Wikipedia-Extrakt
                extract = safe_get(source, 'extract')
                if extract:
                    entity.metadata['description'] = entity.metadata.get('description') or extract
                
                # URL zur Wikipedia-Seite
                url = safe_get(source, 'url')
                if url:
                    entity.metadata['url'] = entity.metadata.get('url') or url
                    entity.metadata['wikipedia_url'] = url
                
                # Thumbnail-Bild
                thumbnail = safe_get(source, 'thumbnail')
                if thumbnail:
                    # Speichere nur in wikipedia_thumbnail, nicht mehr in image_url (Vermeidung von Duplikaten)
                    entity.metadata['wikipedia_thumbnail'] = thumbnail
                
                # Interne Links
                internal_links = safe_get(source, 'internal_links')
                if internal_links:
                    entity.metadata['internal_links'] = internal_links
                    logging.debug(f"[link_with_wikipedia] Interne Links für '{entity.name}' hinzugefügt: {len(internal_links)} Links")
                
                # Koordinaten (neu)
                coordinates = safe_get(source, 'coordinates')
                if coordinates:
                    # Primäre Koordinaten speichern
                    entity.metadata['coordinates'] = {
                        'lat': safe_get(coordinates, 'lat'),
                        'lon': safe_get(coordinates, 'lon'),
                        'type': safe_get(coordinates, 'type', '')
                    }
                    # Alle zusätzlichen Koordinaten speichern, falls vorhanden
                    all_coords = safe_get(coordinates, 'all_coordinates')
                    if all_coords:
                        entity.metadata['all_coordinates'] = all_coords
                    logging.info(f"[link_with_wikipedia] Koordinaten für '{entity.name}' hinzugefügt: {entity.metadata['coordinates']['lat']}, {entity.metadata['coordinates']['lon']}")

                
                # Detaillierte Bildinformationen
                image_info = safe_get(source, 'image_info')
                if image_info:
                    # Einheitlicher Zugriff mit safe_get unabhängig vom Typ
                    entity.metadata['image_info'] = {
                        'url': safe_get(image_info, 'url', ''),
                        'width': safe_get(image_info, 'width', 0),
                        'height': safe_get(image_info, 'height', 0),
                        'mime': safe_get(image_info, 'mime', ''),
                        'title': safe_get(image_info, 'title', '')
                    }
                    logging.debug(f"[link_with_wikipedia] Bild-Informationen für '{entity.name}' hinzugefügt")
            elif status in ['not_found', 'missing']:
                # Bei nicht gefundenen Entitäten keine Metadaten hinzufügen, nur Logging
                logging.info(f"[link_with_wikipedia] Keine Metadaten für '{entity.name}' mit Status '{status}'")
            elif status == 'found' and not has_extract:
                # Inkonsistente Daten - Status 'found', aber kein Extract
                logging.warning(f"[link_with_wikipedia] Inkonsistenz bei '{entity.name}': Status 'found', aber kein Extract")
            # 'partial' Status wurde komplett entfernt
            # Bei nicht gefundenen Entitäten (status='not_found') können wir trotzdem die URL speichern, falls vorhanden
            elif status == 'not_found':
                url = safe_get(source, 'url')
                if url:
                    entity.metadata['wikipedia_url'] = url
                    logging.info(f"[link_with_wikipedia] Nur URL für '{entity.name}' mit Status 'not_found' gesetzt")
    
    return enriched_entities
