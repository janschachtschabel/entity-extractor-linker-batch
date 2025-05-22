"""
link.py

Direkte Schnittstelle zu den Batch-Services für die Entitätsverknüpfung.
"""

from entityextractor.services.batch_wikipedia_service import batch_get_wikipedia_info
from entityextractor.services.batch_wikidata_service import batch_get_wikidata_ids, batch_get_wikidata_entities
from entityextractor.services.batch_dbpedia_service import batch_get_dbpedia_info
import logging

def link_entities(entities, config=None):
    """
    Verknüpft Entitäten mit Wissensquellen (Wikipedia, Wikidata, DBpedia).
    
    Diese Funktion ersetzt die vorherige Abstraktionsebene und greift direkt
    auf die Batch-Services zu, ohne eine zusätzliche Abstraktionsschicht zu verwenden.
    
    Args:
        entities: Liste von Entitäten, die verknüpft werden sollen
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von verknüpften Entitäten mit Informationen aus Wissensquellen
    """
    if not entities:
        return []
    
    # Mapping: Name/URL → Entität (UUID)
    entity_names = {entity.get("name"): entity for entity in entities if entity.get("name")}
    uuid_map = {entity.get("name"): entity.get("id") for entity in entities if entity.get("name")}
    url_map = {entity.get("wikipedia_url"): entity.get("id") for entity in entities if entity.get("wikipedia_url")}
    logging.debug(f"[link_entities] Mapping Name→UUID: {uuid_map}")
    logging.debug(f"[link_entities] Mapping Wikipedia-URL→UUID: {url_map}")
    
    # Wikipedia-Informationen für alle Entitäten in einem Batch holen
    lang = config.get("LANGUAGE", "de") if config else "de"
    wiki_results = batch_get_wikipedia_info(list(entity_names.keys()), lang=lang, config=config)
    
    # Wiki-Ergebnisse in die ursprünglichen Entitäten integrieren
    for name, wiki_info in wiki_results.items():
        entity = entity_names.get(name)
        if not entity:
            logging.warning(f"[link_entities] Keine Entität für Wikipedia-Ergebnis: {name}")
            continue
        if wiki_info and wiki_info.get("status") == "found":
            entity["wikipedia_url"] = wiki_info.get("url", "")
            entity["wikipedia_extract"] = wiki_info.get("extract", "")
            entity["wikipedia_categories"] = wiki_info.get("categories", [])
            entity["wikipedia_title"] = wiki_info.get("title", name)
            logging.debug(f"[link_entities] Wikipedia-Match: Name='{name}', UUID='{entity.get('id')}', URL='{entity.get('wikipedia_url')}'")
        else:
            # Entferne ggf. vorher gesetzte Wikipedia-Infos, wenn keine valide Seite gefunden wurde
            removed_keys = []
            for key in ["wikipedia_url", "wikipedia_extract", "wikipedia_categories", "wikipedia_title"]:
                if key in entity:
                    removed_keys.append(key)
                    del entity[key]
            logging.warning(f"Wikipedia-Linking: Für '{name}' (UUID: {entity.get('id')}) konnte keine valide Wikipedia-Seite gefunden werden. Entferne Felder: {removed_keys}")
    
    # Wikidata-Informationen für Entitäten mit Wikipedia-URLs holen
    if config and config.get("USE_WIKIDATA", True):
        # Mapping Wikipedia-URL zu Entität (UUID)
        entities_for_wikidata = {e.get("wikipedia_url"): e for e in entities if e.get("wikipedia_url")}
        wikidata_results = batch_get_wikidata_ids(entities_for_wikidata, config=config)
        logging.info(f"[link_entities] Wikidata-Batch: {len(entities_for_wikidata)} Entitäten, {sum(1 for v in wikidata_results.values() if v and v.get('status') == 'found')} Treffer.")

        # IDs sammeln für Detailabfrage
        id_map = {}
        for url, wikidata_info in wikidata_results.items():
            if wikidata_info and wikidata_info.get('status') == 'found':
                wid = wikidata_info.get('id', wikidata_info.get('wikidata_id', ''))
                if wid:
                    id_map[url] = wid
        # Detaildaten holen (falls IDs vorhanden)
        wikidata_details = {}
        if id_map:
            wikidata_details = batch_get_wikidata_entities(id_map, config=config)
        # Wikidata-Ergebnisse in die Entitäten integrieren
        for url, entity in entities_for_wikidata.items():
            wikidata_info = wikidata_results.get(url)
            wid = wikidata_info.get('id', wikidata_info.get('wikidata_id', '')) if wikidata_info else ''
            detail = wikidata_details.get(url) if url in wikidata_details else None
            if wikidata_info and wikidata_info.get('status') == 'found':
                if detail and detail.get("status") == "found":
                    # Detaillierte Wikidata-Informationen aus dem Batch-Service
                    entity["wikidata"] = {
                        "id": wid,
                        "url": f"https://www.wikidata.org/wiki/{wid}",
                        "label": detail.get("label", ""),
                        "description": detail.get("description", ""),
                        "types": detail.get("types", []),  # Jetzt mit menschenlesbaren Labels
                        "part_of": detail.get("part_of", []),  # Jetzt mit menschenlesbaren Labels
                        "has_parts": detail.get("has_parts", []),  # Jetzt mit menschenlesbaren Labels
                        "image_url": detail.get("image_url", ""),
                        # Zusätzliche Informationen, falls verfügbar
                        "occupations": detail.get("occupations", []),
                        "citizenships": detail.get("citizenships", []),
                        "birth_place": detail.get("birth_place", ""),
                        "death_place": detail.get("death_place", ""),
                        "member_of": detail.get("member_of", [])
                    }
                    logging.debug(f"[link_entities] Wikidata-Match mit erweiterten Infos: URL='{url}', UUID='{entity.get('id')}', Wikidata-ID='{entity['wikidata']['id']}' (Details übernommen)")
                else:
                    # Fallback: Nur ID, keine Details
                    entity["wikidata"] = {
                        "id": wid,
                        "url": f"https://www.wikidata.org/wiki/{wid}",
                        "id": wikidata_info.get("wikidata_id", wikidata_info.get("id", "")),
                        "url": f"https://www.wikidata.org/wiki/{wikidata_info.get('wikidata_id', wikidata_info.get('id', ''))}" if wikidata_info.get("wikidata_id", wikidata_info.get("id", "")) else "",
                        "label": wikidata_info.get("label") or wikidata_info.get("labels", {}).get("de") or wikidata_info.get("labels", {}).get("en") or "",
                        "description": wikidata_info.get("description") or wikidata_info.get("descriptions", {}).get("de") or wikidata_info.get("descriptions", {}).get("en") or "",
                        "types": wikidata_info.get("types", []),
                        "part_of": wikidata_info.get("part_of", []),
                        "has_parts": wikidata_info.get("has_parts", []),
                        "image_url": wikidata_info.get("image_url", ""),
                        "aliases": wikidata_info.get("aliases", {}).get("de", []) + wikidata_info.get("aliases", {}).get("en", []),
                        "subclasses": wikidata_info.get("subclasses", [])
                    }
                    logging.debug(f"[link_entities] Wikidata-Match: URL='{url}', UUID='{entity.get('id')}', Wikidata-ID='{entity['wikidata']['id']}' (nur Basisdaten)")

            else:
                # Leere Wikidata-Felder setzen, damit sichtbar ist, dass Lookup stattfand
                entity["wikidata"] = {
                    "id": "",
                    "url": "",
                    "label": "",
                    "description": "",
                    "types": [],
                    "part_of": [],
                    "has_parts": [],
                    "image_url": ""
                }
                if wikidata_info:
                    logging.warning(f"Keine Wikidata-Ergebnisse für {entity.get('name')} (URL: {url}): {wikidata_info.get('error', 'Unbekannter Fehler')}")
                else:
                    logging.warning(f"Keine Wikidata-Ergebnisse für {entity.get('name')} (URL: {url}): Keine Antwort vom Service")
        if not entities_for_wikidata:
            logging.info("[link_entities] Keine Entitäten für Wikidata-Lookup verfügbar (keine Wikipedia-URLs).")

    # DBpedia-Informationen für Entitäten mit Wikipedia-URLs holen
    if config and config.get("USE_DBPEDIA", True):
        entity_url_dict = {e.get("name"): e.get("wikipedia_url") for e in entities if e.get("wikipedia_url")}
        dbpedia_results = batch_get_dbpedia_info(entity_url_dict, config=config)
        logging.info(f"[link_entities] DBpedia-Batch: {len(entity_url_dict)} Entitäten, {sum(1 for v in dbpedia_results.values() if v and v.get('status') == 'found')} Treffer.")
        
        # DBpedia-Ergebnisse in die Entitäten integrieren
        for entity_name, dbpedia_info in dbpedia_results.items():
            entity = next((e for e in entities if e.get("name") == entity_name), None)
            if entity:
                if dbpedia_info and dbpedia_info.get("status") == "found":
                    entity["dbpedia"] = {
                        "label": dbpedia_info.get("label", ""),
                        "subjects": dbpedia_info.get("subjects", []),
                        "abstract": dbpedia_info.get("abstract", ""),
                        "types": dbpedia_info.get("types", []),
                        "categories": dbpedia_info.get("categories", []),
                        "part_of": dbpedia_info.get("part_of", []),
                        "has_parts": dbpedia_info.get("has_parts", []),
                        "resource_uri": dbpedia_info.get("resource_uri", ""),
                        "gnd_id": dbpedia_info.get("gnd_id", ""),
                        "homepage": dbpedia_info.get("homepage", ""),
                        "thumbnail": dbpedia_info.get("thumbnail", ""),
                        "coordinates": dbpedia_info.get("coordinates", None)
                    }
                    logging.debug(f"[link_entities] DBpedia-Match: Name='{entity_name}', UUID='{entity.get('id')}', ResourceURI='{entity['dbpedia']['resource_uri']}'")
                else:
                    # Leere DBpedia-Felder setzen
                    entity["dbpedia"] = {
                        "subjects": [],
                        "abstract": "",
                        "part_of": [],
                        "has_parts": [],
                        "resource_uri": "",
                        "gnd_id": ""
                    }
                    if dbpedia_info:
                        logging.warning(f"Keine DBpedia-Ergebnisse für {entity.get('name')} (UUID: {entity.get('id')}): {dbpedia_info.get('error', 'Unbekannter Fehler')}")
                    else:
                        logging.warning(f"Keine DBpedia-Ergebnisse für {entity.get('name')} (UUID: {entity.get('id')}): Keine Antwort vom Service")
                    
                    # Zusätzliche Debug-Informationen für DBpedia-Fehler
                    if entity.get("wikipedia_url"):
                        logging.debug(f"  - Wikipedia-URL: {entity.get('wikipedia_url')}")
                    if dbpedia_info and 'error' in dbpedia_info:
                        logging.debug(f"  - Fehlerdetails: {dbpedia_info['error']}")
        
        if not entity_url_dict:
            logging.info("[link_entities] Keine Entitäten für DBpedia-Lookup verfügbar (keine Wikipedia-URLs).")
    
    return entities
