#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Asynchrone API-Interaktionsmodule für den Wikipedia-Service.

Dieses Modul stellt asynchrone Funktionen für den Abruf von Daten über die Wikipedia-API bereit.
Es ist eine asynchrone Version der ursprünglichen fetchers.py, die mit den verbesserten
SourceData-Objekten kompatibel ist.
"""

import logging
import json
import aiohttp
import asyncio
from typing import List, Dict, Any, Optional, Set, Tuple

from entityextractor.utils.api_request_utils import create_standard_headers
from entityextractor.utils.rate_limiter import RateLimiter
from entityextractor.utils.logging_utils import get_service_logger

# Logger konfigurieren
logger = get_service_logger(__name__, 'wikipedia')

import urllib.parse

async def async_fetch_multilang_wikipedia_data(urls: List[str], user_agent: str, config: Dict[str, Any]) -> Dict[str, Dict[str, Dict]]:
    """
    For each Wikipedia URL, fetch both German and English labels and metadata.
    Returns a dict of {original_url: { 'de': {...}, 'en': {...} }}
    """
    target_langs = ('de', 'en')
    # Step 1: Parse URLs to get language and title
    def parse_wiki_url(url):
        p = urllib.parse.urlparse(url)
        lang = p.netloc.split('.')[0]
        title = urllib.parse.unquote(p.path.split('/wiki/')[1]).replace('_', ' ')
        return lang, title

    # Step 2: For each URL, fetch langlinks for both languages
    async def fetch_langlink_titles(session, lang, title, target_langs):
        URL = f'https://{lang}.wikipedia.org/w/api.php'
        params = {
            'action': 'query',
            'format': 'json',
            'titles': title,
            'prop': 'langlinks',
            'lllimit': 'max',
            'llprop': 'url',
            'lllang': '|'.join([l for l in target_langs if l != lang])
        }
        try:
            async with session.get(URL, params=params, headers=create_standard_headers(user_agent), timeout=config.get('TIMEOUT_THIRD_PARTY', 15)) as resp:
                resp.raise_for_status()
                data = await resp.json()
                pages = data.get('query', {}).get('pages', {})
                result = {l: None for l in target_langs}
                for page in pages.values():
                    result[lang] = title
                    for link in page.get('langlinks', []):
                        ll_lang = link.get('lang')
                        ll_title = link.get('*') or link.get('title')
                        if ll_lang in target_langs:
                            result[ll_lang] = ll_title
                return result
        except Exception as e:
            logger.error(f"Error fetching langlinks for {lang}:{title}: {e}")
            return {l: None for l in target_langs}

    # Step 3: Batch-fetch metadata for a list of titles in a given language
    async def fetch_pages_data(session, titles, lang):
        if not titles:
            return {}
        URL = f'https://{lang}.wikipedia.org/w/api.php'
        params = {
            'action': 'query',
            'format': 'json',
            'titles': '|'.join(titles),
            'prop': 'categories|pageimages|extracts|info',
            'cllimit': 'max',
            'piprop': 'thumbnail',
            'pithumbsize': 500,
            'exintro': True,
            'explaintext': True,
            'inprop': 'url'
        }
        try:
            async with session.get(URL, params=params, headers=create_standard_headers(user_agent), timeout=config.get('TIMEOUT_THIRD_PARTY', 15)) as resp:
                resp.raise_for_status()
                data = await resp.json()
                pages = data.get('query', {}).get('pages', {})
                result = {}
                for page in pages.values():
                    if 'missing' in page:
                        continue
                    label = page['title']
                    result[label] = {
                        'label': label,
                        'description': page.get('extract'),
                        'url': page.get('fullurl'),
                        'categories': [c['title'] for c in page.get('categories', [])] if 'categories' in page else [],
                        'image_url': page.get('thumbnail', {}).get('source') if 'thumbnail' in page else None
                    }
                return result
        except Exception as e:
            logger.error(f"Error fetching page data for {lang} titles {titles}: {e}")
            return {}

    results = {}
    async with aiohttp.ClientSession() as session:
        # Step 1: For each URL, get language and title
        url_lang_title = {url: parse_wiki_url(url) for url in urls}
        # Step 2: For each URL, fetch langlinks (to resolve both de/en titles)
        interlangs = {}
        for url, (lang, title) in url_lang_title.items():
            interlangs[url] = await fetch_langlink_titles(session, lang, title, target_langs)
        # Step 3: Group titles by language for batch fetch
        lang_to_titles = {l: set() for l in target_langs}
        for titles in interlangs.values():
            for lang, title in titles.items():
                if title:
                    lang_to_titles[lang].add(title)
        # Step 4: Batch-fetch metadata for each language
        lang_to_data = {}
        for lang, titles in lang_to_titles.items():
            lang_to_data[lang] = await fetch_pages_data(session, list(titles), lang)
        # Step 5: Combine results per original URL
        for url, titles in interlangs.items():
            results[url] = {}
            for lang in target_langs:
                title = titles.get(lang)
                results[url][lang] = lang_to_data.get(lang, {}).get(title)
        logger.info(f"Multilang Wikipedia fetch complete for {len(urls)} URLs.")
    return results

# Asynchroner Rate-Limiter für API-Anfragen
_async_rate_limiter = RateLimiter(3, 1.0)  # 3 Anfragen pro Sekunde

@_async_rate_limiter
async def async_limited_get(url, headers=None, params=None, timeout=None, config=None):
    """
    Führt einen asynchronen GET-Request mit Rate-Limiting durch.
    
    Args:
        url: URL für den Request
        headers: Optional, HTTP-Header
        params: Optional, URL-Parameter
        timeout: Optional, Timeout in Sekunden
        config: Optional, Konfiguration
        
    Returns:
        JSON-Antwort oder None bei Fehler
    """
    if not config:
        config = {}
        
    if not headers:
        headers = create_standard_headers()
        
    if not timeout:
        timeout = config.get("TIMEOUT_THIRD_PARTY", 15)
    
    # Detailliertes Logging der Anfrageparameter für Diagnose
    logger.debug(f"Wikipedia API: URL={url}, Params={params}")
    
    try:
        # API-Anfrage mit aiohttp und erweiterter Fehlerbehandlung
        async with aiohttp.ClientSession() as session:
            try:
                logger.debug(f"HTTP-Request: URL={url}, Timeout={timeout}s")
                response = await session.get(url, params=params, headers=headers, timeout=timeout)
                logger.debug(f"API Status: {response.status}")
                
                if response.status == 200:
                    try:
                        json_data = await response.json()
                        logger.debug(f"JSON-Antwort: {list(json_data.keys()) if json_data else 'Keine'} Keys")
                        return json_data
                    except Exception as json_error:
                        logger.error(f"Fehler beim Parsen der JSON-Antwort: {str(json_error)}")
                        text = await response.text()
                        logger.debug(f"Rohantwort: {text[:100]}..." if len(text) > 100 else text)
                        return None
                else:
                    logger.error(f"HTTP-Fehler {response.status} bei {url}")
                    try:
                        error_text = await response.text()
                        logger.error(f"Fehlerantwort: {error_text[:200]}..." if len(error_text) > 200 else error_text)
                    except:
                        pass
                    return None
            except aiohttp.ClientError as e:
                logger.error(f"aiohttp ClientError bei Wikipedia API-Anfrage: {str(e)}")
                raise
            except asyncio.TimeoutError:
                logger.error(f"Timeout bei Wikipedia API-Anfrage nach {timeout} Sekunden")
                raise
    except Exception as e:
        logger.error(f"Unbehandelte Exception bei API-Anfrage an {url}: {str(e)}", exc_info=True)
        return None

async def async_fetch_wikipedia_data(titles: List[str], api_url: str, user_agent: str, config: Dict[str, Any]) -> Dict[str, Dict]:
    """
    Ruft Daten für mehrere Titel von der Wikipedia-API ab.
    
    Args:
        titles: Liste von Titeln
        api_url: URL der Wikipedia-API
        user_agent: User-Agent für die API-Anfrage
        config: Konfiguration
        
    Returns:
        Dictionary mit Titel als Schlüssel und Wikipedia-Daten als Wert
    """
    logger.debug(f"async_fetch_wikipedia_data: {len(titles)} Titel, API: {api_url}")
    if not titles:
        logger.warning("No titles provided for Wikipedia data fetching")
        return {}
        
    logger.info(f"Wikipedia-Abfrage gestartet: {len(titles)} Titel, API: {api_url}")
    
    # Ergebnis-Dictionary
    results = {}
    
    # 1. API-Parameter für die Hauptanfrage - direkt aus der funktionierenden Backup-Implementierung übernommen
    # WICHTIGE KORREKTUR: Reihenfolge und exakte Werte der Parameter entsprechend der funktionierenden    # API-Parameter aufbauen
    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'extracts|categories|pageprops|langlinks|info|links|pageimages|images|coordinates',  # Koordinaten hinzugefügt
        'redirects': 'true',  # Wichtig: String statt Integer
        'exintro': 1,
        'explaintext': 1,
        'exsectionformat': 'plain',  # Zusätzlicher Parameter aus Backup-Version
        'exchars': 1200,       # Maximale Anzahl von Zeichen für das Extract (WICHTIG: Nicht mit exsentences kombinieren!)
        'exlimit': 'max',      # Maximale Anzahl von Extracts pro Anfrage
        'cllimit': 'max',      # Maximale Anzahl von Kategorien
        'clshow': '!hidden',   # Versteckte Kategorien ausschließen
        'ppprop': 'wikibase_item',
        'inprop': 'url',
        'llprop': 'url|langname',  # Spezifische Properties für Sprachlinks
        'lllang': 'en|de',     # Nur Links für Deutsch und Englisch
        'lllimit': 500,
        'plnamespace': 0,      # Nur Artikellinks (kein Talk, User, etc.)
        'pllimit': 500,        # Limit für interne Links (erhöht von 200)
        'imlimit': 10,         # Limit für Bilder
        'pilimit': 1,          # Ein Thumbnail pro Seite
        'pithumbsize': 300,    # Thumbnail-Größe in Pixeln
        'colimit': 'max',      # Maximale Anzahl von Koordinaten
        'coprop': 'type|name|dim|country|region',  # Zusätzliche Details für Koordinaten
        'coprimary': 'all',    # Sowohl primäre als auch sekundäre Koordinaten
        'maxlag': 5
        # ACHTUNG: 'exsentences' und 'exchars' können nicht zusammen verwendet werden! (API-Fehler)
    }
    
    # Logging der API-Parameter zur Diagnose
    logger.debug(f"Wikipedia API-Parameter: {params}")
    
    # Teile die Titel in Chunks auf, um die API-Limits einzuhalten
    max_titles_per_request = config.get('WIKIPEDIA_MAX_TITLES_PER_REQUEST', 50)
    
    # Hilfsfunktion für den englischen Fallback
    async def fetch_english_title_from_enwiki(de_title: str) -> Optional[str]:
        """
        Fetch the canonical English Wikipedia title for a given (German) title.
        """
        en_api_url = "https://en.wikipedia.org/w/api.php"
        en_params = {
            'action': 'query',
            'format': 'json',
            'titles': de_title,
            'redirects': 'true',
            'prop': 'info',
            'inprop': 'url',
        }
        headers = create_standard_headers(user_agent)
        logger.debug(f"Fallback: Querying enwiki for German title '{de_title}'")
        try:
            json_response = await async_limited_get(
                en_api_url,
                headers=headers,
                params=en_params,
                timeout=config.get('TIMEOUT_THIRD_PARTY', 15),
                config=config
            )
            if json_response and 'query' in json_response and 'pages' in json_response['query']:
                pages = json_response['query']['pages']
                for page_id, page_data in pages.items():
                    if page_id != '-1' and 'title' in page_data:
                        logger.debug(f"Fallback: Found English title '{page_data['title']}' for German title '{de_title}'")
                        return page_data['title']
        except Exception as e:
            logger.error(f"Fallback: Error fetching English title from enwiki for '{de_title}': {str(e)}")
        logger.debug(f"Fallback: No English title found for German title '{de_title}'")
        return None
    
    # Verarbeite Titel in kleineren Chunks
    for i in range(0, len(titles), max_titles_per_request):
        chunk_titles = titles[i:i + max_titles_per_request]
        
        logger.info(f"Wikipedia-Abfrage: {len(chunk_titles)} von {len(titles)} Titeln")
        
        # Füge die Titel zur Anfrage hinzu
        current_params = params.copy()
        current_params['titles'] = '|'.join(chunk_titles)
        
        try:
            # 2. API-Anfrage an Wikipedia stellen
            headers = create_standard_headers(user_agent)
            logger.info(f"Sende Wikipedia API-Anfrage mit {len(chunk_titles)} Titeln: {chunk_titles[:3]}...")
            json_response = await async_limited_get(
                api_url,
                headers=headers,
                params=current_params,
                timeout=config.get('TIMEOUT_THIRD_PARTY', 15),
                config=config
            )
            
            # Debug-Ausgabe der API-Antwort
            if json_response:
                pages = json_response.get('query', {}).get('pages', {})
                extract_count = sum(1 for p in pages.values() if 'extract' in p and p['extract']) if pages else 0
                pages_count = len(pages) if pages else 0
                logger.info(f"Wikipedia API: {pages_count} Seiten erhalten, {extract_count} mit Extract ({len(str(json_response))} Bytes)")
                    
                    # Bei fehlenden Extracts, Log die ersten 3 Seiten für Debug
                # Nur bei Debug-Level und wenn keine Extracts gefunden wurden
                if extract_count == 0 and len(pages) > 0 and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Keine Extracts in {len(pages)} Seiten gefunden")
            
            if json_response:
                # Fehler im API-Response prüfen
                if 'error' in json_response:
                    logger.warning(f"Wikipedia API-Fehler: {json_response['error']}")
                    continue
                
                # 3. Ergebnisse parsen
                if 'query' in json_response and 'pages' in json_response['query']:
                    pages = json_response['query']['pages']
                    for page_id, page_data in pages.items():
                        # Überspringe fehlende Seiten
                        if page_id == '-1' or 'missing' in page_data:
                            original_title = page_data.get('title')
                            if original_title:
                                results[original_title] = {
                                    'title': original_title,
                                    'status': 'not_found'
                                }
                            continue
                        
                        # Extrahiere die benötigten Daten
                        title = page_data.get('title', '')
                        extract = page_data.get('extract', '')
                        url = page_data.get('canonicalurl') or page_data.get('fullurl', '')
                        wikidata_id = page_data.get('pageprops', {}).get('wikibase_item', '')
                        
                        # Kategorien extrahieren
                        categories = []
                        if 'categories' in page_data:
                            for category in page_data['categories']:
                                category_title = category.get('title', '')
                                if category_title:
                                    # "Category:" oder "Kategorie:" vom Titel entfernen
                                    if ':' in category_title:
                                        # In verschiedenen Sprachen haben die Kategorien unterschiedliche Präfixe
                                        prefixes = ['Category:', 'Kategorie:', 'Catégorie:']
                                        for prefix in prefixes:
                                            if category_title.startswith(prefix):
                                                category_title = category_title[len(prefix):]
                                                break
                                    # Nur hinzufügen, wenn nicht leer
                                    if category_title.strip():
                                        categories.append(category_title)
                            
                            # Debug-Logging für Kategorien
                            if categories:
                                logger.debug(f"Gefundene Kategorien für '{title}': {len(categories)}")
                                logger.debug(f"Beispiel-Kategorien: {categories[:3]}")
                        
                        # Extrahiere Bildinformationen, falls vorhanden
                        thumbnail = None
                        if 'thumbnail' in page_data:
                            thumbnail = page_data['thumbnail'].get('source')
                        # Alternativ aus pageimages extrahieren
                        elif 'pageimages' in page_data and page_data['pageimages']:
                            thumbnail = page_data['pageimages'].get('source')
                        # Alternativ auch direkt aus den Pageimages extrahieren
                        elif 'original' in page_data:
                            thumbnail = page_data['original'].get('source')
                        
                        # Debug-Log für Extracts
                        has_extract = 'extract' in page_data and page_data['extract']
                        if not has_extract:
                            logger.warning(f"Wikipedia API: Seite '{page_data.get('title', 'Unbekannt')}' (ID {page_id}) hat KEIN Extract")
                            if 'extract' in page_data:
                                logger.warning(f"Extract vorhanden aber leer: '{page_data['extract']}'")
                            logger.warning(f"Verfügbare Felder: {list(page_data.keys())}")
                        
                        # Links extrahieren
                        links = []
                        if 'links' in page_data:
                            # Nur Artikel-Links (Namespace 0) behalten
                            for link in page_data['links']:
                                link_title = link.get('title', '')
                                # Namespace prüfen (0 = Artikelnamespace, andere sind Spezialseiten)
                                link_ns = link.get('ns', 0)
                                if link_title and link_title not in links and link_ns == 0:
                                    links.append(link_title)
                            
                            # Debug-Logging für Links
                            if links:
                                logger.debug(f"Gefundene interne Links für '{title}': {len(links)}")
                                logger.debug(f"Beispiel-Links: {links[:5]}")
                        
                        # Sprachlinks extrahieren
                        langlinks = {}
                        if 'langlinks' in page_data:
                            for langlink in page_data['langlinks']:
                                lang = langlink.get('lang', '')
                                if lang:
                                    langlinks[lang] = langlink.get('*', '')
                                    
                        # Koordinaten extrahieren (basierend auf dem funktionierenden Beispielcode)
                        coordinates = None
                        if 'coordinates' in page_data:
                            logger.info(f"Koordinaten-Feld gefunden für '{title}': {len(page_data['coordinates'])} Koordinaten")
                            # Nehme die erste Koordinate (meist die Hauptkoordinate)
                            if page_data['coordinates'] and len(page_data['coordinates']) > 0:
                                coords = page_data['coordinates'][0]
                                coordinates = {
                                    'lat': coords.get('lat'),
                                    'lon': coords.get('lon'),
                                    'type': coords.get('type'),
                                    'name': coords.get('name', ''),
                                    'dim': coords.get('dim', ''),
                                    'country': coords.get('country', ''),
                                    'region': coords.get('region', '')
                                }
                                logger.info(f"Koordinaten für '{title}' extrahiert: {coordinates['lat']}, {coordinates['lon']}")
                                
                                # Alle Koordinaten speichern (für vollständige Daten)
                                all_coordinates = []
                                for coord in page_data['coordinates']:
                                    all_coordinates.append({
                                        'lat': coord.get('lat'),
                                        'lon': coord.get('lon'),
                                        'type': coord.get('type'),
                                        'name': coord.get('name', ''),
                                        'dim': coord.get('dim', ''),
                                        'country': coord.get('country', ''),
                                        'region': coord.get('region', '')
                                    })
                                    
                                # Füge alle Koordinaten zur Haupt-Koordinate hinzu
                            status = 'partial'
                        else:
                            logger.info(f"Extract für '{title}' gefunden, Status wird auf 'found' gesetzt")
                            status = 'found'
                        # Extract English and German titles robustly
                        de_title = title
                        en_title = langlinks.get('en') if langlinks else None
                        de_langlink = langlinks.get('de') if langlinks else None
                        
                        # If no English label, try to fetch from enwiki
                        if not en_title or not en_title.strip():
                            logger.debug(f"No English langlink for '{de_title}'. Attempting secondary fetch to en.wikipedia.org...")
                            en_title_fallback = await fetch_english_title_from_enwiki(de_title)
                            if en_title_fallback:
                                logger.info(f"Secondary fetch succeeded: English title for '{de_title}' is '{en_title_fallback}'")
                                en_title = en_title_fallback
                            else:
                                logger.warning(f"Secondary fetch failed: No English title found for '{de_title}'")
                        # Wikidata fallback for English label
                        if (not en_title or not en_title.strip()) and wikidata_id:
                            logger.debug(f"No English label for '{de_title}' after langlinks and enwiki. Trying Wikidata fallback for '{wikidata_id}'...")
                            try:
                                import aiohttp
                                wikidata_url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
                                async with aiohttp.ClientSession() as session:
                                    async with session.get(wikidata_url, timeout=10) as resp:
                                        if resp.status == 200:
                                            data = await resp.json()
                                            entities = data.get('entities', {})
                                            entity_data = entities.get(wikidata_id, {})
                                            # Try English sitelink (Wikipedia page title)
                                            sitelinks = entity_data.get('sitelinks', {})
                                            enwiki = sitelinks.get('enwiki', {})
                                            if enwiki and enwiki.get('title'):
                                                en_title = enwiki['title']
                                                logger.info(f"Wikidata sitelink fallback: English Wikipedia title for '{de_title}' is '{en_title}'")
                                            else:
                                                # Try English label
                                                labels_wd = entity_data.get('labels', {})
                                                en_label = labels_wd.get('en', {}).get('value')
                                                if en_label:
                                                    en_title = en_label
                                                    logger.info(f"Wikidata label fallback: English label for '{de_title}' is '{en_label}'")
                                                else:
                                                    logger.warning(f"Wikidata fallback failed: No English sitelink or label for '{de_title}' ({wikidata_id})")
                                        else:
                                            logger.warning(f"Wikidata fallback HTTP error {resp.status} for '{wikidata_id}'")
                            except Exception as e:
                                logger.error(f"Wikidata fallback error for '{wikidata_id}': {str(e)}")
                        # Prepare labels dict
                        labels = {'de': de_title, 'en': en_title}
                        logger.info(f"Label extraction for '{de_title}': de='{de_title}', en='{en_title}' (wikidata_id={wikidata_id})")
                        # Compose result entry
                        result_entry = {
                            "extract": extract,
                            "title": title,
                            "labels": labels,
                            "url": url,
                            "language": "de",  # Sprache fest auf Deutsch setzen
                            "categories": categories,
                            "wikidata_id": wikidata_id,
                            "wikidata_url": f"https://www.wikidata.org/wiki/{wikidata_id}" if wikidata_id else None,
                            "thumbnail": thumbnail if thumbnail else "",
                            "internal_links": links[:50] if links else [],  # Interne Links mit Unterstrich für Kompatibilität
                            "externalLinks": [],  # Wird derzeit nicht befüllt
                            "langLinks": langlinks if langlinks else {},
                            "coordinates": coordinates  # Koordinaten hinzugefügt
                        }
                        
                        # Speichere das Ergebnis im Dictionary
                        results[title] = result_entry
                        
                        # Debug-Log für das Ergebnis
                        logger.info(f"Wikipedia-Ergebnis für '{title}': English label='{en_title}', Status={'found' if extract else 'partial'}, Extract vorhanden={bool(extract)}")
        except Exception as e:
            logger.error(f"Fehler bei der Wikipedia-API-Anfrage: {str(e)}")
            # Setze fehlgeschlagene Anfragen auf Fehler-Status
            for title in chunk_titles:
                if title not in results:
                    results[title] = {
                        'title': title,
                        'status': 'error',
                        'error': str(e)
                    }
    
    return results

async def async_fetch_image_info(image_titles: List[str], api_url: str, user_agent: str, config: Dict[str, Any]) -> Dict[str, Dict]:
    """
    Ruft Informationen zu Bildern von der Wikipedia-API ab.
    
    Args:
        image_titles: Liste von Bildtiteln
        api_url: URL der Wikipedia-API
        user_agent: User-Agent für die API-Anfrage
        config: Konfiguration
        
    Returns:
        Dictionary mit Bildtitel als Schlüssel und Bildinformationen als Wert
    """
    if not image_titles:
        return {}
    
    # Ergebnis-Dictionary
    results = {}
    
    # API-Parameter für die Bildanfrage
    params = {
        'action': 'query',
        'prop': 'imageinfo',
        'iiprop': 'url|size|mime',
        'format': 'json'
    }
    
    # Teile die Titel in Chunks auf, um die API-Limits einzuhalten
    max_titles_per_request = config.get('WIKIPEDIA_MAX_TITLES_PER_REQUEST', 50)
    
    # Verarbeite Titel in kleineren Chunks
    for i in range(0, len(image_titles), max_titles_per_request):
        chunk_titles = image_titles[i:i + max_titles_per_request]
        
        # Füge die Titel zur Anfrage hinzu
        current_params = params.copy()
        current_params['titles'] = '|'.join(chunk_titles)
        
        try:
            # Erstelle die vollständige URL für Debugging-Zwecke
            # Füge die URL-Parameter manuell hinzu, um die genaue Reihenfolge zu sehen
            full_url = api_url + "?"
            param_parts = []
            for key, value in current_params.items():
                param_parts.append(f"{key}={value}")
            debug_url = full_url + "&".join(param_parts)
            
            # Zeige die vollständige URL für Debugging
            logger.warning(f"API-URL mit Parametern: {debug_url[:200]}...")
            
            # API-Anfrage an Wikipedia stellen
            headers = create_standard_headers(user_agent)
            logger.warning(f"Sende Wikipedia-API-Anfrage für {len(chunk_titles)} Titel mit Headers: {headers}")
            logger.warning(f"Anfragetitel: {chunk_titles}")
            
            # Konstruiere die vollständige URL für Debug-Zwecke
            url_params = '&'.join([f"{k}={v}" for k, v in current_params.items()])
            full_debug_url = f"{api_url}?{url_params}"
            logger.warning(f"Vollständige Debug-URL: {full_debug_url[:200]}..." if len(full_debug_url) > 200 else full_debug_url)
            
            # Führe die eigentliche Anfrage aus
            try:
                json_response = await async_limited_get(
                    api_url,
                    headers=headers,
                    params=current_params,
                    timeout=config.get('TIMEOUT_THIRD_PARTY', 15),
                    config=config
                )
                logger.warning(f"API-Antwort Status: {json_response is not None}")
            except Exception as e:
                logger.error(f"Fehler bei Wikipedia API-Anfrage: {str(e)}", exc_info=True)
                json_response = None
            
            # Logge die Antwort-Struktur (nur bei Debug-Level)
            if json_response:
                if logger.isEnabledFor(logging.DEBUG):
                    # Zeige die ersten paar Seiten-IDs
                    if 'query' in json_response and 'pages' in json_response['query']:
                        page_count = len(json_response['query']['pages'])
                        page_ids = list(json_response['query']['pages'].keys())[:3]
                        logger.debug(f"API-Antwort: {page_count} Seiten, IDs: {page_ids}")
                        
                        # Für die erste Seite, prüfe auf Extract
                        if page_ids:
                            first_page = json_response['query']['pages'][page_ids[0]]
                            if 'extract' in first_page:
                                extract_len = len(first_page['extract'])
                                logger.debug(f"Erste Seite: Extract {extract_len} Zeichen")
            elif logger.isEnabledFor(logging.DEBUG):
                logger.debug("Keine API-Antwort erhalten")
            
            if json_response:
                # Fehler im API-Response prüfen
                if 'error' in json_response:
                    logger.error(f"Wikipedia API-Fehler: {json_response['error']}")
                    continue
                
                # Ergebnisse parsen
                if 'query' in json_response and 'pages' in json_response['query']:
                    pages = json_response['query']['pages']
                    for page_id, page_data in pages.items():
                        # Überspringe fehlende Seiten
                        if page_id == '-1' or 'missing' in page_data:
                            continue
                        
                        title = page_data.get('title', '')
                        if 'imageinfo' in page_data and len(page_data['imageinfo']) > 0:
                            image_info = page_data['imageinfo'][0]
                            results[title] = {
                                'url': image_info.get('url', ''),
                                'width': image_info.get('width', 0),
                                'height': image_info.get('height', 0),
                                'mime': image_info.get('mime', ''),
                                'title': title
                            }
        except Exception as e:
            logger.error(f"Fehler bei der Wikipedia-Bildanfrage: {str(e)}")
    
    return results
