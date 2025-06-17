#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modul zum Abrufen von DBpedia-Daten.

Dieses Modul stellt Funktionen bereit, um Daten von DBpedia abzurufen,
einschließlich SPARQL-Abfragen und Lookup-API-Anfragen.
"""

from typing import Dict, List, Any, Optional, Set, Tuple
import urllib.parse
import aiohttp
import asyncio
import json
from loguru import logger

from entityextractor.models.data_models import DBpediaData


async def fetch_dbpedia_sparql(
    session: aiohttp.ClientSession, 
    labels: List[str], 
    endpoint: str = "http://dbpedia.org/sparql",
    language: str = "en",
    timeout: int = 30,
    direct_uris: List[str] = None
) -> Dict[str, DBpediaData]:
    """
    Führt eine SPARQL-Abfrage für mehrere Labels oder URIs aus.
    
    Args:
        session: aiohttp.ClientSession für HTTP-Anfragen
        labels: Liste von Labels für die Abfrage
        endpoint: SPARQL-Endpoint-URL
        language: Sprache für die Abfrage (en, de, etc.)
        timeout: Timeout für die Anfrage in Sekunden
        direct_uris: Optional, Liste von direkten DBpedia-URIs für die Abfrage
        
    Returns:
        Dictionary mit Label als Schlüssel und DBpediaData als Wert
    """
    if not labels and not direct_uris:
        return {}
    
    results = {}
    
    # Erstelle die VALUES-Klausel für die Labels
    values_clause = ""
    values_clause_part = ""
    if labels and any(labels):
        values_clause = " ".join([f'\"{label}\"@{language}' for label in labels if label])
        values_clause_part = f"""
        {{  
          VALUES ?searchLabel {{ {values_clause} }}
          ?uri rdfs:label ?searchLabel .
        }}
        """
        logger.debug(f"SPARQL VALUES-Klausel für Labels: {values_clause}")
    
    # Wenn direkte URIs vorhanden sind, füge sie zur Abfrage hinzu
    uri_clause = ""
    uri_clause_part = ""
    if direct_uris and any(direct_uris):
        uri_values = " ".join([f'<{uri}>' for uri in direct_uris if uri])
        uri_clause = f"VALUES ?uri {{ {uri_values} }}"
        uri_clause_part = f"""
        UNION
        {{
          {uri_clause}
        }}
        """
        logger.debug(f"SPARQL URI-Klausel: {uri_clause}")
        
    # Wenn weder Labels noch URIs vorhanden sind, gib leeres Dictionary zurück
    if not values_clause and not uri_clause:
        logger.warning("Weder Labels noch URIs für SPARQL-Abfrage vorhanden")
        return {}
    
    # SPARQL-Abfrage mit VALUES-Klausel für Batch-Verarbeitung
    # Verbesserte Abfrage mit weniger strikten Bedingungen und zusätzlichen Prädikaten
    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbp: <http://dbpedia.org/property/>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX schema: <http://schema.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT DISTINCT ?uri ?label ?abstract ?type ?category ?hasPart ?partOf ?lat ?long ?wikiPage ?image
    WHERE {{
      # Entweder über Labels oder direkt über URIs abfragen
      {{
        # Entweder über Labels...
        {values_clause_part}
        
        # ...oder direkt über URIs
        {uri_clause_part}
      }}
      
      # Immer englisches Label holen
      ?uri rdfs:label ?label .
      FILTER(LANG(?label) = "en")
      
      # Immer englisches Abstract holen mit mehreren möglichen Prädikaten
      OPTIONAL {{
        ?uri dbo:abstract ?abstract .
        FILTER(LANG(?abstract) = "en")
      }}
      OPTIONAL {{
        ?uri schema:description ?abstract .
        FILTER(LANG(?abstract) = "en" && !BOUND(?abstract))
      }}
      OPTIONAL {{
        ?uri dct:description ?abstract .
        FILTER(LANG(?abstract) = "en" && !BOUND(?abstract))
      }}
      OPTIONAL {{
        ?uri rdfs:comment ?abstract .
        FILTER(LANG(?abstract) = "en" && !BOUND(?abstract))
      }}
      
      # Typen mit verschiedenen Prädikaten
      OPTIONAL {{ ?uri a ?type . }}
      OPTIONAL {{ ?uri rdf:type ?type . }}
      
      # Kategorien
      OPTIONAL {{ ?uri dct:subject ?category . }}
      OPTIONAL {{ ?uri skos:subject ?category . }}
      
      # hasPart-Beziehungen
      OPTIONAL {{ ?uri dbo:wikiPageWikiLink ?hasPart . }}
      OPTIONAL {{ ?uri dbo:isPartOf ?hasPart . }}
      
      # partOf-Beziehungen
      OPTIONAL {{ ?partOf dbo:wikiPageWikiLink ?uri . }}
      OPTIONAL {{ ?partOf dbo:isPartOf ?uri . }}
      
      # Geo-Koordinaten
      OPTIONAL {{ ?uri geo:lat ?lat . }}
      OPTIONAL {{ ?uri geo:long ?long . }}
      
      # Wikipedia-Seite
      OPTIONAL {{ ?uri foaf:isPrimaryTopicOf ?wikiPage . }}
      OPTIONAL {{ ?uri dbo:wikiPageExternalLink ?wikiPage . }}
      
      # Bild
      OPTIONAL {{ ?uri foaf:depiction ?image . }}
      OPTIONAL {{ ?uri dbo:thumbnail ?image . }}
    }}
    LIMIT 100
    """
    
    # Debug-Ausgabe der vollständigen Abfrage
    logger.debug(f"SPARQL-Abfrage für Labels {labels}:\n{query}")
    
    try:
        # Überprüfe, ob gültige Labels oder URIs vorhanden sind
        has_valid_labels = labels and any(label for label in labels if label)
        has_valid_uris = direct_uris and any(uri for uri in direct_uris if uri)
        
        if not has_valid_labels and not has_valid_uris:
            logger.warning("Keine gültigen Labels oder URIs für SPARQL-Abfrage vorhanden")
            return {}
            
        # Bereite die SPARQL-Abfrage vor
        headers = {
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'EntityExtractor/1.0'
        }
        params = {
            'query': query,
            'format': 'json',
            'timeout': '30000'  # 30 Sekunden Timeout für komplexe Abfragen
        }
        
        logger.debug(f"Führe SPARQL-Abfrage für {len(labels)} Labels aus: {endpoint}")
        async with session.get(endpoint, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status != 200:
                logger.warning(f"SPARQL-Anfrage fehlgeschlagen mit Status {response.status}: {await response.text()}")
                return {}
                
            # Parse die JSON-Antwort
            try:
                response_text = await response.text()
                logger.debug(f"SPARQL response received, length: {len(response_text)} characters")
                
                if not response_text or response_text.strip() == '':
                    logger.warning(f"SPARQL-Anfrage lieferte leere Antwort")
                    return {}
                    
                response_json = json.loads(response_text)
                # Guard: DBpedia sometimes returns plain 'null' → json.loads -> None
                if not isinstance(response_json, dict):
                    logger.warning("SPARQL returned non-object JSON (possibly 'null'). Skipping.")
                    return {}
                
                # Extrahiere die Ergebnisse
                if not response_json:
                    logger.warning(f"SPARQL-Anfrage lieferte keine gültige JSON-Antwort")
                    return {}
                    
                results_dict = response_json.get('results')
                if not results_dict:
                    logger.warning(f"SPARQL-Antwort enthält kein 'results'-Feld")
                    return {}
                    
                bindings = results_dict.get('bindings', [])
                
                if not bindings:
                    logger.warning(f"SPARQL-Anfrage lieferte keine Ergebnisse für Labels {labels}")
                    return {}
            except json.JSONDecodeError as e:
                logger.error(f"Fehler beim Parsen der SPARQL-JSON-Antwort: {str(e)}")
                logger.debug(f"Erste 200 Zeichen der Antwort: {response_text[:200]}")
                return {}
            
            # Verarbeite die Ergebnisse
            if 'results' in response_json and 'bindings' in response_json['results']:
                bindings = response_json['results']['bindings']
                
                # Debug-Ausgabe der Anzahl der gefundenen Bindings
                logger.debug(f"SPARQL-Abfrage lieferte {len(bindings)} Ergebnisse")
                
                # Gruppiere die Ergebnisse nach URI
                uri_groups = {}
                for binding in bindings:
                    if 'uri' in binding:
                        uri = binding['uri']['value']
                        if uri not in uri_groups:
                            uri_groups[uri] = []
                        uri_groups[uri].append(binding)
                
                # Debug-Ausgabe der gefundenen URIs
                logger.debug(f"Gefundene URIs: {list(uri_groups.keys())}")
                
                # Verarbeite jede URI-Gruppe
                for uri, uri_bindings in uri_groups.items():
                    # Extrahiere den Label-Text aus der URI
                    uri_label = uri.split('/')[-1].replace('_', ' ')
                    
                    # Finde das passende Label aus der Anfrage mit verbesserter Matching-Logik
                    matching_label = None
                    
                    if not response_text or response_text.strip() == '':
                        logger.warning(f"SPARQL-Anfrage lieferte leere Antwort")
                        return {}
                        
                    response_json = json.loads(response_text)
                    # Guard: DBpedia sometimes returns plain 'null' → json.loads -> None
                    if not isinstance(response_json, dict):
                        logger.warning("SPARQL returned non-object JSON (possibly 'null'). Skipping.")
                        return {}
                    
                    # Extrahiere die Ergebnisse
                    if not response_json:
                        logger.warning(f"SPARQL-Anfrage lieferte keine gültige JSON-Antwort")
                        return {}
                        
                    results_dict = response_json.get('results')
                    if not results_dict:
                        logger.warning(f"SPARQL-Antwort enthält kein 'results'-Feld")
                        return {}
                        
                    bindings = results_dict.get('bindings', [])
                    
                    if not bindings:
                        logger.warning(f"SPARQL-Anfrage lieferte keine Ergebnisse für Labels {labels}")
                        return {}
                
                # Verarbeite die Ergebnisse
                if 'results' in response_json and 'bindings' in response_json['results']:
                    bindings = response_json['results']['bindings']
                    
                    # Debug-Ausgabe der Anzahl der gefundenen Bindings
                    logger.debug(f"SPARQL-Abfrage lieferte {len(bindings)} Ergebnisse")
                    
                    # Gruppiere die Ergebnisse nach URI
                    uri_groups = {}
                    for binding in bindings:
                        if 'uri' in binding:
                            uri = binding['uri']['value']
                            if uri not in uri_groups:
                                uri_groups[uri] = []
                            uri_groups[uri].append(binding)
                    
                    # Debug-Ausgabe der gefundenen URIs
                    logger.debug(f"Gefundene URIs: {list(uri_groups.keys())}")
                    
                    # Verarbeite jede URI-Gruppe
                    for uri, uri_bindings in uri_groups.items():
                        # Extrahiere den Label-Text aus der URI
                        uri_label = uri.split('/')[-1].replace('_', ' ')
                        
                        # Finde das passende Label aus der Anfrage mit verbesserter Matching-Logik
                        matching_label = None
                        best_match_score = 0
                        best_match_label = None
                        
                        # Normalisierungsfunktion für besseres Matching
                        def normalize_text(text):
                            return text.lower().replace('-', ' ').replace('_', ' ').strip()
                        
                        # Hilfsfunktion für sicheres Auslesen aus einem Binding
                        def safe_binding_value(binding: dict, key: str):
                            obj = binding.get(key)
                            if isinstance(obj, dict):
                                return obj.get('value')
                            return None
                        
                        normalized_uri_label = normalize_text(uri_label)
                        
                        # 1. Versuche exakten Match nach Normalisierung
                        for label in labels:
                            if normalize_text(label) == normalized_uri_label:
                                matching_label = label
                                break
                        
                        # 2. Wenn kein exakter Match, versuche Teilstring-Matching
                        if not matching_label:
                            for label in labels:
                                normalized_label = normalize_text(label)
                                # Prüfe, ob eines ein Teilstring des anderen ist
                                if normalized_label in normalized_uri_label or normalized_uri_label in normalized_label:
                                    # Berechne Ähnlichkeitsscore basierend auf Länge
                                    score = min(len(normalized_label), len(normalized_uri_label)) / max(len(normalized_label), len(normalized_uri_label))
                                    if score > best_match_score:
                                        best_match_score = score
                                        best_match_label = label
                        
                        # 3. Wenn kein Teilstring-Match, versuche Wort-für-Wort-Matching
                        if not matching_label and best_match_score < 0.7:  # Nur wenn kein guter Teilstring-Match gefunden wurde
                            for label in labels:
                                normalized_label = normalize_text(label)
                                # Zähle übereinstimmende Wörter
                                uri_words = set(normalized_uri_label.split())
                                label_words = set(normalized_label.split())
                                common_words = uri_words.intersection(label_words)
                                
                                if common_words:  # Wenn mindestens ein gemeinsames Wort vorhanden ist
                                    score = len(common_words) / max(len(uri_words), len(label_words))
                                    if score > best_match_score:
                                        best_match_score = score
                                        best_match_label = label
                        
                        # 4. Wenn immer noch kein Match, aber ein guter Kandidat gefunden wurde
                        if not matching_label and best_match_score >= 0.5:  # Mindestens 50% Übereinstimmung
                            matching_label = best_match_label
                            logger.debug(f"Fuzzy-Match gefunden: '{best_match_label}' -> '{uri_label}' mit Score {best_match_score:.2f}")
                        
                        # 5. Fallback: Verwende das erste Binding mit einem Label
                        if not matching_label:
                            for binding in uri_bindings:
                                if 'label' in binding:
                                    label_value = safe_binding_value(binding, 'label')
                                    if label_value in labels:
                                        matching_label = label_value
                                        break
                        
                        if not matching_label:
                            # Wenn immer noch kein Match, überspringe diese URI
                            continue
                        
                        # Erstelle ein DBpediaData-Objekt für diese URI
                        dbpedia_data = DBpediaData(
                            uri=uri,
                            status="linked"
                        )
                        
                        # Label
                        label_values = [safe_binding_value(b, 'label') for b in uri_bindings if safe_binding_value(b, 'label')]
                        if label_values:
                            dbpedia_data.label = {language: label_values[0]}
                            logger.debug(f"Label für URI {uri}: {label_values[0]}")
                        
                        # Abstract
                        abstract_values = [safe_binding_value(b, 'abstract') for b in uri_bindings if safe_binding_value(b, 'abstract')]
                        if abstract_values:
                            dbpedia_data.abstract = {language: abstract_values[0]}
                            logger.debug(f"Abstract für URI {uri} gefunden: {len(abstract_values[0])} Zeichen")
                        else:
                            logger.debug(f"Kein Abstract für URI {uri} gefunden")
                        
                        # Typen
                        types = set()
                        for binding in uri_bindings:
                            type_uri = safe_binding_value(binding, 'type')
                            if type_uri and 'http://dbpedia.org/ontology/' in type_uri:
                                types.add(type_uri)
                        dbpedia_data.types = list(types)
                        
                        # Kategorien
                        categories = set()
                        for binding in uri_bindings:
                            cat_val = safe_binding_value(binding, 'category')
                            if cat_val:
                                categories.add(cat_val)
                        dbpedia_data.categories = list(categories)
                        
                        # hasPart-Beziehungen
                        has_parts = set()
                        for binding in uri_bindings:
                            hp_val = safe_binding_value(binding, 'hasPart')
                            if hp_val:
                                has_parts.add(hp_val)
                        dbpedia_data.has_part = list(has_parts)
                        
                        # partOf-Beziehungen
                        part_ofs = set()
                        for binding in uri_bindings:
                            po_val = safe_binding_value(binding, 'partOf')
                            if po_val:
                                part_ofs.add(po_val)
                        dbpedia_data.part_of = list(part_ofs)
                        
                        # Geo-Koordinaten
                        geo_data = {}
                        for binding in uri_bindings:
                            lat_val = safe_binding_value(binding, 'lat')
                            long_val = safe_binding_value(binding, 'long')
                            if lat_val and long_val:
                                try:
                                    lat = float(lat_val)
                                    long = float(long_val)
                                    geo_data = {'lat': lat, 'long': long}
                                except (ValueError, TypeError):
                                    pass
                        dbpedia_data.geo = geo_data
                        
                        # Wikipedia-Seite
                        wiki_urls = set()
                        for binding in uri_bindings:
                            wp_val = safe_binding_value(binding, 'wikiPage')
                            if wp_val:
                                wiki_urls.add(wp_val)
                        dbpedia_data.wiki_url = next(iter(wiki_urls), None)
                        
                        # Bild
                        image_urls = set()
                        for binding in uri_bindings:
                            img_val = safe_binding_value(binding, 'image')
                            if img_val:
                                image_urls.add(img_val)
                        dbpedia_data.image_url = next(iter(image_urls), None)
                        
                        # Prüfe, ob die Mindestanforderungen erfüllt sind
                        # Prüfe Mindestanforderungen (URI, engl. Label, engl. Abstract)
                        if not dbpedia_data.label:
                            dbpedia_data.status = "not_found"
                            dbpedia_data.error = "missing_required_data"
                            dbpedia_data.message = "Missing label data"
                            logger.warning(f"Entität mit URI {uri} hat kein Label und wird als 'not_found' markiert.")
                        elif not dbpedia_data.abstract or not dbpedia_data.abstract.get(language):
                            dbpedia_data.status = "not_found"
                            dbpedia_data.error = "missing_required_data"
                            dbpedia_data.message = "Missing English abstract"
                            logger.info(f"Entität mit URI {uri} hat kein englisches Abstract. Markiere als 'not_found'.")
                        # Wenn kein Abstract vorhanden ist, setzen wir ein leeres Abstract
                        # aber markieren die Entität trotzdem als verknüpft
                        dbpedia_data.abstract = {language: ""}
                        logger.info(f"SPARQL-Ergebnis für '{matching_label}' (URI: {uri}) hat kein Abstract, verwende Fallback (z. B. deutsches Abstract) und verknüpfe trotzdem.")
                        
                        # Versuche, ein Abstract in einer anderen Sprache zu finden, falls verfügbar
                        try:
                            alt_abstract_query = f"""
                            PREFIX dbo: <http://dbpedia.org/ontology/>
                            SELECT ?abstract WHERE {{
                              <{uri}> dbo:abstract ?abstract .
                              FILTER(LANG(?abstract) IN ("en", "de"))
                            }} LIMIT 1
                            """
                            
                            alt_params = {
                                'query': alt_abstract_query,
                                'format': 'json'
                            }
                            
                            async with session.get(endpoint, params=alt_params, headers=headers, timeout=timeout/2) as alt_response:
                                if alt_response.status == 200:
                                    alt_data = await alt_response.json()
                                    if 'results' in alt_data and 'bindings' in alt_data['results'] and alt_data['results']['bindings']:
                                        alt_abstract = alt_data['results']['bindings'][0]['abstract']['value']
                                        alt_lang = alt_data['results']['bindings'][0]['abstract']['xml:lang']
                                        dbpedia_data.abstract[alt_lang] = alt_abstract
                                        logger.info(f"Alternatives Abstract in Sprache '{alt_lang}' für URI {uri} gefunden.")
                        except Exception as e:
                            logger.debug(f"Fehler beim Abrufen eines alternativen Abstracts: {e}")
                    
                    # Füge das Ergebnis zum Dictionary hinzu
                    results[matching_label] = dbpedia_data
    
    except Exception as e:
        logger.error(f"Fehler bei der SPARQL-Abfrage: {str(e)}")
    
    return results


async def fetch_dbpedia_abstract_sparql(
    session: aiohttp.ClientSession, 
    uri: str, 
    language: str = "en", 
    timeout: int = 20
) -> Optional[Dict[str, str]]:
    """Holt das Abstract für eine URI über SPARQL. Immer auf Englisch für strikte Kriterien."""
    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX schema: <http://schema.org/>
    PREFIX dct: <http://purl.org/dc/terms/>
    
    SELECT ?abstract
    WHERE {{
      {{
        <{uri}> dbo:abstract ?abstract .
        FILTER(LANG(?abstract) = "{language}")
      }}
      UNION
      {{
        <{uri}> rdfs:comment ?abstract .
        FILTER(LANG(?abstract) = "{language}")
      }}
      UNION
      {{
        <{uri}> schema:description ?abstract .
        FILTER(LANG(?abstract) = "{language}")
      }}
      UNION
      {{
        <{uri}> dct:description ?abstract .
        FILTER(LANG(?abstract) = "{language}")
      }}
    }}
    LIMIT 1
    """
    
    headers = {
        'Accept': 'application/sparql-results+json',
        'User-Agent': 'EntityExtractor/1.0'
    }
    params = {
        'query': query,
        'format': 'json'
    }
    
    try:
        async with session.get("http://dbpedia.org/sparql", params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status != 200:
                logger.warning(f"SPARQL-Anfrage fehlgeschlagen mit Status {response.status}: {await response.text()}")
                return None
            
            try:
                response_text = await response.text()
                logger.debug(f"SPARQL response received, length: {len(response_text)} characters")
                
                if not response_text or response_text.strip() == '':
                    logger.warning(f"SPARQL-Anfrage lieferte leere Antwort")
                    return None
                    
                response_json = json.loads(response_text)
                # Guard: DBpedia sometimes returns plain 'null' → json.loads -> None
                if not isinstance(response_json, dict):
                    logger.warning("SPARQL returned non-object JSON (possibly 'null'). Skipping.")
                    return {}
                
                # Extrahiere die Ergebnisse
                if not response_json:
                    logger.warning(f"SPARQL-Anfrage lieferte keine gültige JSON-Antwort")
                    return None
                    
                results_dict = response_json.get('results')
                if not results_dict:
                    logger.warning(f"SPARQL-Antwort enthält kein 'results'-Feld")
                    return None
                    
                bindings = results_dict.get('bindings', [])
                
                if not bindings:
                    logger.warning(f"SPARQL-Anfrage lieferte keine Ergebnisse für URI {uri}")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"Fehler beim Parsen der SPARQL-JSON-Antwort: {str(e)}")
                logger.debug(f"Erste 200 Zeichen der Antwort: {response_text[:200]}")
                return None
            
            # Verarbeite die Ergebnisse
            if 'results' in response_json and 'bindings' in response_json['results']:
                bindings = response_json['results']['bindings']
                
                # Debug-Ausgabe der Anzahl der gefundenen Bindings
                logger.debug(f"SPARQL-Abfrage lieferte {len(bindings)} Ergebnisse")
                
                if bindings:
                    abstract = bindings[0]['abstract']['value']
                    return {language: abstract}
                else:
                    return None
    
    except aiohttp.ClientError as e:
        logger.error(f"HTTP-Fehler bei der SPARQL-Abfrage: {str(e)}")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout bei der SPARQL-Abfrage nach {timeout} Sekunden")
        return None
    except Exception as e:
        logger.error(f"Fehler bei der SPARQL-Abfrage: {str(e)}")
        return None


async def fetch_dbpedia_lookup(
    session: aiohttp.ClientSession, 
    query: str, 
    language: str = "en", 
    timeout: int = 30, 
    max_results: int = 10,
    force_english: bool = True
) -> Optional[DBpediaData]:
    """
    Ruft Daten über die DBpedia Lookup API ab.
    
    Args:
        session: aiohttp.ClientSession für HTTP-Anfragen
        query: Suchanfrage
        language: Sprache für die Anfrage (en, de, etc.)
        timeout: Timeout für die Anfrage in Sekunden
        max_results: Maximale Anzahl von Ergebnissen
        force_english: Erzwingt englische Ergebnisse, unabhängig von der Sprache
        
    Returns:
        DBpediaData-Objekt oder None, wenn keine Daten gefunden wurden
    """
    # Wenn force_english True ist, verwende immer Englisch
    if force_english:
        language = "en"
        
    # Erstelle die URL mit Parametern
    params = {
        'query': query,
        'format': 'json',
        'maxResults': max_results,
        'lang': language
    }
    
    url = "https://lookup.dbpedia.org/api/search"
    
    logger.debug(f"DBpedia Lookup API Anfrage: {url} mit Parametern {params}")
    
    try:
        # Rufe die Lookup API auf
        logger.debug(f"Rufe DBpedia Lookup API für '{query}' auf")
        async with session.get(
            url,
            params=params,
            headers={'Accept': 'application/json'},
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as response:
            if response.status != 200:
                logger.warning(f"Lookup-Anfrage fehlgeschlagen mit Status {response.status}: {await response.text()}")
                return None
                
            # Parse die JSON-Antwort
            try:
                response_text = await response.text()
                logger.debug(f"Lookup API response received, length: {len(response_text)} characters")
                
                if not response_text or response_text.strip() == '':
                    logger.warning(f"Lookup-Anfrage lieferte leere Antwort")
                    return None
                    
                data = json.loads(response_text)
                # Guard: Lookup API may return 'null'
                if not isinstance(data, dict):
                    logger.warning("Lookup API returned non-object JSON (possibly 'null').")
                    return None
                
                if not data:
                    logger.warning(f"Lookup-Anfrage lieferte keine gültige JSON-Antwort")
                    return None
                
                # Extrahiere die Ergebnisse
                results = data.get('results')
                
                if not results:
                    logger.warning(f"Lookup-Anfrage lieferte keine Ergebnisse für '{query}'")
                    return None
                    
                # Wähle das beste Ergebnis aus den Resultaten
                best_result = None
                
                for result in results:
                    # Überprüfe, ob result ein gültiges Dictionary ist
                    if not isinstance(result, dict):
                        logger.warning(f"Ungültiges Lookup-Ergebnis für '{query}': {result}")
                        continue
                    
                    # Nimm das erste gültige Ergebnis
                    if not best_result and result.get("uri"):
                        best_result = result
                        break
                
                if not best_result:
                    logger.warning(f"Kein gültiges Ergebnis in Lookup-Antwort für '{query}'")
                    return None
                
                # Extrahiere die relevanten Daten aus dem besten Ergebnis
                # Stelle sicher, dass wir immer ein englisches Label und Abstract haben
                # Sichere Extraktion der Klassen und Kategorien mit Fehlerbehandlung
                types = []
                categories = []
                
                # Sichere Extraktion der Klassen
                if "classes" in best_result and isinstance(best_result["classes"], list):
                    for cls in best_result["classes"]:
                        if isinstance(cls, dict) and "uri" in cls:
                            types.append(cls["uri"])
                
                # Sichere Extraktion der Kategorien
                if "categories" in best_result and isinstance(best_result["categories"], list):
                    for cat in best_result["categories"]:
                        if isinstance(cat, dict) and "uri" in cat:
                            categories.append(cat["uri"])
                
                dbpedia_data = DBpediaData(
                    uri=best_result.get("uri"),
                    label={"en": best_result.get("label") or ""},  # Immer als englisches Label speichern, leerer String als Fallback
                    abstract={"en": best_result.get("comment") or ""},  # Immer als englisches Abstract speichern, leerer String als Fallback
                    types=types,
                    categories=categories,
                    has_part={},
                    part_of={},
                    geo=None,
                    wiki_url="",
                    image_url=""
                )

                # Zusätzliches Logging für Debugging
                logger.debug(f"DBpedia Lookup API Ergebnis für '{query}': URI={dbpedia_data.uri}, Label={dbpedia_data.label}, Abstract vorhanden: {'en' in dbpedia_data.abstract and bool(dbpedia_data.abstract.get('en'))}")
                
                # Wenn kein Abstract vorhanden ist, versuche es mit einem zusätzlichen SPARQL-Aufruf
                if not dbpedia_data.abstract or not dbpedia_data.abstract.get("en") or not dbpedia_data.abstract["en"]:
                    try:
                        # Versuche, das Abstract über SPARQL zu holen (immer auf Englisch)
                        sparql_result = await fetch_dbpedia_abstract_sparql(session, dbpedia_data.uri, "en")
                        if sparql_result:
                            dbpedia_data.abstract = sparql_result
                            logger.debug(f"Abstract über SPARQL nachgeladen: {'en' in dbpedia_data.abstract and bool(dbpedia_data.abstract.get('en'))}")
                    except Exception as e:
                        logger.warning(f"Fehler beim Abrufen des Abstracts über SPARQL: {e}")

                # Prüfe, ob die Mindestanforderungen erfüllt sind (URI, englisches Label, englisches Abstract)
                if not dbpedia_data.uri:
                    dbpedia_data.status = "not_found"
                    dbpedia_data.error = "missing_required_data"
                    dbpedia_data.message = "Missing URI"
                elif not dbpedia_data.label or 'en' not in dbpedia_data.label:
                    dbpedia_data.status = "not_found"
                    dbpedia_data.error = "missing_required_data"
                    dbpedia_data.message = "Missing English label data"
                elif not dbpedia_data.abstract or 'en' not in dbpedia_data.abstract or not dbpedia_data.abstract.get('en'):
                    dbpedia_data.status = "not_found"
                    dbpedia_data.error = "missing_required_data"
                    dbpedia_data.message = "Missing English abstract data"
                    logger.info(f"Lookup-Ergebnis für '{query}' hat kein englisches Abstract. Markiere als 'not_found'.")
                else:
                    dbpedia_data.status = "linked"
                
                return dbpedia_data
                
            except json.JSONDecodeError as e:
                logger.error(f"Fehler beim Parsen der JSON-Antwort von Lookup API: {str(e)}")
                return None
    
    except aiohttp.ClientError:
        logger.exception("HTTP error during Lookup-Anfrage")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout bei der Lookup-Anfrage nach {timeout} Sekunden")
        return None
    except Exception as e:
        logger.exception("Uncaught exception during Lookup API processing")
        return None
