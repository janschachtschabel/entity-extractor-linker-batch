#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Asynchrone SPARQL-Wrapper-Implementierung für DBpedia-Abfragen
Basiert auf SPARQLWrapper mit asyncio-Unterstützung
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
import concurrent.futures
from functools import partial

from SPARQLWrapper import SPARQLWrapper, JSON
from .formatters import process_sparql_results

logger = logging.getLogger(__name__)

class AsyncSPARQLRate:
    """Simple rate limiter for SPARQL queries"""
    def __init__(self, rate_limit=5):
        self.rate_limit = rate_limit  # queries per second
        self.semaphore = asyncio.Semaphore(rate_limit)

    async def __aenter__(self):
        await self.semaphore.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.sleep(1.0 / self.rate_limit)  # rate limiting
        self.semaphore.release()
        return False

# Globaler Rate Limiter
rate_limiter = AsyncSPARQLRate()

def _execute_sparql_query_sync(query: str, endpoint: str, user_agent: str, timeout: int = 30, ssl_verify: bool = False) -> dict:
    """
    Synchrone Ausführung einer SPARQL-Abfrage mit SPARQLWrapper
    
    Args:
        query: Die SPARQL-Abfrage
        endpoint: Der SPARQL-Endpunkt
        user_agent: Der User-Agent für die Anfrage
        timeout: Timeout in Sekunden
        ssl_verify: SSL-Verifizierung aktivieren/deaktivieren
        
    Returns:
        Dict mit den Abfrage-Ergebnissen
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)  # Wichtig! Explizites Setzen des Rückgabeformats
    
    # User-Agent setzen
    sparql.addCustomHttpHeader('User-Agent', user_agent)
    
    # SSL-Verifizierung konfigurieren
    sparql.setHTTPAuth(None)
    if not ssl_verify:
        import ssl
        from urllib.request import Request
        # Das Standard-SSL-Kontext überschreiben, um die SSL-Verifizierung zu deaktivieren
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        sparql.session.verify = False
    
    # Timeout setzen
    sparql.setTimeout(timeout)
    
    try:
        results = sparql.query().convert()
        return results
    except Exception as e:
        logger.error(f"Fehler bei SPARQL-Abfrage an {endpoint}: {str(e)}")
        raise

async def execute_sparql_query(query: str, endpoint: str, user_agent: str, 
                              timeout: int = 30, ssl_verify: bool = False) -> dict:
    """
    Asynchrone Ausführung einer SPARQL-Abfrage mit SPARQLWrapper in einem Threadpool
    
    Args:
        query: Die SPARQL-Abfrage
        endpoint: Der SPARQL-Endpunkt
        user_agent: Der User-Agent für die Anfrage
        timeout: Timeout in Sekunden
        ssl_verify: SSL-Verifizierung aktivieren/deaktivieren
        
    Returns:
        Dict mit den Abfrage-Ergebnissen
    """
    # Rate limiting anwenden
    async with rate_limiter:
        logger.info(f"Führe SPARQL-Abfrage an {endpoint} aus")
        
        # SPARQLWrapper ist nicht asyncio-kompatibel, daher Ausführung in einem Threadpool
        loop = asyncio.get_running_loop()
        func = partial(_execute_sparql_query_sync, query, endpoint, user_agent, timeout, ssl_verify)
        
        try:
            return await loop.run_in_executor(None, func)
        except Exception as e:
            logger.error(f"Fehler bei asynchroner SPARQL-Ausführung: {str(e)}")
            raise

async def async_sparql_fetch_dbpedia_data(
    dbpedia_uris: List[str], 
    endpoints: Optional[List[str]] = None,
    batch_size: int = 10,
    user_agent: str = 'EntityExtractorClient/1.0',
    ssl_verify: bool = False,
    timeout: int = 30,
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> Dict[str, Any]:
    """
    Führt asynchrone SPARQL-Abfragen für mehrere DBpedia-URIs durch mit SPARQLWrapper
    
    Args:
        dbpedia_uris: Liste der abzufragenden DBpedia-URIs
        endpoints: Liste der zu verwendenden SPARQL-Endpunkte
        batch_size: Anzahl der URIs pro Batch-Abfrage
        user_agent: User-Agent für die Anfragen
        ssl_verify: SSL-Verifizierung aktivieren/deaktivieren
        timeout: Timeout für Anfragen in Sekunden
        max_retries: Maximale Anzahl an Wiederholungsversuchen
        retry_delay: Verzögerung zwischen Wiederholungsversuchen in Sekunden
        
    Returns:
        Dict mit Ergebnissen, indiziert nach URI
    """
    if not dbpedia_uris:
        return {}
        
    if not endpoints:
        # HTTP zuerst für bessere Zuverlässigkeit (keine SSL-Probleme)
        endpoints = [
            "http://dbpedia.org/sparql",
            "https://dbpedia.org/sparql", 
            "http://live.dbpedia.org/sparql"
        ]
    
    # Endpunkte protokollieren
    logger.info(f"Verwende SPARQL-Endpunkte: {endpoints}")
    
    # Ergebnis-Dictionary
    results = {}
    
    # DBpedia-URIs in Batches aufteilen
    batches = [dbpedia_uris[i:i+batch_size] for i in range(0, len(dbpedia_uris), batch_size)]
    logger.info(f"Aufgeteilt in {len(batches)} Batches mit max. {batch_size} URIs pro Batch")
    
    # Batches sequentiell verarbeiten
    for batch_idx, batch in enumerate(batches):
        logger.info(f"Verarbeite Batch {batch_idx + 1}/{len(batches)} mit {len(batch)} URIs")
        
        # VALUES-Klausel für den SPARQL-Query erstellen
        values_clause = "\n    ".join(f"<{uri}>" for uri in batch)
        
        # SPARQL-Query für den aktuellen Batch
        query = f"""
            PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dbo:      <http://dbpedia.org/ontology/>
            PREFIX dcterms:  <http://purl.org/dc/terms/>
            PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX geo:      <http://www.w3.org/2003/01/geo/wgs84_pos#>
            PREFIX foaf:     <http://xmlns.com/foaf/0.1/>

            SELECT ?entity
                ?label
                ?abstract
                ?partOf
                ?hasPart
                ?type
                ?category
                ?lat
                ?long
                ?wiki
                ?homepage
                ?image
            WHERE {{
            VALUES ?entity {{ {values_clause} }}

            OPTIONAL {{
                ?entity rdfs:label ?label .
                FILTER(lang(?label) = "en")
            }}
            OPTIONAL {{
                ?entity dbo:abstract ?abstract .
                FILTER(lang(?abstract) = "en")
            }}
            OPTIONAL {{ ?entity dbo:isPartOf   ?partOf   . }}
            OPTIONAL {{ ?entity dbo:hasPart     ?hasPart  . }}
            OPTIONAL {{ ?entity rdf:type        ?type     . }}
            OPTIONAL {{ ?entity dcterms:subject  ?category . }}
            OPTIONAL {{
                ?entity geo:lat  ?lat ;
                        geo:long ?long .
            }}
            OPTIONAL {{ ?entity foaf:isPrimaryTopicOf ?wiki     . }}
            OPTIONAL {{ ?entity foaf:homepage          ?homepage . }}
            OPTIONAL {{ ?entity dbo:thumbnail           ?image    . }}
            }}
        """
        
        # Jeden Endpunkt versuchen, bis einer erfolgreich ist
        batch_success = False
        for endpoint_idx, endpoint in enumerate(endpoints):
            if batch_success:
                break
                
            # Wiederholungsversuche für diesen Endpunkt
            for retry in range(max_retries + 1):
                try:
                    if retry > 0:
                        logger.info(f"Wiederholungsversuch {retry}/{max_retries} für Batch {batch_idx + 1}")
                        await asyncio.sleep(retry_delay * (2 ** retry))  # Exponentielles Backoff
                    
                    # Abfrage ausführen
                    sparql_response = await execute_sparql_query(
                        query=query,
                        endpoint=endpoint,
                        user_agent=user_agent,
                        timeout=timeout,
                        ssl_verify=ssl_verify
                    )
                    
                    # Ergebnisse verarbeiten
                    if sparql_response and 'results' in sparql_response:
                        batch_results = process_sparql_results(sparql_response, batch)
                        results.update(batch_results)
                        batch_success = True
                        logger.info(f"Batch {batch_idx + 1} erfolgreich mit Endpunkt {endpoint_idx + 1}")
                        break
                    else:
                        logger.warning(f"Leere Ergebnisse für Batch {batch_idx + 1} von {endpoint}")
                        
                except Exception as e:
                    logger.warning(f"Fehler bei Batch {batch_idx + 1} mit Endpunkt {endpoint}: {str(e)}")
                    if retry == max_retries:
                        logger.error(f"Alle Wiederholungsversuche für Batch {batch_idx + 1} mit Endpunkt {endpoint} fehlgeschlagen")
                    # Weitermachen mit dem nächsten Wiederholungsversuch oder Endpunkt
        
        if not batch_success:
            logger.error(f"Konnte keine Daten für Batch {batch_idx + 1} von irgendeinem Endpunkt abrufen")
    
    # Statistik protokollieren
    success_count = sum(1 for uri in dbpedia_uris if uri in results and results[uri].get('status') != 'no_data')
    if success_count > 0:
        logger.info(f"Erfolgreich Daten für {success_count}/{len(dbpedia_uris)} URIs abgerufen")
    else:
        logger.warning(f"Keine Daten für {len(dbpedia_uris)} URIs gefunden")
    
    return results
