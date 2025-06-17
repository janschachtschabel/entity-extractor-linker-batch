#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data formatters for the DBpedia service.

This module provides functions for formatting and validating DBpedia data.
"""

import logging
import json
from typing import Dict, List, Any, Optional, Set
from datetime import datetime

from entityextractor.utils.logging_utils import get_service_logger

# Configure logger
logger = get_service_logger(__name__, 'dbpedia')

def build_sparql_query(uris: List[str], language: str = "en") -> str:
    """
    Build a SPARQL query to fetch data for the given URIs.
    
    Args:
        uris: List of DBpedia URIs to query
        
    Returns:
        SPARQL query string
    """
    # Escape URIs for the SPARQL query
    escaped_uris = [f'<{uri}>' for uri in uris]
    values_clause = " ".join(escaped_uris)
    
    # Ensure language is a string for the f-string
    lang_code = str(language) if language else "en" # Default to 'en' if language is None or empty

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
    return query.strip()


def process_sparql_results(sparql_results: Dict, expected_uris: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Process SPARQL query results into a structured format.
    
    Args:
        sparql_results: Raw SPARQL results
        expected_uris: List of URIs that were queried
        
    Returns:
        Dictionary mapping URIs to their processed data
    """
    if not sparql_results or 'results' not in sparql_results or 'bindings' not in sparql_results['results']:
        logger.warning("Invalid SPARQL results format")
        return {}
        
    # Initialize result structure with all expected URIs
    results = {}
    entity_data = {}
    
    # Process each binding row
    for row in sparql_results['results']['bindings']:
        if 'entity' not in row:
            continue
            
        uri = row['entity']['value']
        
        # Initialize data structure for this URI if not already done
        if uri not in entity_data:
            entity_data[uri] = {
                'uri': uri,
                'label': None,
                'abstract': None,
                'partOf': set(),
                'hasPart': set(),
                'types': set(),
                'categories': set(),
                'lat': None,
                'long': None,
                'wiki': None,
                'homepage': None,
                'image': None
            }
        
        # Extract data from this row
        data = entity_data[uri]
        
        # Single value fields
        if 'label' in row and data['label'] is None:
            data['label'] = row['label']['value']
            
        if 'abstract' in row and data['abstract'] is None:
            data['abstract'] = row['abstract']['value']
            
        if 'lat' in row and data['lat'] is None:
            data['lat'] = row['lat']['value']
            
        if 'long' in row and data['long'] is None:
            data['long'] = row['long']['value']
            
        if 'wiki' in row and data['wiki'] is None:
            data['wiki'] = row['wiki']['value']
            
        if 'homepage' in row and data['homepage'] is None:
            data['homepage'] = row['homepage']['value']
            
        if 'image' in row and data['image'] is None:
            data['image'] = row['image']['value']
        
        # Collection fields - robust error handling for missing keys
        # Use a helper function to safely extract values from SPARQL result fields
        def safe_add_value(result_row, field, target_set):
            try:
                if field in result_row and 'value' in result_row[field]:
                    target_set.add(result_row[field]['value'])
            except (KeyError, TypeError) as e:
                # Log the error but don't crash
                logger.debug(f"Error extracting '{field}' from SPARQL result: {e}")
        
        # Apply safe extraction to all collection fields
        safe_add_value(row, 'partOf', data['partOf'])
        safe_add_value(row, 'hasPart', data['hasPart'])
        safe_add_value(row, 'type', data['types'])
        safe_add_value(row, 'category', data['categories'])
    
    # Convert sets to lists for JSON serialization
    for uri, data in entity_data.items():
        data['partOf'] = list(data['partOf'])
        data['hasPart'] = list(data['hasPart'])
        data['types'] = list(data['types'])
        data['categories'] = list(data['categories'])
        results[uri] = data
        
    # Fill in any missing URIs with empty data
    for uri in expected_uris:
        if uri not in results:
            results[uri] = {
                'uri': uri,
                'status': 'no_data',
                'label': None,
                'abstract': None,
                'partOf': [],
                'hasPart': [],
                'types': [],
                'categories': []
            }
    
    return results

def validate_dbpedia_data(data: Dict[str, Any]) -> bool:
    """
    Validate that the DBpedia data contains the minimum required fields.
    
    Args:
        data: DBpedia data to validate
        
    Returns:
        True if the data is valid, False otherwise
    """
    if not data:
        return False
    
    # Check for required fields - we need URI, label, and abstract for valid data
    required_fields = ['uri', 'label', 'abstract']
    return all(field in data and data[field] for field in required_fields)


def format_dbpedia_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format DBpedia data for entity context.
    
    Args:
        data: Raw DBpedia data
        
    Returns:
        Formatted data structure
    """
    if not data:
        return {}
    
    # Determine if we have all required fields
    has_required = validate_dbpedia_data(data)
    
    # Create formatted structure
    formatted = {
        'uri': data.get('uri'),
        'label': data.get('label'),
        'abstract': data.get('abstract'),
        'partOf': data.get('partOf', []),
        'hasPart': data.get('hasPart', []),
        'types': data.get('types', []),
        'categories': data.get('categories', []),
        'status': 'linked' if has_required else 'not_linked',
        'source': 'dbpedia',
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Add geo data if available
    if 'lat' in data and 'long' in data and data['lat'] and data['long']:
        formatted['geo'] = {
            'lat': data['lat'],
            'long': data['long']
        }
    else:
        formatted['geo'] = {}
    
    # Add optional fields if available
    for field in ['wiki', 'homepage', 'image']:
        if field in data and data[field]:
            formatted[field] = data[field]
    
    # Clean up empty collections
    for field in ['partOf', 'hasPart', 'types', 'categories']:
        if not formatted[field]:
            formatted[field] = []
    
    return formatted
