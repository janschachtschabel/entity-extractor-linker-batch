"""
result_formatter.py

Funktionen zur Formatierung der Extraktionsergebnisse in ein standardisiertes Format.
"""

def format_results(entities, relationships, original_text):
    """
    Formatiert die extrahierten Entitäten und Beziehungen in ein standardisiertes Ergebnisobjekt.
    Alle IDs sind UUID4. Beziehungen referenzieren Entitäten ausschließlich über subject_id und object_id. Labels dienen nur der Anzeige.
    """
    # Optionale Validierung: IDs vorhanden?
    result = {"entities": [], "relationships": []}
    
    # Prüfe und formatiere die Beziehungen
    if relationships and isinstance(relationships, list):
        # Stelle sicher, dass die Beziehungen korrekt formatiert sind
        for rel in relationships:
            if isinstance(rel, dict) and "subject" in rel and "predicate" in rel and "object" in rel:
                result["relationships"].append(rel)
                
        # Debug-Ausgabe
        print(f"Formatierte Beziehungen: {len(result['relationships'])}")
        if result["relationships"]:
            print(f"Beispiel-Beziehung: {result['relationships'][0]}")
    else:
        print("Keine gültigen Beziehungen zum Formatieren gefunden.")
    
    # Formatiere die Entitäten
    for entity in entities:
        # Basisinformationen
        name = entity.get("name", "")
        entity_type = entity.get("type", "")
        inferred = entity.get("inferred", "explicit")
        
        # Extrahiere oder generiere eine Beispielstelle aus dem Text
        citation = entity.get("citation", original_text)
        citation_start = original_text.find(citation) if citation != original_text else 0
        citation_end = citation_start + len(citation) if citation_start != -1 else len(original_text)
        
        # Erstelle das Entitätsobjekt
        formatted_entity = {
            "entity": name,
            "details": {
                "typ": entity_type,
                "inferred": inferred,
                "citation": citation,
                "citation_start": citation_start,
                "citation_end": citation_end
            },
            "sources": {}
        }
        
        # Wikipedia-Informationen
        if entity.get("wikipedia_url"):
            wiki_source = formatted_entity["sources"].setdefault("wikipedia", {})
            wiki_source["label"] = entity.get("wikipedia_title", name)
            wiki_source["url"] = entity.get("wikipedia_url", "")
            if entity.get("wikipedia_extract"):
                wiki_source["extract"] = entity.get("wikipedia_extract", "")
            if entity.get("wikipedia_categories"):
                wiki_source["categories"] = entity.get("wikipedia_categories", [])
        
        # Wikidata-Informationen (direkt aus Linking übernehmen, falls vorhanden)
        if entity.get("wikidata") and isinstance(entity["wikidata"], dict) and entity["wikidata"].get("id"):
            formatted_entity["sources"]["wikidata"] = entity["wikidata"].copy()
        elif entity.get("wikidata_id"):
            wikidata_source = formatted_entity["sources"].setdefault("wikidata", {})
            wikidata_source["id"] = entity.get("wikidata_id", "")
            wikidata_source["url"] = f"https://www.wikidata.org/wiki/{entity.get('wikidata_id', '')}"
            if entity.get("wikidata_label"):
                wikidata_source["label"] = entity.get("wikidata_label", "")
            if entity.get("wikidata_description"):
                wikidata_source["description"] = entity.get("wikidata_description", "")
            if entity.get("wikidata_types"):
                wikidata_source["types"] = entity.get("wikidata_types", [])
            if entity.get("wikidata_part_of"):
                wikidata_source["part_of"] = entity.get("wikidata_part_of", [])
            if entity.get("wikidata_has_parts"):
                wikidata_source["has_parts"] = entity.get("wikidata_has_parts", [])
            if entity.get("wikidata_image_url"):
                wikidata_source["image_url"] = entity.get("wikidata_image_url", "")
        
        # DBpedia-Informationen (direkt aus Linking übernehmen, falls vorhanden)
        if entity.get("dbpedia") and isinstance(entity["dbpedia"], dict) and entity["dbpedia"].get("resource_uri"):
            formatted_entity["sources"]["dbpedia"] = entity["dbpedia"].copy()
        elif entity.get("dbpedia_abstract") or entity.get("dbpedia_subjects"):
            dbpedia_source = formatted_entity["sources"].setdefault("dbpedia", {})
            if entity.get("dbpedia_abstract"):
                dbpedia_source["abstract"] = entity.get("dbpedia_abstract", "")
            if entity.get("dbpedia_subjects"):
                dbpedia_source["subjects"] = entity.get("dbpedia_subjects", [])
            if entity.get("dbpedia_part_of"):
                dbpedia_source["part_of"] = entity.get("dbpedia_part_of", [])
            if entity.get("dbpedia_has_parts"):
                dbpedia_source["has_parts"] = entity.get("dbpedia_has_parts", [])
        
        # Füge die formatierte Entität dem Ergebnis hinzu
        result["entities"].append(formatted_entity)
    
    return result
