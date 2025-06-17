"""
relationships.py

API module for relationship extraction and inference between entities.

This is the official implementation for relationship inference in the Entity Extractor.
It is used by the Orchestrator component and other core modules to
identify and infer relationships between extracted entities.

A central feature is the distinction between explicit (directly mentioned in the text)
and implicit (derived from knowledge) relationships between entities.
"""

from loguru import logger
import logging
import re
import time
import uuid
import openai
import importlib

from entityextractor.utils.openai_utils import call_openai_api
from entityextractor.utils.id_utils import generate_relationship_id
from entityextractor.config.settings import get_config

def infer_entity_relationships(entities, text=None, config=None):
    """
    Infers relationships between extracted entities.
    
    Args:
        entities: List of entities between which relationships should be inferred
        text: Optional - the source text for explicit relationships
        config: Configuration dictionary
        
    Returns:
        List of relationships as triplets (subject, predicate, object)
    """
    if not entities:
        return []
    
    config = get_config(config)
    
    # Only infer relationships if enabled
    if not config.get("RELATION_EXTRACTION", True):
        logger.info("Entity Relationship Inference disabled.")
        return []
    
    # Only use entities with certain types, if configured
    allowed_types = config.get("ALLOWED_ENTITY_TYPES", "auto")
    
    # Consider different possible structures of the entities
    if allowed_types != "auto" and isinstance(allowed_types, list):
        filtered_entities = []
        for e in entities:
            # Check if it's a dictionary object
            if not isinstance(e, dict):
                continue
                
            # Check different possible structures of the entity
            if "details" in e and "typ" in e.get("details", {}):
                entity_type = e.get("details", {}).get("typ", "")
            else:
                entity_type = e.get("type", "")
                
            if entity_type in allowed_types:
                filtered_entities.append(e)
    else:
        filtered_entities = entities
    
    # Create mapping from entity names to types for later completion of triplets
    entity_type_map = {}
    entity_infer_map = {}
    
    for e in filtered_entities:
        # Support for old and new data structure
        if isinstance(e, dict):
            # New data structure uses 'entity' instead of 'name'
            name = e.get("entity", e.get("name", ""))
            
            # Check different possible structures for the type
            if "details" in e and "typ" in e.get("details", {}):
                entity_type = e.get("details", {}).get("typ", "")
            else:
                entity_type = e.get("type", "")
                
            # Check different possible structures for inferred
            if "details" in e and "inferred" in e.get("details", {}):
                inferred = e.get("details", {}).get("inferred", "explicit")
            else:
                inferred = e.get("inferred", "explicit")
        elif isinstance(e, str):
            # Fallback when the entity is passed directly as a string
            name = e
            entity_type = ""
            inferred = "explicit"
        else:
            # Skip unknown format
            logger.warning(f"Unknown entity format: {type(e)}")
            continue
            
        if not name:
            continue
            
        entity_type_map[name] = entity_type
        entity_infer_map[name] = inferred
    
    logger.info(f"Extracted {len(filtered_entities)} entities for relationship extraction")
    logger.info(f"Created entity type map with {len(entity_type_map)} entries")
    logger.info(f"Created entity inference map with {len(entity_infer_map)} entries")
    
    # Create a helper function for normalizing entity names for better matching
    def normalize_entity_name(name):
        """Normalizes an entity name for more robust matching.
        
        Removes bracket expressions like in "dualism (theory)" -> "dualism"
        and normalizes case.
        """
        if not name:
            return ""
            
        # Basic normalization (lowercase and remove spaces)
        result = name.strip().lower()
        # Entferne führende/abschließende eckige Klammern, falls vorhanden
        if result.startswith("[") and result.endswith("]"):
            result = result[1:-1].strip()
        
        # Remove suffix brackets like in "dualism (theory)" -> "dualism"
        if "(" in result and ")" in result:
            # Find the position of the first bracket
            bracket_start = result.find("(")
            # Extract only the text before the bracket
            result = result[:bracket_start].strip()
            
        return result
    
    # Extract explicit relationships if text is available
    all_relationships = []
    explicit_rels = []
    implicit_rels = []
    
    # Check configuration parameters for relationship extraction
    relation_extraction_enabled = (
        config.get("RELATION_EXTRACTION", False) or 
        config.get("ENABLE_RELATIONS_INFERENCE", False)
    )
    
    # Logging for relationship extraction configuration
    logger.info(f"Relationship extraction active: {relation_extraction_enabled} (RELATION_EXTRACTION={config.get('RELATION_EXTRACTION', False)}, ENABLE_RELATIONS_INFERENCE={config.get('ENABLE_RELATIONS_INFERENCE', False)})")
    
    if relation_extraction_enabled:
        if text:
            logger.info("Starting Entity Relationship Inference with text...")
            explicit_rels = extract_explicit_relationships(filtered_entities, text, entity_type_map, entity_infer_map, config)
            all_relationships.extend(explicit_rels)
            logger.info(f"Extracted {len(explicit_rels)} explicit relationships")
        else:
            logger.info("No text available for explicit relationships, skipping explicit extraction.")
    
    # Extract implicit relationships if enabled
    if config.get("ENABLE_RELATIONS_INFERENCE", False):
        logger.info("Starting implicit relationship extraction...")
        implicit_rels = extract_implicit_relationships(filtered_entities, entity_type_map, entity_infer_map, config)
        all_relationships.extend(implicit_rels)
        logger.info(f"Extracted {len(implicit_rels)} implicit relationships")
    else:
        logger.info("Implicit relationship extraction disabled.")
        
    # Optionally deduplicate relationships depending on configuration flag
    relationships = all_relationships
    if config.get("STATISTICS_DEDUPLICATE_RELATIONSHIPS", True):
        # Dynamic import to avoid circular imports
        deduplication_module = importlib.import_module("entityextractor.core.process.deduplication")
        relationships = deduplication_module.deduplicate_relationships(all_relationships, filtered_entities, config)
        logger.info(f"Relationship deduplication enabled: reduced from {len(all_relationships)} to {len(relationships)} records")
    else:
        logger.info("Relationship deduplication disabled by configuration – keeping all relationships")
    
    # Helper function for normalizing entity names for better matching
    def normalize_entity_name(name):
        """Normalizes an entity name for more robust matching.
        
        Removes bracket expressions like in "dualism (theory)" -> "dualism"
        and normalizes case.
        """
        if not name:
            return ""
            
        # Grundnormalisierung (Kleinschreibung und Leerzeichen entfernen)
        result = name.strip().lower()
        # Entferne führende/abschließende eckige Klammern, falls vorhanden
        if result.startswith("[") and result.endswith("]"):
            result = result[1:-1].strip()
        
        # Entferne Suffix-Klammern wie in "dualismus (theorie)" -> "dualismus"
        if "(" in result and ")" in result:
            # Finde die Position der ersten Klammer
            bracket_start = result.find("(")
            # Extrahiere nur den Text vor der Klammer
            result = result[:bracket_start].strip()
            
        return result
    
    # Create normalized versions of entity names for more robust matching
    logger.info("Creating normalized entity maps for more robust matching...")
    entity_names = set(entity_type_map.keys())
    entity_names_normalized = {normalize_entity_name(name): name for name in entity_names}
    
    # Create an ID map with support for old and new data structures
    entity_id_map = {}
    for e in filtered_entities:
        if isinstance(e, dict):
            # Neue Datenstruktur verwendet 'entity' statt 'name'
            name = e.get("entity", e.get("name", ""))
            entity_id = e.get("id", str(uuid.uuid4()))
            if name:
                entity_id_map[name] = entity_id
        elif isinstance(e, str):
            # Fallback, wenn die Entität direkt als String übergeben wird
            entity_id_map[e] = str(uuid.uuid4())
    
    entity_id_map_normalized = {normalize_entity_name(name): uid for name, uid in entity_id_map.items()}
    
    # Logging für Debugging
    logging.info(f"Erste 5 Einträge in entity_names: {list(entity_names)[:5]}")
    logging.info(f"Erste 5 Einträge in entity_id_map: {list(entity_id_map.items())[:5]}")
    logging.info(f"Beispiel einer Relationship: {relationships[0] if relationships else 'Keine Beziehungen'}")
    
    # Debug-Ausgabe der Entitäten für Matching-Diagnose
    logging.info(f"Entity-Map enthält {len(entity_names)} Einträge")
    logging.info(f"Entity-ID-Map enthält {len(entity_id_map)} Einträge")
    
    # Validiere die Beziehungen (prüfen, ob subject und object in der Entitätsliste sind)
    valid_relationships = []
    match_failures = 0
    
    # Bestimme den Modus
    mode = config.get("MODE", "extract")
    is_generate_mode = mode == "generate"
    
    # Im generate-Modus behandeln wir alle Beziehungen direkt als gültig,
    # um das Format-Mismatch-Problem zu umgehen
    if is_generate_mode:
        logging.info(f"Generate-Modus erkannt: Verwende direkte Beziehungsverarbeitung für {len(relationships)} Beziehungen")
        for rel in relationships:
            subject = rel.get("subject", "")
            predicate = rel.get("predicate", "")
            object_ = rel.get("object", "")
            inferred_type = rel.get("inferred", "implicit")
            subject_type = rel.get("subject_type", "")
            object_type = rel.get("object_type", "")
            
            # Erzeuge UUIDs für die Beziehung und die Entitäten
            rel_id = generate_relationship_id()
            subject_id = generate_relationship_id()
            object_id = generate_relationship_id()
            
            # Erstelle validierte Beziehung
            rel_out = {
                "id": rel_id,
                "subject": subject,
                "predicate": predicate,
                "object": object_,
                "inferred": inferred_type,  # Im generate-Modus sind alle implizit
                "subject_type": subject_type,
                "object_type": object_type,
                "subject_id": subject_id,
                "object_id": object_id,
                "subject_label": subject,
                "object_label": object_,
                "subject_inferred": "implicit",
                "object_inferred": "implicit"
            }
            valid_relationships.append(rel_out)
            
        logging.info(f"Generate-Modus: Alle {len(valid_relationships)} Beziehungen akzeptiert")
        return valid_relationships
    
    # Debug: Generiere Liste normalisierter und ursprünglicher Entitätsnamen
    entity_names_debug = sorted(list(entity_names))[:5]
    entity_names_norm_debug = sorted([(norm, orig) for norm, orig in entity_names_normalized.items()])[:5]
    logging.info(f"Normalisierungs-Debug - Original: {entity_names_debug}")
    logging.info(f"Normalisierungs-Debug - Normalisiert: {entity_names_norm_debug}")
    
    for rel in relationships:
        subject = rel.get("subject")
        object_ = rel.get("object")
        predicate = rel.get("predicate", "")
        
        # Debug-Info für diese Beziehung
        logging.info(f"Verarbeite Beziehung: '{subject}' -> '{predicate}' -> '{object_}'")
        
        # Normalisiere für das Matching
        subject_norm = normalize_entity_name(subject)
        object_norm = normalize_entity_name(object_)
        
        logging.info(f"Normalisiert: '{subject}' -> '{subject_norm}', '{object_}' -> '{object_norm}'")
        
        # Im generate-Modus betrachten wir alle Beziehungen als gültig
        if is_generate_mode:
            # Wir vertrauen im generate-Modus dem LLM und akzeptieren alle Entitäten
            subject_match = True
            object_match = True
            
            # Versuche trotzdem, die normalisierten Namen zu finden
            # Wenn nicht gefunden, verwende die Originalnamen
            orig_subject = subject
            orig_object = object_
            
            # Für Subjekt: Prüfe, ob originale oder normalisierte Form in den Entitäten vorkommt
            if subject in entity_names:
                orig_subject = subject
                logging.info(f"Subjekt '{subject}' direkt in Entitäten gefunden")
            elif subject_norm in entity_names_normalized:
                orig_subject = entity_names_normalized.get(subject_norm, subject)
                logging.info(f"Subjekt '{subject}' -> '{subject_norm}' -> '{orig_subject}' nach Normalisierung gefunden")
            else:
                logging.info(f"Subjekt '{subject}' nicht in Entitätsliste gefunden, wird akzeptiert im generate-Modus")
                
            # Für Objekt: Gleiche Logik
            if object_ in entity_names:
                orig_object = object_
                logging.info(f"Objekt '{object_}' direkt in Entitäten gefunden")
            elif object_norm in entity_names_normalized:
                orig_object = entity_names_normalized.get(object_norm, object_)
                logging.info(f"Objekt '{object_}' -> '{object_norm}' -> '{orig_object}' nach Normalisierung gefunden")
            else:
                logging.info(f"Objekt '{object_}' nicht in Entitätsliste gefunden, wird akzeptiert im generate-Modus")
            
        else:
            # Prüfe sowohl exaktes als auch normalisiertes Matching
            subject_match = subject in entity_names or subject_norm in entity_names_normalized
            object_match = object_ in entity_names or object_norm in entity_names_normalized
            
            if subject_match and object_match:
                # Ermittle die korrekten Original-Namen
                orig_subject = subject if subject in entity_names else entity_names_normalized.get(subject_norm, subject)
                orig_object = object_ if object_ in entity_names else entity_names_normalized.get(object_norm, object_)
            
        # Wenn wir gültige Entitäten haben (im extract-Modus) oder im generate-Modus sind
        if subject_match and object_match:
            # Die Original-Namen wurden bereits im jeweiligen Modus-Zweig gesetzt
            # Jetzt ermittle die UUIDs
            subject_id = ""
            object_id = ""
            
            if is_generate_mode:
                # Im generate-Modus versuchen wir zunächst einen Lookup, generieren aber immer eine UUID falls nötig
                # Hole UUIDs, zuerst direkt, dann über normalisierte Namen
                subject_id = entity_id_map.get(orig_subject) or entity_id_map_normalized.get(subject_norm)
                object_id = entity_id_map.get(orig_object) or entity_id_map_normalized.get(object_norm)
                
                # Für Subjekt-ID
                if not subject_id:
                    subject_id = generate_relationship_id()
                    logging.info(f"Generate-Modus: Neue UUID für Subjekt '{subject}' generiert")
                
                # Für Objekt-ID
                if not object_id:
                    object_id = generate_relationship_id()
                    logging.info(f"Generate-Modus: Neue UUID für Objekt '{object_}' generiert")
            else:
                # Im extract-Modus müssen wir valid UUIDs aus den Entitäten haben
                subject_id = entity_id_map.get(orig_subject) or entity_id_map_normalized.get(subject_norm, "")
                object_id = entity_id_map.get(orig_object) or entity_id_map_normalized.get(object_norm, "")
                
                # Validierungsfehler im extract-Modus
                if not subject_id or not object_id:
                    logging.warning(f"UUID-Mapping fehlgeschlagen für Beziehung: {subject} -- {predicate} --> {object_}")
                    if not subject_id:
                        logging.warning(f"  Subjekt '{subject}' ({subject_norm}) konnte nicht auf UUID gemappt werden")
                    if not object_id:
                        logging.warning(f"  Objekt '{object_}' ({object_norm}) konnte nicht auf UUID gemappt werden")
                    match_failures += 1
                    continue
            
            # Erzeuge UUID4 für die Beziehung
            rel_id = generate_relationship_id()
            rel_out = dict(rel)  # Kopie
            rel_out["id"] = rel_id
            rel_out["subject_id"] = subject_id
            rel_out["object_id"] = object_id
            rel_out["subject_label"] = orig_subject
            rel_out["object_label"] = orig_object
            
            # Ergänze entity_type Informationen - zuerst über Original-Namen, dann über normalisierte Namen
            # Für subject_type
            if "subject_type" not in rel_out or not rel_out["subject_type"]:
                rel_out["subject_type"] = entity_type_map.get(orig_subject, "")
                if not rel_out["subject_type"] and subject_norm in entity_names_normalized:
                    # Versuche, über normalisierte Namen den Typ zu finden
                    orig_name = entity_names_normalized.get(subject_norm)
                    if orig_name:
                        rel_out["subject_type"] = entity_type_map.get(orig_name, "")
            
            # Für object_type
            if "object_type" not in rel_out or not rel_out["object_type"]:
                rel_out["object_type"] = entity_type_map.get(orig_object, "")
                if not rel_out["object_type"] and object_norm in entity_names_normalized:
                    # Versuche, über normalisierte Namen den Typ zu finden
                    orig_name = entity_names_normalized.get(object_norm)
                    if orig_name:
                        rel_out["object_type"] = entity_type_map.get(orig_name, "")
            
            valid_relationships.append(rel_out)
    
    # Debug-Ausgabe für die Beziehungen
    if valid_relationships:
        logging.info(f"Validierte Beziehungen: {len(valid_relationships)}")
        for i, rel in enumerate(valid_relationships[:3]):
            logging.info(f"Beispiel-Beziehung {i+1}: {rel.get('subject_label', '')} -- {rel.get('predicate', '')} --> {rel.get('object_label', '')}")
            
        print(f"Returning {len(valid_relationships)} validated relationships")
        return valid_relationships
    else:
        logger.warning("No valid relationships found!")
        return []

def extract_explicit_relationships(entities, text, entity_type_map, entity_infer_map, config):
    """
    Extracts explicit relationships between entities in the text.
    In "extract" mode, explicit relationships are extracted.
    In "generate" mode, implicit relationships are extracted (also in the first prompt).
    
    Args:
        entities: List of entities
        text: The text in which to search for relationships
        entity_type_map: Mapping from entity names to types
        entity_infer_map: Mapping from entity names to inference status
        config: Configuration dictionary
        
    Returns:
        List of explicit or implicit relationships, depending on the mode
    """
    model = config.get("MODEL", "gpt-4.1-mini")
    language = config.get("LANGUAGE", "de")
    max_relations = config.get("MAX_RELATIONS", 20)
    temperature = 0.2  # Low temperature for consistent answers
    
    logger.info(f"Calling OpenAI API for explicit relationships (model {model})...")
    
    # Create the prompt for explicit relationships
    entity_items = []
    for e in entities:
        if isinstance(e, dict):
            # Support for old and new data structure and additional fallback keys
            name = e.get("entity") or e.get("name") or e.get("entity_name")
            
            # Extract type from different possible structures
            if "details" in e and "typ" in e.get("details", {}):
                entity_type = e.get("details", {}).get("typ", "Entity")
            else:
                entity_type = e.get("type", "Entity")
                
            if name:
                entity_items.append(f"- {name} ({entity_type})")
        elif isinstance(e, str):
            # Fallback, wenn die Entität direkt als String übergeben wird
            entity_items.append(f"- {e} (Entity)")
    
    entity_list = "\n".join(entity_items)
    
    # Bestimme den Modus
    mode = config.get("MODE", "extract")
    is_generate_mode = mode == "generate"
    
    # Sprachspezifische Prompts
    if language.startswith("de"):
        if is_generate_mode:
            system_prompt = """Du bist ein Assistent für die Erstellung von Wissensgraphen. Deine Aufgabe ist es, sinnvolle Beziehungen zwischen den gegebenen Entitäten zu identifizieren.

Wichtige Regeln:
1. Erstelle bedeutungsvolle Beziehungen zwischen den gegebenen Entitäten basierend auf allgemeinem Wissen
2. Subjekt und Objekt MÜSSEN aus der bereitgestellten Entitätsliste stammen
3. Verwende keine Entitäten, die nicht in der Liste stehen
4. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an, einen pro Zeile
5. Verwende als Subjekt und Objekt EXAKT die Namen aus der Entitätsliste
6. Verwende ausschließlich kleingeschriebene Prädikate in der dritten Person Singular (z.B. "enthält", "ist_teil_von")
7. Verwende keine erklärenden Sätze oder Einleitungen
8. Erstelle nur offensichtliche und allgemein gültige Verbindungen"""

            user_prompt = f"""Hier ist das Thema: {text}

Hier sind die Entitäten, für die Beziehungen generiert werden sollen:
{entity_list}

Bitte generiere sinnvolle Beziehungen zwischen diesen Entitäten. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an."""
        else:
            system_prompt = """Du bist ein Assistent für die Erstellung von Wissensgraphen. Deine Aufgabe ist es, Beziehungen zwischen Entitäten zu identifizieren, die im Text EXPLIZIT erwähnt werden. 

Wichtige Regeln:
1. Extrahiere NUR Beziehungen, die DIREKT im Text erwähnt werden
2. Subjekt und Objekt MÜSSEN aus der bereitgestellten Entitätsliste stammen
3. Verwende keine Entitäten, die nicht in der Liste stehen
4. Erfinde keine Beziehungen, die nicht aus dem Text hervorgehen
5. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an, einen pro Zeile
6. Verwende als Subjekt und Objekt EXAKT die Namen aus der Entitätsliste
7. Verwende ausschließlich kleingeschriebene Prädikate in der dritten Person Singular (z.B. "enthält", "ist_teil_von")
8. Verwende keine erklärenden Sätze oder Einleitungen"""

            user_prompt = f"""Hier ist ein Text mit Entitäten:

{text}

Hier sind die extrahierten Entitäten:
{entity_list}

Bitte identifiziere alle EXPLIZITEN Beziehungen zwischen diesen Entitäten, die DIREKT im Text erwähnt werden. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an."""
    else:
        if is_generate_mode:
            system_prompt = """You are an assistant for creating knowledge graphs. Your task is to identify meaningful relationships between the given entities.

Important rules:
1. Create meaningful relationships between the given entities based on general knowledge
2. Subject and object MUST be from the provided entity list
3. Do not use entities that are not in the list
4. Provide the relationships in the format "Subject; Predicate; Object", one per line
5. Use EXACTLY the names from the entity list for subject and object
6. Use only lowercase predicates in the third person singular (e.g. "contains", "is_part_of")
7. Do not use explanatory sentences or introductions
8. Only create obvious and generally valid connections"""

            user_prompt = f"""Here is the topic: {text}

Here are the entities for which relationships should be generated:
{entity_list}

Please generate meaningful relationships between these entities. Provide the relationships in the format "Subject; Predicate; Object"."""
        else:
            system_prompt = """You are an assistant for creating knowledge graphs. Your task is to identify relationships between entities that are EXPLICITLY mentioned in the text.

Important rules:
1. Extract ONLY relationships that are DIRECTLY mentioned in the text
2. Subject and object MUST be from the provided entity list
3. Do not use entities that are not in the list
4. Do not invent relationships that do not appear in the text
5. Provide the relationships in the format "Subject; Predicate; Object", one per line
6. Use EXACTLY the names from the entity list for subject and object
7. Use only lowercase predicates in the third person singular (e.g. "contains", "is_part_of")
8. Do not use explanatory sentences or introductions"""

            user_prompt = f"""Here is a text with entities:

{text}

Here are the extracted entities:
{entity_list}

Please identify all EXPLICIT relationships between these entities that are DIRECTLY mentioned in the text. Provide the relationships in the format "Subject; Predicate; Object"."""
    
    # Rufe die OpenAI API auf
    start_time = time.time()
    response = call_openai_api(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=temperature,
        config=config
    )
    
    # Extrahiere die Beziehungen aus der Antwort
    elapsed = time.time() - start_time
    relationships = []
    
    if response:
        answer = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        logger.info(f"Received response (explicit): {answer[:100]}...")
        logger.info(f"First prompt completed in {elapsed:.2f} seconds")
        
        # Verarbeite die Antwort Zeile für Zeile
        for line in answer.strip().split("\n"):
            if not line or ";" not in line:
                continue
            
            parts = [p.strip() for p in line.split(";")]
            if len(parts) >= 3:
                subject, predicate, object_ = parts[0], parts[1], parts[2]
                
                # Normalisierung des Prädikats
                predicate = predicate.lower().strip()
                
                # Determine the inference type based on the mode
                mode = config.get("MODE", "extract")
                inferred_type = "implicit" if mode == "generate" else "explicit"
                
                # Determine the inference status for subject and object based on the mode
                subject_inferred = "implicit" if mode == "generate" else entity_infer_map.get(subject, "explicit")
                object_inferred = "implicit" if mode == "generate" else entity_infer_map.get(object_, "explicit")
                
                # Erstelle die Beziehung mit Entitätstypen
                relationship = {
                    "subject": subject,
                    "predicate": predicate,
                    "object": object_,
                    "inferred": inferred_type,
                    "subject_type": entity_type_map.get(subject, ""),
                    "object_type": entity_type_map.get(object_, ""),
                    "subject_inferred": subject_inferred,
                    "object_inferred": object_inferred
                }
                
                relationships.append(relationship)
    
    logger.info(f"{len(relationships)} valid explicit relationships found")
    return relationships

def extract_implicit_relationships(entities, entity_type_map, entity_infer_map, config, existing_relationships=None):
    """
    Infers implicit relationships between entities based on KG knowledge.
    
    Args:
        entities: List of entities
        entity_type_map: Mapping from entity names to types
        entity_infer_map: Mapping from entity names to inference status
        config: Configuration dictionary
        
    Returns:
        List of implicit relationships
    """
    model = config.get("MODEL", "gpt-4.1-mini")
    language = config.get("LANGUAGE", "de")
    max_relations = config.get("MAX_RELATIONS", 20)
    temperature = 0.3  # Slightly higher temperature for more variation in implicit relationships
    max_new_relations = config.get("IMPLICIT_REL_LIMIT", 20)
    
    logger.info(f"Calling OpenAI API for implicit relationships (model {model})...")
    
    # Create the prompt for implicit relationships
    # Fallback to 'entity_name' if 'name' key missing
    # Build entity list with robust key fallback
    # Build numbered, strict entity list with canonical token in brackets
    canonical_entities = []
    for idx, e in enumerate(entities, 1):
        canonical = (e.get("entity") or e.get("name") or e.get("entity_name"))
        if not canonical:
            continue
        ent_type = e.get("type", "Entity")
        canonical_entities.append(f"{idx}) {canonical} [{canonical}] ({ent_type})")
    entity_list = "\n".join(canonical_entities)
    
    # Create entity information for the prompt
    entity_info = []
    for entity in entities:
        name = entity.get("entity") or entity.get("name") or entity.get("entity_name", "")
        description = entity.get("wikipedia_extract", "")[:150]  # Shortened description
        if name and description:
            entity_info.append(f"- {name}: {description}...")
    
    entity_info_text = "\n".join(entity_info)

    # Debug: Log the entities passed to implicit inference
    logger.info(f"[implicit] Using {len(entities)} entities for inference. First 5: {[ (e.get('entity') or e.get('name') or e.get('entity_name')) for e in entities[:5] ]}")

    # Prepare list of already known relationships to avoid duplicates
    existing_rel_text = ""
    if existing_relationships:
        rel_lines = [f"{idx+1}. {r['subject']}; {r['predicate']}; {r['object']}" for idx, r in enumerate(existing_relationships)]
        existing_rel_text = "\n".join(rel_lines)
    
    # Sprachspezifische Prompts
    if language.startswith("de"):
        system_prompt = """Du bist ein Assistent für die Erstellung von Wissensgraphen. Deine Aufgabe ist es, IMPLIZITE Beziehungen zwischen Entitäten zu identifizieren, basierend auf allgemeinem Wissen und Kontext.

Wichtige Regeln:
1. Subjekt und Objekt MÜSSEN aus der bereitgestellten Entitätsliste stammen
2. Verwende keine Entitäten, die nicht in der Liste stehen
3. Inferiere Beziehungen basierend auf allgemeinem Wissen
4. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an, einen pro Zeile
5. Verwende als Subjekt und Objekt EXAKT die Namen aus der Entitätsliste
6. Verwende ausschließlich kleingeschriebene Prädikate in der dritten Person Singular (z.B. "enthält", "ist_teil_von")
7. Verwende keine erklärenden Sätze oder Einleitungen"""

        user_prompt = f"""Hier sind die extrahierten Entitäten mit kurzen Beschreibungen:\n\n{entity_info_text}\n\nErlaubte Entitäten (verwende **exakt** die Zeichenfolge in eckigen Klammern als Subjekt/Objekt):\n{entity_list}\n\nBereits erkannte Beziehungen (bitte KEINE Umformulierungen oder logischen Dubletten dazu liefern):\n{existing_rel_text}\n\nAufgabe:\n- Finde maximal {max_new_relations} weitere, klar neue IMPLIZITE Beziehungen.\n- Verwende **ausschließlich** die Tokens in eckigen Klammern als Subjekt/Objekt.\n- Keine neuen Entitäten, keine Aliasnamen, keine rein sprachlichen Varianten.\n- Gib die Beziehungen exakt im Format \"[Subjekt]; Prädikat; [Objekt]\" (inklusive der eckigen Klammern um Subjekt und Objekt) ohne weitere Erklärungen.\n- Beispiel NICHT erlaubt: Einstein; beeinflusst; Relativitätstheorie
- Beispiel ERLAUBT: [Albert Einstein]; beeinflusst; [Relativitätstheorie]"""
    else:
        system_prompt = """You are an assistant for creating knowledge graphs. Your task is to identify IMPLICIT relationships between entities, based on general knowledge and context.

Important rules:
1. Subject and object MUST be from the provided entity list
2. Do not use entities that are not in the list
3. Infer relationships based on general knowledge
4. Provide the relationships in the format "Subject; Predicate; Object", one per line
5. Use EXACTLY the names from the entity list for subject and object
6. Use only lowercase predicates in the third person singular (e.g. "contains", "is_part_of")
7. Do not use explanatory sentences or introductions
8. Avoid obvious or trivial relationships and duplicates"""

        user_prompt = f"""Here are the extracted entities with short descriptions:\n\n{entity_info_text}\n\nEntity list:\n{entity_list}\n\nExisting relationships (do NOT provide paraphrases or logical duplicates):\n{existing_rel_text}\n\nTask:\n- Provide at most {max_new_relations} additional IMPLICIT relationships that are genuinely new in meaning.\n- No mere linguistic rephrasings.\n- Output ONLY NEW relationships in the exact format \"Subject; Predicate; Object\" with no explanations."""
    
    # Rufe die OpenAI API auf
    start_time = time.time()
    response = call_openai_api(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=temperature,
        config=config
    )
    
    # Extrahiere die Beziehungen aus der Antwort
    elapsed = time.time() - start_time
    relationships = []
    
    if response:
        answer = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        logger.info(f"Received response (implicit): {answer[:100]}...")
        logger.info(f"Second prompt completed in {elapsed:.2f} seconds")
        
        # Verarbeite die Antwort Zeile für Zeile
        for line in answer.strip().split("\n"):
            if not line or ";" not in line:
                continue
            
            parts = [p.strip() for p in line.split(";")]
            if len(parts) >= 3:
                subject, predicate, object_ = parts[0], parts[1], parts[2]
                
                # Normalisierung des Prädikats
                predicate = predicate.lower().strip()
                
                # Erstelle die Beziehung mit Entitätstypen
                relationship = {
                    "subject": subject,
                    "predicate": predicate,
                    "object": object_,
                    "inferred": "implicit",
                    "subject_type": entity_type_map.get(subject, ""),
                    "object_type": entity_type_map.get(object_, ""),
                    "subject_inferred": entity_infer_map.get(subject, "explicit"),
                    "object_inferred": entity_infer_map.get(object_, "explicit")
                }
                
                relationships.append(relationship)
    
    # --------------------------------------------------------------
    # Vorab-Filter: Entferne Dubletten (richtungslos + Prädikat)
    # --------------------------------------------------------------
    def _rel_key(rel):
        return (frozenset([rel.get("subject"), rel.get("object")]), rel.get("predicate", "").lower().strip())

    existing_keys = {_rel_key(r) for r in (existing_relationships or [])}
    filtered = []
    for rel in relationships:
        key = _rel_key(rel)
        if key in existing_keys:
            logger.debug(f"[implicit] Überspringe Dublette aus bestehender Liste: {rel['subject']} - {rel['predicate']} - {rel['object']}")
            continue
        if key in {_rel_key(r) for r in filtered}:
            logger.debug(f"[implicit] Überspringe interne Dublette: {rel['subject']} - {rel['predicate']} - {rel['object']}")
            continue
        filtered.append(rel)
        existing_keys.add(key)

    # Hard limit
    if len(filtered) > max_new_relations:
        logger.info(f"[implicit] Kürze Ergebnis von {len(filtered)} auf {max_new_relations} Beziehungen gemäß Limit")
        filtered = filtered[:max_new_relations]

    logger.info(f"{len(filtered)} valid implicit relationships after pre-filter")
    return filtered
