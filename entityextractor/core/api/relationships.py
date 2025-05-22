"""
relationships.py

API-Modul für die Beziehungsextraktion und -inferenz zwischen Entitäten.

Dies ist die offizielle Implementierung für die Beziehungsinferenz im Entity Extractor.
Sie wird von der Orchestrator-Komponente und anderen Kernmodulen verwendet, um
Beziehungen zwischen extrahierten Entitäten zu identifizieren und zu inferieren.

Zentrales Feature ist die Unterscheidung zwischen expliziten (direkt im Text genannten)
und impliziten (aus Wissen abgeleiteten) Beziehungen zwischen Entitäten.
"""

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
    Inferiert Beziehungen zwischen extrahierten Entitäten.
    
    Args:
        entities: Liste von Entitäten, zwischen denen Beziehungen inferiert werden sollen
        text: Optional - der Ursprungstext für explizite Beziehungen
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von Beziehungen als Tripel (subject, predicate, object)
    """
    if not entities:
        return []
    
    config = get_config(config)
    
    # Beziehungen nur inferieren, wenn aktiviert
    if not config.get("RELATION_EXTRACTION", True):
        logging.info("Entity Relationship Inference deaktiviert.")
        return []
    
    # Nur Entitäten mit bestimmten Typen verwenden, falls konfiguriert
    allowed_types = config.get("ALLOWED_ENTITY_TYPES", "auto")
    
    # Berücksichtige verschiedene mögliche Strukturen der Entitäten
    if allowed_types != "auto" and isinstance(allowed_types, list):
        filtered_entities = []
        for e in entities:
            # Prüfe verschiedene mögliche Strukturen der Entität
            if "details" in e and "typ" in e.get("details", {}):
                entity_type = e.get("details", {}).get("typ", "")
            else:
                entity_type = e.get("type", "")
                
            if entity_type in allowed_types:
                filtered_entities.append(e)
    else:
        filtered_entities = entities
    
    # Erstelle Mapping von Entitätsnamen zu Typen für spätere Vervollständigung der Tripel
    entity_type_map = {}
    entity_infer_map = {}
    
    for e in filtered_entities:
        name = e.get("name", "")
        if not name:
            continue
            
        # Prüfe verschiedene mögliche Strukturen für den Typ
        if "details" in e and "typ" in e.get("details", {}):
            entity_type = e.get("details", {}).get("typ", "")
        else:
            entity_type = e.get("type", "")
            
        # Prüfe verschiedene mögliche Strukturen für inferred
        if "details" in e and "inferred" in e.get("details", {}):
            inferred = e.get("details", {}).get("inferred", "explicit")
        else:
            inferred = e.get("inferred", "explicit")
            
        entity_type_map[name] = entity_type
        entity_infer_map[name] = inferred
    
    logging.info(f"Extrahierte {len(filtered_entities)} Entitäten für Beziehungsextraktion")
    logging.info(f"Erstellt Entitätstyp-Map mit {len(entity_type_map)} Einträgen")
    logging.info(f"Erstellt Entität-Inferenz-Map mit {len(entity_infer_map)} Einträgen")
    
    # Erstelle eine Hilfsfunktion zur Normalisierung von Entitätsnamen für besseres Matching
    def normalize_entity_name(name):
        """Normalisiert einen Entitätsnamen für robusteres Matching.
        
        Entfernt Klammerausdrücke wie in "dualismus (theorie)" -> "dualismus"
        und normalisiert Groß-/Kleinschreibung.
        """
        if not name:
            return ""
            
        # Grundnormalisierung (Kleinschreibung und Leerzeichen entfernen)
        result = name.strip().lower()
        
        # Entferne Suffix-Klammern wie in "dualismus (theorie)" -> "dualismus"
        if "(" in result and ")" in result:
            # Finde die Position der ersten Klammer
            bracket_start = result.find("(")
            # Extrahiere nur den Text vor der Klammer
            result = result[:bracket_start].strip()
            
        return result
    
    # Extrahiere explizite Beziehungen, wenn Text vorhanden
    all_relationships = []
    explicit_rels = []
    implicit_rels = []
    
    # Prüfe die Konfigurationsparameter für die Beziehungsextraktion
    relation_extraction_enabled = (
        config.get("RELATION_EXTRACTION", False) or 
        config.get("ENABLE_RELATIONS_INFERENCE", False)
    )
    
    # Logging für die Beziehungsextraktion-Konfiguration
    logging.info(f"Beziehungsextraktion aktiv: {relation_extraction_enabled} (RELATION_EXTRACTION={config.get('RELATION_EXTRACTION', False)}, ENABLE_RELATIONS_INFERENCE={config.get('ENABLE_RELATIONS_INFERENCE', False)})")
    
    if relation_extraction_enabled:
        if text:
            logging.info("Starte Entity Relationship Inference mit Text...")
            explicit_rels = extract_explicit_relationships(filtered_entities, text, entity_type_map, entity_infer_map, config)
            all_relationships.extend(explicit_rels)
            logging.info(f"Extrahierte {len(explicit_rels)} explizite Beziehungen")
        else:
            logging.info("Kein Text für explizite Beziehungen vorhanden, überspringe explizite Extraktion.")
    
    # Extrahiere implizite Beziehungen, wenn aktiviert
    if config.get("ENABLE_RELATIONS_INFERENCE", False):
        logging.info("Starte implizite Beziehungsextraktion...")
        implicit_rels = extract_implicit_relationships(filtered_entities, entity_type_map, entity_infer_map, config)
        all_relationships.extend(implicit_rels)
        logging.info(f"Extrahierte {len(implicit_rels)} implizite Beziehungen")
    else:
        logging.info("Implizite Beziehungsextraktion deaktiviert.")
        
    # Dedupliziere alle Beziehungen mit dem LLM-basierten Verfahren
    # Dynamischer Import um zirkuläre Importe zu vermeiden
    deduplication_module = importlib.import_module("entityextractor.core.process.deduplication")
    relationships = deduplication_module.deduplicate_relationships(all_relationships, filtered_entities, config)
    
    # Hilfsfunktion zur Normalisierung von Entitätsnamen für besseres Matching
    def normalize_entity_name(name):
        """Normalisiert einen Entitätsnamen für robusteres Matching.
        
        Entfernt Klammerausdrücke wie in "dualismus (theorie)" -> "dualismus"
        und normalisiert Groß-/Kleinschreibung.
        """
        if not name:
            return ""
            
        # Grundnormalisierung (Kleinschreibung und Leerzeichen entfernen)
        result = name.strip().lower()
        
        # Entferne Suffix-Klammern wie in "dualismus (theorie)" -> "dualismus"
        if "(" in result and ")" in result:
            # Finde die Position der ersten Klammer
            bracket_start = result.find("(")
            # Extrahiere nur den Text vor der Klammer
            result = result[:bracket_start].strip()
            
        return result
    
    # Erstelle normalisierte Versionen der Entitätsnamen für robusteres Matching
    logging.info("Erstelle normalisierte Entity-Maps für robusteres Matching...")
    entity_names = set(entity_type_map.keys())
    entity_names_normalized = {normalize_entity_name(name): name for name in entity_names}
    entity_id_map = {e.get("name", ""): e.get("id", "") for e in filtered_entities}
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
            
        print(f"Gebe {len(valid_relationships)} validierte Beziehungen zurück")
        return valid_relationships
    else:
        logging.warning("Keine gültigen Beziehungen gefunden!")
        return []

def extract_explicit_relationships(entities, text, entity_type_map, entity_infer_map, config):
    """
    Extrahiert explizite Beziehungen zwischen Entitäten im Text.
    Im "extract"-Modus werden explizite Beziehungen extrahiert.
    Im "generate"-Modus werden implizite Beziehungen extrahiert (auch im ersten Prompt).
    
    Args:
        entities: Liste von Entitäten
        text: Der Text, in dem nach Beziehungen gesucht wird
        entity_type_map: Mapping von Entitätsnamen zu Typen
        entity_infer_map: Mapping von Entitätsnamen zu Inferenz-Status
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von expliziten oder impliziten Beziehungen, je nach Modus
    """
    model = config.get("MODEL", "gpt-4.1-mini")
    language = config.get("LANGUAGE", "de")
    max_relations = config.get("MAX_RELATIONS", 20)
    temperature = 0.2  # Niedrige Temperatur für konsistente Antworten
    
    logging.info(f"Rufe OpenAI API für explizite Beziehungen auf (Modell {model})...")
    
    # Erstelle den Prompt für explizite Beziehungen
    entity_list = "\n".join([f"- {e.get('name')} ({e.get('type', 'Entity')})" for e in entities])
    
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
        logging.info(f"Erhaltene Antwort (explizit): {answer[:100]}...")
        logging.info(f"Erster Prompt abgeschlossen in {elapsed:.2f} Sekunden")
        
        # Verarbeite die Antwort Zeile für Zeile
        for line in answer.strip().split("\n"):
            if not line or ";" not in line:
                continue
            
            parts = [p.strip() for p in line.split(";")]
            if len(parts) >= 3:
                subject, predicate, object_ = parts[0], parts[1], parts[2]
                
                # Normalisierung des Prädikats
                predicate = predicate.lower().strip()
                
                # Bestimme den Inferenztyp basierend auf dem Modus
                mode = config.get("MODE", "extract")
                inferred_type = "implicit" if mode == "generate" else "explicit"
                
                # Bestimme den Inferenz-Status für Subjekt und Objekt basierend auf dem Modus
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
    
    logging.info(f"{len(relationships)} gültige explizite Beziehungen gefunden")
    return relationships

def extract_implicit_relationships(entities, entity_type_map, entity_infer_map, config):
    """
    Inferiert implizite Beziehungen zwischen Entitäten basierend auf KG-Wissen.
    
    Args:
        entities: Liste von Entitäten
        entity_type_map: Mapping von Entitätsnamen zu Typen
        entity_infer_map: Mapping von Entitätsnamen zu Inferenz-Status
        config: Konfigurationswörterbuch
        
    Returns:
        Liste von impliziten Beziehungen
    """
    model = config.get("MODEL", "gpt-4.1-mini")
    language = config.get("LANGUAGE", "de")
    max_relations = config.get("MAX_RELATIONS", 20)
    temperature = 0.3  # Leicht höhere Temperatur für mehr Variation bei impliziten Beziehungen
    
    logging.info(f"Rufe OpenAI API für implizite Beziehungen auf (Modell {model})...")
    
    # Erstelle den Prompt für implizite Beziehungen
    entity_list = "\n".join([f"- {e.get('name')} ({e.get('type', 'Entity')})" for e in entities])
    
    # Erstelle Entitätsinformationen für den Prompt
    entity_info = []
    for entity in entities:
        name = entity.get("name", "")
        description = entity.get("wikipedia_extract", "")[:150]  # Gekürzte Beschreibung
        if name and description:
            entity_info.append(f"- {name}: {description}...")
    
    entity_info_text = "\n".join(entity_info)
    
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

        user_prompt = f"""Hier sind die extrahierten Entitäten mit kurzen Beschreibungen:

{entity_info_text}

Entitätsliste:
{entity_list}

Bitte identifiziere IMPLIZITE Beziehungen zwischen diesen Entitäten, basierend auf allgemeinem Wissen. Vermeide offensichtliche oder triviale Beziehungen. Gib die Beziehungen im Format "Subjekt; Prädikat; Objekt" an."""
    else:
        system_prompt = """You are an assistant for creating knowledge graphs. Your task is to identify IMPLICIT relationships between entities, based on general knowledge and context.

Important rules:
1. Subject and object MUST be from the provided entity list
2. Do not use entities that are not in the list
3. Infer relationships based on general knowledge
4. Provide the relationships in the format "Subject; Predicate; Object", one per line
5. Use EXACTLY the names from the entity list for subject and object
6. Use only lowercase predicates in the third person singular (e.g. "contains", "is_part_of")
7. Do not use explanatory sentences or introductions"""

        user_prompt = f"""Here are the extracted entities with short descriptions:

{entity_info_text}

Entity list:
{entity_list}

Please identify IMPLICIT relationships between these entities, based on general knowledge. Avoid obvious or trivial relationships. Provide the relationships in the format "Subject; Predicate; Object"."""
    
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
        logging.info(f"Erhaltene Antwort (implizit): {answer[:100]}...")
        logging.info(f"Zweiter Prompt abgeschlossen in {elapsed:.2f} Sekunden")
        
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
    
    logging.info(f"{len(relationships)} gültige implizite Beziehungen gefunden")
    return relationships
