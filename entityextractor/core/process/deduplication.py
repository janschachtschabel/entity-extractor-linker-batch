"""
deduplication.py

Funktionen zur Deduplizierung von Entitäten und Beziehungen.
"""

import logging
import openai
import time
import difflib
import re
from importlib import import_module
from entityextractor.prompts.deduplication_prompts import (
    get_system_prompt_semantic_dedup_de,
    get_user_prompt_semantic_dedup_de,
    get_system_prompt_semantic_dedup_en,
    get_user_prompt_semantic_dedup_en
)
from entityextractor.utils.openai_utils import call_openai_api

# Maximale Anzahl geloggter Beispiele für entfernte Beziehungen
MAX_LOGGED_REMOVED = 7

def deduplicate_entities(entities):
    """
    Entfernt doppelte Entitäten basierend auf IDs (UUID4).
    """
    seen = set()
    unique_entities = []
    for entity in entities:
        eid = entity.get("id")
        if eid and eid not in seen:
            seen.add(eid)
            unique_entities.append(entity)
    return unique_entities

def deduplicate_relationships(relationships, entities, config):
    """
    Dedupliziert Beziehungen unter Berücksichtigung von expliziten vs. impliziten Beziehungen
    und semantischer Ähnlichkeit.
    
    Args:
        relationships: Liste von Beziehungen
        entities: Liste deduplizierter Entitäten
        config: Konfiguration
        
    Returns:
        Liste deduplizierter Beziehungen
    """
    if not relationships:
        return []
    
    # Schritt 1: Deduplizierung basierend auf exakt gleichen Tripeln,
    # wobei explizite Beziehungen Vorrang haben
    rel_map = {}
    duplicates = []
    
    for rel in relationships:
        key = (rel.get("subject"), rel.get("predicate"), rel.get("object"))
        
        if key in rel_map:
            existing = rel_map[key]
            # Bevorzuge explizite Beziehungen gegenüber impliziten
            if existing.get("inferred") == "implicit" and rel.get("inferred") == "explicit":
                # Ersetze implizite durch explizite Beziehung
                duplicates.append((existing, rel, "implicit_replaced_by_explicit"))
                rel_map[key] = rel
            else:
                # Behalte die bestehende Beziehung
                duplicates.append((rel, existing, "duplicate_exact_triple"))
        else:
            rel_map[key] = rel
    
    deduped_rels = list(rel_map.values())
    
    # Detailliertes Logging zur Nachvollziehbarkeit
    reduction = len(relationships) - len(deduped_rels)
    if reduction > 0:
        logging.info(f"Basisbeziehungs-Deduplizierung: Von {len(relationships)} auf {len(deduped_rels)} reduziert ({reduction} Duplikate entfernt)")
        
        # Zeige Details zu den entfernten Duplikaten
        for i, (removed, kept, reason) in enumerate(duplicates[:5]):
            subj = removed.get("subject", "")
            pred = removed.get("predicate", "")
            obj = removed.get("object", "")
            inf_removed = removed.get("inferred", "unknown")
            
            kept_subj = kept.get("subject", "")
            kept_pred = kept.get("predicate", "")
            kept_obj = kept.get("object", "")
            inf_kept = kept.get("inferred", "unknown")
            
            logging.info(f"  Basis-Deduplikation [{i+1}]: Entfernt '{subj} -- {pred} --> {obj}' ({inf_removed})")
            logging.info(f"    Beibehalten: '{kept_subj} -- {kept_pred} --> {kept_obj}' ({inf_kept})")
            
        if len(duplicates) > 5:
            logging.info(f"  ...und {len(duplicates) - 5} weitere Duplikate (nicht angezeigt)")
    else:
        logging.info(f"Basisbeziehungs-Deduplizierung: Keine exakten Duplikate gefunden in {len(relationships)} Beziehungen")
    
    # Schritt 2: LLM-basierte semantische Deduplizierung, falls konfiguriert
    if config.get("SEMANTIC_DEDUPLICATION", True) and deduped_rels:
        deduped_rels = deduplicate_relationships_llm(deduped_rels, entities, config)
    
    # Schritt 3: Validierung - stellen Sie sicher, dass alle Subjekte und Objekte in der Entitätsliste vorhanden sind
    # Prüfe zuerst, ob wir im generate-Modus sind
    mode = config.get("MODE", "extract")
    is_generate_mode = mode == "generate"
    
    # Im generate-Modus überspringen wir die Validierung und akzeptieren alle Beziehungen
    if is_generate_mode:
        logging.info(f"Generate-Modus erkannt: Überspringe Validierung für {len(deduped_rels)} Beziehungen")
        return deduped_rels
    
    # Für extract-Modus: Normales Validierungsverfahren 
    # Erstelle sowohl original als auch normalisierte Entity-Namen-Sets für robusteres Matching
    entity_names = set()
    entity_names_lower = set()
    
    for e in entities:
        # Berücksichtige verschiedene mögliche Strukturen
        name = ""
        if "entity" in e:
            name = e.get("entity", "")
            entity_names.add(name)
        elif "name" in e:
            name = e.get("name", "")
            entity_names.add(name)
            
        # Füge auch die normalisierte Version hinzu
        if name:
            entity_names_lower.add(name.lower())
            
    valid_relationships = []
    
    for rel in deduped_rels:
        subject = rel.get("subject")
        object_ = rel.get("object")
        subject_lower = subject.lower() if subject else ""
        object_lower = object_.lower() if object_ else ""
        
        # Prüfe sowohl exakte als auch Case-insensitive Matches
        subject_valid = subject in entity_names or subject_lower in entity_names_lower
        object_valid = object_ in entity_names or object_lower in entity_names_lower
        
        if subject_valid and object_valid:
            valid_relationships.append(rel)
        else:
            if not subject_valid:
                logging.debug(f"Ungültige Beziehung entfernt: Subjekt '{subject}' nicht in Entitätsliste")
            if not object_valid:
                logging.debug(f"Ungültige Beziehung entfernt: Objekt '{object_}' nicht in Entitätsliste")
    
    logging.info(f"Beziehungs-Validierung: Von {len(deduped_rels)} auf {len(valid_relationships)} reduziert")
    return valid_relationships

def deduplicate_relationships_llm(relationships, entities, config):
    """
    Verwendet ein LLM, um semantisch ähnliche Beziehungen zu deduplizieren.
    
    Args:
        relationships: Liste von Beziehungen
        entities: Liste von Entitäten
        config: Konfiguration
        
    Returns:
        Liste deduplizierter Beziehungen
    """
    if not relationships or len(relationships) <= 1:
        return relationships
    
    model = config.get("MODEL", "gpt-4.1-mini")
    language = config.get("LANGUAGE", "de")
    
    # Erstelle eine Übersicht der Beziehungen für den Prompt
    relations_text = []
    for i, rel in enumerate(relationships, 1):
        subject = rel.get("subject", "")
        predicate = rel.get("predicate", "")
        object_ = rel.get("object", "")
        inferred = rel.get("inferred", "explicit")
        relations_text.append(f"{i}. {subject} → {predicate} → {object_} ({inferred})")
    
    relations_prompt = "\n".join(relations_text)
    
    # Sprachspezifische Prompts aus der zentralen Prompt-Bibliothek importieren
    if language.startswith("de"):
        system_prompt = get_system_prompt_semantic_dedup_de()
        user_prompt = get_user_prompt_semantic_dedup_de(relations_prompt)
    else:
        system_prompt = get_system_prompt_semantic_dedup_en()
        user_prompt = get_user_prompt_semantic_dedup_en(relations_prompt)
    
    # Rufe die OpenAI API auf
    start_time = time.time()
    response = call_openai_api(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.1,  # Niedrige Temperatur für konsistente Antworten
        config=config
    )
    
    # Extrahiere die zu behaltenden Beziehungen aus der Antwort
    if response:
        answer = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        logging.info(f"LLM-Deduplizierungsantwort erhalten in {time.time() - start_time:.2f} Sekunden")
        
        # Extrahiere die Indizes aus der Antwort
        kept_indices = []
        if "KEPT:" in answer:
            kept_part = answer.split("KEPT:")[1].strip()
            try:
                # Extrahiere alle Zahlen aus der Antwort
                import re
                indices = re.findall(r'\d+', kept_part)
                kept_indices = [int(idx) for idx in indices]
            except:
                logging.warning("Konnte Indizes nicht aus LLM-Antwort extrahieren, behalte alle Beziehungen")
                return relationships
        
        # Behalte nur die angegebenen Beziehungen
        if kept_indices:
            # Korrigiere Indizes (da wir bei 1 statt 0 angefangen haben)
            kept_indices = [idx - 1 for idx in kept_indices if 0 < idx <= len(relationships)]
            deduped_rels = [relationships[idx] for idx in kept_indices]
            
            # Detaillierte Logging-Informationen zur Nachvollziehbarkeit
            reduction = len(relationships) - len(deduped_rels)
            logging.info(f"LLM-Deduplizierung: Von {len(relationships)} auf {len(deduped_rels)} reduziert ({reduction} semantische Duplikate entfernt)")
            
            # Erstelle ein Set der beibehaltenen Indizes für leichtere Prüfung
            kept_set = set(kept_indices)
            
            # Zeige Informationen über entfernte Beziehungen
            removed_count = 0
            for i, rel in enumerate(relationships):
                if i not in kept_set:
                    subj = rel.get("subject", "")
                    pred = rel.get("predicate", "")
                    obj = rel.get("object", "")
                    inf = rel.get("inferred", "unknown")

                    # Finde ähnliche beibehaltene Beziehungen für dieses Subjekt-Objekt-Paar
                    related_kept = []
                    for kidx in kept_indices:
                        krel = relationships[kidx]
                        if (krel.get("subject", "") == subj and krel.get("object", "") == obj) or \
                           (krel.get("subject", "") == obj and krel.get("object", "") == subj):
                            related_kept.append((kidx, krel))

                    if removed_count < MAX_LOGGED_REMOVED:
                        logging.info(f"  LLM-Deduplikation: Entfernt '{subj} -- {pred} --> {obj}' ({inf})")
                        for kidx, krel in related_kept:
                            ksubj = krel.get("subject", "")
                            kpred = krel.get("predicate", "")
                            kobj = krel.get("object", "")
                            kinf = krel.get("inferred", "unknown")
                            logging.info(f"    Beibehalten [{kidx+1}]: '{ksubj} -- {kpred} --> {kobj}' ({kinf})")
                    removed_count += 1

            if removed_count > MAX_LOGGED_REMOVED:
                logging.info(f"  ...und {removed_count - MAX_LOGGED_REMOVED} weitere semantisch ähnliche Beziehungen entfernt (nicht angezeigt)")
            if removed_count == 0:
                logging.info("  LLM-Deduplikation: Keine semantischen Duplikate entfernt – alle Beziehungen wurden beibehalten.")

            logging.info(f"  LLM-Deduplikation: Insgesamt {removed_count} Beziehungen entfernt, {len(deduped_rels)} beibehalten.")
            return deduped_rels
    
    # Bei Fehler oder leerer Antwort, behalte alle Beziehungen
    logging.warning("LLM-Deduplizierung nicht erfolgreich, behalte alle Beziehungen")
    return relationships


def filter_semantically_similar_relationships(relationships, similarity_threshold=0.85):
    """
    Entfernt Beziehungen zwischen denselben Entitäten (unabhängig von Reihenfolge),
    deren Prädikat semantisch/fuzzy sehr ähnlich ist.
    Nur das Triple mit dem "prägnantesten" Prädikat (kürzester String) bleibt erhalten.
    
    Args:
        relationships: Liste von Beziehungen
        similarity_threshold: Schwellenwert für die Ähnlichkeit (0.0-1.0)
        
    Returns:
        Liste deduplizierter Beziehungen
    """
    if not relationships or len(relationships) <= 1:
        return relationships
        
    grouped = defaultdict(list)
    for rel in relationships:
        # Gruppieren nach Entity-Paar unabhängig von Richtung
        key = frozenset([rel["subject"], rel["object"]])
        grouped[key].append(rel)
        
    result = []
    for key_set, rels in grouped.items():
        kept = []
        used = set()
        
        for i, r1 in enumerate(rels):
            if i in used:
                continue
                
            similar = [r1]
            for j, r2 in enumerate(rels):
                if j <= i or j in used:
                    continue
                    
                ratio = difflib.SequenceMatcher(None, r1["predicate"], r2["predicate"]).ratio()
                if ratio >= similarity_threshold:
                    similar.append(r2)
                    used.add(j)
                    
            # Behalte das kürzeste Prädikat (prägnanteste Formulierung)
            shortest = min(similar, key=lambda r: len(r["predicate"]))
            kept.append(shortest)
            used.add(i)
            
        result.extend(kept)
        
    logging.info(f"Semantische Deduplizierung: Von {len(relationships)} auf {len(result)} Beziehungen reduziert")
    return result
