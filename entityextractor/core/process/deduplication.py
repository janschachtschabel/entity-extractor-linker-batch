"""
deduplication.py

Functions for deduplicating entities and relationships.
Supports both the traditional dictionary-based and the
new context-based architecture.
"""

import openai
import time
import difflib
import re
from collections import defaultdict
from typing import List, Dict, Any, Optional, Union, Set, Tuple
from importlib import import_module

from loguru import logger

from entityextractor.prompts.deduplication_prompts import (
    get_system_prompt_semantic_dedup_de,
    get_user_prompt_semantic_dedup_de,
    get_system_prompt_semantic_dedup_en,
    get_user_prompt_semantic_dedup_en
)
from entityextractor.utils.openai_utils import call_openai_api
from entityextractor.core.context import EntityProcessingContext

# Maximum number of logged examples for removed relationships
MAX_LOGGED_REMOVED = 7

def deduplicate_entities(entities):
    """Deduplicate entities and emit an INFO log summarising the reduction."""
    """
    Removes duplicate entities based on IDs (UUID4).
    Supports both dictionary entities and EntityProcessingContext objects.
    
    Args:
        entities: List of entities (Dictionary or EntityProcessingContext)
        
    Returns:
        List of deduplicated entities
    """
    if not entities:
        return []
        
    # Check the type of entities
    if isinstance(entities[0], EntityProcessingContext):
        # Context-based entities
        seen = set()
        unique_contexts = []
        for context in entities:
            if context.entity_id and context.entity_id not in seen:
                seen.add(context.entity_id)
                unique_contexts.append(context)
        reduction = len(entities) - len(unique_contexts)
        logger.info(f"Entity deduplication: Reduced from {len(entities)} to {len(unique_contexts)} ({reduction} duplicates removed)")
        return unique_contexts
    else:
        # Legacy dictionary entities
        seen = set()
        unique_entities = []
        for entity in entities:
            eid = entity.get("id")
            if eid and eid not in seen:
                seen.add(eid)
                unique_entities.append(entity)
        reduction = len(entities) - len(unique_entities)
        logger.info(f"Entity deduplication: Reduced from {len(entities)} to {len(unique_entities)} ({reduction} duplicates removed)")
        return unique_entities

def deduplicate_relationships(relationships, entities, config):
    """
    Deduplicates relationships considering explicit vs. implicit relationships
    and semantic similarity.
    Supports both dictionary entities and EntityProcessingContext objects.
    
    Args:
        relationships: List of relationships
        entities: List of deduplicated entities (Dictionary or EntityProcessingContext)
        config: Configuration
        
    Returns:
        List of deduplicated relationships
    """
    if not relationships:
        return []
    
    # Step 1: Deduplication based on exactly matching triples,
    # where explicit relationships take precedence
    rel_map = {}
    duplicates = []
    
    for rel in relationships:
        # Treat explicit and implicit variants as distinct
        key = (
            rel.get("subject"),
            rel.get("predicate"),
            rel.get("object"),
            # normalise inferred to string for consistency ("explicit" | "implicit")
            str(rel.get("inferred", "explicit")).lower()
        )

        if key in rel_map:
            duplicates.append((rel, rel_map[key], "duplicate_exact_triple"))
        else:
            rel_map[key] = rel
    
    deduped_rels = list(rel_map.values())
    
    # Detailed logging for traceability
    reduction = len(relationships) - len(deduped_rels)
    if reduction > 0:
        logger.info(f"Base relationship deduplication: Reduced from {len(relationships)} to {len(deduped_rels)} ({reduction} duplicates removed)")
        
        # Show details of removed duplicates
        for i, (removed, kept, reason) in enumerate(duplicates[:5]):
            subj = removed.get("subject", "")
            pred = removed.get("predicate", "")
            obj = removed.get("object", "")
            inf_removed = removed.get("inferred", "unknown")
            
            kept_subj = kept.get("subject", "")
            kept_pred = kept.get("predicate", "")
            kept_obj = kept.get("object", "")
            inf_kept = kept.get("inferred", "unknown")
            
            logger.info(f"  Basis-Deduplikation [{i+1}]: Entfernt '{subj} -- {pred} --> {obj}' ({inf_removed})")
            logger.info(f"    Beibehalten: '{kept_subj} -- {kept_pred} --> {kept_obj}' ({inf_kept})")
            
        if len(duplicates) > 5:
            logger.info(f"  ...und {len(duplicates) - 5} weitere Duplikate (nicht angezeigt)")
    else:
        logger.info(f"Basisbeziehungs-Deduplizierung: Keine exakten Duplikate gefunden in {len(relationships)} Beziehungen")
    
    # Zusätzliche INFO-Logs: Beziehungen pro Entitätspaar (richtungsunabhängig)
    pair_groups = defaultdict(list)
    for rel in deduped_rels:
        pair_key = frozenset([rel.get("subject"), rel.get("object")])
        pair_groups[pair_key].append(rel)
    for pair_key, rels in pair_groups.items():
        if len(rels) > 1:
            subj, obj = list(pair_key)
            logger.info(f"[dedup] Vor semantischer Deduplizierung: {subj} <-> {obj} hat {len(rels)} Beziehungen:")
            for r in rels:
                logger.info(f"        - {r.get('predicate')} ({r.get('inferred','explicit')})")

    # Schritt 2: LLM-basierte semantische Deduplizierung, falls konfiguriert
    if config.get("SEMANTIC_DEDUPLICATION", True) and deduped_rels:
        deduped_rels = deduplicate_relationships_llm(deduped_rels, entities, config)
    
    # Schritt 3: Validierung - stellen Sie sicher, dass alle Subjekte und Objekte in der Entitätsliste vorhanden sind
    # Prüfe zuerst, ob wir im generate-Modus sind
    mode = config.get("MODE", "extract")
    is_generate_mode = mode == "generate"
    
    # Im generate-Modus überspringen wir die Validierung und akzeptieren alle Beziehungen
    if is_generate_mode:
        logger.info(f"Generate-Modus erkannt: Überspringe Validierung für {len(deduped_rels)} Beziehungen")
        return deduped_rels
    
    # --------------------------------------------------
    # Optionale Entity-Normalisierung via LLM
    # --------------------------------------------------
    if config.get("ENABLE_ENTITY_NORMALIZATION_PROMPT", False):
        # Build candidate list (subject/object not matching current entity_names sets)
        candidates = set()
        for rel in deduped_rels:
            for side in (rel.get("subject"), rel.get("object")):
                if side and side not in entity_names and _norm_name(side) not in entity_names_normalized:
                    candidates.add(side)
        # Limit to 100 candidates for prompt length
        candidates = list(candidates)[:100]
        if candidates:
            language = config.get("LANGUAGE", "de")
            model = config.get("MODEL", "gpt-4.1-mini")
            # prepare canonical list text
            canonical_lines = [f"{idx+1}) {e} [{e}]" for idx, e in enumerate(entity_names)]
            entity_block = "\n".join(canonical_lines)
            cand_block = "\n".join(candidates)
            system_prompt = (
                "Du bist ein Assistent für Entity-Normalisierung. "
                "Ordne jeden Kandidaten der passenden Entität zu oder gib 'NONE' zurück, falls keine passt."
            ) if language == "de" else (
                "You are an assistant for entity normalization. Map each candidate to the correct entity or return 'NONE'."
            )
            user_prompt = (
                f"Bekannte Entitäten (bitte exakt in eckigen Klammern ausgeben):\n{entity_block}\n\n"
                f"Kandidaten (jeweils eine Zeile):\n{cand_block}\n\n"
                "Antwortformat: Kandidat => [Entität] oder Kandidat => NONE"
            )
            try:
                response = call_openai_api(
                    model=model,
                    messages=[{"role":"system","content":system_prompt},{"role":"user","content":user_prompt}],
                    temperature=0.0,
                    max_tokens=512,
                    config=config,
                )
                mapping = {}
                for line in response.strip().splitlines():
                    if "=>" in line:
                        cand, mapped = [s.strip() for s in line.split("=>",1)]
                        if mapped.upper() != "NONE" and mapped.startswith("[") and mapped.endswith("]"):
                            mapping[cand] = mapped[1:-1]
                if mapping:
                    logger.info(f"LLM-Entity-Normalisierung: {len(mapping)} Kandidaten gemappt")
                    # apply mapping in deduped_rels
                    for rel in deduped_rels:
                        if rel.get("subject") in mapping:
                            rel["subject"] = mapping[rel["subject"]]
                        if rel.get("object") in mapping:
                            rel["object"] = mapping[rel["object"]]
            except Exception as e:
                logger.warning(f"LLM-Entity-Normalisierung fehlgeschlagen: {e}")

    # Für extract-Modus: Normales Validierungsverfahren 
    # Erstelle sowohl original als auch normalisierte Entity-Namen-Sets für robusteres Matching
    # ------------------------------------------------------------------
    # Gather entity names and their normalized variants for fast lookup
    # ------------------------------------------------------------------
    def _norm_name(name: str) -> str:
        """Normalize entity name by lowercasing, trimming and removing suffix brackets.
        This local helper avoids cross-module imports that can lead to circular
        dependencies during runtime."""
        if not name:
            return ""
        result = name.strip().lower()
        # Remove surrounding square brackets like "[Albert Einstein]"
        if result.startswith("[") and result.endswith("]"):
            result = result[1:-1].strip()
        if "(" in result and ")" in result:
            result = result[:result.find("(")].strip()
        return result

    entity_names: Set[str] = set()
    entity_names_normalized: Set[str] = set()

    for ent in entities:
        if isinstance(ent, dict):
            name = ent.get("name") or ent.get("entity")
        else:
            name = getattr(ent, "entity_name", None)
        if not name:
            continue
        entity_names.add(name)
        # Add normalized variants (both original and lowercase) so that
        # relationships using simplified names such as "dualism" instead of
        # "Dualism (theory)" are still considered valid.
        normalized = _norm_name(name)
        if normalized:
            entity_names_normalized.add(normalized)
            entity_names_normalized.add(normalized.lower())

    # Also keep lowercase variants of the original names for case-insensitive match
    entity_names_lower: Set[str] = {n.lower() for n in entity_names}

    valid_relationships = []
    
    for rel in deduped_rels:
        subject = rel.get("subject")
        object_ = rel.get("object")
        subject_lower = subject.lower() if subject else ""
        object_lower = object_.lower() if object_ else ""
        
        # --------------------------------------------------------------
        # Validate subjects/objects against entity list using
        # 1) exact match
        # 2) case-insensitive match
        # 3) match after normalization (bracket removal etc.)
        # --------------------------------------------------------------
        subject_norm = _norm_name(subject_lower)
        object_norm = _norm_name(object_lower)

        subject_valid = (
            subject in entity_names
            or subject_lower in entity_names_lower
            or subject_norm in entity_names_normalized
        )
        object_valid = (
            object_ in entity_names
            or object_lower in entity_names_lower
            or object_norm in entity_names_normalized
        )
        
        if subject_valid and object_valid:
            valid_relationships.append(rel)
        else:
            if not subject_valid:
                logger.debug(f"Ungültige Beziehung entfernt: Subjekt '{subject}' nicht in Entitätsliste")
            if not object_valid:
                logger.debug(f"Ungültige Beziehung entfernt: Objekt '{object_}' nicht in Entitätsliste")
    
    logger.info(f"Beziehungs-Validierung: Von {len(deduped_rels)} auf {len(valid_relationships)} reduziert")
        # --------------------------------------------------------------
    # Final INFO logs: relationship groups per unordered entity pair
    # after all deduplication and validation steps
    # --------------------------------------------------------------
    final_groups = defaultdict(list)
    for rel in valid_relationships:
        pair_key = frozenset([rel.get("subject"), rel.get("object")])
        final_groups[pair_key].append(rel)
    for pair_key, rels in final_groups.items():
        if len(rels) > 1:
            subj, obj = list(pair_key)
            logger.info(f"[dedup] Nach Deduplizierung: {subj} <-> {obj} hat {len(rels)} Beziehungen:")
            for r in rels:
                logger.info(f"        - {r.get('predicate')} ({r.get('inferred','explicit')})")

    return valid_relationships

def deduplicate_relationships_llm(relationships, entities, config):
    """
    Uses an LLM to deduplicate semantically similar relationships.
    
    Args:
        relationships: List of relationships
        entities: List of entities
        config: Configuration
        
    Returns:
        List of deduplicated relationships
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
        logger.info(f"LLM-Deduplizierungsantwort erhalten in {time.time() - start_time:.2f} Sekunden")
        
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
                logger.warning("Konnte Indizes nicht aus LLM-Antwort extrahieren, behalte alle Beziehungen")
                return relationships
        
        # Behalte nur die angegebenen Beziehungen
        if kept_indices:
            # Korrigiere Indizes (da wir bei 1 statt 0 angefangen haben)
            kept_indices = [idx - 1 for idx in kept_indices if 0 < idx <= len(relationships)]
            deduped_rels = [relationships[idx] for idx in kept_indices]
            
            # Detaillierte Logging-Informationen zur Nachvollziehbarkeit
            reduction = len(relationships) - len(deduped_rels)
            logger.info(f"LLM-Deduplizierung: Von {len(relationships)} auf {len(deduped_rels)} reduziert ({reduction} semantische Duplikate entfernt)")
            
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
                        logger.info(f"  LLM-Deduplikation: Entfernt '{subj} -- {pred} --> {obj}' ({inf})")
                        for kidx, krel in related_kept:
                            ksubj = krel.get("subject", "")
                            kpred = krel.get("predicate", "")
                            kobj = krel.get("object", "")
                            kinf = krel.get("inferred", "unknown")
                            logger.info(f"    Beibehalten [{kidx+1}]: '{ksubj} -- {kpred} --> {kobj}' ({kinf})")
                    removed_count += 1

            if removed_count > MAX_LOGGED_REMOVED:
                logger.info(f"  ...und {removed_count - MAX_LOGGED_REMOVED} weitere semantisch ähnliche Beziehungen entfernt (nicht angezeigt)")
            if removed_count == 0:
                logger.info("  LLM-Deduplikation: Keine semantischen Duplikate entfernt – alle Beziehungen wurden beibehalten.")

            logger.info(f"  LLM-Deduplikation: Insgesamt {removed_count} Beziehungen entfernt, {len(deduped_rels)} beibehalten.")
            return deduped_rels
    
    # Bei Fehler oder leerer Antwort, behalte alle Beziehungen
    logger.warning("LLM-Deduplizierung nicht erfolgreich, behalte alle Beziehungen")
    return relationships


def filter_semantically_similar_relationships(relationships, similarity_threshold=0.85):
    """
    Removes relationships between the same entities (regardless of order),
    whose predicates are semantically/fuzzy very similar.
    Only the triple with the "most concise" predicate (shortest string) is kept.
    
    Args:
        relationships: List of relationships
        similarity_threshold: Threshold for similarity (0.0-1.0)
        
    Returns:
        List of deduplicated relationships
    """
    if not relationships or len(relationships) <= 1:
        return relationships
        
    grouped = defaultdict(list)
    for rel in relationships:
        # Group by entity pair regardless of direction
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
                    
            # Keep the shortest predicate (most concise formulation)
            shortest = min(similar, key=lambda r: len(r["predicate"]))
            kept.append(shortest)
            used.add(i)
            
        result.extend(kept)
        
    logger.info(f"Semantic deduplication: Reduced from {len(relationships)} to {len(result)} relationships")
    return result


def deduplicate_relationships_from_contexts(contexts, config):
    """
    Deduplicates relationships from a list of EntityProcessingContext objects.
    
    Args:
        contexts: List of EntityProcessingContext objects
        config: Configuration
        
    Returns:
        List of deduplicated relationships
    """
    if not contexts:
        return []
    
    # Extract all relationships from the contexts
    all_relationships = []
    for context in contexts:
        all_relationships.extend(context.relationships)
    
    # Deduplicate the relationships
    deduped_relationships = deduplicate_relationships(all_relationships, contexts, config)
    
    # Update the relationships in each context
    for context in contexts:
        # Filter the deduplicated relationships for this context
        context_relationships = []
        for rel in deduped_relationships:
            if rel.get("subject") == context.entity_id or rel.get("object") == context.entity_id:
                context_relationships.append(rel)
        
        # Set the deduplicated relationships
        context.relationships = context_relationships
    
    return deduped_relationships
