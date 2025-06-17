"""
OpenAI service module for the Entity Extractor.

This module provides functions for interacting with the OpenAI API
to extract entities from text.
"""

import json
import os
import time
from typing import List, Dict, Any, Optional
from openai import OpenAI
from loguru import logger
import logging

from entityextractor.config.settings import DEFAULT_CONFIG
from entityextractor.utils.text_utils import clean_json_from_markdown
from entityextractor.prompts.extract_prompts import (
    get_system_prompt_en, get_system_prompt_de,
    USER_PROMPT_EN, USER_PROMPT_DE,
    TYPE_RESTRICTION_TEMPLATE_EN, TYPE_RESTRICTION_TEMPLATE_DE
)
from entityextractor.utils.prompt_utils import apply_type_restrictions
from entityextractor.prompts.compendium_prompts import get_educational_block_de, get_educational_block_en


class OpenAIService:
    """
    Service class for OpenAI operations in the Entity Extractor.
    
    This class encapsulates all functions for interacting with the OpenAI API,
    including entity extraction, relationship extraction, and translation functions.
    """
    
    def __init__(self, config=None):
        """
        Initializes the OpenAI service.
        
        Args:
            config: Configuration dictionary with API key and model settings
        """
        from entityextractor.config.settings import DEFAULT_CONFIG
        self.config = config or DEFAULT_CONFIG
        self.api_key = self.config.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
        self.model = self.config.get("MODEL", "gpt-4o-mini")
        self.language = self.config.get("LANGUAGE", "de")
        
        if not self.api_key:
            logger.warning("No OpenAI API key found. Some functions will not be available.")
    
    def extract_entities(self, text):
        """
        Extracts entities from text using the OpenAI API.
        
        Args:
            text: The text from which to extract entities
            
        Returns:
            A list of extracted entities or an empty list in case of errors
        """
        return extract_entities_with_openai(text, self.config)
    
    def extract_relationships(self, entities, text):
        """
        Extrahiert Beziehungen zwischen Entitäten mit der OpenAI-API.
        
        Args:
            entities: Liste von Entitäts-Dictionaries
            text: Der Quelltext für die Beziehungsextraktion
            
        Returns:
            Eine Liste von Beziehungstripeln (Subjekt, Prädikat, Objekt) mit Entitätstypen
        """
        return extract_relationships_with_openai(entities, text, self.config)
    
    async def generate_synonyms(self, entity_name: str) -> List[str]:
        """
        Generiert Synonyme für einen Entitätsnamen unter Berücksichtigung von Wikidata-Namenskonventionen.
        
        Args:
            entity_name: Der Name der Entität
            
        Returns:
            Eine Liste von Synonymen oder eine leere Liste bei Fehlern
        """
        if not self.api_key or not entity_name:
            return []
            
        try:
            client = OpenAI(api_key=self.api_key)
            
            # System-Prompt für die Generierung von Wikidata-kompatiblen Synonymen
            system_prompt = (
                "Du bist ein hilfreicher Assistent, der Fachbegriffe in verschiedene Schreibweisen und Formate bringt, "
                "wie sie in Wikidata vorkommen. Generiere bis zu 10 alternative Bezeichnungen für den gegebenen Begriff, "
                "die in Wikidata-Suchen verwendet werden könnten. Berücksichtige dabei:"
                "\n- Alternative Schreibweisen (z.B. mit/ohne Bindestriche, Leerzeichen, Schrägstriche)"
                "\n- Abkürzungen und deren ausgeschriebene Formen"
                "\n- Synonyme in derselben Sprache"
                "\n- Englische Übersetzungen bei deutschen Begriffen"
                "\n- Wissenschaftliche und umgangssprachliche Bezeichnungen"
                "\n- Singular/Plural-Formen"
                "\n- Alternative Schreibweisen mit Sonderzeichen"
                "\n\nAntworte NUR mit einer JSON-Liste der Synonyme, ohne weitere Erklärungen oder zusätzlichen Text."
            )
            
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Generiere Wikidata-kompatible Synonyme für: {entity_name}"}
                ],
                temperature=0.7,
                max_tokens=200,
                response_format={"type": "json_object"}
            )
            
            # Extrahiere die Antwort und parse das JSON
            content = response.choices[0].message.content
            try:
                result = json.loads(content)
                # Versuche verschiedene mögliche JSON-Formate zu parsen
                if isinstance(result, dict):
                    if "synonyms" in result:
                        return result["synonyms"]
                    elif "alternatives" in result:
                        return result["alternatives"]
                    else:
                        # Nehme alle Werte, die Listen sind
                        for value in result.values():
                            if isinstance(value, list):
                                return value
                return []
            except json.JSONDecodeError:
                # Falls die Antwort kein gültiges JSON ist, versuche es mit Zeilenweise-Parsing
                lines = [line.strip('"\' ,') for line in content.split('\n') if line.strip()]
                return [line for line in lines if line and len(line) > 1]
                
        except Exception as e:
            logging.error(f"Fehler bei der Synonym-Generierung für '{entity_name}': {str(e)}")
            return []
    
    def translate(self, text, target_language):
        """
        Translates a text into a target language.
        
        Args:
            text: The text to be translated
            target_language: The target language (e.g., 'en', 'de')
            
        Returns:
            The translated text or None in case of errors
        """
        # Simple implementation - can be extended later
        return None
        
    async def generate_wikidata_synonyms(self, term: str, language: str = 'de') -> list:
        """
        Generates synonyms for a search term to be used in Wikidata search.
        
        Args:
            term: The search term for which synonyms should be generated
            language: The language of the search term ('de' or 'en')
            
        Returns:
            A list of synonyms or an empty list in case of errors
        """
        if not self.api_key:
            logger.warning("No OpenAI API key configured. Cannot generate synonyms.")
            return []
            
        try:
            client = OpenAI(api_key=self.api_key)
            
            # System prompt based on language
            if language == 'de':
                system_prompt = (
                    "Du bist ein hilfreicher Assistent, der Suchbegriffe für die Suche in der Wissensdatenbank "
                    "Wikidata generiert. Erstelle 3-5 alternative Suchbegriffe oder Schreibweisen für den gegebenen "
                    "Begriff. Gib die Begriffe als JSON-Array zurück."
                )
                user_prompt = f"Generiere Synonyme für den Begriff: {term}"
            else:
                system_prompt = (
                    "You are a helpful assistant that generates search terms for querying the Wikidata "
                    "knowledge base. Create 3-5 alternative search terms or spellings for the given term. "
                    "Return the terms as a JSON array."
                )
                user_prompt = f"Generate synonyms for the term: {term}"
            
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=150,
                response_format={"type": "json_object"}
            )
            
            # Extrahiere die Antwort und parse das JSON
            content = response.choices[0].message.content
            result = json.loads(content)
            
            # Extract synonyms from the response
            synonyms = []
            for key, value in result.items():
                if isinstance(value, list):
                    synonyms.extend(value)
                else:
                    synonyms.append(value)
            
            # Remove duplicates and the original term
            synonyms = list({s.strip() for s in synonyms if s.strip().lower() != term.lower()})
            
            logger.debug(f"Generated synonyms for '{term}': {synonyms}")
            return synonyms[:5]  # Limit to maximum 5 synonyms
            
        except Exception as e:
            logger.error(f"Error generating synonyms for '{term}': {str(e)}")
            return []

def extract_entities_with_openai(text, config=None):
    """
    Extract entities from text using OpenAI's API.
    
    Args:
        text: The text to extract entities from
        config: Configuration dictionary with API key and model settings
        
    Returns:
        A list of extracted entities or an empty list if extraction failed
    """
    # Ensure we merge with default config
    from entityextractor.config.settings import get_config
    config = get_config(config)
        
    api_key = config.get("OPENAI_API_KEY")
    if not api_key:
        api_key = os.environ.get("OPENAI_API_KEY")
        
    if not api_key:
        logger.error("No OpenAI API key provided. Set OPENAI_API_KEY in config or environment.")
        return []
        
    model = config.get("MODEL")  # No fallback needed, will use the one from default config
    language = config.get("LANGUAGE", "de")
    max_entities = config.get("MAX_ENTITIES", 10)
    allowed_entity_types = config.get("ALLOWED_ENTITY_TYPES", "auto")
    
    # LLM-Konfigurationsmerkmale
    base_url = config.get("LLM_BASE_URL", "https://api.openai.com/v1")
    max_tokens = config.get("MAX_TOKENS", 12000)
    temperature = config.get("TEMPERATURE", None)

    # Create the OpenAI client
    client = OpenAI(api_key=api_key, base_url=base_url)
    
    # Prüfe den Modus (extract oder generate)
    mode = config.get("MODE", "extract")
    
    # Wenn der Modus explizit auf "extract" gesetzt ist, stellen wir sicher, dass wir im Extraktionsmodus sind
    if mode != "extract" and mode != "generate":
        logger.warning(f"Unknown MODE '{mode}' specified. Defaulting to 'extract'.")
        mode = "extract"
    
    # Build system prompt and user message
    system_prompt = get_system_prompt_en(max_entities) if language == "en" else get_system_prompt_de(max_entities)
    system_prompt = apply_type_restrictions(system_prompt, allowed_entity_types, language)
    
    # Bildungsmodus: Zusätzliche Strukturierungsaspekte für Bildungswissen hinzufügen
    if config.get("COMPENDIUM_EDUCATIONAL_MODE", False):
        edu_block = get_educational_block_de() if language == "de" else get_educational_block_en()
        system_prompt = f"{system_prompt.strip()}\n\n{edu_block}"
    
    user_msg = USER_PROMPT_EN.format(text=text) if language == "en" else USER_PROMPT_DE.format(text=text)

    try:
        start_time = time.time()
        logger.info(f"Extracting entities with OpenAI model {model}...")
        
        # Messages for OpenAI request
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg}
        ]
        # LLM-Request: max_tokens und base_url immer setzen, temperature nur wenn angegeben
        # Nur Modelle mit JSON-Mode erlauben response_format
        json_mode_models = [
            "gpt-3.5-turbo-1106", "gpt-3.5-turbo-0125", "gpt-4-1106-preview", "gpt-4-turbo-preview", "gpt-4-0125-preview", "gpt-4o", "gpt-4o-2024-05-13"
        ]
        openai_kwargs = dict(
            model=model,
            messages=messages,
            stream=False,
            stop=None,
            timeout=60,
            max_tokens=max_tokens
        )
        if model in json_mode_models:
            openai_kwargs["response_format"] = {"type": "json_object"}

        if temperature is not None:
            openai_kwargs["temperature"] = temperature
        response = client.chat.completions.create(**openai_kwargs)
        
        # Parse semicolon-separated entity lines
        raw_output = response.choices[0].message.content.strip()
        lines = raw_output.splitlines()
        processed_entities = []
        for ln in lines:
            parts = [p.strip() for p in ln.split(";")]
            if len(parts) >= 6:
                # New unified format: name_de; name_en; type; url_de; url_en; citation
                name_de, name_en, typ, url_de, url_en, citation = parts[:6]
                inferred_flag = "explicit" if mode == "extract" else "implicit"
                if language == "de":
                    primary_name = name_de or name_en
                    primary_url = url_de or url_en
                else:
                    primary_name = name_en or name_de
                    primary_url = url_en or url_de
                processed_entities.append({
                    "name": primary_name,
                    "label_de": name_de,
                    "label_en": name_en,
                    "type": typ,
                    "wikipedia_url": primary_url,
                    "wikipedia_url_de": url_de,
                    "wikipedia_url_en": url_en,
                    "citation": citation,
                    "inferred": inferred_flag
                })
            elif len(parts) >= 4:
                # Fallback to old format for backward compatibility
                name, typ, url, citation = parts[:4]
                inferred_flag = "explicit" if mode == "extract" else "implicit"
                processed_entities.append({
                    "name": name,
                    "type": typ,
                    "wikipedia_url": url,
                    "citation": citation,
                    "inferred": inferred_flag
                })
        elapsed_time = time.time() - start_time
        logger.info(f"Extracted {len(processed_entities)} entities in {elapsed_time:.2f} seconds")
        # Save training data if enabled
        if config.get("COLLECT_TRAINING_DATA", False):
            save_training_data(text, processed_entities, config)
        return processed_entities
    except Exception as e:
        logger.error(f"Error calling OpenAI API: {e}")
        return []

def save_training_data(text, entities, config=None):
    """
    Save training data for future fine-tuning.
    
    Args:
        text: The input text
        entities: The extracted entities
        config: Configuration dictionary with training data path
    """
    if config is None:
        from entityextractor.config.settings import DEFAULT_CONFIG
        config = DEFAULT_CONFIG
        
    training_data_path = config.get("TRAINING_DATA_PATH", "entity_extractor_training_data.jsonl")
    
    try:
        # Get system prompt based on language
        language = config.get("LANGUAGE", "de")
        system_prompt = ""
        
        if language == "en":
            system_prompt = "You are a helpful AI system for recognizing and linking entities. Your task is to identify the most important entities from a given text and link them to their Wikipedia pages."
        else:
            system_prompt = "Du bist ein hilfreiches KI-System zur Erkennung und Verknüpfung von Entitäten. Deine Aufgabe ist es, die wichtigsten Entitäten aus einem gegebenen Text zu identifizieren und mit ihren Wikipedia-Seiten zu verknüpfen."
        
        # Build semicolon-separated assistant content (multilingual schema)
        # Format: name_de; name_en; type; wikipedia_url_de; wikipedia_url_en; citation
        assistant_content = "\n".join(
            f"{ent.get('label_de','')}; {ent.get('label_en','')}; {ent['type']}; "
            f"{ent.get('wikipedia_url_de','')}; {ent.get('wikipedia_url_en','')}; "
            f"{ent.get('citation','')}" for ent in entities
        )
        example = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Identify the main entities in the following text as semicolon-separated lines: name_de; name_en; type; wikipedia_url_de; wikipedia_url_en; citation. Text: {text}"},
                {"role": "assistant", "content": assistant_content}
            ]
        }
        
        # Speichere nur im OpenAI-Format
        training_data_path = config.get("OPENAI_TRAINING_DATA_PATH", "entity_extractor_openai_format.jsonl")  # Path to JSONL file for training data
        with open(training_data_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(example, ensure_ascii=False) + "\n")
            
        logging.info(f"Saved training example to {training_data_path}")
    except Exception as e:
        logging.error(f"Error saving training data: {e}")

def save_relationship_training_data(system_prompt, user_prompt, relationships, config=None):
    """
    Save training data for relationship inference.

    Args:
        system_prompt: The system prompt used for relation inference
        user_prompt: The user prompt used for relation inference
        relationships: List of relationship dicts
        config: Configuration dictionary
    """
    if config is None:
        from entityextractor.config.settings import DEFAULT_CONFIG
        config = DEFAULT_CONFIG
    training_data_path = config.get("OPENAI_RELATIONSHIP_TRAINING_DATA_PATH", "entity_relationship_training_data.jsonl")
    try:
        # Build semicolon-separated assistant content for relationships
        assistant_content = "\n".join(
            f"{rel['subject']}; {rel['predicate']}; {rel['object']}" for rel in relationships
        )
        example = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": assistant_content}
            ]
        }
        with open(training_data_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(example, ensure_ascii=False) + "\n")
        logging.info(f"Saved relationship training example to {training_data_path}")
    except Exception as e:
        logging.error(f"Error saving relationship training data: {e}")


def extract_relationships_with_openai(entities, text, config=None):
    """
    Extract relationships between entities using OpenAI's API.
    
    Args:
        entities: List of entity dictionaries
        text: The source text to extract relationships from
        config: Configuration dictionary with API key and model settings
        
    Returns:
        A list of relationship triplets (subject, predicate, object) with entity types
    """
    # Ensure we merge with default config
    from entityextractor.config.settings import get_config
    config = get_config(config)
        
    api_key = config.get("OPENAI_API_KEY")
    if not api_key:
        api_key = os.environ.get("OPENAI_API_KEY")
        
    if not api_key:
        logger.error("No OpenAI API key provided. Set OPENAI_API_KEY in config or environment.")
        return []
        
    model = config.get("MODEL")  # No fallback needed, will use the one from default config
    language = config.get("LANGUAGE", "de")
    max_relations = config.get("MAX_RELATIONSHIPS", 20)
    
    # LLM-Konfigurationsmerkmale
    base_url = config.get("LLM_BASE_URL", "https://api.openai.com/v1")
    max_tokens = config.get("MAX_TOKENS", 4000)
    temperature = config.get("TEMPERATURE", 0.2)

    # Create the OpenAI client
    client = OpenAI(api_key=api_key, base_url=base_url)
    
    # Erstelle die Entitätsliste für den Prompt
    entity_list = "\n".join([f"- {e['entity']} (Typ: {e['details']['typ']})" for e in entities])
    
    # Erstelle eine Zuordnungstabelle für die Entitätstypen
    entity_type_map = {e['entity']: e['details']['typ'] for e in entities}
    
    # Systemanweisung für die Beziehungsextraktion
    if language == "en":
        system_prompt = f"""You are a helpful assistant that extracts relationships between entities from text. 

Your task is to identify explicit and implicit relationships between entities in the given text. 
- Explicit relationships are directly mentioned in the text.
- Implicit relationships can be inferred from the context but are not explicitly stated.

For each relationship, provide:
1. Subject: The entity that is the subject of the relationship
2. Predicate: The relationship between the subject and object (always lowercase)
3. Object: The entity that is the object of the relationship
4. Inference type: Whether the relationship is "explicit" or "implicit"

Rules:
- BOTH subject AND object MUST be from the provided entity list
- Do NOT invent new entities
- Use ONLY entity names exactly as they appear in the list
- Predicates should be lowercase and concise
- At least 30% of relationships should be implicit
- Maximum number of relationships: {max_relations}
"""
        user_prompt = f"""Here is the text:

{text}

And here is the list of entities:
{entity_list}

Extract the relationships between these entities in JSON format:
```json
{{
  "relationships": [
    {{
      "subject": "Entity A",
      "predicate": "relation to",
      "object": "Entity B",
      "inferred": "explicit or implicit"
    }},
    // more relationships...
  ]
}}
```"""
    else:
        system_prompt = f"""Du bist ein hilfreicher Assistent, der Beziehungen zwischen Entitäten aus einem Text extrahiert.

Deine Aufgabe ist es, explizite und implizite Beziehungen zwischen Entitäten im gegebenen Text zu identifizieren.
- Explizite Beziehungen werden direkt im Text erwähnt.
- Implizite Beziehungen können aus dem Kontext abgeleitet werden, sind aber nicht explizit genannt.

Für jede Beziehung gib an:
1. Subjekt: Die Entität, die das Subjekt der Beziehung ist
2. Prädikat: Die Beziehung zwischen Subjekt und Objekt (immer kleingeschrieben)
3. Objekt: Die Entität, die das Objekt der Beziehung ist
4. Inferenztyp: Ob die Beziehung "explicit" oder "implicit" ist

Regeln:
- SOWOHL Subjekt ALS AUCH Objekt MÜSSEN aus der bereitgestellten Entitätsliste stammen
- Erfinde KEINE neuen Entitäten
- Verwende NUR Entitätsnamen exakt wie sie in der Liste erscheinen
- Prädikate sollten kleingeschrieben und prägnant sein
- Mindestens 30% der Beziehungen sollten implizit sein
- Maximale Anzahl an Beziehungen: {max_relations}
"""
        user_prompt = f"""Hier ist der Text:

{text}

Und hier ist die Liste der Entitäten:
{entity_list}

Extrahiere die Beziehungen zwischen diesen Entitäten im JSON-Format:
```json
{{
  "relationships": [
    {{
      "subject": "Entität A",
      "predicate": "beziehung zu",
      "object": "Entität B",
      "inferred": "explicit oder implicit"
    }},
    // weitere Beziehungen...
  ]
}}
```"""

    try:
        start_time = time.time()
        logger.info(f"Extracting relationships with OpenAI model {model}...")
        
        # Messages for OpenAI request
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        # LLM-Request
        json_mode_models = [
            "gpt-3.5-turbo-1106", "gpt-3.5-turbo-0125", "gpt-4-1106-preview", 
            "gpt-4-turbo-preview", "gpt-4-0125-preview", "gpt-4o", "gpt-4o-2024-05-13"
        ]
        openai_kwargs = dict(
            model=model,
            messages=messages,
            stream=False,
            stop=None,
            timeout=60,
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        if model in json_mode_models:
            openai_kwargs["response_format"] = {"type": "json_object"}
            
        # Make the API call
        response = client.chat.completions.create(**openai_kwargs)
        
        # Process the response
        result = response.choices[0].message.content
        relationship_data = clean_json_from_markdown(result)
        
        if isinstance(relationship_data, str):
            relationship_data = json.loads(relationship_data)
            
        relationships = relationship_data.get("relationships", [])
        
        # Post-process: keep only relationships whose subject AND object exist in the provided entity list
        filtered_relationships = []
        for rel in relationships:
            subject = rel.get("subject")
            object_ = rel.get("object")

            if subject in entity_type_map and object_ in entity_type_map:
                # Enrich with entity types
                rel["subject_type"] = entity_type_map[subject]
                rel["object_type"] = entity_type_map[object_]
                filtered_relationships.append(rel)
            else:
                logger.debug(
                    f"Discarding relationship because of unknown entity: {subject} — {rel.get('predicate')} — {object_}"
                )
        relationships = filtered_relationships

        # ------------------------------
        # Optional deduplication step
        # ------------------------------
        if config.get("STATISTICS_DEDUPLICATE_RELATIONSHIPS", True):
            try:
                before = len(relationships)
                if before:
                    from entityextractor.core.process.deduplication import deduplicate_relationships
                    relationships = deduplicate_relationships(relationships, entities, config)
                    logger.info(
                        f"Relationship deduplication: Reduced from {before} to {len(relationships)} relationships"
                    )
            except Exception as dedup_err:
                logger.warning(f"Relationship deduplication skipped due to error: {dedup_err}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Extracted {len(relationships)} relationships in {elapsed_time:.2f} seconds")
        
        # Save training data if enabled
        if config.get("COLLECT_TRAINING_DATA", False):
            save_relationship_training_data(system_prompt, user_prompt, relationships, config)
            
        return relationships
    except Exception as e:
        logger.error(f"Error extracting relationships with OpenAI: {e}")
        return []
