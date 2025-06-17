#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compendium Service for generating comprehensive text about a topic based on extracted entities.

This service uses OpenAI's API to generate a compendium (comprehensive text) about a topic
based on the extracted entities and their relationships.
"""

import os
import time
from typing import Dict, List, Any, Optional, Tuple

from loguru import logger
from openai import OpenAI

from entityextractor.config.settings import get_config
from entityextractor.prompts.compendium_prompts import get_system_prompt_compendium_de, get_system_prompt_compendium_en

def generate_compendium(topic: str, entities: List[Dict[str, Any]], relationships: List[Dict[str, Any]], user_config: Optional[Dict[str, Any]] = None) -> Tuple[str, List[str]]:
    """
    Generate a comprehensive text about a topic based on extracted entities and relationships.
    
    Args:
        topic: The main topic for the compendium
        entities: List of extracted entities with their sources
        relationships: List of relationships between entities
        user_config: Optional configuration dictionary
        
    Returns:
        Tuple containing the generated compendium text and a list of references
    """
    config = get_config(user_config)
    api_key = config.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key, base_url=config.get("LLM_BASE_URL"))
    length = config.get("COMPENDIUM_LENGTH", 8000)
    temperature = config.get("TEMPERATURE", 0.2)

    # Build knowledge context from extracted entities and relationships
    knowledge_parts = []
    for e in entities:
        parts = []
        src = e.get("sources", {})
        wp = src.get("wikipedia", {})
        if wp.get("extract"):
            parts.append(f"Wikipedia extract for {e.get('entity')}: {wp.get('extract')}")
        if wp.get("url"):
            parts.append(f"Wikipedia URL for {e.get('entity')}: {wp.get('url')}")
        if wp.get("categories"):
            parts.append(f"Categories for {e.get('entity')}: {', '.join(wp.get('categories', []))}")
        wd = src.get("wikidata", {})
        if wd.get("id"):
            parts.append(f"Wikidata ID for {e.get('entity')}: {wd.get('id')}")
        if wd.get("description"):
            parts.append(f"Wikidata description for {e.get('entity')}: {wd.get('description')}")
        if wd.get("types"):
            parts.append(f"Wikidata types for {e.get('entity')}: {', '.join(wd.get('types', []))}")
        db = src.get("dbpedia", {})
        if db.get("abstract"):
            parts.append(f"DBpedia abstract for {e.get('entity')}: {db.get('abstract')}")
        if db.get("resource_uri"):
            parts.append(f"DBpedia URI for {e.get('entity')}: {db.get('resource_uri')}")
        # relationship fields already in relationships list
        if parts:
            knowledge_parts.append("\n".join(parts))
    knowledge = "\n\n".join(knowledge_parts)

    # Build references list for prompt
    refs = []
    for e in entities:
        src = e.get("sources", {})
        # Wikipedia URLs
        wp = src.get("wikipedia", {})
        if wp.get("url"):
            refs.append(wp["url"])
        # Wikidata URLs or IDs
        wd = src.get("wikidata", {})
        if wd.get("url"):
            refs.append(wd["url"])
        elif wd.get("id"):
            refs.append(f"https://www.wikidata.org/wiki/{wd['id']}")
        # DBpedia URIs
        db = src.get("dbpedia", {})
        if db.get("resource_uri"):
            refs.append(db["resource_uri"])
    # Deduplicate while preserving order
    refs = list(dict.fromkeys(refs))

    # ------------------------------------------------------------------
    # Re-order references: preferred Wikipedia language first (de/en)
    # ------------------------------------------------------------------
    lang_pref = config.get("LANGUAGE", "de").lower().split("-")[0]
    if lang_pref in {"de", "en"}:
        def lang_score(url):
            if f"//{lang_pref}.wikipedia.org" in url:
                return 0
            elif "wikipedia.org" in url:
                return 1
            return 2
        refs.sort(key=lang_score)

    # ------------------------------------------------------------------
    # Build enumerated reference strings for the JSON output. We keep the
    # raw URL list `refs` for the prompt where numbering is added separately
    # by the prompt helper (get_system_prompt_compendium_*). The enumerated
    # version avoids injecting the reference number directly into the URL
    # itself and therefore looks like "(1) https://...", "(2) https://...".
    # ------------------------------------------------------------------
    enumerated_refs = [f"({i + 1}) {url}" for i, url in enumerate(refs)]

    lang = config.get("LANGUAGE", "de").lower()
    # Use compendium prompts with educational flag
    educational = config.get("COMPENDIUM_EDUCATIONAL_MODE", False)
    if lang.startswith("en"):
        prompt = get_system_prompt_compendium_en(topic, length, refs, educational)
    else:
        prompt = get_system_prompt_compendium_de(topic, length, refs, educational)
    prompt += "\n### Knowledge from sources:\n" + knowledge

    try:
        logger.info("Generating compendium...")
        start = time.time()
        response = client.chat.completions.create(
            model=config.get("MODEL"),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=length,
            temperature=temperature
        )
        comp_text = response.choices[0].message.content.strip()
        elapsed = time.time() - start
        logger.info(f"Generated compendium in {elapsed:.2f}s")
        # Return the compendium text together with the enumerated reference
        # list so that downstream consumers can display a bibliography that
        # matches the citation numbers used inside the text.
        return comp_text, enumerated_refs
    except Exception as e:
        logger.error(f"Error generating compendium: {e}")
        return "", []
