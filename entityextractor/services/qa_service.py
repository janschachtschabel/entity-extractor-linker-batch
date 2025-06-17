#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QA Service
Generates short question–answer pairs for a topic using OpenAI.
Placed at the end of the pipeline after compendium generation.
"""

from __future__ import annotations

import json
import os
import time
from typing import List, Any, Dict, Tuple, Optional

from loguru import logger
from openai import OpenAI

from entityextractor.config.settings import get_config
from entityextractor.prompts.qa_prompts import get_system_prompt_qa_en, get_system_prompt_qa_de


def _parse_json_response(raw: str) -> List[Dict[str, str]]:
    """Safely parse the JSON returned by the LLM."""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            valid = [p for p in parsed if isinstance(p, dict) and "question" in p and "answer" in p]
            return valid
    except json.JSONDecodeError as exc:
        logger.error(f"[qa_service] JSON decode error: {exc}")
    return []


def generate_qa_pairs(topic_or_text: str,
                      compendium_text: str | None,
                      references: List[str] | None,
                      user_config: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, str]], List[str]]:
    """Generate QA pairs.

    Returns list of QA dictionaries and reference list (unchanged).
    """
    config = get_config(user_config)
    # Abort early if QA pairs globally disabled
    if not config.get("ENABLE_QA_PAIRS", True):
        logger.info("[qa_service] ENABLE_QA_PAIRS is False – skipping QA generation")
        return [], references or []

    api_key = config.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key, base_url=config.get("LLM_BASE_URL"))

    qa_count: int = int(config.get("QA_PAIR_COUNT", 10))

    # Ensure reference order matches language preference similar to compendium_service
    if references:
        lang_pref = config.get("LANGUAGE", "de").lower().split("-")[0]
        if lang_pref in {"de", "en"}:
            def lang_score(url):
                if f"//{lang_pref}.wikipedia.org" in url:
                    return 0
                elif "wikipedia.org" in url:
                    return 1
                return 2
            references.sort(key=lang_score)
    qa_len: int = int(config.get("QA_PAIR_LENGTH", 250))
    lang = config.get("LANGUAGE", "de").lower()

    # Build context: prefer compendium, else original/topic text
    context_parts: List[str] = []
    if compendium_text:
        context_parts.append(compendium_text)
    if topic_or_text:
        context_parts.append(topic_or_text)
    context = "\n\n".join(context_parts).strip()

    # Choose prompt builder
    if lang.startswith("en"):
        prompt = get_system_prompt_qa_en(context, qa_count, qa_len, references)
    else:
        prompt = get_system_prompt_qa_de(context, qa_count, qa_len, references)

    try:
        logger.info("[qa_service] Generating QA pairs…")
        start = time.time()
        resp = client.chat.completions.create(
            model=config.get("MODEL"),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=min(qa_count * 200, 2048),
            temperature=config.get("TEMPERATURE", 0.2),
        )
        raw_content = resp.choices[0].message.content.strip()
        qa_pairs = _parse_json_response(raw_content)
        elapsed = time.time() - start
        if qa_pairs:
            logger.info(f"[qa_service] Generated {len(qa_pairs)} QA pairs in {elapsed:.2f}s")
        else:
            logger.warning("[qa_service] QA generation returned no valid pairs")
        return qa_pairs, references or []
    except Exception as exc:
        logger.error(f"[qa_service] Error generating QA pairs: {exc}")
        return [], references or []
