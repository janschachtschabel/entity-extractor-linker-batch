"""translation_service.py

Provides a lightweight OpenAI-based translation helper for single terms.
The function focuses on translating German science/technical terms into the
exact English Wikipedia/DBpedia page title.  It keeps the interface minimal so
it can be used as a fallback in the DBpediaService when no English label was
found via langlinks or Wikidata.
"""

from __future__ import annotations

import time
import json
from typing import Optional
from loguru import logger

from openai import OpenAI
from entityextractor.config.settings import get_config

_TRANSLATION_CACHE: dict[str, str] = {}


def translate_term_to_en(term: str, config: Optional[dict] = None) -> Optional[str]:
    """Translate a single German term into the English Wikipedia/DBpedia title.

    The translation is performed with an OpenAI chat completion using an ultra-
    short prompt to minimise cost.  If the same term is requested again within
    the runtime of the process, a small in-memory cache avoids repeat calls.

    Args:
        term: The German term (1-3 words) to translate.
        config: Optional configuration dict; if *None*, global config is used.

    Returns:
        The translated English title, or *None* if the translation failed.
    """
    if not term:
        return None

    if term in _TRANSLATION_CACHE:
        return _TRANSLATION_CACHE[term]

    # Merge with project config
    config = get_config(config)
    api_key = config.get("OPENAI_API_KEY") or None
    if not api_key:
        logger.error("translate_term_to_en: OPENAI_API_KEY missing – cannot translate term '%s'", term)
        return None

    model = config.get("MODEL", "gpt-3.5-turbo-1106")
    base_url = config.get("LLM_BASE_URL", "https://api.openai.com/v1")

    # Build prompts – explicit, deterministic, cheap
    system_prompt = (
        "You are a concise translator. "
        "Translate the given German scientific term into the exact English title "
        "used for its Wikipedia page. Respond ONLY with the title (max 4 words)."
    )
    user_prompt = term

    client = OpenAI(api_key=api_key, base_url=base_url)

    try:
        start = time.time()
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            max_tokens=10,
            temperature=0.0,
            timeout=30,
        )
        translation = response.choices[0].message.content.strip().strip('"')
        # Basic sanitation: remove markup, keep words & parentheses, replace whitespace >1 with space
        import re
        translation = re.sub(r"\s+", " ", translation)
        if translation:
            _TRANSLATION_CACHE[term] = translation
            elapsed = time.time() - start
            logger.info(f"Translated '{term}' -> '{translation}' in {elapsed:.1f}s via OpenAI")
            return translation
    except Exception as exc:
        logger.error(f"translate_term_to_en: OpenAI call failed for '{term}': {exc}")

    return None
