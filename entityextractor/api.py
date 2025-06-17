"""
api.py

Hauptmoduldatei mit der öffentlichen API für den Entity Extractor.
"""

import logging
from loguru import logger
from entityextractor.config.settings import get_config

# Configure logging EARLY using default settings so that any DEBUG logs
# emitted during subsequent imports respect the LOG_LEVEL from settings.py
from entityextractor.utils.logging_utils import configure_logging as _configure_logging
_config = get_config()
_configure_logging(_config)

# Importiere die neuen API-Funktionen aus den spezifischen Modulen
from entityextractor.core.api.extract import extract_entities
from entityextractor.core.api.generate import generate_entities
from entityextractor.core.api.link import link_entities
from entityextractor.core.api.relationships import infer_entity_relationships
from entityextractor.core.process.orchestrator import process_entities as _process_entities
from entityextractor.services.openai_service import extract_entities_with_openai

# ---------------------------------------------------------------------------
# High-level convenience functions
# ---------------------------------------------------------------------------

async def extract_and_link_entities(text, config=None):
    """
    Extrahiert Entitäten aus einem Text und verknüpft sie mit Wissensquellen.
    
    Dies ist die Hauptfunktion für die Entitätsextraktion aus einem bestehenden Text.
    Sie nutzt die optimierte Batch-Verarbeitung für maximale Effizienz.
    
    Args:
        text: Der Text, aus dem Entitäten extrahiert werden sollen
        config: Konfigurationswörterbuch (optional) mit Parametern wie:
            - LANGUAGE: Sprache des Textes ("de" oder "en")
            - USE_WIKIDATA: Wikidata-Verknüpfung aktivieren (default: True)
            - USE_DBPEDIA: DBpedia-Verknüpfung aktivieren (default: True)
            - RELATION_EXTRACTION: Beziehungsextraktion aktivieren (default: True)
            - ENABLE_RELATIONS_INFERENCE: Implizite Beziehungen aktivieren (default: False)
            - TEXT_CHUNKING: Text in Chunks aufteilen (default: False)
            - VISUALIZE_GRAPH: Graph visualisieren (default: True)
        
    Returns:
        Ein Dictionary mit extrahierten Entitäten und optional Beziehungen und Visualisierungen
    """
    # Ensure we merge with default config
    from entityextractor.config.settings import get_config
    
    # Create a copy of the user config or an empty dict
    user_config = {} if config is None else config.copy()
    user_config["MODE"] = "extract"
    
    # Merge with default config
    merged_config = get_config(user_config)
    
    from entityextractor.utils.text_utils import chunk_text

    # ------------------------------------------------------------
    # 1. Extract entities (with optional automatic text chunking)
    # ------------------------------------------------------------
    if merged_config.get("TEXT_CHUNKING", False):
        size = int(merged_config.get("TEXT_CHUNK_SIZE", 1000))
        overlap = int(merged_config.get("TEXT_CHUNK_OVERLAP", 50))

        # Split text and aggregate entities from all chunks
        chunks = chunk_text(text, size, overlap)
        all_entities = []
        for chunk in chunks:
            all_entities.extend(extract_entities_with_openai(chunk, merged_config))

        # Simple deduplication by lowercase name
        seen = set()
        deduped_entities = []
        for ent in all_entities:
            name = (ent.get("name") or "").lower()
            if name and name not in seen:
                deduped_entities.append(ent)
                seen.add(name)
        extracted_entities = deduped_entities
    else:
        extracted_entities = extract_entities_with_openai(text, merged_config)

    # ------------------------------------------------------------
    # Optional Entity Inference (implicit entities)
    # ------------------------------------------------------------
    if merged_config.get("ENABLE_ENTITY_INFERENCE", False):
        from entityextractor.core.entity_inference import infer_entities
        from entityextractor.utils.id_utils import generate_entity_id
        logger.info("[api] Entity inference enabled – generating implicit entities …")
        extracted_entities = infer_entities(text, extracted_entities, merged_config)
        # Assign IDs to any new inferred entities
        for ent in extracted_entities:
            if 'id' not in ent:
                ent['id'] = generate_entity_id()

    # ------------------------------------------------------------
    # 2. Link entities & downstream processing
    # ------------------------------------------------------------
    return await _process_entities(extracted_entities, original_text=text, config=merged_config)

async def generate_and_link_entities(topic, config=None):
    """
    Generiert Entitäten zu einem Thema und verknüpft sie mit Wissensquellen.
    
    Diese Funktion generiert relevante Entitäten zu einem bestimmten Thema
    anstatt sie aus einem Text zu extrahieren. Sie eignet sich besonders,
    um Wissensgraphen zu erstellen oder Lehrmaterial zu ergänzen.
    
    Args:
        topic: Das Thema, zu dem Entitäten generiert werden sollen
        config: Konfigurationswörterbuch (optional) mit Parametern wie:
            - LANGUAGE: Sprache des Themas ("de" oder "en")
            - MAX_ENTITIES: Maximale Anzahl zu generierender Entitäten (default: 10)
            - USE_WIKIDATA: Wikidata-Verknüpfung aktivieren (default: True)
            - USE_DBPEDIA: DBpedia-Verknüpfung aktivieren (default: True)
            - RELATION_EXTRACTION: Beziehungsextraktion aktivieren (default: True)
            - ENABLE_RELATIONS_INFERENCE: Implizite Beziehungen aktivieren (default: False)
            - VISUALIZE_GRAPH: Graph visualisieren (default: True)
        
    Returns:
        Ein Dictionary mit generierten Entitäten und optional Beziehungen
    """
    from entityextractor.config.settings import get_config
    from entityextractor.core.generator import generate_entities as _raw_generate_entities

    # Prepare user config
    user_config = {} if config is None else config.copy()
    user_config["MODE"] = "generate"

    # Merge with defaults
    merged_config = get_config(user_config)

    # Generate raw entities (no linking)
    raw_entities = _raw_generate_entities(topic, merged_config)

    # Clean-up: If a generated entity only has a German Wikipedia URL, drop it so the
    # WikipediaService will perform its normal lookup incl. langlinks/Wikidata for an
    # English equivalent. This mimics extract-mode behaviour.
    for ent in raw_entities:
        url = ent.get("wikipedia_url", "")
        if url.startswith("http") and "de.wikipedia" in url and "en.wikipedia" not in url:
            ent.pop("wikipedia_url", None)

    # Unified linking & relationship pipeline
    return await _process_entities(raw_entities, original_text=topic, config=merged_config)

async def create_knowledge_compendium(topic, config=None):
    """
    Erstellt ein umfassendes Wissenskompendium zu einem Thema.
    
    Diese Funktion generiert ein strukturiertes Kompendium mit Entitäten,
    Beziehungen und einem zusammenfassenden Text zum Thema.
    
    Args:
        topic: Das Thema, zu dem ein Kompendium erstellt werden soll
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Ein Dictionary mit dem Kompendium, Entitäten, Beziehungen und Referenzen
    """
    # Ensure we merge with default config
    from entityextractor.config.settings import get_config
    
    # Create a copy of the user config or an empty dict
    user_config = {} if config is None else config.copy()
    user_config["MODE"] = "compendium"
    user_config["ENABLE_COMPENDIUM"] = True
    
    # Merge with default config
    merged_config = get_config(user_config)
    
    # Generate entities and create compendium
    generated_entities = extract_entities_with_openai(topic, merged_config)
    
    # Process the entities and create the compendium
    return await _process_entities(generated_entities, original_text=topic, config=merged_config)

# ---------------------------------------------------------------------------
# Universal wrapper that keeps backward-compatibility with both the old and
# new calling styles
# ---------------------------------------------------------------------------

async def process_entities(input_data, user_config=None, **kwargs):
    """Universal entry point for entity extraction/generation & linking.

    This wrapper allows a single import::

        from entityextractor.api import process_entities

    to be used in three different scenarios:

    1. Text extraction mode (``MODE='extract'``):
       ``input_data`` is a *str* containing the source text. The wrapper
       calls :pyfunc:`extract_and_link_entities` internally.

    2. Entity generation mode (``MODE='generate'``):
       ``input_data`` is a *str* containing the topic. The wrapper calls
       :pyfunc:`generate_and_link_entities`.

    3. Pre-extracted entities list: ``input_data`` is a *list* of entity
       dictionaries. The wrapper forwards directly to the orchestrator’s
       :pyfunc:`process_entities` (imported as ``_process_entities``).

    The mode is primarily taken from ``user_config['MODE']`` if supplied.
    If not provided, the wrapper falls back to type introspection of
    ``input_data`` (``str`` → *extract*, ``list`` → orchestrator).
    """

    # Bring user config into a mutable dict and merge with defaults once
    from entityextractor.config.settings import get_config as _get_config

    base_cfg: Dict[str, Any] = {}
    if user_config:
        base_cfg.update(user_config)
    if "config" in kwargs and isinstance(kwargs["config"], dict):
        base_cfg.update(kwargs["config"])

    # Fallback: infer MODE when missing and ``input_data`` is a str
    if "MODE" not in base_cfg and isinstance(input_data, str):
        base_cfg["MODE"] = "extract"

    # Merge with defaults once – downstream helpers will receive an already
    # merged config and can skip their own merging to avoid duplication.
    merged_config = _get_config(base_cfg)

    mode = merged_config.get("MODE")

    # Branch based on mode or input type
    if mode == "extract" or (mode is None and isinstance(input_data, str)):
        return await extract_and_link_entities(input_data, merged_config)

    if mode == "generate":
        return await generate_and_link_entities(input_data, merged_config)

    # Fallback: assume we already have an entity list
    return await _process_entities(input_data, original_text=kwargs.get("original_text"), config=merged_config)

__all__ = [
    "extract_and_link_entities",
    "generate_and_link_entities",
    "create_knowledge_compendium",
    "process_entities",  # Für fortgeschrittene Anwendungsfälle
    
    # Einzelne Komponenten für flexible Integration
    "extract_entities",
    "generate_entities",
    "link_entities",
    "infer_entity_relationships"
]
