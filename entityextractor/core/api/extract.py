"""
extract.py

Konsolidiertes API-Modul für die Entitätsextraktion.

Dieses Modul kombiniert die Basis-Extraktionsfunktionalität aus entityextractor.core.extractor
mit der Verknüpfungsfunktionalität aus entityextractor.core.api.link, um einen vollständigen
Extraktions- und Verknüpfungsprozess anzubieten.

Dies ist die empfohlene öffentliche API für die Entitätsextraktion mit Verknüpfung.
"""

import logging
import time
from entityextractor.services.openai_service import extract_entities_with_openai
from entityextractor.core.entity_inference import infer_entities
from entityextractor.core.api.link import link_entities
from entityextractor.config.settings import get_config
from entityextractor.utils.id_utils import generate_entity_id

def extract_entities(text, config=None):
    """
    Extrahiert und verknüpft Entitäten aus einem Text.
    
    Args:
        text: Der Text, aus dem Entitäten extrahiert werden sollen
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Liste der extrahierten und verknüpften Entitäten
    """
    config = get_config(config)
    logging.info("[extract_api] Starting extraction and linking...")
    start_extraction = time.time()
    
    # Extrahiere explizite Entitäten mit OpenAI
    logging.info("Starting entity extraction...")
    entities = extract_entities_with_openai(text, config)

    # Vergib UUID4 für jede Entität
    for entity in entities:
        if 'id' not in entity:
            entity['id'] = generate_entity_id()
    
    # Führe ggf. Entitätsinferenz durch
    if config.get("ENABLE_ENTITY_INFERENCE", False):
        logging.info("Entity Inference aktiviert: Erzeuge implizite Entitäten via LLM...")
        entities = infer_entities(text, entities, config)
        logging.info("Entitäten nach Inferenz: %d", len(entities))
        # Nach der Inferenz ggf. neuen Entitäten IDs zuweisen
        for entity in entities:
            if 'id' not in entity:
                entity['id'] = generate_entity_id()
    
    elapsed_time = time.time() - start_extraction
    logging.info(f"Entity extraction completed in {elapsed_time:.2f} seconds")
    logging.info("[extract_api] Extracted %d entities", len(entities))
    
    # Verknüpfe alle Entitäten mit Wissensquellen
    linked_entities = link_entities(entities, config)
    logging.info("[extract_api] Linked %d entities", len(linked_entities))
    
    return linked_entities
