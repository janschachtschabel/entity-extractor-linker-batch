"""
generate.py

Konsolidiertes API-Modul für die Entitätsgenerierung.

Dieses Modul kombiniert die Basis-Generierungsfunktionalität aus entityextractor.core.generator
mit der Verknüpfungsfunktionalität aus entityextractor.core.api.link, um einen vollständigen
Generierungs- und Verknüpfungsprozess anzubieten.

Dies ist die empfohlene öffentliche API für die Entitätsgenerierung mit Verknüpfung zu Wissensquellen.
"""

import logging
from entityextractor.core.generator import generate_entities as generate_entities_internal
from entityextractor.core.api.link import link_entities
from entityextractor.config.settings import get_config

def generate_entities(topic, config=None):
    """
    Generiert und verknüpft Entitäten zu einem bestimmten Thema.
    
    Args:
        topic: Das Thema, zu dem Entitäten generiert werden sollen
        config: Konfigurationswörterbuch (optional)
        
    Returns:
        Liste der generierten und verknüpften Entitäten
    """
    config = get_config(config)
    logging.info(f"[generate_api] Starting generation for topic: {topic}")
    
    # Generiere Entitäten zum angegebenen Thema
    entities = generate_entities_internal(topic, config)
    logging.info(f"[generate_api] Generated {len(entities)} entities")
    
    # Verknüpfe die generierten Entitäten mit Wissensquellen
    linked_entities = link_entities(entities, config)
    logging.info(f"[generate_api] Linked {len(linked_entities)} entities")
    
    return linked_entities
