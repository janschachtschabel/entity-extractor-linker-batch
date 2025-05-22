"""
orchestrator.py

Orchestriert den gesamten Entitätsextraktionsworkflow, einschließlich Chunking,
Entitäts-/Beziehungsdeduplizierung und optionaler Visualisierung.
Diese Version ist stark vereinfacht im Vergleich zur vorherigen Version.
"""
import logging
import time

from entityextractor.config.settings import get_config
from entityextractor.utils.logging_utils import configure_logging
from entityextractor.utils.text_utils import chunk_text

from entityextractor.core.api.extract import extract_entities
from entityextractor.core.api.generate import generate_entities
from entityextractor.core.api.relationships import infer_entity_relationships
from entityextractor.core.process.deduplication import deduplicate_entities, deduplicate_relationships
from entityextractor.core.process.statistics import generate_statistics
from entityextractor.core.process.result_formatter import format_results
from entityextractor.core.visualization.graph_builder import build_graph
from entityextractor.services.compendium_service import generate_compendium

def process_entities(input_text: str, user_config: dict = None):
    """
    Orchestriert den gesamten Entitätsextraktions- und Verarbeitungsworkflow.
    
    Diese Funktion delegiert an spezialisierte Module für die verschiedenen
    Verarbeitungsschritte und wurde erheblich vereinfacht.
    
    Args:
        input_text: Der Text oder das Thema, für das Entitäten extrahiert/generiert werden sollen
        user_config: Benutzerkonfiguration (optional)
        
    Returns:
        Ein strukturiertes Ergebnisobjekt mit Entitäten, Beziehungen und Statistiken
    """
    config = get_config(user_config)
    configure_logging(config)
    start = time.time()
    mode = config.get("MODE", "extract")
    logging.info("[orchestrator] Starting process: MODE=%s", mode)

    # Chunking-Pfad
    if config.get("TEXT_CHUNKING", False):
        result = process_with_chunking(input_text, config)
    else:
        # Normaler Extraktions-/Generierungspfad
        result = process_single_pass(input_text, config)
    
    # Ergänze Statistiken
    stats = generate_statistics(result)
    result["statistics"] = stats
    
    # Generiere optional ein Kompendium
    if config.get("ENABLE_COMPENDIUM", False):
        comp_text, refs = generate_compendium(input_text, result["entities"], result["relationships"], config)
        # Strukturierte Referenzen mit Nummern
        structured_refs = [{"number": idx+1, "url": url} for idx, url in enumerate(refs)]
        result["compendium"] = {"text": comp_text, "references": structured_refs}
    
    # Berechne die Gesamtzeit
    elapsed = time.time() - start
    logging.info(f"[orchestrator] Total processing time: {elapsed:.2f} sec")
    
    return result

def process_with_chunking(input_text, config):
    """
    Verarbeitet Text in Chunks für größere Dokumente.
    
    Args:
        input_text: Der zu verarbeitende Text
        config: Konfiguration
        
    Returns:
        Ergebnisobjekt mit deduplizierten Entitäten und Beziehungen
    """
    size = config.get("TEXT_CHUNK_SIZE", 2000)
    overlap = config.get("TEXT_CHUNK_OVERLAP", 50)
    logging.info("[orchestrator] Chunking: size=%d, overlap=%d", size, overlap)
    
    chunks = chunk_text(input_text, size, overlap)
    all_entities, all_relationships = [], []
    
    for i, chunk in enumerate(chunks, 1):
        logging.info("[orchestrator] Chunk %d/%d", i, len(chunks))
        
        # Verarbeite jeden Chunk einzeln
        chunk_result = process_single_pass(chunk, config)
        
        # Füge Entitäten zum Gesamtergebnis hinzu
        if "entities" in chunk_result and isinstance(chunk_result["entities"], list):
            all_entities.extend(chunk_result["entities"])
            logging.info(f"[orchestrator] {len(chunk_result['entities'])} Entitäten aus Chunk {i} hinzugefügt")
        
        # Füge Beziehungen zum Gesamtergebnis hinzu
        if "relationships" in chunk_result and isinstance(chunk_result["relationships"], list):
            chunk_relationships = chunk_result["relationships"]
            # Protokolliere die Beziehungen für Debugging
            logging.info(f"[orchestrator] Beziehungen in Chunk {i}: {len(chunk_relationships)}")
            for j, rel in enumerate(chunk_relationships[:3]):
                logging.info(f"[orchestrator] Beispiel-Beziehung {j+1}: {rel.get('subject', '')} -- {rel.get('predicate', '')} --> {rel.get('object', '')}")
            
            # Füge die Beziehungen hinzu
            all_relationships.extend(chunk_relationships)
            logging.info(f"[orchestrator] {len(chunk_relationships)} Beziehungen aus Chunk {i} hinzugefügt")
        else:
            logging.warning(f"[orchestrator] Keine Beziehungen in Chunk {i} gefunden oder ungültiges Format")
    
    # Dedupliziere Entitäten und Beziehungen
    deduped_entities = deduplicate_entities(all_entities)
    
    # Protokolliere die Anzahl der Beziehungen vor der Deduplizierung
    logging.info(f"[orchestrator] Beziehungen vor Deduplizierung: {len(all_relationships)}")
    
    # Dedupliziere Beziehungen nur, wenn welche vorhanden sind
    if all_relationships:
        deduped_relationships = deduplicate_relationships(all_relationships, deduped_entities, config)
        logging.info(f"[orchestrator] Beziehungen nach Deduplizierung: {len(deduped_relationships)}")
    else:
        deduped_relationships = []
        logging.info("[orchestrator] Keine Beziehungen zum Deduplizieren gefunden")
    
    # Formatiere das Ergebnis
    result = format_results(deduped_entities, deduped_relationships, input_text)
    
    # Erstelle optional eine Knowledge Graph Visualisierung
    if config.get("ENABLE_GRAPH_VISUALIZATION", False):
        from entityextractor.core.visualization.visualizer import visualize_graph
        visualize_graph(result, config)
    
    return result

def process_single_pass(input_text, config):
    """
    Verarbeitet den Text in einem einzelnen Durchlauf.
    
    Args:
        input_text: Der zu verarbeitende Text oder das Thema
        config: Konfiguration
        
    Returns:
        Ergebnisobjekt mit Entitäten und Beziehungen
    """
    mode = config.get("MODE", "extract")
    start_time = time.time()
    
    # Extrahiere oder generiere Entitäten je nach Modus
    if mode == "extract":
        entities = extract_entities(input_text, config)
    elif mode in ["generate", "compendium"]:
        entities = generate_entities(input_text, config)
    else:
        logging.warning(f"Unbekannter Modus: {mode}, verwende 'extract'")
        entities = extract_entities(input_text, config)
    
    # Extrahiere Beziehungen, wenn aktiviert
    relationships = []
    if config.get("RELATION_EXTRACTION", False) or config.get("ENABLE_RELATIONS_INFERENCE", False):
        # Extrahiere explizite und (optional) implizite Beziehungen
        text_for_relationships = input_text if mode == "extract" else None
        extracted_relationships = infer_entity_relationships(entities, text_for_relationships, config)
        
        if extracted_relationships and len(extracted_relationships) > 0:
            relationships = extracted_relationships
            # Keine Duplikate der Beziehungs-Logs hier, die bereits in relationships.py vorhanden sind
        else:
            logging.info("[orchestrator] Keine Beziehungen extrahiert")
    else:
        logging.info("[orchestrator] Beziehungsextraktion deaktiviert")
    
    # Keine Debug-Ausgabe für Beziehungen vor der Formatierung hier, da dies bereits in relationships.py erfolgt ist
    
    # Formatiere das Ergebnis und stelle sicher, dass die Beziehungen korrekt übertragen werden
    result = format_results(entities, relationships, input_text)
    
    # Debug-Ausgabe für Beziehungen nach der Formatierung
    if "relationships" in result and result["relationships"]:
        logging.info(f"[orchestrator] Beziehungen nach Formatierung: {len(result['relationships'])}")
    else:
        logging.warning("[orchestrator] Keine Beziehungen im formatierten Ergebnis!")
    
    # Erstelle optional eine Knowledge Graph Visualisierung
    if config.get("ENABLE_GRAPH_VISUALIZATION", False):
        from entityextractor.core.visualization.visualizer import visualize_graph
        visualize_graph(result, config)
    
    # Log processing time
    elapsed = time.time() - start_time
    logging.info("[orchestrator] Single-pass done in %.2f sec", elapsed)
    
    return result
