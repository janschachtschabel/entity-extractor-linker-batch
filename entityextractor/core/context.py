#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EntityProcessingContext

Zentrale Klasse für den strukturierten Datenaustausch zwischen Services.
Verwaltet den gesamten Verarbeitungszustand einer Entität und stellt Methoden
für die Kommunikation zwischen Services bereit.
"""

import logging
from typing import Dict, Any, Optional, List, Set

logger = logging.getLogger(__name__)

class EntityProcessingContext:
    """
    Zentrales Repository für alle Daten einer Entität während der Verarbeitung.
    Enthält den aktuellen Verarbeitungsstatus, interne Kommunikationsdaten und die finale Output-Struktur.
    """
    
    def __init__(self, entity_name: str, entity_id: Optional[str] = None, entity_type: Optional[str] = None, 
                 original_text: Optional[str] = None):
        """
        Initialisiert einen neuen Verarbeitungskontext für eine Entität.
        
        Args:
            entity_name: Name der Entität
            entity_id: Optionale ID der Entität (wird generiert, falls nicht angegeben)
            entity_type: Optionaler Typ der Entität
            original_text: Optionaler Originaltext, aus dem die Entität stammt
        """
        self.entity_name = entity_name
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.original_text = original_text
        
        # Interner Datenaustausch zwischen Services
        self.processing_data = {}
        
        # Zitationsinformationen und Text-Kontext
        self.citation = None
        
        # Beziehungen zu anderen Entitäten
        self.relationships = []
        
        # Zusätzliche Daten (für Metadaten und andere nicht kategorisierte Informationen)
        self.additional_data = {}
        
        # Finale Output-Struktur (wird schrittweise aufgebaut)
        self.output_data = {
            "entity": entity_name,
            "details": {
                "typ": entity_type if entity_type else "",
                "inferred": "explicit"  # Standard: explizite Entität (aus Text extrahiert)
            },
            "sources": {}
        }
        
        if entity_id:
            self.output_data["id"] = entity_id
        
        # Verarbeitungsstatus
        self.processed_by_services = set()
        
        # Service-spezifische Daten für die Serialisierung
        self.service_data = {}
        
        logger.debug(f"Verarbeitungskontext für Entität '{entity_name}' erstellt")
    
    def add_service_data(self, service_name: str, data: Dict[str, Any]) -> None:
        """
        Fügt Daten von einem Service hinzu und aktualisiert den Output.
        
        Args:
            service_name: Name des Services, der die Daten liefert
            data: Die Service-Daten im standardisierten Format
        """
        # Interne Daten speichern
        self.processing_data[service_name] = data
        
        # Speichere auch in service_data für die Serialisierung
        self.service_data[service_name] = data
        
        # Detailliertes Logging vor der Verarbeitung
        logger.info(f"Service-Daten für '{self.entity_name}' von '{service_name}' werden hinzugefügt")
        logger.info(f"Datenformat: {list(data.keys()) if isinstance(data, dict) else 'Kein Dictionary'}")
        
        # Initialisiere service_data mit den Rohdaten
        service_data = data
        
        # Alle Service-Daten gehören unter sources.service_name
        if service_name in ["wikidata", "wikipedia", "dbpedia"]:
            # Service-Daten direkt in sources speichern
            if service_name == "dbpedia":
                # DBpedia-Daten direkt in sources.dbpedia speichern
                self.output_data["sources"][service_name] = data
                logger.info(f"DBpedia-Daten für '{self.entity_name}' in sources.dbpedia gespeichert")
            elif service_name == "wikidata":
                # Wikidata-Daten direkt in sources.wikidata speichern
                self.output_data["sources"][service_name] = data
                # Zusätzlich in processing_data speichern für spätere Verwendung
                self.processing_data["wikidata_data"] = data
                logger.info(f"Wikidata-Daten für '{self.entity_name}' in sources.wikidata gespeichert")
            elif service_name == "wikipedia":
                # Wikipedia-Daten in sources.wikipedia speichern
                self.output_data["sources"][service_name] = data
                logger.info(f"Wikipedia-Daten für '{self.entity_name}' in sources.wikipedia gespeichert")
        
        # Andere Services mit möglicher Verschachtelung
        elif service_name in data:
            # Altes Format: data enthält service_name als Schlüssel
            self.output_data["sources"][service_name] = data[service_name]
            service_data = data[service_name]
            logger.info(f"Verschachtelte {service_name}-Daten für '{self.entity_name}' in sources gespeichert")
        else:
            # Neues Format: data enthält direkt die Service-Daten
            self.output_data["sources"][service_name] = data
            service_data = data
            logger.info(f"Direkte {service_name}-Daten für '{self.entity_name}' in sources gespeichert")
            
        # Extrahiere wichtige Felder in die details
        self._update_details_from_service(service_name, service_data)
        
        # Service als verarbeitet markieren
        self.processed_by_services.add(service_name)
        
        # Detailliertes Logging nach der Verarbeitung
        logger.info(f"Service-Daten für '{self.entity_name}' von '{service_name}' hinzugefügt: {list(service_data.keys()) if isinstance(service_data, dict) else 'Keine Daten'}")
        logger.info(f"Quellen nach Hinzufügen: {list(self.output_data['sources'].keys())}")
        if service_name in self.output_data['sources']:
            source_data = self.output_data['sources'][service_name]
            logger.info(f"Daten in {service_name}-Quelle: {list(source_data.keys()) if isinstance(source_data, dict) else 'Kein Dictionary'}")
        else:
            logger.warning(f"Service {service_name} nicht in sources vorhanden!")

    
    def _update_details_from_service(self, service_name: str, service_data: Dict[str, Any]) -> None:
        """
        Aktualisiert details mit wichtigen Informationen aus den Service-Daten.
        Nur grundlegende Informationen wie typ und inferred werden in details gespeichert.
        Service-spezifische Informationen werden nur in den jeweiligen Service-Bereichen gespeichert.
        
        Args:
            service_name: Name des Services
            service_data: Die Service-Daten
        """
        # Keine Aktualisierung, wenn keine Service-Daten vorhanden sind
        if not service_data:
            return
            
        # Im neuen Format speichern wir nur grundlegende Informationen in details
        # Alle service-spezifischen Informationen werden nur in den jeweiligen Service-Bereichen gespeichert
        
        # Wir aktualisieren nur den Typ, wenn er aus dem Service abgeleitet werden kann und noch nicht gesetzt ist
        if service_name == "wikidata" and "instance_of" in service_data and not self.entity_type:
            # Wir könnten hier den Typ aus instance_of ableiten, aber nur wenn noch kein Typ gesetzt ist
            # Diese Logik könnte in einer separaten Methode implementiert werden
            pass
        
        # Alle anderen service-spezifischen Informationen werden nicht mehr in details gespeichert
        # Sie sind bereits in den jeweiligen Service-Bereichen unter sources.service_name verfügbar
    
    def get_service_data(self, service_name: str) -> Dict[str, Any]:
        """
        Ruft die von einem bestimmten Service hinzugefügten Daten ab. Stellt sicher, dass nur Dictionaries zurückgegeben werden.
        
        Args:
            service_name: Name des Services
            
        Returns:
            Die Service-Daten oder None
        """
        # Versuche, Daten aus output_data['sources'] zu holen
        data_from_sources = self.output_data.get('sources', {}).get(service_name)
        if isinstance(data_from_sources, dict):
            # Logge nur, wenn es tatsächlich ein nicht-leeres Dictionary ist
            keys_info = list(data_from_sources.keys()) if data_from_sources else 'Leeres Dict'
            logging.debug(f"Service-Daten für '{self.entity_name}' (Service: {service_name}) in sources gefunden: {keys_info}")
            return data_from_sources
        elif data_from_sources is not None: # Es gibt Daten, aber es ist kein Dictionary
            logging.warning(f"Service-Daten für '{self.entity_name}' (Service: {service_name}) in sources gefunden, aber es ist kein Dictionary (Typ: {type(data_from_sources)}). Wird ignoriert.")

        # Fallback auf processing_data, wenn nicht in sources gefunden oder nicht die erwartete Struktur hatte
        data_from_processing = self.processing_data.get(service_name)
        if isinstance(data_from_processing, dict):
            keys_info = list(data_from_processing.keys()) if data_from_processing else 'Leeres Dict'
            logging.debug(f"Service-Daten für '{self.entity_name}' (Service: {service_name}) in processing_data gefunden: {keys_info}")
            return data_from_processing
        elif data_from_processing is not None: # Es gibt Daten, aber es ist kein Dictionary
            # Hier trat der Fehler auf, wenn data_from_processing ein String war
            logging.warning(f"Service-Daten für '{self.entity_name}' (Service: {service_name}) in processing_data gefunden, aber es ist kein Dictionary (Typ: {type(data_from_processing)}). Wird ignoriert.")
        
        logging.debug(f"Keine gültigen Service-Daten (als Dict) für '{self.entity_name}' von Service '{service_name}' gefunden.")
        return None
    
    def update_details(self, details_updates: Dict[str, Any]) -> None:
        """
        Aktualisiert das details-Dictionary mit neuen Informationen.
        
        Args:
            details_updates: Die zu aktualisierenden Details
        """
        self.output_data["details"].update(details_updates)
        logger.debug(f"Details für '{self.entity_name}' aktualisiert mit {len(details_updates)} Feldern")
    
    def set_processing_info(self, key: str, value: Any) -> None:
        """
        Setzt eine Information im processing_data-Dictionary für die Kommunikation zwischen Services.
        
        Args:
            key: Der Schlüssel für die Information
            value: Der Wert
        """
        self.processing_data[key] = value
        logger.debug(f"Processing-Info für '{self.entity_name}' gesetzt: {key}")
    
    def get_processing_info(self, key: str, default: Any = None) -> Any:
        """
        Gibt eine Information aus dem processing_data-Dictionary zurück.
        
        Args:
            key: Der Schlüssel für die Information
            default: Standardwert, falls der Schlüssel nicht existiert
            
        Returns:
            Der Wert oder der Standardwert
        """
        return self.processing_data.get(key, default)
    
    def is_processed_by(self, service_name: str) -> bool:
        """
        Prüft, ob die Entität bereits von einem bestimmten Service verarbeitet wurde.
        
        Args:
            service_name: Name des Services
            
        Returns:
            True, wenn der Service die Entität bereits verarbeitet hat oder wenn es verknüpfte Daten mit Status 'linked' gibt
        """
        if service_name in self.processed_by_services:
            return True
        service_data = self.output_data.get('sources', {}).get(service_name, {})
        return bool(service_data.get('status') == 'linked')
    
    def get_available_services(self) -> List[str]:
        """
        Gibt eine Liste der verfügbaren Services zurück.
        
        Returns:
            Liste der Service-Namen
        """
        return list(self.processing_data.keys())
        
    def has_source(self, source_name: str) -> bool:
        """
        Prüft, ob die Entität Daten aus der angegebenen Quelle hat.
        
        Args:
            source_name: Name der Quelle (z.B. 'wikipedia', 'wikidata', 'dbpedia')
            
        Returns:
            True, wenn die Quelle existiert, sonst False
        """
        return source_name in self.output_data.get("sources", {})
        
    def set_citation(self, citation: str) -> None:
        """
        Setzt die Zitationsinformation für die Entität.
        
        Args:
            citation: Der Text, in dem die Entität gefunden wurde
        """
        self.citation = citation
        self.output_data["details"]["citation"] = citation
        logger.debug(f"Zitationsinformation für '{self.entity_name}' gesetzt")
        
    def get_citation(self) -> Optional[str]:
        """
        Gibt die Zitationsinformation zurück, falls vorhanden.
        
        Returns:
            Der Zitationstext oder None
        """
        return self.citation
    
    def get_original_text(self) -> Optional[str]:
        """
        Gibt den Originaltext zurück, falls vorhanden.
        
        Returns:
            Der Originaltext oder None
        """
        return self.original_text
        
    def add_relationship(self, relationship: Dict[str, Any]) -> None:
        """
        Fügt eine Beziehung zu einer anderen Entität hinzu.
        
        Args:
            relationship: Dictionary mit Beziehungsinformationen (subject, predicate, object, etc.)
        """
        if relationship not in self.relationships:
            self.relationships.append(relationship)
            logger.debug(f"Beziehung für '{self.entity_name}' hinzugefügt: {relationship['subject']} -> {relationship['predicate']} -> {relationship['object']}")
            
    def get_relationships(self) -> List[Dict[str, Any]]:
        """
        Gibt alle Beziehungen dieser Entität zurück.
        
        Returns:
            Liste von Beziehungs-Dictionaries
        """
        return self.relationships
    
    def add_additional_data(self, key: str, value: Any) -> None:
        """
        Fügt zusätzliche Daten zum Kontext hinzu.
        
        Args:
            key: Schlüssel für die Daten
            value: Wert
        """
        self.additional_data[key] = value
        logger.debug(f"Zusätzliche Daten für '{self.entity_name}' hinzugefügt: {key}")
        
    def get_additional_data(self, key: Optional[str] = None) -> Any:
        """
        Gibt zusätzliche Daten zurück.
        
        Args:
            key: Schlüssel für spezifische Daten, None für alle Daten
            
        Returns:
            Die angeforderten Daten oder None
        """
        if key is None:
            return self.additional_data
        return self.additional_data.get(key)
    
    def set_as_inferred(self, inferred_type: str = "inferred") -> None:
        """
        Markiert die Entität als abgeleitet/implizit.
        
        Args:
            inferred_type: Art der Ableitung (z.B. "inferred", "reference", etc.)
        """
        self.output_data["details"]["inferred"] = inferred_type
        logger.debug(f"Entität '{self.entity_name}' als '{inferred_type}' markiert")
    
    def get_output(self) -> Dict[str, Any]:
        """
        Gibt die finalen Output-Daten zurück.
        
        Returns:
            Die formatierten Entitätsdaten
        """
        # Beziehungen zum Output hinzufügen, falls vorhanden
        if self.relationships:
            self.output_data["relationships"] = self.relationships
        
        # Zusätzliche Daten in details integrieren
        for key, value in self.additional_data.items():
            if key not in self.output_data["details"]:
                self.output_data["details"][key] = value
                
        return self.output_data
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Gibt Statistiken über den Verarbeitungsstatus zurück.
        
        Returns:
            Statistik-Dictionary
        """
        stats = {
            "entity_name": self.entity_name,
            "entity_type": self.entity_type,
            "processed_by_services": list(self.processed_by_services),
            "source_count": len(self.output_data["sources"]),
            "sources": list(self.output_data["sources"].keys()),
            "relationship_count": len(self.relationships),
            "additional_data_keys": list(self.additional_data.keys())
        }
        return stats
    
    def set_processing_info(self, key: str, value: Any) -> None:
        """
        Speichert Informationen im processing_data Dictionary.
        
        Args:
            key: Schlüssel für die Information
            value: Zu speichernder Wert
        """
        self.processing_data[key] = value
        logger.debug(f"Processing-Info '{key}' für '{self.entity_name}' gesetzt")
    
    def log_summary(self, level: int = logging.INFO) -> None:
        """
        Gibt eine Zusammenfassung des Verarbeitungsstatus in die Logs aus.
        
        Args:
            level: Log-Level (default: INFO)
        """
        relationship_info = ""
        if self.relationships:
            relationship_info = f", {len(self.relationships)} Beziehungen"
            
        inferred_info = ""
        if self.output_data["details"].get("inferred") != "explicit":
            inferred_info = f" (abgeleitet: {self.output_data['details']['inferred']})"
            
        summary = (
            f"Entität '{self.entity_name}' [{self.entity_type or 'untyped'}]{inferred_info}: "
            f"Verarbeitet von {len(self.processed_by_services)} Services "
            f"({', '.join(self.processed_by_services)}), "
            f"{len(self.output_data['sources'])} Quellen{relationship_info}"
        )
        logger.log(level, summary)
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Konvertiert den EntityProcessingContext in ein serialisierbares Dictionary.
        
        Returns:
            Dictionary-Repräsentation des Kontexts
        """
        context_dict = {
            "entity_name": self.entity_name,
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "original_text": self.original_text,
            "processing_data": self.processing_data,  # Wichtig: processing_data vollständig serialisieren
            "citation": self.citation,
            "relationships": self.relationships,
            "additional_data": self.additional_data,
            "output_data": self.output_data,
            "processed_by_services": list(self.processed_by_services),
            "service_data": self.service_data
        }
        
        # Debug-Logging für wikipedia_multilang
        if "wikipedia_multilang" in self.processing_data:
            logger.info(f"[DEBUG] to_dict für '{self.entity_name}': wikipedia_multilang wird serialisiert: {self.processing_data.get('wikipedia_multilang')}")
        else:
            logger.warning(f"[DEBUG] to_dict für '{self.entity_name}': Kein wikipedia_multilang in processing_data!")
            
        return context_dict
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EntityProcessingContext':
        """
        Erstellt einen EntityProcessingContext aus einem Dictionary.
        
        Args:
            data: Dictionary-Repräsentation des Kontexts
            
        Returns:
            Neu erstellter EntityProcessingContext
        """
        context = cls(
            entity_name=data.get("entity_name", ""),
            entity_id=data.get("entity_id"),
            entity_type=data.get("entity_type"),
            original_text=data.get("original_text")
        )
        
        # Wichtig: processing_data vollständig wiederherstellen
        if "processing_data" in data:
            context.processing_data = data["processing_data"]
            
            # Debug-Logging für wikipedia_multilang
            if "wikipedia_multilang" in context.processing_data:
                logger.info(f"[DEBUG] from_dict für '{context.entity_name}': wikipedia_multilang wurde wiederhergestellt: {context.processing_data.get('wikipedia_multilang')}")
            else:
                logger.warning(f"[DEBUG] from_dict für '{context.entity_name}': Kein wikipedia_multilang in wiederhergestellten processing_data!")
        
        context.citation = data.get("citation")
        context.relationships = data.get("relationships", [])
        context.additional_data = data.get("additional_data", {})
        context.output_data = data.get("output_data", {"entity": {}, "sources": {}, "details": {}})
        
        # Stelle processed_by_services wieder her
        if "processed_by_services" in data:
            context.processed_by_services = set(data["processed_by_services"])
        
        # Stelle service_data wieder her
        if "service_data" in data:
            context.service_data = data["service_data"]
            
        return context
