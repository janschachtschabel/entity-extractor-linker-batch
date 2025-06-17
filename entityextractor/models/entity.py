"""
Entity-Modell mit Unterstützung für mehrsprachige Inhalte und externe Referenzen.
Kompatibel mit der neuen Kontext-basierten Architektur des EntityExtractors.
"""
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, ClassVar

from .base import LocalizedText, SourceData, LanguageCode
from entityextractor.schemas.service_schemas import (
    validate_wikipedia_data,
    validate_wikidata_data,
    validate_dbpedia_data
)
from entityextractor.core.context import EntityProcessingContext

@dataclass
class Entity:
    """
    Repräsentiert eine Entität mit mehrsprachigen Bezeichnungen und Verknüpfungen
    zu externen Wissensquellen (Wikipedia, Wikidata, DBpedia).
    
    Diese Klasse ist kompatibel mit der neuen Kontext-basierten Architektur
    und unterstützt die Validierung gemäß den definierten Schemas.
    """
    # Schema-Validatoren für verschiedene Quellen
    SCHEMA_VALIDATORS = {
        "wikipedia": validate_wikipedia_data,
        "wikidata": validate_wikidata_data,
        "dbpedia": validate_dbpedia_data
    }
    
    # Identifikation
    id: str  # UUID
    name: str  # Primärer Name
    type: Optional[str] = None  # z.B. "PERSON", "ORGANIZATION", "LOCATION"
    inferred: str = "explicit"   # "explicit" oder "inferred"
    
    # Mehrsprachige Bezeichnungen
    label: LocalizedText = field(default_factory=LocalizedText)
    description: LocalizedText = field(default_factory=LocalizedText)
    aliases: Dict[str, List[str]] = field(default_factory=dict)  # Key ist Sprachcode
    
    # Externe Referenzen - für Legacy-Kompatibilität
    wikipedia_url: Optional[str] = None
    wikidata_id: Optional[str] = None
    dbpedia_uri: Optional[str] = None
    
    # Quellendaten - strukturierte Informationen von verschiedenen Services
    sources: Dict[str, SourceData] = field(default_factory=dict)
    
    # Zitationsinformationen
    citation: Optional[str] = None
    citation_start: Optional[int] = None
    citation_end: Optional[int] = None
    
    # Beziehungen zu anderen Entitäten
    relationships: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadaten
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def add_alias(self, alias: str, lang: str = "de"):
        """Fügt einen Alias in der angegebenen Sprache hinzu"""
        if not alias:
            return
            
        try:
            lang_code = LanguageCode(lang.lower())
            alias = alias.strip()
            
            if alias and alias not in self.aliases.get(lang_code, []):
                self.aliases.setdefault(lang_code, []).append(alias)
                self.updated_at = datetime.utcnow()
        except ValueError:
            # Falls der Sprachcode nicht unterstützt wird, ignorieren
            pass
    
    def get_best_label(self, preferred_langs: List[str] = None) -> str:
        """Gibt das beste verfügbare Label zurück"""
        if not preferred_langs:
            preferred_langs = ["de", "en"]
            
        # Versuche die bevorzugten Sprachen der Reihe nach
        for lang in preferred_langs:
            if label := self.label.get(lang):
                return label
                
        # Fallback: Ersten nicht-leeren Wert zurückgeben
        for lang_code in LanguageCode:
            if label := self.label.get(lang_code.value):
                return label
                
        return self.name  # Letzter Ausweg: den primären Namen zurückgeben
    
    def add_source(self, source_name: str, source_data: Dict):
        """Fügt Quelldaten hinzu oder aktualisiert sie
        
        Args:
            source_name: Name der Quelle (z.B. 'wikipedia', 'wikidata', 'dbpedia')
            source_data: Dictionary mit Quelldaten oder SourceData-Objekt
        """
        if not source_data:
            return
            
        # Wenn bereits ein SourceData-Objekt übergeben wurde, direkt verwenden
        if isinstance(source_data, SourceData):
            self.sources[source_name] = source_data
        else:
            # Sitelinks werden nicht mehr verarbeitet
            
            # SourceData aus Dictionary erstellen
            self.sources[source_name] = SourceData(
                id=source_data.get("id", ""),
                url=source_data.get("url"),
                data={k: v for k, v in source_data.items() 
                       if k not in ["id", "url", "status", "error"]},
                status=source_data.get("status", "pending"),
                error=source_data.get("error")
            )
        self.updated_at = datetime.utcnow()
    
    def has_source(self, source_name: str) -> bool:
        """Prüft, ob die Entität Daten aus der angegebenen Quelle hat"""
        return source_name in self.sources
        
    def get_source(self, source_name: str) -> Optional[Dict[str, Any]]:
        """Gibt die Quelldaten für die angegebene Quelle zurück"""
        if source_name in self.sources:
            source = self.sources[source_name]
            # Nutze die to_dict-Methode von SourceData
            return source.to_dict()
        return None
        
    def validate_source_data(self, source_name: str, source_data: Dict[str, Any]) -> bool:
        """Validiert die Quelldaten gegen das entsprechende Schema
        
        Args:
            source_name: Name der Quelle (z.B. 'wikipedia', 'wikidata', 'dbpedia')
            source_data: Die zu validierenden Daten
            
        Returns:
            True wenn die Daten gültig sind, sonst False
        """
        if source_name not in self.SCHEMA_VALIDATORS:
            return True  # Keine Validierung für unbekannte Quellen
            
        # Daten für die Validierung vorbereiten
        data_to_validate = {}
        
        # Neue Datenstrukturnamen verwenden
        if source_name == "wikipedia":
            data_to_validate = {"wikipedia_data": source_data}
        elif source_name == "wikidata":
            data_to_validate = {"wikidata_data": source_data}
        elif source_name == "dbpedia":
            data_to_validate = {"dbpedia_data": source_data}
        else:
            # Fallback für unbekannte Quellen
            data_to_validate = {source_name: source_data}
            
        validator = self.SCHEMA_VALIDATORS[source_name]
        return validator(data_to_validate)
        
    @classmethod
    def from_context(cls, context: EntityProcessingContext) -> 'Entity':
        """Erstellt eine Entity-Instanz aus einem EntityProcessingContext
        
        Args:
            context: Der Verarbeitungskontext mit allen Service-Daten
            
        Returns:
            Eine neue Entity-Instanz mit Daten aus dem Kontext
        """
        # ID aus Kontext übernehmen oder neue generieren
        entity_id = context.entity_id or str(uuid.uuid4())
        
        # Neue Entity erstellen
        entity = cls(
            id=entity_id,
            name=context.entity_name,
            type=context.entity_type or ""
        )
        
        # Zitationsinformationen hinzufügen
        citation = context.get_citation()
        if citation:
            entity.citation = citation
            original_text = context.get_original_text()
            if original_text:
                citation_start = original_text.find(citation)
                if citation_start != -1:
                    entity.citation_start = citation_start
                    entity.citation_end = citation_start + len(citation)
        
        # Service-Daten hinzufügen
        for service_name in context.get_available_services():
            service_data = context.get_service_data(service_name)
            if service_data:
                # Neue Datenstrukturen verwenden
                if service_name == "wikipedia" and "wikipedia_data" in service_data:
                    entity.add_source(service_name, service_data["wikipedia_data"])
                    if "url" in service_data["wikipedia_data"]:
                        entity.wikipedia_url = service_data["wikipedia_data"]["url"]
                elif service_name == "wikidata" and "wikidata_data" in service_data:
                    entity.add_source(service_name, service_data["wikidata_data"])
                    if "id" in service_data["wikidata_data"]:
                        entity.wikidata_id = service_data["wikidata_data"]["id"]
                elif service_name == "dbpedia" and "dbpedia_data" in service_data:
                    entity.add_source(service_name, service_data["dbpedia_data"])
                    if "uri" in service_data["dbpedia_data"]:
                        entity.dbpedia_uri = service_data["dbpedia_data"]["uri"]
        
        # Beziehungen hinzufügen, falls vorhanden
        relationships = context.get_relationships()
        if relationships:
            entity.relationships = relationships
            
        # Zusätzliche Daten hinzufügen
        additional_data = context.get_additional_data()
        if additional_data:
            entity.metadata.update(additional_data)
            
        return entity
        
    def to_context(self) -> EntityProcessingContext:
        """Konvertiert die Entity-Instanz in einen EntityProcessingContext
        
        Returns:
            Ein neuer EntityProcessingContext mit Daten aus dieser Entity
        """
        # Kontext mit Basis-Informationen erstellen
        context = EntityProcessingContext(self.name)
        context.entity_id = self.id
        context.entity_type = self.type
        
        # Service-Daten hinzufügen
        for source_name, source_data in self.sources.items():
            if source_data:
                # Neue Datenstrukturnamen verwenden
                if source_name == "wikipedia":
                    context.add_service_data(source_name, {"wikipedia_data": source_data.to_dict()})
                elif source_name == "wikidata":
                    context.add_service_data(source_name, {"wikidata_data": source_data.to_dict()})
                elif source_name == "dbpedia":
                    context.add_service_data(source_name, {"dbpedia_data": source_data.to_dict()})
                else:
                    # Fallback für andere Quellen
                    context.add_service_data(source_name, {source_name: source_data.to_dict()})
        
        # Beziehungen hinzufügen
        if self.relationships:
            for rel in self.relationships:
                context.add_relationship(rel)
        
        # Zitationsinformationen hinzufügen
        if self.citation:
            context.set_citation(self.citation)
            
        # Metadaten als zusätzliche Daten hinzufügen
        if self.metadata:
            for key, value in self.metadata.items():
                context.add_additional_data(key, value)
                
        return context
    
    def merge_from(self, other: 'Entity') -> None:
        """Übernimmt fehlende Daten aus einer anderen Entität"""
        # Externe Referenzen übernehmen, wenn sie fehlen
        if not self.wikipedia_url and other.wikipedia_url:
            self.wikipedia_url = other.wikipedia_url
        
        if not self.wikidata_id and other.wikidata_id:
            self.wikidata_id = other.wikidata_id
        
        if not self.dbpedia_uri and other.dbpedia_uri:
            self.dbpedia_uri = other.dbpedia_uri
        
        # Labels und Beschreibungen übernehmen
        for lang_code in LanguageCode:
            lang = lang_code.value
            if not self.label.get(lang) and other.label.get(lang):
                self.label.set(lang, other.label.get(lang))
            
            if not self.description.get(lang) and other.description.get(lang):
                self.description.set(lang, other.description.get(lang))
        
        # Aliase übernehmen
        for lang_code, aliases in other.aliases.items():
            for alias in aliases:
                self.add_alias(alias, str(lang_code))
        
        # Quellen übernehmen
        for source_name, source_data in other.sources.items():
            if source_name not in self.sources:
                self.sources[source_name] = source_data
        
        # Zitationsinformationen übernehmen
        if not self.citation and other.citation:
            self.citation = other.citation
            self.citation_start = other.citation_start
            self.citation_end = other.citation_end
            
        # Beziehungen zusammenführen
        if other.relationships:
            existing_relationships = {self._relationship_key(rel) for rel in self.relationships}
            for rel in other.relationships:
                if self._relationship_key(rel) not in existing_relationships:
                    self.relationships.append(rel)
        
        # Metadaten übernehmen und zusammenführen
        for key, value in other.metadata.items():
            if key not in self.metadata:
                self.metadata[key] = value
            elif isinstance(self.metadata[key], list) and isinstance(value, list):
                # Listen zusammenführen und Duplikate entfernen
                self.metadata[key] = list(set(self.metadata[key] + value))
            elif isinstance(self.metadata[key], dict) and isinstance(value, dict):
                # Dictionaries zusammenführen
                self.metadata[key].update(value)
        
        self.updated_at = datetime.utcnow()
        
    def _relationship_key(self, relationship: Dict[str, Any]) -> str:
        """Erzeugt einen eindeutigen Schlüssel für eine Beziehung"""
        subject = relationship.get("subject", "")
        predicate = relationship.get("predicate", "")
        obj = relationship.get("object", "")
        return f"{subject}|{predicate}|{obj}"
        
    def to_dict(self) -> Dict[str, Any]:
        """Konvertiert die Entity in ein standardisiertes Dictionary-Format
        
        Returns:
            Ein Dictionary mit allen Entity-Daten im standardisierten Format
        """
        # Basisstruktur erstellen
        result = {
            "entity": self.name,
            "id": self.id,
            "details": {
                "typ": self.type,
                "inferred": self.inferred
            },
            "sources": {}
        }
        
        # Zitationsinformationen hinzufügen
        if self.citation:
            result["details"]["citation"] = self.citation
            if self.citation_start is not None:
                result["details"]["citation_start"] = self.citation_start
                result["details"]["citation_end"] = self.citation_end
        
        # Service-Daten hinzufügen
        for source_name, source_data in self.sources.items():
            result["sources"][source_name] = source_data.to_dict()
            
            # Legacy-Informationen in details übernehmen
            if source_name == "wikipedia" and "extract" in source_data:
                result["details"]["extract"] = source_data["extract"]
            elif source_name == "dbpedia" and "abstract" in source_data:
                result["details"]["abstract"] = source_data["abstract"]
        
        # Beziehungen hinzufügen, falls vorhanden
        if self.relationships:
            result["relationships"] = self.relationships
        
        # Zusätzliche Metadaten
        for key, value in self.metadata.items():
            if key not in result["details"]:
                result["details"][key] = value
                
        return result
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """Erstellt eine Entity aus einem Dictionary im standardisierten Format
        
        Args:
            data: Ein Dictionary im standardisierten Format
            
        Returns:
            Eine neue Entity-Instanz
        """
        # ID aus Daten übernehmen oder neue generieren
        entity_id = data.get("id") or str(uuid.uuid4())
        entity_name = data.get("entity", "")
        
        # Details extrahieren
        details = data.get("details", {})
        entity_type = details.get("typ") or details.get("type", "")
        inferred = details.get("inferred", "explicit")
        
        # Entity erstellen
        entity = cls(
            id=entity_id,
            name=entity_name,
            type=entity_type,
            inferred=inferred
        )
        
        # Zitationsinformationen hinzufügen
        if "citation" in details:
            entity.citation = details["citation"]
            if "citation_start" in details:
                entity.citation_start = details["citation_start"]
                entity.citation_end = details.get("citation_end")
        
        # Quellen hinzufügen
        sources = data.get("sources", {})
        for source_name, source_data in sources.items():
            entity.add_source(source_name, source_data)
            
            # Legacy-Felder aktualisieren
            if source_name == "wikipedia" and "url" in source_data:
                entity.wikipedia_url = source_data["url"]
            elif source_name == "wikidata" and "id" in source_data:
                entity.wikidata_id = source_data["id"]
            elif source_name == "dbpedia" and "uri" in source_data:
                entity.dbpedia_uri = source_data["uri"]
        
        # Beziehungen hinzufügen
        if "relationships" in data:
            entity.relationships = data["relationships"]
            
        # Zusätzliche Metadaten übernehmen
        for key, value in details.items():
            if key not in ["typ", "type", "inferred", "citation", "citation_start", "citation_end", "extract", "abstract"]:
                entity.metadata[key] = value
                
        return entity
