#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relationship Model für den Entity Extractor.

Dieses Modul definiert das Datenmodell für Beziehungen zwischen Entitäten,
einschließlich der Erfassung der Entitätstypen und der Art der Inferenz.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime
import uuid


@dataclass
class Relationship:
    """Repräsentiert eine Beziehung zwischen zwei Entitäten."""
    
    subject: str           # Name der Subjekt-Entität
    predicate: str         # Beziehung zwischen Subjekt und Objekt (Prädikat)
    object: str            # Name der Objekt-Entität
    
    # Typen der beteiligten Entitäten
    subject_type: Optional[str] = None
    object_type: Optional[str] = None
    
    # Art der Inferenz
    inferred: str = "explicit"  # "explicit", "implicit" oder "reference"
    
    # Metadaten
    confidence: float = 1.0
    source: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # IDs für Referenzen
    subject_id: Optional[str] = None
    object_id: Optional[str] = None
    
    # Verwaltungsattribute
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Konvertiert die Beziehung in ein Dictionary."""
        return {
            "id": self.id,
            "subject": self.subject,
            "subject_type": self.subject_type,
            "predicate": self.predicate,
            "object": self.object,
            "object_type": self.object_type,
            "inferred": self.inferred,
            "confidence": self.confidence,
            "source": self.source,
            "metadata": self.metadata,
            "subject_id": self.subject_id,
            "object_id": self.object_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relationship':
        """Erstellt eine Beziehung aus einem Dictionary."""
        # Konvertiere Zeitstempel zurück in datetime-Objekte
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            data["created_at"] = datetime.fromisoformat(created_at)
        
        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            data["updated_at"] = datetime.fromisoformat(updated_at)
        
        return cls(**data)
    
    def update(self, **kwargs: Any) -> None:
        """Aktualisiert die Attribute der Beziehung und setzt updated_at."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        self.updated_at = datetime.utcnow()
