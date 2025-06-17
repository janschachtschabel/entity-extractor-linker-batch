"""
Basisklassen für die Entitätsmodelle mit Unterstützung für mehrsprachige Inhalte.
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any

class LanguageCode(str, Enum):
    """Standardisierte Sprachcodes für mehrsprachige Inhalte"""
    DE = "de"  # Deutsch
    EN = "en"  # Englisch
    # Weitere Sprachen können bei Bedarf hinzugefügt werden

@dataclass
class LocalizedText:
    """Repräsentiert einen Text in mehreren Sprachen"""
    de: Optional[str] = None
    en: Optional[str] = None
    
    def get(self, lang: str, default: str = "") -> str:
        """Holt den Text in der angegebenen Sprache"""
        lang = lang.lower()
        if hasattr(self, lang):
            return getattr(self, lang) or default
        return default
    
    def set(self, lang: str, value: str):
        """Setzt den Text für eine bestimmte Sprache"""
        if value and hasattr(self, lang.lower()):
            setattr(self, lang.lower(), value.strip())
    
    def to_dict(self) -> Dict[str, str]:
        """Konvertiert in ein Wörterbuch mit Sprachcodes als Schlüssel"""
        return {k: v for k, v in self.__dict__.items() if v is not None}

@dataclass
class SourceData:
    """Daten aus einer bestimmten Quelle (Wikipedia, Wikidata, DBpedia)
    
    Diese Klasse unterstützt vollständige Dictionary-Operationen:
    - Lesen: source["key"] oder source.get("key")
    - Schreiben: source["key"] = value
    - Löschen: del source["key"]
    - Prüfen: "key" in source
    
    Gleichzeitig bleibt der Attribut-Zugriff erhalten: source.attribute
    """
    id: str
    url: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    status: str = "pending"  # pending, found, not_found, error
    error: Optional[str] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def __getitem__(self, key):
        """Ermöglicht Dictionary-ähnlichen Zugriff zum Lesen: source["key"]"""
        if hasattr(self, key):
            return getattr(self, key)
        elif key in self.data:
            return self.data[key]
        raise KeyError(f"Attribut oder Datenschlüssel '{key}' nicht gefunden")
    
    def __setitem__(self, key, value):
        """Ermöglicht Dictionary-ähnlichen Zugriff zum Schreiben: source["key"] = value"""
        # Bei Kern-Attributen direkt setzen
        if key in ["id", "url", "status", "error", "last_updated"]:
            setattr(self, key, value)
        else:
            # Sonst in data speichern
            self.data[key] = value
    
    def __delitem__(self, key):
        """Ermöglicht Löschen von Keys: del source["key"]"""
        if key in self.data:
            del self.data[key]
        elif hasattr(self, key) and key not in ["id", "data"]:  # Einige Felder sollten nicht gelöscht werden
            setattr(self, key, None)  # Bei Kern-Attributen auf None setzen
        else:
            raise KeyError(f"Schlüssel '{key}' nicht gefunden oder kann nicht gelöscht werden")
    
    def get(self, key, default=None):
        """Dictionary-ähnlicher Zugriff mit Standardwert: source.get("key", default)"""
        try:
            return self[key]
        except KeyError:
            return default
    
    def update(self, other=None, **kwargs):
        """Aktualisiert mehrere Werte gleichzeitig, wie dict.update()"""
        if other is not None:
            # Wenn other ein Dict ist
            if isinstance(other, dict):
                for key, value in other.items():
                    self[key] = value
            # Wenn other ein SourceData-Objekt ist
            elif isinstance(other, SourceData):
                # Basis-Attribute kopieren
                for attr in ["id", "url", "status", "error"]:
                    if getattr(other, attr) is not None:
                        setattr(self, attr, getattr(other, attr))
                # data-Dictionary aktualisieren
                self.data.update(other.data)
        
        # kwargs verarbeiten
        for key, value in kwargs.items():
            self[key] = value
    
    def __contains__(self, key):
        """Unterstützt 'in'-Operator: 'key' in source"""
        return hasattr(self, key) or key in self.data
    
    def to_dict(self):
        """Konvertiert das Objekt in ein Dictionary"""
        result = {}
        # Basis-Attribute hinzufügen
        for key in ["id", "url", "status", "error"]:
            if hasattr(self, key) and getattr(self, key) is not None:
                result[key] = getattr(self, key)
        
        # Daten hinzufügen
        if self.data:
            result.update(self.data)
            
        return result
