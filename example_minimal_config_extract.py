#!/usr/bin/env python3
"""
example_minimal.py
Minimales Beispielskript für Entity Extractor mit Default-Konfiguration.
"""
import sys
import json
from entityextractor.api import extract_and_link_entities

# UTF-8-Ausgabe sicherstellen
sys.stdout.reconfigure(encoding='utf-8')

if __name__ == "__main__":
    # Beispieltext
    text = "Bill Gates gründete Microsoft und spielte eine entscheidende Rolle in der Entwicklung von Windows."
    # Aufruf ohne eigene Konfiguration: nutzt DEFAULT_CONFIG aus settings.py
    result = extract_and_link_entities(text)
    # Ausgabe als formatiertes JSON
    print(json.dumps(result, ensure_ascii=False, indent=2))
