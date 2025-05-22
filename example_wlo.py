#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Beispielskript: Generiere 10 Entitäten zu einem WLO-Bildungsinhalt anhand seiner ID (Titel, Beschreibung, Keywords).
Die drei Metadaten werden als Topic für die Entitätsgenerierung genutzt.
"""
import sys
import os
import json
import logging
import requests
from entityextractor.api import generate_and_link_entities

# Wikipedia2Vec Integration
import warnings
warnings.filterwarnings("ignore")
try:
    from wikipedia2vec import Wikipedia2Vec
except ImportError:
    Wikipedia2Vec = None
    print("[WARN] wikipedia2vec is not installed. Please install it via pip if you want similarity features.")

sys.stdout.reconfigure(encoding='utf-8')

# === WLO API-Konfiguration ===
WLO_API_BASE = "https://redaktion.openeduhub.net/edu-sharing/rest"


def get_wlo_metadata(wlo_id):
    """
    Sucht einen WLO-Inhalt anhand einer ID (oder Suchbegriff) über die neue WLO-API (POST-Request).
    Gibt ein Dict mit allen relevanten Metadaten zurück (title, description, keywords, subject, educationalContext, wwwUrl, previewUrl, resourceType).
    """
    search_url = "https://repository.staging.openeduhub.net/edu-sharing/rest/search/v1/queries/-home-/mds_oeh/ngsearch?contentType=FILES&maxItems=1&skipCount=0&propertyFilter=-all-&combineMode=AND"
    criteria = [
        {"property": "ngsearchword", "values": [wlo_id]},
    ]
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "WLO-KI-EntityExtractor"
    }
    try:
        resp = requests.post(search_url, headers=headers, json={"criteria": criteria}, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        print("=== RAW API RESPONSE ===\n" + json.dumps(data, indent=2, ensure_ascii=False) + "\n========================\n")
        nodes = data.get("nodes", [])
        if not nodes:
            logging.warning(f"Keine WLO-Ressource gefunden für Suchbegriff/ID: {wlo_id}")
            return None
        node = nodes[0]
        props = node.get("properties", {})
        print("[DEBUG] node['properties']:\n" + json.dumps(props, indent=2, ensure_ascii=False))
        title = props.get("cclom:title", [""])[0]
        description = props.get("cclom:general_description", [""])[0]
        keywords_list = props.get("cclom:general_keyword", [])
        keyword = keywords_list[0] if keywords_list else ""
        www_url = props.get("ccm:wwwurl", [None])[0]
        print(f"[DEBUG] Extracted WLO fields:\n  title: {title}\n  description: {description}\n  keyword: {keyword}\n  wwwUrl: {www_url}\n")
        return {
            "title": title,
            "description": description,
            "keyword": keyword,
            "wwwUrl": www_url,
        }
    except Exception as e:
        logging.error(f"Fehler beim Abruf der WLO-Metadaten (Such-API) für '{wlo_id}': {e}")
        return None


def main():
    # Beispiel-WLO-ID (ersetzen durch gewünschte ID)
    # Musterinhalt: https://redaktion.openeduhub.net/edu-sharing/components/render/c1057013-9540-4a07-8fd8-58a55be29240?closeOnBack=true
    wlo_id = "Leben ohne den Mond: eine wissenschaftliche Spekulation"

    # --- Metadaten abrufen ---
    meta = get_wlo_metadata(wlo_id)
    if not meta:
        print(f"Keine Metadaten für WLO-ID {wlo_id} gefunden.")
        return

    # --- Metadaten formatiert ausgeben ---
    print("\n===== WLO-Metadaten =====")
    print(f"Titel      : {meta['title']}")
    print(f"Beschreibung: {meta['description']}")
    print(f"Stichwort  : {meta['keyword']}")
    print(f"URL        : {meta['wwwUrl']}")
    print("========================\n")

    # --- Topic-String für Generierung bauen ---
    topic = f"{meta['title']}\n\n{meta['description']}\n\nStichworte: {meta['keyword']}"
    print("=== Topic für Generierung ===\n", topic, "\n===========================\n")

    # --- Konfiguration für die Entitätsgenerierung ---
    config = {
        "LLM_BASE_URL": "https://api.openai.com/v1",
        "MODEL": "gpt-4.1-mini",
        "OPENAI_API_KEY": None,  # oder per Umgebungsvariable
        "MAX_TOKENS": 16000,
        "TEMPERATURE": 0.2,
        "LANGUAGE": "de",
        "MODE": "compendium",
        "MAX_ENTITIES": 10,
        "ALLOWED_ENTITY_TYPES": "auto",
        "ENABLE_ENTITY_INFERENCE": False,
        "RELATION_EXTRACTION": True,
        "ENABLE_RELATIONS_INFERENCE": False,
        "MAX_RELATIONS": 15,
        "USE_WIKIPEDIA": True,
        "USE_WIKIDATA": False,
        "USE_DBPEDIA": False,
        "ADDITIONAL_DETAILS": False,
        "ENABLE_GRAPH_VISUALIZATION": True,
        "ENABLE_KGC": False,
        "GRAPH_LAYOUT_METHOD": "spring",
        "GRAPH_PNG_SCALE": 0.30,
        "GRAPH_HTML_INITIAL_SCALE": 10,
        "COLLECT_TRAINING_DATA": False,
        "TIMEOUT_THIRD_PARTY": 20,
        "SHOW_STATUS": True,
        "SUPPRESS_TLS_WARNINGS": True,
        "CACHE_ENABLED": True,
        "CACHE_DIR": os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache"),
        "CACHE_WIKIPEDIA_ENABLED": True,
        "CACHE_WIKIDATA_ENABLED": True,
        "CACHE_DBPEDIA_ENABLED": True,
    }

    # --- Entitäten generieren ---
    result = generate_and_link_entities(topic, config)

    print("\n=== Generierte Entitäten und Beziehungen ===")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # === Wikipedia2Vec: Entity Vectors & Similarity ===
    model_path = os.path.join(os.path.dirname(__file__), "dewiki_20180420_100d.pkl")
    if Wikipedia2Vec is None or not os.path.exists(model_path):
        print("\n[WARN] Wikipedia2Vec-Modell nicht gefunden oder wikipedia2vec nicht installiert. Überspringe Vektor- und Ähnlichkeitsberechnung.")
        return

    wiki2vec = Wikipedia2Vec.load(model_path)
    print("\n===== Wikipedia2Vec-Analyse =====")
    entities = result.get("entities", [])
    entity_names = []
    entity_vectors = []
    similar_entities_set = set()
    entity_similarities = {}

    # 1. Entity-Vektoren sammeln und ausgeben
    import numpy as np
    entity_vec_map = {}
    for ent in entities:
        name = ent.get("entity") or ent.get("name")
        if not name:
            continue
        try:
            vec = wiki2vec.get_entity_vector(name)
            entity_names.append(name)
            entity_vectors.append(vec)
            entity_vec_map[name] = vec
        except KeyError:
            print(f"[Wikipedia2Vec] Entität nicht im Modell gefunden: {name}")
            continue

    if not entity_vectors:
        print("Keine Entitäten mit Wikipedia2Vec-Vektor gefunden.")
        return

    print("\n===== Vektoren der extrahierten Entitäten (erste 10 Dimensionen) =====")
    for name, vec in entity_vec_map.items():
        print(f"{name}: {vec[:10]} ...")
    print("\n(Optional: Komplette Vektoren als JSON)")
    print(json.dumps({k: v.tolist() for k, v in entity_vec_map.items()}, ensure_ascii=False, indent=2))

    # 2. Durchschnittsvektor berechnen und ausgeben
    avg_vec = np.mean(entity_vectors, axis=0)
    print(f"\n===== Durchschnittsvektor aller Entitäten =====")
    print(f"Erste 10 Dimensionen: {avg_vec[:10]} ...")
    print("Vektor als JSON:")
    print(json.dumps(avg_vec.tolist(), ensure_ascii=False, indent=2))

    # 3. Top-5 ähnliche Entitäten für jede extrahierte Entität
    print("Top 5 ähnliche Entitäten pro extrahierter Entität:")
    for name in entity_names:
        try:
            entity_obj = wiki2vec.get_entity(name)
            similars = wiki2vec.most_similar(entity_obj, count=5)
            entity_similarities[name] = []
            for sim_ent, score in similars:
                label = str(sim_ent)
                if label not in entity_names:
                    similar_entities_set.add(label)
                entity_similarities[name].append((label, float(score)))
            print(f"\n{name}:")
            for rank, (label, score) in enumerate(entity_similarities[name], 1):
                print(f"  {rank}. {label} (Score: {score:.4f})")
        except Exception as e:
            print(f"[Wikipedia2Vec] Fehler bei {name}: {e}")
            continue
    # 4. Deduplication and summary
    deduped_similars = sorted(similar_entities_set)
    print("\n===== Deduped Liste aller ähnlichen Entitäten (nicht in Original-Extraktion): =====")
    print(json.dumps(deduped_similars, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
