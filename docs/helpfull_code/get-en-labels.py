# labels_all_dbpedia.py – DE/EN-Labels + DBpedia-URIs (fix: build from langlink)
import sys
if sys.version_info < (3, 8):
    raise RuntimeError("Dieses Skript benötigt Python 3.8 oder höher.")

import requests, itertools

API_DE = "https://de.wikipedia.org/w/api.php"
API_WD = "https://www.wikidata.org/w/api.php"
S = requests.Session()

def batched(seq, n=50):
    it = iter(seq)
    while (chunk := list(itertools.islice(it, n))):
        yield chunk

def fetch_labels_and_dbpedia(titles_de):
    results, need_qid = {}, set()

    # 1. MediaWiki: DE-Titel, EN-Langlink, Q-ID
    for block in batched(titles_de):
        params = {
            "action": "query",
            "redirects": 1,
            "titles": "|".join(block),
            "prop": "langlinks|pageprops",
            "lllang": "en",
            "lllimit": "max",
            "ppprop": "wikibase_item",
            "format": "json"
        }
        data = S.get(API_DE, params=params, timeout=10).json()
        redirects = {rd["from"]: rd["to"]
                     for rd in data.get("query", {}).get("redirects", [])}

        for page in data["query"]["pages"].values():
            src_title = None if "missing" in page else page.get("title")
            original = next((t for t in block if redirects.get(t, t) == src_title), None)
            ll = page.get("langlinks", [])
            tgt_title = ll[0]["*"] if ll else None
            qid = page.get("pageprops", {}).get("wikibase_item")

            results[original] = {
                "label_de": src_title,
                "label_en": tgt_title,
                "qid": qid,
                "dbpedia_uri": "http://dbpedia.org/resource/" + tgt_title.replace(" ", "_") if tgt_title else None
            }

            if tgt_title is None and qid:
                need_qid.add(qid)

    # 2. Wikidata fallback (nur wenn kein langlink da war)
    if need_qid:
        params = {
            "action": "wbgetentities",
            "ids": "|".join(need_qid),
            "props": "sitelinks|labels",
            "languages": "en",
            "sitefilter": "enwiki",
            "format": "json"
        }
        data = S.get(API_WD, params=params, timeout=10).json()
        for ent in data.get("entities", {}).values():
            qid = ent["id"]
            sitelink = ent.get("sitelinks", {}).get("enwiki", {}).get("title")
            label = ent.get("labels", {}).get("en", {}).get("value")
            for rec in results.values():
                if rec["qid"] == qid and rec["label_en"] is None:
                    rec["label_en"] = sitelink or label
                    if sitelink:
                        rec["dbpedia_uri"] = "http://dbpedia.org/resource/" + sitelink.replace(" ", "_")

    # 3. Manuelle Fixes
    manual_fixes = {
        "Beugung": "Diffraction",
        "Optische Achse": "Optical axis",
        "Abbildung": "Image formation"
    }
    for key, rec in results.items():
        if rec["label_en"] is None and key in manual_fixes:
            label = manual_fixes[key]
            rec["label_en"] = label
            rec["dbpedia_uri"] = "http://dbpedia.org/resource/" + label.replace(" ", "_")

    return results

# ---------------- Demo -----------------
if __name__ == "__main__":
    german_terms = [
        "Lichtbrechung", "Reflexion", "Brechungsgesetz", "Linsen", "Spiegel",
        "Optische Achse", "Refraktometer", "Totalreflexion", "Strahlenoptik",
        "Polarisation", "Lichtgeschwindigkeit", "Brennweite",
        "Optische Täuschung", "Abbildung", "Interferenz", "Beugung"
    ]

    mapping = fetch_labels_and_dbpedia(german_terms)

    print(f"{'Eingabe':<22} → DE-Label{'':<22} | EN-Label{'':<30} | DBpedia URI")
    print("-" * 120)
    for term in german_terms:
        rec = mapping.get(term, {})
        label_de = rec.get("label_de", "–")
        label_en = rec.get("label_en", "–")
        uri = rec.get("dbpedia_uri", "–")
        print(f"{term:<22} → {label_de:<30} | {label_en:<30} | {uri}")
