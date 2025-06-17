from SPARQLWrapper import SPARQLWrapper, JSON
import re, json

def wikipedia_to_dbpedia_url(url: str) -> str:
    m = re.match(r'https?://en\.wikipedia\.org/wiki/(.+)', url)
    if not m:
        raise ValueError(f"Invalid English Wikipedia URL: {url}")
    return f"http://dbpedia.org/resource/{m.group(1)}"

def batch_query(wikipedia_urls):
    uris = [wikipedia_to_dbpedia_url(u) for u in wikipedia_urls]
    values_clause = " ".join(f"<{u}>" for u in uris)

    sparql = SPARQLWrapper("http://dbpedia.org/sparql")
    sparql.setReturnFormat(JSON)

    # ---------- SPARQL -----------
    query = """
    PREFIX dbo:  <http://dbpedia.org/ontology/>
    PREFIX dbp:  <http://dbpedia.org/property/>
    PREFIX dct:  <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?entity ?label ?abstract ?type
           ?partOf ?hasPart
           ?seeAlso ?isPrimaryTopicOf ?primaryTopicDoc
           ?subject ?subjectLabel
    WHERE {{
      VALUES ?entity {{ {values_clause} }}

      OPTIONAL {{ ?entity rdfs:label   ?label    FILTER (lang(?label) = "en") }}
      OPTIONAL {{ ?entity dbo:abstract ?abstract FILTER (lang(?abstract) = "en") }}
      OPTIONAL {{ ?entity rdf:type      ?type }}

      OPTIONAL {{
        ?entity (dbo:isPartOf | dct:isPartOf | dbp:partof | dbp:partOf
               | ^dbo:hasPart | ^dct:hasPart | ^dbp:hasPart | ^dbp:haspart
               | dbo:country | dbo:state | dbo:region | dbo:province
               | dbo:territory | ^dbo:subdivision) ?partOf .
        FILTER(STRSTARTS(STR(?partOf), "http://dbpedia.org/resource/"))
      }}

      OPTIONAL {{
        ?entity (dbo:hasPart | dct:hasPart | dbp:hasPart | dbp:haspart
               | ^dbo:isPartOf | ^dct:isPartOf | ^dbp:partof | ^dbp:partOf
               | dbo:district | dbo:subdivision | dbo:component
               | dbo:municipality | dbo:borough | dbo:department) ?hasPart .
        FILTER(STRSTARTS(STR(?hasPart), "http://dbpedia.org/resource/"))
      }}

      OPTIONAL {{ ?entity rdfs:seeAlso ?seeAlso }}
      OPTIONAL {{ ?entity foaf:isPrimaryTopicOf ?isPrimaryTopicOf }}
      OPTIONAL {{ ?primaryTopicDoc foaf:primaryTopic ?entity }}

      OPTIONAL {{
        ?entity dct:subject ?subject .
        OPTIONAL {{ ?subject rdfs:label ?subjectLabel FILTER (lang(?subjectLabel) = "de") }}
      }}
    }}
    """.format(values_clause=values_clause)   # only real placeholder

    sparql.setQuery(query)
    data = sparql.query().convert()

    # ---------- Aggregation ----------
    bag = {}
    for b in data["results"]["bindings"]:
        e = b["entity"]["value"]
        rec = bag.setdefault(e, {
            "entity": e, "label": None, "abstract": None,
            "types": [], "partOf": [], "hasPart": [],
            "seeAlso": [], "isPrimaryTopicOf": [],
            "primaryTopicOf": [], "subjects": []
        })

        if "label" in b and not rec["label"]:
            rec["label"] = b["label"]["value"]
        if "abstract" in b and not rec["abstract"]:
            rec["abstract"] = b["abstract"]["value"]

        for var, key in [("type", "types"), ("partOf", "partOf"), ("hasPart", "hasPart"),
                         ("seeAlso", "seeAlso"), ("isPrimaryTopicOf", "isPrimaryTopicOf"),
                         ("primaryTopicDoc", "primaryTopicOf")]:
            if var in b:
                val = b[var]["value"]
                if val not in rec[key]:
                    rec[key].append(val)

        if "subject" in b:
            subj = b["subject"]["value"]
            label = b.get("subjectLabel", {}).get("value")
            entry = {"uri": subj, "label_de": label}
            if entry not in rec["subjects"]:
                rec["subjects"].append(entry)

    return list(bag.values())

# ------------ Demo ------------
if __name__ == "__main__":
    urls = [
        "https://en.wikipedia.org/wiki/Laser",
        "https://en.wikipedia.org/wiki/Berlin",
        "https://en.wikipedia.org/wiki/Germany"
    ]
    print(json.dumps(batch_query(urls), indent=2, ensure_ascii=False))
