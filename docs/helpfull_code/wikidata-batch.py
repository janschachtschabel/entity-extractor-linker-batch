import requests

endpoint = "https://query.wikidata.org/sparql"
entities = ["wd:Q42", "wd:Q64", "wd:Q90"]

query = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT ?item ?itemLabel ?itemDesc
       (GROUP_CONCAT(DISTINCT ?typeLabel; separator="|") AS ?types)
       (GROUP_CONCAT(DISTINCT ?partOfLabel; separator="|") AS ?partsOf)
       (GROUP_CONCAT(DISTINCT ?hasPartLabel; separator="|") AS ?hasParts)
WHERE {
  VALUES ?item { """ + " ".join(entities) + """ }

  OPTIONAL {
    ?item wdt:P31 ?type.
    ?type rdfs:label ?typeLabel.
    FILTER(lang(?typeLabel) IN("de","en"))
  }
  OPTIONAL {
    ?item wdt:P361 ?partOf.
    ?partOf rdfs:label ?partOfLabel.
    FILTER(lang(?partOfLabel) IN("de","en"))
  }
  OPTIONAL {
    ?item wdt:P527 ?hasPart.
    ?hasPart rdfs:label ?hasPartLabel.
    FILTER(lang(?hasPartLabel) IN("de","en"))
  }

  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "de,en".
    ?item rdfs:label ?itemLabel.
    ?item schema:description ?itemDesc.
  }
}
GROUP BY ?item ?itemLabel ?itemDesc
"""

r = requests.post(endpoint, data={"query": query}, headers={"Accept": "application/sparql-results+json"})
r.raise_for_status()
rows = r.json()["results"]["bindings"]

for row in rows:
    print("----")
    print("Item:", row["itemLabel"]["value"])
    print("Beschreibung:", row["itemDesc"]["value"])
    print("Typ(en):", row.get("types", {}).get("value", "–"))
    print("Teil von:", row.get("partsOf", {}).get("value", "–"))
    print("Hat Teil:", row.get("hasParts", {}).get("value", "–"))
