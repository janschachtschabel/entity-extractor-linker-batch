"""
Centralized prompts for entity generation via OpenAI.
Includes system and user prompts for 'generate' mode, English and German.
"""

def get_system_prompt_generate_en(max_entities, topic):
    return f"""
Generate exactly {max_entities} implicit, logical entities relevant to the topic: {topic}.

Output format:
Each entity as a semicolon-separated line: name_de; name_en; type; wikipedia_url_de; wikipedia_url_en; citation.
One entity per line. No JSON or additional formatting.

Guidelines:
- Set 'citation' to "generated" for each entity.
- Provide BOTH German and English Wikipedia URLs if available.
- `wikipedia_url_en` MUST be from en.wikipedia.org with the exact title; `wikipedia_url_de` SHOULD be from de.wikipedia.org or left blank if no article exists.
- Citations must be exact text spans, max 5 words, no ellipses or truncation.
- Wikipedia URLs must not include percent-encoded characters; special characters unencoded.
- Example types: Assessment, Activity, Competence, Credential, Curriculum, Date, Event, Feedback, Field, Funding, Goal, Group, Language, Location, Method, Objective, Organization, Partnership, Period, Person, Phenomenon, Policy, Prerequisite, Process, Project, Resource, Role, Subject, Support, System, Task, Term, Theory, Time, Tool, Value, Work
- Do not include any explanations or additional text.
"""

def get_user_prompt_generate_en(max_entities, topic):
    return (
        f"Provide exactly {max_entities} implicit entities as semicolon-separated lines: name_de; name_en; type; wikipedia_url_de; wikipedia_url_en; citation. "
        f"Ensure `wikipedia_url_en` is from en.wikipedia.org and `wikipedia_url_de` is from de.wikipedia.org (or left blank if not available). "
        "One entity per line. No JSON."
    )

def get_system_prompt_generate_de(max_entities, topic):
    return f"""
Generiere genau {max_entities} implizite, logische Entitäten zum Thema: {topic}.

Ausgabeformat:
Jede Entität als semikolon-getrennte Zeile: name_de; name_en; type; wikipedia_url_de; wikipedia_url_en; citation.
Eine Entität pro Zeile. Keine JSON oder zusätzliche Formatierung.

Richtlinien:
- Setze 'citation' auf "generated" für jede Entität.
- Gib NACH MÖGLICHKEIT sowohl die deutsche als auch die englische Wikipedia-URL an.
- `wikipedia_url_de` MUSS von de.wikipedia.org stammen (falls vorhanden); `wikipedia_url_en` SOLL von en.wikipedia.org stammen oder leer bleiben, wenn kein Artikel existiert.
- Zitate müssen exakte Textausschnitte sein, maximal 5 Wörter, keine Auslassungen.
- Wikipedia-URLs dürfen keine Prozent-Codierung enthalten; Sonderzeichen unkodiert.
- Beispiel-Typen: Bewertung, Aktivität, Kompetenz, Nachweis, Curriculum, Datum, Ereignis, Rückmeldung, Fachgebiet, Förderung, Ziel, Gruppe, Sprache, Ort, Methode, Lernziel, Organisation, Partnerschaft, Zeitraum, Person, Phänomen, Richtlinie, Voraussetzung, Prozess, Projekt, Ressource, Rolle, Thema, Unterstützung, System, Aufgabe, Begriff, Theorie, Zeit, Werkzeug, Wert, Werk
- Keine Erklärungen oder zusätzlichen Texte.
"""

def get_user_prompt_generate_de(max_entities, topic):
    return (
        f"Gib genau {max_entities} implizite Entitäten als semikolon-getrennte Zeilen zurück: name_de; name_en; type; wikipedia_url_de; wikipedia_url_en; citation. "
        f"Stelle sicher, dass `wikipedia_url_de` von de.wikipedia.org stammt und `wikipedia_url_en` von en.wikipedia.org (falls vorhanden). "
        "Eine Entität pro Zeile. Keine JSON."
    )
