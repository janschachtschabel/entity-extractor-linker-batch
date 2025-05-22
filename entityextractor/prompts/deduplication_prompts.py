"""
Centralized prompts for relationship deduplication via OpenAI.
Provides system and user prompts for both English and German for semantic deduplication.
"""

def get_system_prompt_semantic_dedup_de():
    """
    Deutsches System-Prompt für semantische Deduplizierung von Beziehungen.
    """
    return """Du bist ein Assistent für Wissensrepräsentation. Deine Aufgabe ist es, semantisch doppelte oder redundante Beziehungen zwischen Entitäten zu identifizieren.

Wichtige Regeln:
1. Identifiziere semantisch äquivalente Beziehungen (gleiche Bedeutung, unterschiedliche Formulierung)
2. Bevorzuge explizite Beziehungen gegenüber impliziten Beziehungen
3. Bevorzuge spezifischere Prädikate gegenüber allgemeineren
4. Behalte "inverse" Beziehungen (A→B und B→A), da diese unterschiedliche Richtungen darstellen
5. Gib die Nummern der Beziehungen an, die du behalten würdest"""


def get_user_prompt_semantic_dedup_de(relations_prompt):
    """
    Deutsches User-Prompt für semantische Deduplizierung von Beziehungen.
    
    Args:
        relations_prompt: Formatierte Liste der Beziehungen
        
    Returns:
        Formatiertes User-Prompt
    """
    return f"""Hier sind die Beziehungen:

{relations_prompt}

Identifiziere semantische Duplikate und gib nur die Nummern der Beziehungen an, die du behalten würdest. Antworte im Format:
KEPT: 1, 3, 5, ...
"""


def get_system_prompt_semantic_dedup_en():
    """
    English system prompt for semantic deduplication of relationships.
    """
    return """You are a knowledge representation assistant. Your task is to identify semantically duplicate or redundant relationships between entities.

Important rules:
1. Identify semantically equivalent relationships (same meaning, different wording)
2. Prefer explicit relationships over implicit relationships
3. Prefer more specific predicates over more general ones
4. Keep "inverse" relationships (A→B and B→A) as they represent different directions
5. Return the numbers of the relationships that you would keep"""


def get_user_prompt_semantic_dedup_en(relations_prompt):
    """
    English user prompt for semantic deduplication of relationships.
    
    Args:
        relations_prompt: Formatted list of relationships
        
    Returns:
        Formatted user prompt
    """
    return f"""Here are the relationships:

{relations_prompt}

Identify semantic duplicates and return only the numbers of the relationships you would keep. Answer in the format:
KEPT: 1, 3, 5, ...
"""
