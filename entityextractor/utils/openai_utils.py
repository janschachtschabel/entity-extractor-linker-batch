"""
openai_utils.py

Hilfsfunktionen für die Interaktion mit der OpenAI API.
"""

import logging
import os
import time
from openai import OpenAI

from entityextractor.config.settings import DEFAULT_CONFIG

def call_openai_api(model, messages, temperature=0.2, config=None):
    """
    Ruft die OpenAI API mit den angegebenen Parametern auf.
    
    Args:
        model: OpenAI Modell (z.B. gpt-4o-mini, gpt-4-turbo)
        messages: Liste von Nachrichtenobjekten (role, content)
        temperature: Temperatur für die Kreativität der Antwort
        config: Konfigurationswörterbuch
        
    Returns:
        OpenAI API Antwort oder None bei Fehler
    """
    if config is None:
        config = DEFAULT_CONFIG
        
    api_key = config.get("OPENAI_API_KEY")
    if not api_key:
        api_key = os.environ.get("OPENAI_API_KEY")
        
    if not api_key:
        logging.error("No OpenAI API key provided. Set OPENAI_API_KEY in config or environment.")
        return None
    
    # LLM-Konfigurationsmerkmale
    base_url = config.get("LLM_BASE_URL", "https://api.openai.com/v1")
    max_tokens = config.get("MAX_TOKENS", 2000)
    
    # Create the OpenAI client
    client = OpenAI(api_key=api_key, base_url=base_url)
    
    try:
        # Log des Modells und des Aufrufs
        start_time = time.time()
        logging.info(f"Calling OpenAI API with model {model}...")
        
        # LLM-Request: max_tokens und base_url immer setzen, temperature explizit übergeben
        # Nur Modelle mit JSON-Mode erlauben response_format
        json_mode_models = [
            "gpt-3.5-turbo-1106", "gpt-3.5-turbo-0125", "gpt-4-1106-preview", 
            "gpt-4-turbo-preview", "gpt-4-0125-preview", "gpt-4o", "gpt-4o-2024-05-13"
        ]
        
        openai_kwargs = dict(
            model=model,
            messages=messages,
            stream=False,
            stop=None,
            timeout=60,
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        if model in json_mode_models:
            openai_kwargs["response_format"] = {"type": "json_object"}
        
        # API-Aufruf
        response = client.chat.completions.create(**openai_kwargs)
        
        # Erfolgsmeldung und Timing
        elapsed_time = time.time() - start_time
        logging.info(f"HTTP Request: POST {base_url}/chat/completions \"HTTP/1.1 200 OK\"")
        logging.info(f"OpenAI API call completed in {elapsed_time:.2f} seconds")
        
        # Konvertiere von OpenAI Response Objekt in Dictionary
        response_dict = {
            "id": response.id,
            "choices": [{"message": {"content": choice.message.content}} for choice in response.choices],
            "usage": {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens
            }
        }
        
        return response_dict
    except Exception as e:
        logging.error(f"Error calling OpenAI API: {e}")
        return None
