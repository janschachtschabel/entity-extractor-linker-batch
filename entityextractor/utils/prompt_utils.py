from entityextractor.prompts.extract_prompts import TYPE_RESTRICTION_TEMPLATE_EN, TYPE_RESTRICTION_TEMPLATE_DE


def apply_type_restrictions(system_prompt: str, allowed_entity_types, language: str) -> str:
    """
    Append a type restriction to the system prompt if allowed_entity_types is not 'auto'.
    
    Args:
        system_prompt: The system prompt to append type restrictions to.
        allowed_entity_types: Either "auto", a comma-separated string of types, or a list of types.
        language: The language code ("en" or "de").
        
    Returns:
        The system prompt with type restrictions appended if applicable.
    """
    # Handle different input types for allowed_entity_types
    if allowed_entity_types and allowed_entity_types != "auto":
        if isinstance(allowed_entity_types, list):
            # Already a list, just join
            types_str = ", ".join([t.strip() for t in allowed_entity_types])
        elif isinstance(allowed_entity_types, str):
            # String that needs to be split
            types = [t.strip() for t in allowed_entity_types.split(",")]
            types_str = ", ".join(types)
        else:
            # Unsupported type, default to no restrictions
            return system_prompt
            
        template = TYPE_RESTRICTION_TEMPLATE_EN if language.lower() == "en" else TYPE_RESTRICTION_TEMPLATE_DE
        system_prompt += template.format(entity_types=types_str)
    return system_prompt
