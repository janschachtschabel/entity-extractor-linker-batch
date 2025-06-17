"""
source_utils.py

Utility functions for uniform and safe access to various data structures,
especially for handling SourceData objects.
"""

from typing import Any, Dict, Optional, TypeVar, Union
from loguru import logger

T = TypeVar('T')

def safe_get(obj: Any, key_or_attr: str, default: Optional[T] = None) -> Union[Any, T]:
    """
    Safe access to attributes or dictionary keys.
    
    This function attempts to access an attribute or dictionary key,
    and returns a default value if the access fails.
    
    Args:
        obj: The object or dictionary to access
        key_or_attr: The name of the attribute or dictionary key
        default: The default value to return if access fails
        
    Returns:
        The value of the attribute/key or the default value
    """
    # If None, immediately return the default value
    if obj is None:
        return default
    
    # Try to access as attribute
    if hasattr(obj, key_or_attr):
        return getattr(obj, key_or_attr, default)
    
    # Try to access as dictionary key
    if isinstance(obj, dict) and key_or_attr in obj:
        return obj[key_or_attr]
    
    # If all fails, return the default value
    return default

def safe_source_access(entity, source_name: str, attribute: str, default: Any = None) -> Any:
    """
    Safe access to an attribute of a SourceData for an entity.
    
    This function handles different scenarios:
    1. When the entity has a has_source/sources structure (Entity object)
    2. When the entity is a dictionary with 'sources'
    3. When the entity uses the new data structures (wikipedia_data, wikidata_data, dbpedia_data)
    
    Args:
        entity: The entity (object or dictionary)
        source_name: The name of the source (e.g., 'wikipedia', 'wikidata', 'dbpedia')
        attribute: The attribute of the source to access
        default: The default value to return if access fails
        
    Returns:
        The value of the attribute or the default value
    """
    # Check if it's an Entity object
    if hasattr(entity, 'has_source') and hasattr(entity, 'sources'):
        # Entity object
        if entity.has_source(source_name):
            source = entity.sources.get(source_name)
            return safe_get(source, attribute, default)
    elif isinstance(entity, dict):
        # First check for the new data structures
        data_field = f"{source_name}_data"
        if data_field in entity:
            return safe_get(entity[data_field], attribute, default)
        
        # Fallback to the old sources structure
        sources = entity.get('sources', {})
        if source_name in sources:
            source = sources[source_name]
            return safe_get(source, attribute, default)
    
    # If no access is possible, return the default value
    return default

def ensure_dict_format(obj: Any) -> Dict:
    """
    Ensures that an object is represented as a dictionary.
    
    For dictionary-like objects, a dictionary is returned.
    For other objects, an attempt is made to convert it to a dictionary.
    
    Args:
        obj: The object to convert
        
    Returns:
        A dictionary representing the object
    """
    if isinstance(obj, dict):
        return obj
    
    # If the object has a to_dict method, use it
    if hasattr(obj, 'to_dict') and callable(getattr(obj, 'to_dict')):
        return obj.to_dict()
    
    # If the object has __dict__, use it
    if hasattr(obj, '__dict__'):
        return {k: v for k, v in obj.__dict__.items() 
                if not k.startswith('_') and not callable(v)}
    
    # Fallback: Try to convert the object to a dictionary
    try:
        return dict(obj)
    except (TypeError, ValueError):
        logger.warning(f"Could not convert object of type {type(obj)} to dictionary")
        return {}
