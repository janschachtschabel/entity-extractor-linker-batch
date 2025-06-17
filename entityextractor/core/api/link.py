"""
link.py

Imports the linking functionality from the modular entity_linker package.
This file serves as a compatibility layer for existing code.
Supports both the traditional dictionary-based and the
new context-based architecture.
"""

# Import from the new modular system
from entityextractor.core.api.entity_linker.main import link_entities, link_contexts

# For type hints
from typing import List, Dict, Any, Optional, Union
from entityextractor.core.context import EntityProcessingContext

# link_entities and link_contexts are directly imported and exported from the entity_linker.main module
