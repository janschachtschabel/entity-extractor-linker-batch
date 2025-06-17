"""
Logging utilities for the Entity Extractor.

This module provides functions for configuring and managing logging
throughout the application using loguru.
"""

from loguru import logger
import sys
import io
import urllib3
from typing import Optional, Dict, Any, Union
from entityextractor.config.settings import DEFAULT_CONFIG

# Dictionary to track which loggers have been configured
_configured_loggers = {}

def get_service_logger(name: str, service_name: str, config: Optional[Dict[str, Any]] = None) -> logger:
    """
    Get a logger with consistent formatting for a specific service.
    
    Args:
        name: The name of the logger (usually __name__)
        service_name: The service name to use in log messages (e.g., 'wikipedia', 'wikidata', 'dbpedia')
        config: Optional configuration dictionary
        
    Returns:
        loguru logger instance
    """
    # With loguru, we don't need to create separate logger instances
    # The service_name can be used in the context for structured logging if needed
    bound_logger = logger.bind(service=service_name)
    # Provide stdlib-logging compatibility helper so code can call isEnabledFor()
    if not hasattr(bound_logger, "isEnabledFor"):
        def _is_enabled_for(level):
            try:
                # Map to loguru numeric levels; treat level names not present as DEBUG baseline
                debug_no = logger.level("DEBUG").no
                return level >= debug_no
            except Exception:
                # Fallback: enable for everything
                return True
        # Monkey-patch method (loguru logger supports attribute assignment on bound logger)
        bound_logger.isEnabledFor = _is_enabled_for  # type: ignore
    return bound_logger

def configure_logging(config=None):
    """
    Configure logging based on configuration settings.
    
    Args:
        config: Configuration dictionary with logging settings
    """
    from entityextractor.config.settings import DEFAULT_CONFIG
    
    if config is None:
        config = DEFAULT_CONFIG
    
    # Remove all existing handlers from loguru
    logger.remove()
    
    # Import the logging_config module which sets up loguru
    from entityextractor.utils.logging_config import setup_logging
    
    # Use the loguru configuration from logging_config
    setup_logging(config)

    # Console handler and formatting is now handled by logging_config.py
    # which uses loguru for all logging

    # Suppress SSL warnings (if configured)
    if config.get("SUPPRESS_TLS_WARNINGS", True):
        urllib3.disable_warnings()
    
    # With loguru, we don't need to configure individual loggers with formatters
    # All logging is handled through the global logger instance with configuration from logging_config.py
    
    # Log a message to confirm logging is configured
    logger.info("Logging configured with loguru")
    
    # Debug information about configuration if in debug mode
    debug_mode = config.get('DEBUG_MODE', False)
    if debug_mode:
        logger.debug(f"Logging configuration: {config.get('LOG_LEVEL', 'INFO')}, DEBUG_MODE={debug_mode}")
        logger.debug(f"Using loguru for all logging across the application")
