#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Loguru configuration for the Entity Extractor.

This module provides functions for configuring Loguru throughout the application.
"""

import sys
import os
from datetime import datetime
from loguru import logger


def setup_logging(config=None):
    """
    Configure Loguru for the Entity Extractor.
    
    Args:
        config: Optional configuration dictionary
    """
    if config is None:
        config = {}
    
    # Remove all existing handlers
    logger.remove()
    
    # Get configuration values
    debug_mode = config.get("DEBUG_MODE", False)
    log_level_str = config.get("LOG_LEVEL", "INFO").upper()
    
    # Determine effective log level
    if debug_mode:
        log_level = "DEBUG"
    else:
        log_level = log_level_str
    
    # Log directory
    log_dir = config.get("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Current date for the filename
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"entityextractor_{current_date}.log")
    
    # Console logger
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=True
    )
    
    # File logger
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=log_level,
        rotation="10 MB",
        compression="zip",
        retention="30 days"
    )
    
    logger.info(f"Logging configured with loguru: level={log_level}, file={log_file}")
    
    return logger


def get_module_logger(module_name):
    """
    Returns a logger for a specific module.
    
    Args:
        module_name: Name of the module
        
    Returns:
        Loguru logger object
    """
    return logger.bind(module=module_name)


def get_service_logger(service_name):
    """
    Returns a logger for a specific service.
    
    Args:
        service_name: Name of the service (e.g., 'wikipedia', 'wikidata', 'dbpedia')
        
    Returns:
        Loguru logger object
    """
    return logger.bind(service=service_name)


# Alias for backward compatibility
configure_logging = setup_logging
