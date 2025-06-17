#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loguru configuration for example scripts.

This module provides a consistent way to configure loguru for all example scripts.
"""

import sys
from loguru import logger

def configure_logging(level="INFO"):
    """
    Configure loguru with a consistent format for example scripts.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    # Remove default handler
    logger.remove()
    
    # Add a new handler with desired format
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
        level=level,
        colorize=True
    )
    
    return logger
