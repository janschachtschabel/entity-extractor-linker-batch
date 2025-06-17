#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DBpedia Service

This module provides services for fetching and processing data from DBpedia.
It includes functionality for batch processing of entities with fallback mechanisms.
"""

from .service import DBpediaService
from .batch_service import BatchDBpediaService, batch_get_dbpedia_info

__all__ = [
    'DBpediaService',
    'BatchDBpediaService',
    'batch_get_dbpedia_info',
]
