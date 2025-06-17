#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wikipedia Service für den Entity Extractor.

Dieses Modul stellt die öffentliche API für den Wikipedia-Service bereit.
"""

from entityextractor.services.wikipedia.service import WikipediaService, wikipedia_service
from entityextractor.services.wikipedia.batch_service import BatchWikipediaService, batch_get_wikipedia_pages

__all__ = ['WikipediaService', 'wikipedia_service', 'BatchWikipediaService', 'batch_get_wikipedia_pages']
