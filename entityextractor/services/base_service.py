#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Base class for all services in the Entity Extractor.

This class provides common functionality for all services,
such as session management, logging, and basic processing methods.
"""

from typing import Dict, List, Any, Optional, TypeVar, Generic
import aiohttp
import asyncio
from loguru import logger

from entityextractor.models.data_models import EntityData

# Generic type for input and output data
T = TypeVar('T')


class BaseService(Generic[T]):
    """Base class for all services with common functionality."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initializes the BaseService.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.session = None
        self.logger = logger
        self.debug_mode = self.config.get("DEBUG", False)
        
    async def create_session(self) -> aiohttp.ClientSession:
        """
        Creates an aiohttp.ClientSession if none exists.
        
        Returns:
            The active ClientSession
        """
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self.config.get("TIMEOUT", 30),
                connect=self.config.get("CONNECT_TIMEOUT", 10)
            )
            headers = {
                'User-Agent': self.config.get(
                    'USER_AGENT', 
                    'EntityExtractor/1.0 (https://github.com/windsurf/entityextractor)'
                )
            }
            self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
            self.logger.debug(f"{self.__class__.__name__}: New session created")
        return self.session
        
    async def close_session(self) -> None:
        """Closes the aiohttp.ClientSession if it exists."""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.debug(f"{self.__class__.__name__}: Session closed")
            self.session = None
            
    async def __aenter__(self):
        """Async context manager entry."""
        await self.create_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_session()
        
    async def process_entity(self, entity: EntityData) -> EntityData:
        """
        Processes a single entity. To be implemented by subclasses.
        
        Args:
            entity: The entity to process
            
        Returns:
            The processed entity
            
        Raises:
            NotImplementedError: This method must be implemented by subclasses
        """
        raise NotImplementedError("This method must be implemented by subclasses.")
        
    async def process_batch(self, entities: List[EntityData]) -> List[EntityData]:
        """
        Processes multiple entities in parallel.
        
        Args:
            entities: List of entities to process
            
        Returns:
            List of processed entities
        """
        if not entities:
            return []
            
        # Create session
        await self.create_session()
        
        # Split processing into tasks
        tasks = [self.process_entity(entity) for entity in entities]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Error handling
        processed_entities = []
        for entity, result in zip(entities, results):
            if isinstance(result, Exception):
                self.logger.error(f"Error processing {entity.entity_name}: {result}")
                processed_entities.append(entity)  # Keep original entity
            else:
                processed_entities.append(result)
                
        return processed_entities
