"""
Rate limiting utilities for the Entity Extractor.

This module provides rate limiting decorators for both synchronous and asynchronous functions.
It includes exponential backoff for HTTP 429 errors and detailed logging.
"""

import time
import threading
import random
from functools import wraps
from loguru import logger

# Re-export AsyncRateLimiter for convenience
from entityextractor.utils.async_rate_limiter import AsyncRateLimiter

class RateLimiter:
    """
    A thread-safe rate limiter with exponential backoff on HTTP 429 errors.
    For asynchronous functions, use AsyncRateLimiter instead.
    """
    def __init__(self, max_calls, period, backoff_base=1, backoff_max=60):
        """
        Initialize a rate limiter.
        
        Args:
            max_calls: Maximum number of calls allowed in the period
            period: Time period in seconds
            backoff_base: Base value for exponential backoff calculation
            backoff_max: Maximum backoff time in seconds
        """
        self.max_calls = max_calls
        self.period = period
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.lock = threading.Lock()
        self.calls = []
        self._retry_attempts = {}

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self.lock:
                now = time.monotonic()
                # Retain only calls within period
                self.calls = [t for t in self.calls if t > now - self.period]
                
                # Log the current rate status
                logger.debug(f"[RateLimiter] Current rate: {len(self.calls)}/{self.max_calls} calls within {self.period}s period for {func.__name__}")
                
                if len(self.calls) >= self.max_calls:
                    sleep_duration = self.calls[0] + self.period - now
                    if sleep_duration > 0:
                        logger.info(f"[RateLimiter] Rate limit reached for {func.__name__}. Sleeping for {sleep_duration:.2f}s.")
                        time.sleep(sleep_duration)
                        logger.info(f"[RateLimiter] Resumed {func.__name__} after {sleep_duration:.2f}s sleep.")
                
                self.calls.append(time.monotonic())
            
            # Unique key for tracking retries for this specific call
            # Convert any unhashable types to their string representation
            try:
                call_key = (func.__name__, args, frozenset(kwargs.items()) if kwargs else None)
            except TypeError:
                # If we have unhashable types in kwargs, use a simpler key
                call_key = (func.__name__, str(args), str(kwargs) if kwargs else None)
            
            try:
                logger.debug(f"[RateLimiter] Making API call: {func.__name__}")
                result = func(*args, **kwargs)
                self._retry_attempts.pop(call_key, None)  # Reset attempts on success
                return result
            except Exception as e:
                # Check if the exception is related to HTTP 429 (Too Many Requests)
                is_http_429 = False
                
                # Check for requests.Response object
                resp = getattr(e, 'response', None)
                if resp is not None and getattr(resp, 'status_code', None) == 429:
                    is_http_429 = True
                
                if is_http_429:
                    current_attempts = self._retry_attempts.get(call_key, 0)
                    self._retry_attempts[call_key] = current_attempts + 1
                    
                    # Exponential backoff with jitter
                    backoff_duration = self.backoff_base * (2 ** current_attempts)
                    backoff_duration = min(backoff_duration, self.backoff_max)
                    jitter = backoff_duration * random.uniform(-0.1, 0.1)  # Add/subtract up to 10% jitter
                    actual_sleep = max(0, backoff_duration + jitter)  # Ensure sleep is not negative
                    
                    logger.warning(f"[RateLimiter] Received HTTP 429 for {func.__name__} (Attempt {current_attempts + 1}). Backing off for {actual_sleep:.2f}s.")
                    time.sleep(actual_sleep)
                    logger.info(f"[RateLimiter] Retrying {func.__name__} after 429 backoff.")
                    return wrapper(*args, **kwargs)  # Retry the call
                else:
                    logger.error(f"[RateLimiter] Exception during {func.__name__}: {e}")
                    self._retry_attempts.pop(call_key, None)  # Reset attempts on non-429 error
                    raise  # Re-raise other exceptions
        return wrapper
