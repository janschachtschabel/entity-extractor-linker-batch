import asyncio
import time
import random
from functools import wraps
from loguru import logger

# No need to configure logger with loguru as it's pre-configured

class AsyncRateLimiter:
    """
    An asynchronous rate limiter with exponential backoff on HTTP 429 errors.
    Suitable for use with asyncio and aiohttp.
    """
    def __init__(self, max_calls, period_seconds, backoff_base=1, backoff_max_seconds=60):
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self.backoff_base = backoff_base
        self.backoff_max_seconds = backoff_max_seconds
        self._lock = asyncio.Lock()
        self._calls = []
        self._retry_attempts = {}

    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with self._lock:
                now = time.monotonic()
                # Remove calls older than the period
                self._calls = [t for t in self._calls if t > now - self.period_seconds]

                logger.debug(f"[AsyncRateLimiter] Current rate: {len(self._calls)}/{self.max_calls} calls within {self.period_seconds}s period for {func.__name__}")

                if len(self._calls) >= self.max_calls:
                    oldest_call_in_window = self._calls[0]
                    sleep_duration = (oldest_call_in_window + self.period_seconds) - now
                    if sleep_duration > 0:
                        logger.info(f"[AsyncRateLimiter] Rate limit reached for {func.__name__}. Sleeping for {sleep_duration:.2f}s.")
                        await asyncio.sleep(sleep_duration)
                        logger.info(f"[AsyncRateLimiter] Resumed {func.__name__} after {sleep_duration:.2f}s sleep.")
                
                self._calls.append(time.monotonic())
            
            # Unique key for tracking retries for this specific call (e.g., based on args)
            # This is a simple approach; more robust key generation might be needed for complex args
            call_key = (func.__name__, args, frozenset(kwargs.items()))

            try:
                logger.debug(f"[AsyncRateLimiter] Making API call: {func.__name__} with args: {args}, kwargs: {kwargs}")
                result = await func(*args, **kwargs)
                self._retry_attempts.pop(call_key, None) # Reset attempts on success
                return result
            except Exception as e:
                # Check if the exception is an aiohttp.ClientResponseError with status 429
                is_http_429 = hasattr(e, 'status') and e.status == 429
                
                if is_http_429:
                    current_attempts = self._retry_attempts.get(call_key, 0)
                    self._retry_attempts[call_key] = current_attempts + 1
                    
                    # Exponential backoff with jitter
                    backoff_duration = self.backoff_base * (2 ** current_attempts)
                    backoff_duration = min(backoff_duration, self.backoff_max_seconds)
                    jitter = backoff_duration * random.uniform(-0.1, 0.1) # Add/subtract up to 10% jitter
                    actual_sleep = max(0, backoff_duration + jitter) # Ensure sleep is not negative
                    
                    logger.warning(f"[AsyncRateLimiter] Received HTTP 429 for {func.__name__} (Attempt {current_attempts + 1}). Backing off for {actual_sleep:.2f}s.")
                    await asyncio.sleep(actual_sleep)
                    logger.info(f"[AsyncRateLimiter] Retrying {func.__name__} after 429 backoff.")
                    return await wrapper(*args, **kwargs) # Retry the call
                else:
                    logger.error(f"[AsyncRateLimiter] Exception during {func.__name__}: {e}")
                    self._retry_attempts.pop(call_key, None) # Reset attempts on non-429 error
                    raise # Re-raise other exceptions
        return wrapper
