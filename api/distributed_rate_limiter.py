"""
Distributed rate limiter for coordination across multiple containers.
All containers (scanner, metadata_worker, api) share a single Redis-backed rate limit.
This ensures the global API limit is respected regardless of which container makes the request.
"""
import time
import logging
from redis import Redis
from api.phase import attach_phase_filter, temp_phase

logger = logging.getLogger(__name__)

# Ensure console handler includes phase information when module is imported directly
if not logger.handlers:
    h = logging.StreamHandler()
    fmt = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s [%(phase)s]: %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ')
    h.setFormatter(fmt)
    attach_phase_filter(h)
    logger.addHandler(h)

# Redis keys for distributed rate limiting
REDIS_KEY_LAST_API_CALL = "pineapple:api:last_call_timestamp"
REDIS_KEY_API_CALL_COUNT = "pineapple:api:call_count_1min"


class DistributedRateLimiter:
    """
    Distributed rate limiter using Redis for coordination across containers.
    Enforces both:
    1. Minimum delay between any two API calls (API_RATE_DELAY_SECONDS)
    2. Maximum calls per minute (API_MAX_CALLS_MINUTE)
    """
    
    def __init__(self, redis_url: str, min_delay_seconds: float, max_calls_per_minute: int):
        """
        Initialize the distributed rate limiter.
        
        Args:
            redis_url: Redis connection URL
            min_delay_seconds: Minimum delay between API calls (e.g., 7 seconds)
            max_calls_per_minute: Maximum API calls allowed per minute (e.g., 8)
        """
        self.redis_client = Redis.from_url(redis_url)
        self.min_delay_seconds = min_delay_seconds
        self.max_calls_per_minute = max_calls_per_minute
        self.container_name = "unknown"
        
        try:
            self.redis_client.ping()
            logger.debug("Connected to Redis for distributed rate limiting")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def set_container_name(self, name: str):
        """Set the container name for logging purposes."""
        self.container_name = name
    
    def wait_if_needed(self) -> float:
        """
        Block if necessary to maintain rate limits.
        Returns the duration slept in seconds.
        
        Returns:
            Duration slept (0 if no wait was needed)
        """
        current_time = time.time()
        sleep_duration = 0.0
        
        try:
            # Check minimum delay between calls
            last_call_str = self.redis_client.get(REDIS_KEY_LAST_API_CALL)
            if last_call_str:
                last_call_time = float(last_call_str)
                elapsed = current_time - last_call_time
                
                if elapsed < self.min_delay_seconds:
                    sleep_duration = self.min_delay_seconds - elapsed
                        with temp_phase('Rate Limiting + Retries'):
                            logger.info(f"Rate limit: sleeping {sleep_duration:.2f}s (min delay)")
                            time.sleep(sleep_duration)
                            current_time = time.time()
            
            # Check calls per minute limit
            call_count_str = self.redis_client.get(REDIS_KEY_API_CALL_COUNT)
            call_count = int(call_count_str) if call_count_str else 0
            
            if call_count >= self.max_calls_per_minute:
                with temp_phase('Rate Limiting + Retries'):
                    logger.warning(f"Reached {self.max_calls_per_minute} calls/minute limit")
                    # Sleep and reset counter (conservative approach: wait the full min delay)
                    sleep_duration_extra = self.min_delay_seconds
                    logger.info(f"Sleeping {sleep_duration_extra}s due to per-minute limit")
                    time.sleep(sleep_duration_extra)
                    sleep_duration += sleep_duration_extra
                    current_time = time.time()
                    # Reset counter (will be incremented after call succeeds)
                    self.redis_client.delete(REDIS_KEY_API_CALL_COUNT)
            
        except Exception as e:
            logger.error(f"Distributed rate limiter error: {e}")
            # Fall back to local delay if Redis fails
            with temp_phase('Rate Limiting + Retries'):
                time.sleep(self.min_delay_seconds)
                sleep_duration += self.min_delay_seconds
        
        return sleep_duration
    
    def record_api_call(self):
        """
        Record that an API call was just made.
        Must be called AFTER a successful API call to the Reddit API.
        """
        current_time = time.time()
        
        try:
            # Update last call timestamp
            self.redis_client.set(REDIS_KEY_LAST_API_CALL, str(current_time))
            
            # Increment per-minute counter with 60-second expiry
            self.redis_client.incr(REDIS_KEY_API_CALL_COUNT)
            self.redis_client.expire(REDIS_KEY_API_CALL_COUNT, 60)
            
            logger.debug(f"Recorded API call to Redis")
        except Exception as e:
            logger.error(f"Failed to record API call: {e}")
    
    def get_stats(self) -> dict:
        """Get current rate limit statistics."""
        try:
            last_call_str = self.redis_client.get(REDIS_KEY_LAST_API_CALL)
            call_count_str = self.redis_client.get(REDIS_KEY_API_CALL_COUNT)
            
            return {
                "last_api_call": float(last_call_str) if last_call_str else None,
                "calls_this_minute": int(call_count_str) if call_count_str else 0,
                "max_calls_per_minute": self.max_calls_per_minute,
                "min_delay_seconds": self.min_delay_seconds,
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {}
