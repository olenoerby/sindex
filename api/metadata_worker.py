#!/usr/bin/env python3
"""
Simple Redis queue consumer for metadata refresh tasks.
Processes subreddit names from metadata_refresh_queue and fetches their metadata.
Uses distributed rate limiter to coordinate with scanner and respect global API limits.
"""
import os
import sys
import time
import logging
from redis import Redis
from api.tasks import refresh_subreddit_job
from api.distributed_rate_limiter import DistributedRateLimiter

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logger = logging.getLogger('metadata_worker')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ'))
logger.addHandler(handler)
logger.propagate = False

REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
QUEUE_NAME = 'metadata_refresh_queue'
POLL_INTERVAL = 1.0  # seconds

# Load API rate limit settings (must match scanner settings)
API_RATE_DELAY_SECONDS = float(os.getenv('API_RATE_DELAY_SECONDS', '7'))
API_MAX_CALLS_MINUTE = int(os.getenv('API_MAX_CALLS_MINUTE', '8'))

# Initialize distributed rate limiter for coordination with scanner
try:
    global_rate_limiter = DistributedRateLimiter(
        redis_url=REDIS_URL,
        min_delay_seconds=API_RATE_DELAY_SECONDS,
        max_calls_per_minute=API_MAX_CALLS_MINUTE
    )
    global_rate_limiter.set_container_name("metadata_worker")
except Exception as e:
    logger.error(f"Failed to initialize distributed rate limiter: {e}")
    sys.exit(1)

def main():
    """Consume from metadata_refresh_queue and process tasks."""
    logger.info(f"=== Metadata Worker Starting ===")
    logger.info(f"Redis URL: {REDIS_URL}")
    logger.info(f"Queue: {QUEUE_NAME}")
    logger.info(f"Log level: {LOG_LEVEL}")
    logger.info(f"Rate limiting: {API_RATE_DELAY_SECONDS}s min delay, {API_MAX_CALLS_MINUTE} calls/min (SHARED with scanner)")
    logger.debug(f"Poll interval: {POLL_INTERVAL}s")
    
    try:
        redis_client = Redis.from_url(REDIS_URL)
        redis_client.ping()
        logger.info("Connected to Redis successfully")
        logger.debug(f"Redis connection: {redis_client}")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)
    
    processed_count = 0
    error_count = 0
    
    logger.info("Starting queue processing loop...")
    logger.info("Waiting for metadata refresh tasks from scanner...")
    
    while True:
        try:
            # Block and pop from queue (BLPOP with timeout)
            logger.debug(f"Waiting for tasks from queue '{QUEUE_NAME}' (timeout=10s)...")
            result = redis_client.blpop(QUEUE_NAME, timeout=10)
            
            if result:
                queue_name, subreddit_name = result
                subreddit_name = subreddit_name.decode('utf-8') if isinstance(subreddit_name, bytes) else subreddit_name
                
                logger.debug(f"Received task from queue: /r/{subreddit_name}")
                
                # CRITICAL: Use distributed rate limiter to coordinate with scanner
                # This blocks if needed to maintain the global API rate limit
                logger.debug("Checking distributed rate limit...")
                sleep_duration = global_rate_limiter.wait_if_needed()
                if sleep_duration > 0:
                    logger.info(f"Rate limit enforcement: slept {sleep_duration:.2f}s")
                
                logger.info(f"Processing metadata refresh for /r/{subreddit_name}")
                try:
                    refresh_subreddit_job(subreddit_name)
                    # Record this API call to the global rate limiter
                    global_rate_limiter.record_api_call()
                    processed_count += 1
                    logger.info(f"✓ Completed metadata refresh for /r/{subreddit_name} (total processed: {processed_count})")
                except Exception as e:
                    # Record the call even on error (API call was made)
                    global_rate_limiter.record_api_call()
                    error_count += 1
                    logger.error(f"✗ Error refreshing metadata for /r/{subreddit_name}: {e}")
                    logger.debug(f"Total errors: {error_count}")
                    # Re-queue on error (exponential backoff could be added)
                    try:
                        redis_client.rpush(QUEUE_NAME, subreddit_name)
                        logger.warning(f"Re-queued /r/{subreddit_name} for retry")
                    except Exception as e2:
                        logger.error(f"Failed to re-queue /r/{subreddit_name}: {e2}")
            else:
                logger.debug("No tasks available in queue (timeout reached)")
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
            break
        except Exception as e:
            logger.error(f"Worker error: {e}")
            logger.debug("Sleeping 5s before retry...")
            time.sleep(5)
    
    # Print final stats
    stats = global_rate_limiter.get_stats()
    logger.info(f"Metadata worker shutting down. Stats: processed={processed_count}, errors={error_count}")
    logger.info(f"Rate limit stats: {stats}")

if __name__ == '__main__':
    main()
