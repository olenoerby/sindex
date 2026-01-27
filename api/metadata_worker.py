#!/usr/bin/env python3
"""
Simple Redis queue consumer for metadata refresh tasks.
Processes subreddit names from metadata_refresh_queue and fetches their metadata.
"""
import os
import sys
import time
import logging
from redis import Redis
from api.tasks import refresh_subreddit_job

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logger = logging.getLogger('metadata_worker')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ'))
logger.addHandler(handler)

REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
QUEUE_NAME = 'metadata_refresh_queue'
POLL_INTERVAL = 1.0  # seconds

def main():
    """Consume from metadata_refresh_queue and process tasks."""
    logger.info(f"Starting metadata worker on {REDIS_URL}, queue={QUEUE_NAME}")
    
    try:
        redis_client = Redis.from_url(REDIS_URL)
        redis_client.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)
    
    while True:
        try:
            # Block and pop from queue (BLPOP with timeout)
            result = redis_client.blpop(QUEUE_NAME, timeout=10)
            
            if result:
                queue_name, subreddit_name = result
                subreddit_name = subreddit_name.decode('utf-8') if isinstance(subreddit_name, bytes) else subreddit_name
                
                logger.info(f"Processing metadata refresh for /r/{subreddit_name}")
                try:
                    refresh_subreddit_job(subreddit_name)
                    logger.info(f"Completed metadata refresh for /r/{subreddit_name}")
                except Exception as e:
                    logger.exception(f"Error refreshing metadata for /r/{subreddit_name}: {e}")
                    # Re-queue on error (exponential backoff could be added)
                    try:
                        redis_client.rpush(QUEUE_NAME, subreddit_name)
                        logger.debug(f"Re-queued /r/{subreddit_name} for retry")
                    except Exception as e2:
                        logger.error(f"Failed to re-queue /r/{subreddit_name}: {e2}")
        except Exception as e:
            logger.error(f"Worker error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
