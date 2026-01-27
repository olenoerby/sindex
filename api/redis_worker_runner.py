#!/usr/bin/env python3
"""
Wrapper for RQ worker with enhanced logging support.
Respects LOG_LEVEL environment variable for DEBUG, INFO, WARNING, ERROR output.
"""
import os
import sys
import logging
from rq import Worker
from redis import Redis

# Setup logging based on LOG_LEVEL environment variable
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logger = logging.getLogger('redis_worker')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Configure handler with ISO 8601 UTC timestamps
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%SZ'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

# Also configure RQ's logger
rq_logger = logging.getLogger('rq')
rq_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
rq_logger.addHandler(handler)

# Configure root logger to catch all output
root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
root_logger.addHandler(handler)

def main():
    """Start RQ worker with proper logging."""
    logger.info("=== RQ Worker Starting ===")
    logger.info(f"Log level: {LOG_LEVEL}")
    
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    logger.info(f"Redis URL: {redis_url}")
    
    try:
        redis_conn = Redis.from_url(redis_url)
        redis_conn.ping()
        logger.info("Connected to Redis successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)
    
    # Start the worker
    logger.info("Starting worker on 'default' queue...")
    logger.debug("Worker will process jobs from Redis queue")
    
    try:
        worker = Worker(['default'], connection=redis_conn)
        logger.info("Worker initialized successfully")
        logger.info("Listening for jobs on 'default' queue...")
        
        # Run the worker
        worker.work(logging_level=LOG_LEVEL.lower())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Worker error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

if __name__ == '__main__':
    main()
