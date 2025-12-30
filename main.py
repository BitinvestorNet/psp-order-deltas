#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv()
import logging
import logger
import signal
import atexit
import redis
from config import HOURS_BACK_SEARCH
from post_to_slack import alert_slack
from filter_duplicates import load_seen_order_ids, save_seen_order_ids
from monitor import monitor_deltas
from redis_client import get_redis

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
for name in ["urllib3", "requests", "stripe", "httpx"]:
    logging.getLogger(name).setLevel(logging.WARNING)

LOCK_KEY = "psp-order-deltas:job-lock"
_redis_client = None
_lock_held = False

def acquire_lock():
    global _redis_client, _lock_held
    _redis_client = get_redis()
    if _redis_client.setnx(LOCK_KEY, "1"):
        _lock_held = True
        logger.info("Job lock acquired")
        return True
    else:
        logger.warning("Another job is already running, exiting")
        return False


def release_lock():
    global _redis_client, _lock_held
    if _lock_held and _redis_client:
        try:
            _redis_client.delete(LOCK_KEY)
            logger.info("Job lock released")
        except Exception as e:
            logger.error(f"Failed to release lock: {e}")
        _lock_held = False

def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}, cleaning up...")
    release_lock()
    exit(1)

def main():
    if not acquire_lock():
        return

    atexit.register(release_lock)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        logger.info(f"Fetching payments from last {HOURS_BACK_SEARCH} hours")
        mismatches = monitor_deltas()
        if len(mismatches) > 0:
            # Filter NEW mismatches only
            seen_order_ids = load_seen_order_ids()
            new_mismatches = mismatches[~mismatches['order_id'].astype(str).isin(seen_order_ids)]

            if len(new_mismatches) > 0:
                logger.info(f"Found {len(new_mismatches)} NEW mismatches (total {len(mismatches)})")
                alert_slack(new_mismatches)

                # Update state with ALL seen (new + old)
                all_seen = set(new_mismatches['order_id'].astype(str))
                save_seen_order_ids(all_seen)
            else:
                logger.info("No new mismatches (all previously seen)")
    finally:
        release_lock()


if __name__ == "__main__":
    main()
