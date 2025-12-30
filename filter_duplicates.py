from datetime import datetime
import json
import redis
from redis_client import get_redis

r = get_redis()

def save_seen_order_ids(order_ids: set):
    key = f"psp_state:{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    r.sadd(key, *order_ids)
    r.expire(key, 7*86400) #86400 seconds per day => Expire after 1 week

def load_seen_order_ids() -> set:
    all_ids = set()
    for key in r.scan_iter("psp_state:*"):
        all_ids.update(r.smembers(key))
    return all_ids
