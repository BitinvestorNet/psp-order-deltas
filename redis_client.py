import redis
from redis.connection import ConnectionPool

_pool = None

def get_redis():
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            host='localhost', 
            port=6379, 
            db=0, 
            max_connections=10,
            decode_responses=True 
        )
    return redis.Redis(connection_pool=_pool)