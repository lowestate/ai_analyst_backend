import logging
import sys
import os

USE_REAL_REDIS = os.getenv("USE_REAL_REDIS", "0").lower() in ("1", "true")

class DummyRedis:
    """Простая in-memory заглушка для локальной разработки без Docker/Redis"""
    def __init__(self):
        self._store = {}
        
    def set(self, key: str, value: str):
        self._store[key] = value
        
    def get(self, key: str) -> str | None:
        return self._store.get(key)


if USE_REAL_REDIS:
    import redis
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
else:
    redis_client = DummyRedis()

assert redis_client is not None, "Redis client must be initialized"

def setup_logger():
    logger = logging.getLogger("agent_backend")
    logger.setLevel(logging.INFO)
    
    # Чтобы логи не дублировались, если логгер уже инициализирован
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(module)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

logger = setup_logger()