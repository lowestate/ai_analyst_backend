from pydantic_settings import BaseSettings
from typing import Optional
import logging
import sys

class Settings(BaseSettings):
    GOOGLE_API_KEY: Optional[str] = None
    REDIS_URL: str = "redis://redis:6379/0"
    DATABASE_URL: str = "postgresql://postgres:postgres@db:5432/ai_analyst"
    
    class Config:
        env_file = ".env"


settings = Settings()

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