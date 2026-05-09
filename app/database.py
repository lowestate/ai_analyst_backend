import os
import logging
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

DB_URI = os.getenv("APP_DB_URI")

# Пул соединений для LangGraph Checkpointer
pool = AsyncConnectionPool(
    conninfo=DB_URI,
    max_size=20,
    kwargs={"autocommit": True, "prepare_threshold": 0},
    open=False
)

async def init_custom_tables():
    """Создает таблицу для быстрого рендера сайдбара без необходимости парсить JSON стейты LangGraph"""
    async with pool.connection() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_sessions (
                chat_id TEXT PRIMARY KEY,
                dataset_name TEXT,
                filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Таблица chat_sessions инициализирована.")