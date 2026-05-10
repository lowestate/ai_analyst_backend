import os
import json
import pickle
from app.config import logger
from psycopg_pool import AsyncConnectionPool
from typing import Optional, AsyncIterator, Any
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple
)

DB_URI = os.getenv("APP_DB_URI")

# Пул соединений для LangGraph Checkpointer
pool = AsyncConnectionPool(
    conninfo=DB_URI,
    max_size=20,
    kwargs={"autocommit": True, "prepare_threshold": 0},
    open=False
)


class CustomAsyncPostgresSaver(BaseCheckpointSaver):
    def __init__(self, pool: Any):
        super().__init__()
        self.pool = pool
        logger.info("CustomAsyncPostgresSaver успешно инициализирован.")

    async def setup(self) -> None:
        pass

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: dict
    ) -> RunnableConfig:
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id")
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = checkpoint["id"]
        
        logger.info(f"[Saver] Сохранение стейта... chat_id: {thread_id}, checkpoint_id: {checkpoint_id}")
        
        # 1. Данные стейта в бинарник (для колонки bytea)
        data_bytes = pickle.dumps(checkpoint) 
        
        # 2. Метаданные в JSON строку (для колонки jsonb). 
        # default=str спасает, если внутри попадутся даты или UUID объекты
        meta_json = json.dumps(metadata, default=str)
        
        query = """
            INSERT INTO chat_checkpoints (chat_id, checkpoint_id, checkpoint_ns, data, metadata)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE SET 
                checkpoint_id = EXCLUDED.checkpoint_id,
                checkpoint_ns = EXCLUDED.checkpoint_ns,
                data = EXCLUDED.data,
                metadata = EXCLUDED.metadata,
                updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            async with self.pool.connection() as conn:
                await conn.execute(query, (thread_id, checkpoint_id, checkpoint_ns, data_bytes, meta_json))
            logger.info(f"[Saver] Стейт {checkpoint_id} успешно записан в БД.")
        except Exception as e:
            logger.error(f"[Saver] КРИТИЧЕСКАЯ ОШИБКА записи в БД: {e}")
            raise
        
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id")
        
        logger.info(f"[Saver] Запрос на чтение стейта... chat_id: {thread_id}")
        
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT checkpoint_id, checkpoint_ns, data, metadata FROM chat_checkpoints WHERE chat_id = %s", 
                        (thread_id,)
                    )
                    row = await cur.fetchone()
        except Exception as e:
            logger.error(f"[Saver] Ошибка чтения из БД: {e}")
            raise
            
        if not row:
            logger.info(f"[Saver] Стейт для chat_id={thread_id} не найден. Это нормально для нового чата.")
            return None
            
        checkpoint_id, checkpoint_ns, data, meta = row
        logger.info(f"[Saver] Стейт {checkpoint_id} успешно поднят из БД.")
        
        # psycopg драйвер очень умный: когда он достает jsonb из Postgres, 
        # он может автоматически конвертировать его в python-словарь.
        # Поэтому мы проверяем тип на всякий случай.
        if isinstance(meta, str):
            parsed_meta = json.loads(meta)
        elif isinstance(meta, dict):
            parsed_meta = meta
        else:
            parsed_meta = {}

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            },
            checkpoint=pickle.loads(data),
            metadata=parsed_meta,
            parent_config=None
        )

    # Обязательные заглушки для интерфейса базового класса
    def put(self, config, checkpoint, metadata, new_versions): pass
    def get_tuple(self, config): return None
    def list(self, config, filter, before, limit): yield from []
    async def aput_writes(self, config, writes, task_id): pass
    
    async def alist(self, config, filter, before, limit) -> AsyncIterator[CheckpointTuple]:
        checkpoint = await self.aget_tuple(config)
        if checkpoint:
            yield checkpoint