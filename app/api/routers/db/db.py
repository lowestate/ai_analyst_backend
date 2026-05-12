# app/api/routers/db.py
from fastapi import APIRouter, HTTPException, Depends
from typing import Any
import asyncpg

from app.api.routers.db.models import RefreshSchemaRequest
from app.api.dependencies import get_app_graph
from app.users_db_interaction import DBCredentials, extract_schema_from_db
from app.config import logger

router = APIRouter(tags=["Database"])


@router.post("/test_connection")
async def test_db_connection(creds: DBCredentials):
    logger.info(f"Проверка подключения к БД database={creds.database} host={creds.host}")

    try:
        conn = await asyncpg.connect(
            user=creds.user,
            password=creds.password,
            database=creds.database,
            host=creds.host,
            port=creds.port,
            timeout=5.0
        )

        logger.info(f"Подключение к БД успешно database={creds.database}")

        await conn.close()

        logger.info(f"Подключение к БД закрыто database={creds.database}")

        return {"status": "success"}

    except Exception as e:
        logger.error(f"Ошибка подключения к БД database={creds.database}: {str(e)}", exc_info=True)

        # Возвращаем текст ошибки на фронтенд
        return {"status": "error", "message": str(e)}

@router.post("/refresh_schema")
async def refresh_schema_endpoint(
    req: RefreshSchemaRequest,
    graph: Any = Depends(get_app_graph)
):
    logger.info(f"Обновление схемы БД chat_id={req.chat_id}")

    # Достаем креды из памяти графа (мы их туда клали при изначальном upload)
    config = {"configurable": {"thread_id": req.chat_id, "chat_id": req.chat_id}}

    # ИСПРАВЛЕНИЕ: Сначала дожидаемся (await) результата, потом берем values
    state_snap = await graph.aget_state(config)
    
    if not state_snap or not state_snap.values:
        logger.warning(f"Стейт графа пуст chat_id={req.chat_id}")
        raise HTTPException(status_code=404, detail="История чата или стейт не найдены")
        
    state = state_snap.values

    logger.info(f"State graph загружен chat_id={req.chat_id}")

    creds_dict = state.get("db_credentials")

    if not creds_dict:
        logger.warning(f"db_credentials отсутствуют chat_id={req.chat_id}")
        raise HTTPException(status_code=400, detail="Креды для БД не найдены в текущей сессии")

    creds = DBCredentials(**creds_dict)

    logger.info(f"Извлечение новой схемы database={creds.database}")

    # Заново извлекаем схему без дублирования SQL-кода
    new_db_schema = await extract_schema_from_db(creds)

    logger.info(f"Новая схема получена tables={len(new_db_schema['tables'])}")

    # Обновляем схему в стейте, чтобы сабагенты тоже видели новые колонки
    await graph.aupdate_state(
        config,
        {"db_schema": new_db_schema},
        as_node="__start__"
    )

    logger.info(f"Схема обновлена в graph chat_id={req.chat_id}")

    return new_db_schema