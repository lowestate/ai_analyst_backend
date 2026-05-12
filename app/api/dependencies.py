from typing import Any
from fastapi import HTTPException
from app.config import logger
from app.agents.graph import app_graph

def get_app_graph() -> Any:
    """Возвращает глобальный скомпилированный граф LangGraph"""
    logger.info("Запрос app_graph")
    if app_graph is None:
        logger.error("app_graph не инициализирован")
        raise HTTPException(status_code=503, detail="Ошибка инициализации графа на сервере")
    return app_graph