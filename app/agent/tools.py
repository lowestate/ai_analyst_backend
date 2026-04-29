import json
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig

from app.core.config import logger
from app.agent.utils import get_column_stats_data, get_correlation_data

@tool
def analyze_columns(config: RunnableConfig) -> str:
    """Возвращает статистику по числовым и категориальным столбцам датасета."""
    chat_id = config["configurable"]["chat_id"] # type: ignore
    try:
        # Используем нашу новую функцию, чтобы логика работала и для реальной LLM!
        stats = get_column_stats_data(chat_id) 
    except Exception as e:
        return str(e)
        
    logger.info(f"chat_id={chat_id} analyze_columns tool DONE")
    return json.dumps({"tool_type": "column_stats", "data": stats})

@tool
def correlation_matrix(config: RunnableConfig) -> str:
    """Возвращает матрицу корреляции для ВСЕХ признаков датасета."""
    chat_id = config["configurable"]["chat_id"] # type: ignore
    try:
        corr = get_correlation_data(chat_id)
    except Exception as e:
        return str(e)
        
    logger.info(f"chat_id={chat_id} correlation_matrix tool DONE")
    return json.dumps({"tool_type": "correlation", "data": corr})