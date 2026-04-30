import json
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig

from app.core.config import logger
from app.agent.base_analysis import (
    get_column_stats_data,
    get_correlation_data,
    get_outliers_data,
    get_cross_dependencies_data,
    get_trend_data,
    get_dependency_data
)

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

@tool
def detect_outliers(config: RunnableConfig) -> str:
    """Выявляет аномалии и выбросы в числовых столбцах датасета."""
    chat_id = config["configurable"]["chat_id"] # type: ignore
    try:
        data = get_outliers_data(chat_id)
        return json.dumps({"tool_type": "outliers", "stats": data["stats"]})
    except Exception as e:
        return str(e)
    
@tool
def cross_dependencies(config: RunnableConfig) -> str:
    """Анализирует взаимосвязи между всеми признаками датасета (включая категориальные)."""
    chat_id = config["configurable"]["chat_id"] # type: ignore
    try:
        data = get_cross_dependencies_data(chat_id)
        return json.dumps({"tool_type": "cross_dependencies", "matrix": data["matrix"]})
    except Exception as e:
        return str(e)

@tool
def analyze_trends(config: RunnableConfig) -> str:
    """
    Строит графики трендов (линейные графики) во времени для всех числовых признаков.
    Автоматически находит столбец с датой/временем и использует его как ось X.
    """
    chat_id = config["configurable"]["chat_id"] # type: ignore
    try:
        data = get_trend_data(chat_id)
        return json.dumps({"tool_type": "trend_line", "data": data})
    except Exception as e:
        return str(e)
    
@tool
def analyze_dependency(col1: str, col2: str, config: RunnableConfig) -> str:
    """Анализирует зависимость между двумя столбцами."""
    chat_id = config["configurable"]["chat_id"] # type: ignore
    try:
        data = get_dependency_data(chat_id, col1, col2)
        return json.dumps({"tool_type": "dependency", "data": data})
    except Exception as e:
        return str(e)