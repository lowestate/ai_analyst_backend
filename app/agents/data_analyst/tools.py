import json
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig

from app.config import logger
from app.agents.core.utils import aget_df_from_db
from app.agents.data_analyst.base_analysis import (
    get_column_stats_data,
    get_correlation_data,
    get_outliers_data,
    get_trend_data,
    get_dependency_data,
    get_pairplot_data,
    get_all_relationships_data,
    get_feature_importances,
    get_feature_tree,
)

@tool
def request_db_query(query_description: str) -> str:
    """ОБЯЗАТЕЛЬНО используй этот инструмент, если источник данных - база данных (db).
    В 'query_description' опиши БИЗНЕС-СУТЬ того, что нужно найти (например: 'Посчитай LTV, CAC и выручку по каналам привлечения').
    ВНИМАНИЕ: НЕ проси агента узнать структуру БД или названия таблиц! У SQL-агента уже есть полная схема БД. Просто четко поставь ему аналитическую задачу."""
    logger.info(f"request_db_query вызван query_description={query_description}")
    return "Ожидание выполнения SQL-запроса в БД..."


@tool
async def analyze_columns(config: RunnableConfig) -> str:
    """Возвращает статистику по числовым и категориальным столбцам датасета."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"analyze_columns start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        stats = get_column_stats_data(df)
        logger.info(f"analyze_columns data loaded chat_id={chat_id}")
    except Exception as e:
        logger.error(f"analyze_columns error chat_id={chat_id} error={str(e)}")
        return str(e)

    logger.info(f"chat_id={chat_id} analyze_columns tool DONE")
    return json.dumps({"tool_type": "column_stats", "data": stats})


@tool
async def correlation_matrix(config: RunnableConfig) -> str:
    """Возвращает матрицу корреляции для ВСЕХ признаков датасета."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"correlation_matrix start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        corr = get_correlation_data(df)
        logger.info(f"correlation_matrix computed chat_id={chat_id}")
    except Exception as e:
        logger.error(f"correlation_matrix error chat_id={chat_id} error={str(e)}")
        return str(e)

    logger.info(f"chat_id={chat_id} correlation_matrix tool DONE")
    return json.dumps({"tool_type": "correlation", "data": corr})


@tool
async def detect_outliers(config: RunnableConfig) -> str:
    """Выявляет аномалии и выбросы в числовых столбцах датасета."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"detect_outliers start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        data = get_outliers_data(df)
        logger.info(f"detect_outliers computed chat_id={chat_id}")
        return json.dumps({"tool_type": "outliers", "stats": data["stats"]})
    except Exception as e:
        logger.error(f"detect_outliers error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def analyze_trends(config: RunnableConfig) -> str:
    """
    Строит графики трендов во времени для числовых признаков.
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"analyze_trends start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        data = get_trend_data(df)
        logger.info(f"analyze_trends computed chat_id={chat_id}")
        return json.dumps({"tool_type": "trend_line", "data": data})
    except Exception as e:
        logger.error(f"analyze_trends error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def analyze_dependency(col1: str, col2: str, config: RunnableConfig) -> str:
    """Анализирует зависимость между двумя столбцами."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"analyze_dependency start chat_id={chat_id} col1={col1} col2={col2}")

    try:
        df = await aget_df_from_db(chat_id)
        data = get_dependency_data(df, col1, col2)
        logger.info(f"analyze_dependency computed chat_id={chat_id}")
        return json.dumps({"tool_type": "dependency", "data": data})
    except Exception as e:
        logger.error(f"analyze_dependency error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def handle_all_relationships(config: RunnableConfig) -> str:
    """Генерирует набор графиков связей между признаками."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"handle_all_relationships start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        data = get_all_relationships_data(df)
        logger.info(f"handle_all_relationships computed chat_id={chat_id}")
        return json.dumps(data)
    except Exception as e:
        logger.error(f"handle_all_relationships error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def pairplot_tool(config: RunnableConfig, cols_to_remove: list[str] = []) -> dict:
    """Матрица рассеяния (Pairplot)."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"pairplot_tool start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        data = get_pairplot_data(df, cols_to_remove)
        logger.info(f"pairplot_tool computed chat_id={chat_id}")
        return {
            "message": "Матрица рассеяния построена",
            "charts": [{"type": "pairplot", "data": data}]
        }
    except Exception as e:
        logger.error(f"pairplot_tool error chat_id={chat_id} error={str(e)}")
        return {"message": str(e), "charts": []}


@tool
async def feature_importances_tool(target_col: str, config: RunnableConfig, cols_to_remove: list[str] = []) -> dict:
    """Feature importance через Random Forest."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"feature_importances_tool start chat_id={chat_id} target_col={target_col}")

    try:
        df = await aget_df_from_db(chat_id)
        data = get_feature_importances(df, target_col, cols_to_remove)
        logger.info(f"feature_importances_tool computed chat_id={chat_id}")
        return {
            "message": f"Feature importance рассчитан для {data['target']}",
            "charts": [{"type": "feature_importances", "data": data}]
        }
    except Exception as e:
        logger.error(f"feature_importances_tool error chat_id={chat_id} error={str(e)}")
        return {"message": str(e), "charts": []}


@tool
async def feature_tree_tool(config: RunnableConfig, cols_to_remove: list[str] = []) -> dict:
    """Дендрограмма признаков."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"feature_tree_tool start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        data = get_feature_tree(df, cols_to_remove)
        logger.info(f"feature_tree_tool computed chat_id={chat_id}")
        return {
            "message": "Дендрограмма построена",
            "charts": [{"type": "feature_tree", "data": data}]
        }
    except Exception as e:
        logger.error(f"feature_tree_tool error chat_id={chat_id} error={str(e)}")
        return {"message": str(e), "charts": []}