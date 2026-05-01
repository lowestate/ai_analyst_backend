import json
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig

from app.core.config import logger
from app.agent.base_analysis import (
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
    
@tool
def handle_all_relationships(chat_id: str):
    """
    Используй этот инструмент ВСЕГДА, когда пользователь просит показать "связи признаков", 
    "взаимосвязи", "корреляции" или зависимости между колонками.
    Этот инструмент автоматически генерирует сразу три графика: матрицу корреляций, 
    граф связей и матрицу рассеяния.
    Инструмент не требует уточнения колонок — он сам проанализирует весь датасет.
    """
    return get_all_relationships_data(chat_id)

@tool
def pairplot_tool(chat_id: str, cols_to_remove: list[str] = []) -> dict:
    """
    Построение матрицы рассеяния (Pairplot / Scatter matrix).
    Используется для визуального анализа попарных нелинейных зависимостей, распределений и выбросов между несколькими числовыми признаками одновременно.
    Инструмент относится к категории 'Связи в данных'.
    """
    data = get_pairplot_data(chat_id, cols_to_remove)
    return {
        "message": "Матрица рассеяния успешно построена.", 
        "charts": [{"type": "pairplot", "data": data}]
    }

@tool
def feature_importances_tool(chat_id: str, target_col: str, cols_to_remove: list[str] = []) -> dict:
    """
    Вычисляет важность признаков (Feature Importances) для заданной целевой переменной.
    Использует алгоритм машинного обучения Random Forest, чтобы определить, какие столбцы (факторы) 
    сильнее всего влияют на выбранную колонку `target_col`.
    
    Используйте этот инструмент, когда пользователь спрашивает: 
    - "От чего зависит X?"
    - "Какие факторы влияют на Y?"
    - "Важность признаков для Z".
    
    ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР: `target_col` (название столбца, для которого ищем зависимости).
    """
    try:
        data = get_feature_importances(chat_id, target_col, cols_to_remove)
        return {
            "message": f"Анализ важности признаков для колонки **{data['target']}** успешно завершен. Модель Random Forest выявила топ факторов, оказывающих наибольшее влияние на эту метрику.", 
            "charts": [{"type": "feature_importances", "data": data}]
        }
    except ValueError as e:
        # Полезно возвращать агенту понятную ошибку, если колонка не найдена или данных мало
        return {
            "message": f"Не удалось вычислить важность признаков: {str(e)}",
            "charts": []
        }

@tool
def feature_tree_tool(chat_id: str, cols_to_remove: list[str] = []) -> dict:
    """
    Построение дендрограммы признаков (Иерархическая кластеризация).
    Используется для поиска скрытых групп (кластеров) похожих столбцов и выявления дублирующейся информации (мультиколлинеарности).
    Инструмент относится к категории 'Связи в данных'.
    """
    data = get_feature_tree(chat_id, cols_to_remove)
    return {
        "message": "Дендрограмма признаков успешно построена. Теперь видны кластеры параметров.", 
        "charts": [{"type": "feature_tree", "data": data}]
    }