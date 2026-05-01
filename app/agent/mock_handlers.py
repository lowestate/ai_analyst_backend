import re
from enum import Enum

from app.agent.base_analysis import (
    get_correlation_data,
    get_column_stats_data,
    get_cross_dependencies_data,
    get_outliers_data,
    get_trend_data,
    get_dependency_data,
    get_pairplot_data,
    get_feature_importances
)
from app.agent.mock import (
    mock_correlation_report,
    mock_column_report,
    mock_cross_dependencies_report,
    mock_outliers_report,
    mock_trend_report,
    mock_dependency_report,
    mock_feature_importances_report
)


class MockCommands(str, Enum):
    # ^ означает начало строки, $ - конец строки (чтобы избежать ложных срабатываний)
    CORR_MATRIX = r"^корреляционная матрица$"
    COLUMN_ANALYSIS = r"^анализ столбцов$"
    ANOMALIES = r"^аномалии$"
    CROSS_DEP = r"^кросс-зависимости$"
    TREND = r"^тренд$"
    PAIRPLOT = r"^связи признаков$"
    ALL_RELATIONS = r"^корреляционный анализ$"

    # ПАТТЕРН ДЛЯ ПЕРЕМЕННЫХ: 
    # Группа 1 ловит первый столбец, Группа 2 ловит второй столбец
    DEPENDANCY = r"^зависимость\s+(.+?)\s+от\s+(.+?)$"
    FEATURE_IMPORTACES = r"^важность признаков для\s+(.+?)$"


MOCK_REGISTRY = {}

def register_mock(command: str):
    def wrapper(func):
        # Компилируем регулярное выражение сразу при регистрации (re.IGNORECASE сделает его нечувствительным к регистру)
        MOCK_REGISTRY[re.compile(command, re.IGNORECASE)] = func
        return func
    return wrapper

@register_mock(MockCommands.CORR_MATRIX.value)
def handle_correlation(chat_id: str, cols_to_remove: list[str]):
    corr_data = get_correlation_data(chat_id, cols_to_remove)
    msg = mock_correlation_report(corr_data)
    charts = [{"type": "correlation", "data": corr_data}]
    return msg, charts

@register_mock(MockCommands.COLUMN_ANALYSIS.value)
def handle_columns(chat_id: str, cols_to_remove: list[str]):
    stats_data = get_column_stats_data(chat_id, cols_to_remove)
    msg = mock_column_report(stats_data)
    charts = []
    
    for col, counts in stats_data.get("categorical_charts", {}).items():
        charts.append({
            "type": "category_count",
            "data": {"column_name": col, "counts": counts}
        })
        
    for col, hist_data in stats_data.get("numeric_charts", {}).items():
        charts.append({
            "type": "numeric_hist",
            "data": {"column_name": col, "x": hist_data["x"], "y": hist_data["y"]}
        })
        
    return msg, charts

@register_mock(MockCommands.ANOMALIES.value)
def handle_outliers(chat_id: str, cols_to_remove: list[str]):
    outliers_data = get_outliers_data(chat_id, cols_to_remove)
    msg = mock_outliers_report(outliers_data)
    charts = outliers_data["charts"]
    return msg, charts

@register_mock(MockCommands.CROSS_DEP.value)
def handle_cross_deps(chat_id: str, cols_to_remove: list[str]):
    cross_data = get_cross_dependencies_data(chat_id, cols_to_remove)
    msg = mock_cross_dependencies_report(cross_data)
    charts = [{"type": "cross_deps", "data": cross_data}]
    return msg, charts

@register_mock(MockCommands.TREND.value)
def handle_trends(chat_id: str, cols_to_remove: list[str]):
    trend_data = get_trend_data(chat_id, cols_to_remove)
    msg = mock_trend_report(trend_data)
    # Оборачиваем данные в правильный формат для фронтенда
    charts = [{"type": "trend_line", "data": trend_data}] 
    return msg, charts

@register_mock(MockCommands.DEPENDANCY.value)
def handle_dependency_mock(chat_id: str, col1: str, col2: str, cols_to_remove: list[str]):
    # col1 и col2 прилетят прямо из регулярного выражения!
    data = get_dependency_data(chat_id, col1, col2, cols_to_remove)
    msg = mock_dependency_report(data)
    charts = [{"type": "dependency", "data": data}]
    return msg, charts

@register_mock(MockCommands.PAIRPLOT.value)
def handle_pairplot_mock(chat_id: str, cols_to_remove: list[str]):
    data = get_pairplot_data(chat_id, cols_to_remove)
    msg = (
        "**Матрица рассеяния (Pairplot)**\n\n"
        # ТУТ ИСПРАВЛЕНО: используем default_columns вместо columns
        f"Построена попарная зависимость для {len(data['default_columns'])} наиболее вариативных числовых признаков.\n"
        "Вы можете добавить или убрать признаки с помощью панели управления над графиком, чтобы найти скрытые нелинейные зависимости, кластеры и выбросы."
    )
    charts = [{"type": "pairplot", "data": data}]
    return msg, charts

@register_mock(MockCommands.ALL_RELATIONS.value)
def handle_all_relationships(chat_id: str, cols_to_remove: list[str]):
    combined_msg_parts = []
    combined_charts = []

    # 1. Запрашиваем корреляционную матрицу
    try:
        msg, charts = handle_correlation(chat_id, cols_to_remove)
        combined_msg_parts.append(msg)
        combined_charts.extend(charts)
    except Exception as e:
        combined_msg_parts.append(f"⚠️ **Корреляционная матрица:** Ошибка при построении ({e})")

    # 2. Запрашиваем граф кросс-зависимостей
    try:
        msg, charts = handle_cross_deps(chat_id, cols_to_remove)
        combined_msg_parts.append(msg)
        combined_charts.extend(charts)
    except Exception as e:
        combined_msg_parts.append(f"⚠️ **Кросс-зависимости:** Ошибка при построении ({e})")

    # 3. Запрашиваем матрицу рассеяния
    try:
        msg, charts = handle_pairplot_mock(chat_id, cols_to_remove)
        combined_msg_parts.append(msg)
        combined_charts.extend(charts)
    except Exception as e:
        combined_msg_parts.append(f"⚠️ **Матрица рассеяния:** Ошибка при построении ({e})")

    # Склеиваем все текстовые сообщения через горизонтальную линию Markdown
    final_msg = "\n\n---\n\n".join(combined_msg_parts)
    
    return final_msg, combined_charts

@register_mock(MockCommands.FEATURE_IMPORTACES.value)
def handle_feature_importances(chat_id: str, col: str, cols_to_remove: list[str]):
    # col1 и col2 прилетят прямо из регулярного выражения!
    data = get_feature_importances(chat_id, col, cols_to_remove)
    msg = mock_feature_importances_report(data)
    charts = [{"type": "feature_importances", "data": data}]
    return msg, charts