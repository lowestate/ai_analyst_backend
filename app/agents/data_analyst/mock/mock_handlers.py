import re
import pandas as pd
from enum import Enum

from app.config import logger
from app.agents.data_analyst.base_analysis import (
    get_correlation_data,
    get_column_stats_data,
    get_outliers_data,
    get_trend_data,
    get_dependency_data,
    get_pairplot_data,
    get_feature_importances,
    get_feature_tree
)
from app.agents.data_analyst.mock.mock_reports import (
    mock_correlation_report,
    mock_column_report,
    mock_outliers_report,
    mock_trend_report,
    mock_dependency_report,
    mock_feature_importances_report,
    mock_feature_tree_report
)


class DAMockCommands(str, Enum):
    CORR_MATRIX = r"^корреляционная матрица$"
    COLUMN_ANALYSIS = r"^анализ столбцов$"
    ANOMALIES = r"^аномалии$"
    TREND = r"^тренд$"
    PAIRPLOT = r"^связи признаков$"
    ALL_RELATIONS = r"^корреляционный анализ$"
    FEATURE_TREE = r"^дерево признаков$"

    # Группа 1 ловит первый столбец, Группа 2 ловит второй столбец
    DEPENDANCY = r"^зависимость\s+(.+?)\s+от\s+(.+?)$"
    FEATURE_IMPORTACES = r"^важность признаков для\s+(.+?)$"

DA_MOCK_REGISTRY = {}

def register_mock(command: str):
    def wrapper(func):
        DA_MOCK_REGISTRY[re.compile(command, re.IGNORECASE)] = func
        return func
    return wrapper

@register_mock(DAMockCommands.CORR_MATRIX.value)
def handle_correlation(df: pd.DataFrame, cols_to_remove: list[str]):
    logger.info("Запуск handle_correlation")

    corr_data = get_correlation_data(df, cols_to_remove)
    logger.info("Correlation data получены")

    msg = mock_correlation_report(corr_data)
    logger.info("Correlation report сформирован")

    charts = [{"type": "correlation", "data": corr_data}]
    logger.info("Correlation charts сформированы")

    return msg, charts


@register_mock(DAMockCommands.COLUMN_ANALYSIS.value)
def handle_columns(df: pd.DataFrame, cols_to_remove: list[str]):
    logger.info("Запуск handle_columns")

    stats_data = get_column_stats_data(df, cols_to_remove)
    logger.info("Column stats получены")

    msg = mock_column_report(stats_data)
    logger.info("Column report сформирован")

    charts = []

    for col, counts in stats_data.get("categorical_charts", {}).items():
        charts.append({
            "type": "category_count",
            "data": {"column_name": col, "counts": counts}
        })
        logger.info(f"Category chart добавлен column={col}")

    for col, hist_data in stats_data.get("numeric_charts", {}).items():
        charts.append({
            "type": "numeric_hist",
            "data": {"column_name": col, "x": hist_data['x'], "y": hist_data['y']}
        })
        logger.info(f"Numeric histogram добавлен column={col}")

    logger.info(f"handle_columns завершен charts={len(charts)}")

    return msg, charts


@register_mock(DAMockCommands.ANOMALIES.value)
def handle_outliers(df: pd.DataFrame, cols_to_remove: list[str]):
    logger.info("Запуск handle_outliers")

    outliers_data = get_outliers_data(df, cols_to_remove)
    logger.info("Outliers data получены")

    msg = mock_outliers_report(outliers_data)
    logger.info("Outliers report сформирован")

    charts = outliers_data["charts"]
    logger.info(f"Outliers charts подготовлены count={len(charts)}")

    return msg, charts


@register_mock(DAMockCommands.TREND.value)
def handle_trends(df: pd.DataFrame, cols_to_remove: list[str]):
    logger.info("Запуск handle_trends")

    trend_data = get_trend_data(df, cols_to_remove)
    logger.info("Trend data получены")

    msg = mock_trend_report(trend_data)
    logger.info("Trend report сформирован")

    charts = [{"type": "trend_line", "data": trend_data}]
    logger.info("Trend chart сформирован")

    return msg, charts


@register_mock(DAMockCommands.DEPENDANCY.value)
def handle_dependency_mock(
    df: pd.DataFrame,
    cols_to_remove: list[str],
    col1: str,
    col2: str
):
    logger.info(f"Запуск handle_dependency_mock col1={col1} col2={col2}")

    data = get_dependency_data(df, col1, col2, cols_to_remove)
    logger.info("Dependency data получены")

    msg = mock_dependency_report(data)
    logger.info("Dependency report сформирован")

    charts = [{"type": "dependency", "data": data}]
    logger.info("Dependency chart сформирован")

    return msg, charts


@register_mock(DAMockCommands.PAIRPLOT.value)
def handle_pairplot_mock(df: pd.DataFrame, cols_to_remove: list[str]):
    logger.info("Запуск handle_pairplot_mock")

    data = get_pairplot_data(df, cols_to_remove)
    logger.info("Pairplot data получены")

    msg = (
        "**Матрица рассеяния (Pairplot)**\n\n"
        f"Построена попарная зависимость для {len(data['default_columns'])} наиболее вариативных числовых признаков.\n"
        "Вы можете добавить или убрать признаки с помощью панели управления над графиком, чтобы найти скрытые нелинейные зависимости, кластеры и выбросы."
    )

    logger.info("Pairplot report сформирован")

    charts = [{"type": "pairplot", "data": data}]
    logger.info("Pairplot chart сформирован")

    return msg, charts


@register_mock(DAMockCommands.ALL_RELATIONS.value)
def handle_all_relationships(df: pd.DataFrame, cols_to_remove: list[str]):
    logger.info("Запуск handle_all_relationships")

    combined_msg_parts = []
    combined_charts = []

    try:
        msg, charts = handle_correlation(df, cols_to_remove)
        logger.info("Correlation analysis успешно выполнен")

        combined_msg_parts.append(msg)
        combined_charts.extend(charts)

    except Exception as e:
        logger.error(f"Ошибка correlation analysis: {str(e)}", exc_info=True)

        combined_msg_parts.append(
            f"⚠️ **Корреляционная матрица:** Ошибка при построении ({e})"
        )

    try:
        msg, charts = handle_pairplot_mock(df, cols_to_remove)
        logger.info("Pairplot analysis успешно выполнен")

        combined_msg_parts.append(msg)
        combined_charts.extend(charts)

    except Exception as e:
        logger.error(f"Ошибка pairplot analysis: {str(e)}", exc_info=True)

        combined_msg_parts.append(
            f"⚠️ **Матрица рассеяния:** Ошибка при построении ({e})"
        )

    final_msg = "\n\n---\n\n".join(combined_msg_parts)
    logger.info(f"handle_all_relationships завершен charts={len(combined_charts)}")

    return final_msg, combined_charts


@register_mock(DAMockCommands.FEATURE_IMPORTACES.value)
def handle_feature_importances(
    df: pd.DataFrame,
    cols_to_remove: list[str],
    col: str
):
    logger.info(f"Запуск handle_feature_importances target={col}")

    data = get_feature_importances(df, col, cols_to_remove)
    logger.info("Feature importances data получены")

    msg = mock_feature_importances_report(data)
    logger.info("Feature importances report сформирован")

    charts = [{"type": "feature_importances", "data": data}]
    logger.info("Feature importances chart сформирован")

    return msg, charts


@register_mock(DAMockCommands.FEATURE_TREE.value)
def handle_feature_tree(df: pd.DataFrame, cols_to_remove: list[str]):
    logger.info("Запуск handle_feature_tree")

    data = get_feature_tree(df, cols_to_remove)
    logger.info("Feature tree data получены")

    msg = mock_feature_tree_report(data)
    logger.info("Feature tree report сформирован")

    charts = [{"type": "feature_tree", "data": data}]
    logger.info("Feature tree chart сформирован")

    return msg, charts