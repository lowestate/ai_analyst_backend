import re
import pandas as pd
from enum import Enum

from app.config import logger
from app.agents.core.utils import filter_dataframe 
from app.agents.finance_agent.base_analysis import (
    calc_cash_flow,
    calc_pnl,
    expense_structure,
    calc_unit_economics,
    calc_revenue_forecast,
    calc_abc_analysis,
    calc_cohort_analysis
)
from app.agents.finance_agent.mock.mock_reports import (
    mock_cash_flow_report,
    mock_pnl_report,
    mock_expense_report,
    mock_abc_report,
    mock_forecast_report,
    mock_unit_report
)

class FinMockCommands(str, Enum):
    CASH_FLOW = r"^денежный поток\s+(.+?)\s+(.+?)$"
    
    # Группа 1: колонка сумм
    PNL = r"^pnl\s+(.+?)$"
    
    # Группа 1: колонка категорий, Группа 2: колонка сумм
    EXPENSES = r"^структура расходов\s+(.+?)\s+(.+?)$"

    ABC = r"^abc-анализ\s+(.+?)\s+(.+?)$"
    UNIT = r"^юнит-экономика\s+(.+?)\s+(.+?)\s+(.+?)\s+(.+?)$"
    FORECAST = r"^прогноз выручки\s+(.+?)\s+(.+?)$"
    COHORT = r"^когортный анализ\s+(.+?)\s+(.+?)$"

FIN_MOCK_REGISTRY = {}

def register_mock(command: str):
    def wrapper(func):
        FIN_MOCK_REGISTRY[re.compile(command, re.IGNORECASE)] = func
        return func
    return wrapper

def _get_clean_df(df: pd.DataFrame, cols_to_remove: list[str]) -> pd.DataFrame:
    """Вспомогательная функция: фильтрация датафрейма"""
    df_clean = filter_dataframe(df, cols_to_remove)
    logger.info(f"_get_clean_df выполнен cols_to_remove={len(cols_to_remove)}")
    return df_clean


@register_mock(FinMockCommands.CASH_FLOW.value)
def handle_cash_flow(df: pd.DataFrame, cols_to_remove: list[str], date_col: str, amount_col: str):
    df_clean = _get_clean_df(df, cols_to_remove)
    logger.info(f"handle_cash_flow start date_col={date_col} amount_col={amount_col}")

    try:
        data = calc_cash_flow(df_clean, date_col, amount_col)
        logger.info(f"handle_cash_flow calc_cash_flow DONE")
    except Exception as e:
        logger.error(f"handle_cash_flow error={str(e)}")
        raise

    msg = mock_cash_flow_report(data["data"])
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    logger.info(f"handle_cash_flow DONE")
    return msg, charts


@register_mock(FinMockCommands.PNL.value)
def handle_pnl(df: pd.DataFrame, cols_to_remove: list[str], amount_col: str):
    df_clean = _get_clean_df(df, cols_to_remove)
    logger.info(f"handle_pnl start amount_col={amount_col}")

    try:
        data = calc_pnl(df_clean, amount_col)
        logger.info(f"handle_pnl calc_pnl DONE")
    except Exception as e:
        logger.error(f"handle_pnl error={str(e)}")
        raise

    msg = mock_pnl_report(data["data"])
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    logger.info(f"handle_pnl DONE")
    return msg, charts


@register_mock(FinMockCommands.EXPENSES.value)
def handle_expense_structure(df: pd.DataFrame, cols_to_remove: list[str], category_col: str, amount_col: str):
    df_clean = _get_clean_df(df, cols_to_remove)
    logger.info(f"handle_expense_structure start category_col={category_col} amount_col={amount_col}")

    try:
        data = expense_structure(df_clean, category_col, amount_col)
        logger.info(f"handle_expense_structure calc DONE")
    except Exception as e:
        logger.error(f"handle_expense_structure error={str(e)}")
        raise

    msg = mock_expense_report(data["data"])
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    logger.info(f"handle_expense_structure DONE")
    return msg, charts


@register_mock(FinMockCommands.ABC.value)
def handle_abc(df: pd.DataFrame, cols_to_remove: list[str], category_col: str, amount_col: str):
    df_clean = _get_clean_df(df, cols_to_remove)
    logger.info(f"handle_abc start category_col={category_col} amount_col={amount_col}")

    try:
        data = calc_abc_analysis(df_clean, category_col, amount_col)
        logger.info(f"handle_abc calc DONE")
    except Exception as e:
        logger.error(f"handle_abc error={str(e)}")
        raise

    msg = mock_abc_report(data["data"])
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    logger.info(f"handle_abc DONE")
    return msg, charts


@register_mock(FinMockCommands.UNIT.value)
def handle_unit_econ(df: pd.DataFrame, cols_to_remove: list[str], source_col: str, amount_col: str, cac_col: str, user_col: str):
    df_clean = _get_clean_df(df, cols_to_remove)
    logger.info(f"handle_unit_econ start source_col={source_col} amount_col={amount_col}")

    try:
        data = calc_unit_economics(df_clean, source_col, amount_col, cac_col, user_col)
        logger.info(f"handle_unit_econ calc DONE")
    except Exception as e:
        logger.error(f"handle_unit_econ error={str(e)}")
        raise

    msg = mock_unit_report(data["data"])
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    logger.info(f"handle_unit_econ DONE")
    return msg, charts


@register_mock(FinMockCommands.FORECAST.value)
def handle_forecast(df: pd.DataFrame, cols_to_remove: list[str], date_col: str, amount_col: str):
    df_clean = _get_clean_df(df, cols_to_remove)
    logger.info(f"handle_forecast start date_col={date_col} amount_col={amount_col}")

    try:
        data = calc_revenue_forecast(df_clean, date_col, amount_col)
        logger.info(f"handle_forecast calc DONE")
    except Exception as e:
        logger.error(f"handle_forecast error={str(e)}")
        raise

    msg = mock_forecast_report(data["data"])
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    logger.info(f"handle_forecast DONE")
    return msg, charts


@register_mock(FinMockCommands.COHORT.value)
def handle_cohort(df: pd.DataFrame, cols_to_remove: list[str], date_col: str, user_col: str):
    df_clean = _get_clean_df(df, cols_to_remove)
    logger.info(f"handle_cohort start date_col={date_col} user_col={user_col}")

    try:
        data = calc_cohort_analysis(df_clean, date_col, user_col)
        logger.info(f"handle_cohort calc DONE")
    except Exception as e:
        logger.error(f"handle_cohort error={str(e)}")
        raise

    msg = "**Когортный анализ (Retention Rate):** Тепловая карта удержания клиентов. Строки — месяц первой покупки. Столбцы — периоды жизни когорты."
    logger.info(f"handle_cohort DONE")
    return msg, [{"type": data["tool_type"], "data": data["data"]}]