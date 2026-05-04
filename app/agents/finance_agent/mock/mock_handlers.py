import re
from enum import Enum

from app.agents.core.utils import get_df_from_redis
from app.agents.finance_agent.base_analysis import (
    calc_cash_flow,
    calc_pnl,
    expense_structure,
    calc_unit_economics,
    calc_revenue_forecast,
    calc_abc_analysis
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
    # Группа 1: колонка дат, Группа 2: колонка сумм
    CASH_FLOW = r"^денежный поток\s+(.+?)\s+(.+?)$"
    
    # Группа 1: колонка сумм
    PNL = r"^pnl\s+(.+?)$"
    
    # Группа 1: колонка категорий, Группа 2: колонка сумм
    EXPENSES = r"^структура расходов\s+(.+?)\s+(.+?)$"

    ABC = r"^abc-анализ\s+(.+?)\s+(.+?)$"
    UNIT = r"^юнит-экономика\s+(.+?)\s+(.+?)\s+(.+?)$"
    FORECAST = r"^прогноз выручки\s+(.+?)\s+(.+?)$"

FIN_MOCK_REGISTRY = {}

def register_mock(command: str):
    def wrapper(func):
        FIN_MOCK_REGISTRY[re.compile(command, re.IGNORECASE)] = func
        return func
    return wrapper

def _get_clean_df(chat_id: str, cols_to_remove: list[str]):
    """Вспомогательная функция для загрузки и очистки DF"""
    df = get_df_from_redis(chat_id)
    if cols_to_remove:
        # Убираем только те колонки, которые реально есть в датафрейме
        df = df.drop(columns=[c for c in cols_to_remove if c in df.columns])
    return df

@register_mock(FinMockCommands.CASH_FLOW.value)
def handle_cash_flow(chat_id: str, cols_to_remove: list[str], date_col: str, amount_col: str):
    df = _get_clean_df(chat_id, cols_to_remove)
    data = calc_cash_flow(df, date_col, amount_col)
    
    msg = mock_cash_flow_report(data["data"])
    # ИСПРАВЛЕНИЕ: переименовываем tool_type в type для Pydantic
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    return msg, charts

@register_mock(FinMockCommands.PNL.value)
def handle_pnl(chat_id: str, cols_to_remove: list[str], amount_col: str):
    df = _get_clean_df(chat_id, cols_to_remove)
    data = calc_pnl(df, amount_col)
    
    msg = mock_pnl_report(data["data"])
    # ИСПРАВЛЕНИЕ: переименовываем tool_type в type для Pydantic
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    return msg, charts

@register_mock(FinMockCommands.EXPENSES.value)
def handle_expense_structure(chat_id: str, cols_to_remove: list[str], category_col: str, amount_col: str):
    df = _get_clean_df(chat_id, cols_to_remove)
    data = expense_structure(df, category_col, amount_col)
    
    msg = mock_expense_report(data["data"])
    # ИСПРАВЛЕНИЕ: переименовываем tool_type в type для Pydantic
    charts = [{"type": data["tool_type"], "data": data["data"]}]
    return msg, charts

@register_mock(FinMockCommands.ABC.value)
def handle_abc(chat_id: str, cols_to_remove: list[str], category_col: str, amount_col: str):
    df = _get_clean_df(chat_id, cols_to_remove)
    data = calc_abc_analysis(df, category_col, amount_col)
    return mock_abc_report(data["data"]), [{"type": data["tool_type"], "data": data["data"]}]

@register_mock(FinMockCommands.UNIT.value)
def handle_unit_econ(chat_id: str, cols_to_remove: list[str], source_col: str, amount_col: str, cac_col: str):
    df = _get_clean_df(chat_id, cols_to_remove)
    data = calc_unit_economics(df, source_col, amount_col, cac_col)
    return mock_unit_report(data["data"]), [{"type": data["tool_type"], "data": data["data"]}]

@register_mock(FinMockCommands.FORECAST.value)
def handle_forecast(chat_id: str, cols_to_remove: list[str], date_col: str, amount_col: str):
    df = _get_clean_df(chat_id, cols_to_remove)
    data = calc_revenue_forecast(df, date_col, amount_col)
    return mock_forecast_report(data["data"]), [{"type": data["tool_type"], "data": data["data"]}]