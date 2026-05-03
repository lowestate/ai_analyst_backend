import json
from langchain_core.tools import tool
from langchain_core.runnables.config import RunnableConfig

from app.agents.core.utils import get_df_from_redis 
from app.agents.finance_agent.base_analysis import calc_cash_flow, calc_pnl, expense_structure

@tool
def calculate_cash_flow_tool(date_col: str, amount_col: str, freq: str = 'M', config: RunnableConfig = None) -> str:
    """
    Рассчитывает Cash Flow (денежный поток) по времени.
    Args:
        date_col: Название колонки с датами.
        amount_col: Название колонки с денежными суммами (где + это доход, а - это расход).
        freq: 'M' для месяцев, 'W' для недель, 'D' для дней.
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    df = get_df_from_redis(chat_id)
    pd_freq = 'ME' if freq.upper() == 'M' else freq # Конвертация для свежих версий Pandas
    
    res = calc_cash_flow(df, date_col, amount_col, pd_freq)
    return json.dumps(res, ensure_ascii=False)

@tool
def calculate_pnl_tool(amount_col: str, config: RunnableConfig = None) -> str:
    """
    Рассчитывает Отчет о прибылях и убытках (P&L): доходы, расходы, чистая прибыль и маржинальность.
    Args:
        amount_col: Название колонки с денежными суммами (доходы > 0, расходы < 0).
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    df = get_df_from_redis(chat_id)
    
    res = calc_pnl(df, amount_col)
    return json.dumps(res, ensure_ascii=False)

@tool
def analyze_expense_structure_tool(category_col: str, amount_col: str, config: RunnableConfig = None) -> str:
    """
    Анализирует структуру расходов по категориям (возвращает данные для pie chart).
    Args:
        category_col: Название колонки с категориями/статьями бюджета.
        amount_col: Название колонки с суммами транзакций.
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    df = get_df_from_redis(chat_id)
    
    res = expense_structure(df, category_col, amount_col)
    return json.dumps(res, ensure_ascii=False)