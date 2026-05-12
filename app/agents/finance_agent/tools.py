import json
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig

from app.config import logger
from app.agents.core.utils import aget_df_from_db 
from app.agents.finance_agent.base_analysis import (
    calc_cash_flow,
    calc_pnl,
    expense_structure,
    calc_abc_analysis,
    calc_revenue_forecast,
    calc_unit_economics,
    calc_cohort_analysis
)

@tool
def request_db_query(query_description: str) -> str:
    """ОБЯЗАТЕЛЬНО используй этот инструмент, если источник данных - база данных (db).
    В 'query_description' опиши БИЗНЕС-СУТЬ того, что нужно найти (например: 'Посчитай LTV, CAC и выручку по каналам привлечения').
    ВНИМАНИЕ: НЕ проси агента узнать структуру БД или названия таблиц! У SQL-агента уже есть полная схема БД. Просто четко поставь ему аналитическую задачу."""
    logger.info(f"request_db_query вызван query_description={query_description}")
    return "Ожидание выполнения SQL-запроса в БД..."


@tool
async def calculate_cash_flow_tool(
    date_col: str,
    amount_col: str,
    freq: str = 'M',
    config: RunnableConfig = None
) -> str:
    """
    Рассчитывает Cash Flow (денежный поток) по времени.
    Args:
        date_col: Название колонки с датами.
        amount_col: Название колонки с денежными суммами (где + это доход, а - это расход).
        freq: 'M' для месяцев, 'W' для недель, 'D' для дней.
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"calculate_cash_flow_tool start chat_id={chat_id} date_col={date_col} amount_col={amount_col} freq={freq}")

    try:
        df = await aget_df_from_db(chat_id)
        pd_freq = 'ME' if freq.upper() == 'M' else freq
        res = calc_cash_flow(df, date_col, amount_col, pd_freq)
        logger.info(f"calculate_cash_flow_tool done chat_id={chat_id}")
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        logger.error(f"calculate_cash_flow_tool error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def calculate_pnl_tool(
    amount_col: str,
    config: RunnableConfig = None
) -> str:
    """
    Рассчитывает Отчет о прибылях и убытках (P&L): доходы, расходы, чистая прибыль и маржинальность.
    Args:
        amount_col: Название колонки с денежными суммами (доходы > 0, расходы < 0).
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"calculate_pnl_tool start chat_id={chat_id} amount_col={amount_col}")

    try:
        df = await aget_df_from_db(chat_id)
        res = calc_pnl(df, amount_col)
        logger.info(f"calculate_pnl_tool done chat_id={chat_id}")
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        logger.error(f"calculate_pnl_tool error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def analyze_expense_structure_tool(
    category_col: str,
    amount_col: str,
    config: RunnableConfig = None
) -> str:
    """
    Анализирует структуру расходов по категориям (возвращает данные для pie chart).
    Args:
        category_col: Название колонки с категориями/статьями бюджета.
        amount_col: Название колонки с суммами транзакций.
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"analyze_expense_structure_tool start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        res = expense_structure(df, category_col, amount_col)
        logger.info(f"analyze_expense_structure_tool done chat_id={chat_id}")
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        logger.error(f"analyze_expense_structure_tool error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def analyze_abc_tool(
    category_col: str,
    amount_col: str,
    config: RunnableConfig = None
) -> str:
    """Проводит ABC-анализ (Парето). Показывает, какие категории приносят основную выручку."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"analyze_abc_tool start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        res = calc_abc_analysis(df, category_col, amount_col)
        logger.info(f"analyze_abc_tool done chat_id={chat_id}")
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        logger.error(f"analyze_abc_tool error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def analyze_unit_economics_tool(
    source_col: str,
    amount_col: str,
    cac_col: str,
    user_col: str = None,
    config: RunnableConfig = None
) -> str:
    """
    [КРИТИЧЕСКИ ВАЖНО] ОБЯЗАТЕЛЬНО вызывай этот инструмент, если пользователь просит анализ юнит-экономики! 
    Это необходимо для отрисовки интерактивного графика на фронтенде. 
    Вызывай этот тул ДАЖЕ ЕСЛИ ты уже получил все нужные цифры через SQL-запрос (request_db_query).
    
    ПРАВИЛО: Имена колонок (source_col, amount_col, cac_col, user_col) передавай СТРОГО так, как они называются в схеме базы данных (на английском). КАТЕГОРИЧЕСКИ ЗАПРЕЩАЕТСЯ переводить названия колонок на русский язык!
    """
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"analyze_unit_economics_tool start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        res = calc_unit_economics(df, source_col, amount_col, cac_col, user_col)
        logger.info(f"analyze_unit_economics_tool done chat_id={chat_id}")
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        logger.error(f"analyze_unit_economics_tool error chat_id={chat_id} error={str(e)}")
        return str(e)

@tool
async def forecast_revenue_tool(
    date_col: str,
    amount_col: str,
    forecast_periods: int = 3,
    config: RunnableConfig = None
) -> str:
    """Строит прогноз выручки на будущие периоды на основе исторических данных."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"forecast_revenue_tool start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        res = calc_revenue_forecast(df, date_col, amount_col, forecast_periods)
        logger.info(f"forecast_revenue_tool done chat_id={chat_id}")
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        logger.error(f"forecast_revenue_tool error chat_id={chat_id} error={str(e)}")
        return str(e)


@tool
async def analyze_cohorts_tool(
    date_col: str,
    user_col: str,
    config: RunnableConfig = None
) -> str:
    """Проводит Когортный анализ (Retention). Показывает, как долго пользователи продолжают покупать."""
    chat_id = config.get("configurable", {}).get("chat_id")
    logger.info(f"analyze_cohorts_tool start chat_id={chat_id}")

    try:
        df = await aget_df_from_db(chat_id)
        res = calc_cohort_analysis(df, date_col, user_col)
        logger.info(f"analyze_cohorts_tool done chat_id={chat_id}")
        return json.dumps(res, ensure_ascii=False)
    except Exception as e:
        logger.error(f"analyze_cohorts_tool error chat_id={chat_id} error={str(e)}")
        return str(e)