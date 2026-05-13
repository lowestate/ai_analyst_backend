import json
import time
import pickle
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict
from langchain_core.messages import ToolMessage, AIMessage

from app.config import logger
from app.database import pool
from app.agents.core.state import AgentState
from app.agents.client import llm
from app.agents.core.utils import get_llm_request_metadata
from app.agents.text_to_sql_agent.prompts import TEXT_TO_SQL_PROMPT, TEXT_TO_SQL_REGENERATE_PROMPT
from app.agents.text_to_sql_agent.models import SQLQueryOutput
from app.users_db_interaction import execute_sql_query, log_llm_request

async def text_to_sql_generate_node(state: AgentState):
    """Узел генерации или перегенерации SQL-запроса."""
    last_message = state["messages"][-1]

    query_description = ""
    origin_agent = state.get("origin_agent", "data_analyst_model")

    logger.info("text_to_sql_generate_node старт")

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        query_description = last_message.tool_calls[0]["args"].get("query_description", "")
        origin_agent = "data_analyst_model" if "data_analyst" in state["next_agent"] else "finance_agent_model"
        logger.info(f"text_to_sql_generate_node tool_call detected query_description_len={len(query_description)} origin_agent={origin_agent}")
    else:
        query_description = "Пожалуйста, сформируй запрос на основе истории сообщений."
        logger.warning("text_to_sql_generate_node fallback query_description использован")

    # include_raw=True — получаем и AIMessage (с метаданными), и разобранный объект
    llm_instance = llm(temp=0).with_structured_output(SQLQueryOutput, include_raw=True)

    if state.get("sql_action") == "reject":
        prompt = TEXT_TO_SQL_REGENERATE_PROMPT.format(
            db_schema=json.dumps(state.get("db_schema", {}), ensure_ascii=False, indent=2),
            query_description=query_description,
            previous_sql=state.get("sql_query", ""),
            sql_feedback=state.get("sql_feedback", "Запрос не подошел")
        )
        logger.info("text_to_sql_generate_node режим regenerate")
    else:
        prompt = TEXT_TO_SQL_PROMPT.format(
            db_schema=json.dumps(state.get("db_schema", {}), ensure_ascii=False, indent=2),
            query_description=query_description
        )
        logger.info("text_to_sql_generate_node режим generate")

    created_at = datetime.now(timezone.utc)
    start_ts = time.monotonic()

    try:
        raw_result: Dict[str, Any] = await llm_instance.ainvoke([{"role": "user", "content": prompt}])  # type: ignore[assignment]
        logger.info("text_to_sql_generate_node LLM выполнен успешно")
    except Exception as e:
        duration_ms = int((time.monotonic() - start_ts) * 1000)
        logger.error(f"text_to_sql_generate_node ошибка LLM error={str(e)}")
        await log_llm_request(
            request_id=None,
            user_id=state.get("user_id"),
            chat_id=state.get("chat_id"),
            request_text=query_description,
            input_tokens=None,
            output_tokens=None,
            request_status=500,
            model=None,
            created_at=created_at,
            duration_ms=duration_ms,
            initiator="text_to_sql_agent",
            error_msg=str(e),
        )
        return {
            "sql_query": "",
            "waiting_for_sql_approval": True,
            "sql_action": None,
            "sql_feedback": None,
            "origin_agent": origin_agent
        }

    duration_ms = int((time.monotonic() - start_ts) * 1000)

    # raw_result — dict с ключами "raw" (AIMessage), "parsed" (SQLQueryOutput), "parsing_error"
    raw_ai_message = raw_result.get("raw")
    response = raw_result.get("parsed")

    meta = get_llm_request_metadata(raw_ai_message) if raw_ai_message else None

    await log_llm_request(
        request_id=meta.request_id if meta else None,
        user_id=state.get("user_id"),
        chat_id=state.get("chat_id"),
        request_text=query_description,
        input_tokens=meta.input_tokens if meta else None,
        output_tokens=meta.output_tokens if meta else None,
        request_status=200,
        model=meta.model_name if meta else None,
        created_at=created_at,
        duration_ms=duration_ms,
        initiator="text_to_sql_agent",
        error_msg=raw_result.get("parsing_error"),
    )

    if isinstance(response, dict):
        sql_query = response.get("sql_query", "")
    else:
        sql_query = getattr(response, "sql_query", "")

    logger.info(f"text_to_sql_generate_node sql_query_len={len(sql_query)}")

    return {
        "sql_query": sql_query,
        "waiting_for_sql_approval": True,
        "sql_action": None,
        "sql_feedback": None,
        "origin_agent": origin_agent
    }


async def text_to_sql_execute_node(state: AgentState):
    """Выполняет подтвержденный SQL запрос и возвращает ToolMessage сабагенту."""
    query = str(state.get("sql_query", ""))

    logger.info("text_to_sql_execute_node старт")

    query = query.strip()
    if query.startswith("```sql"):
        query = query[6:]
    elif query.startswith("```"):
        query = query[3:]
    if query.endswith("```"):
        query = query[:-3]
    query = query.strip()

    logger.info(f"text_to_sql_execute_node query_cleaned_len={len(query)}")

    creds = state.get("db_credentials")

    tool_call_id = "unknown"
    for msg in reversed(state["messages"]):
        if getattr(msg, "type", "") == "ai" and getattr(msg, "tool_calls", None):
            tool_call_id = msg.tool_calls[0]["id"] # type: ignore
            break

    logger.info(f"text_to_sql_execute_node tool_call_id={tool_call_id}")

    try:
        if not creds:
            raise ValueError("Креды для БД отсутствуют.")

        data = await execute_sql_query(creds, query)
        result_str = json.dumps(data, ensure_ascii=False, default=str)

        logger.info(f"text_to_sql_execute_node SQL выполнен rows_size={len(result_str)}")

        # Сохраняем результат SQL как DataFrame в dataset_encoded,
        # чтобы тулы анализа (aget_df_from_db) могли его подхватить
        if data and isinstance(data, list):
            chat_id = state.get("chat_id")
            try:
                df = pd.DataFrame(data)
                dataset_bytes = pickle.dumps(df)
                async with pool.connection() as conn:
                    await conn.execute(
                        "UPDATE chats SET dataset_encoded = %s WHERE chat_id = %s",
                        (dataset_bytes, chat_id)
                    )
                logger.info(f"text_to_sql_execute_node SQL результат сохранен как DataFrame chat_id={chat_id} rows={len(df)}")
            except Exception as save_err:
                logger.warning(f"text_to_sql_execute_node не удалось сохранить DataFrame: {save_err}")

        if len(result_str) > 15000:
            result_str = result_str[:15000] + "\n...[ДАННЫЕ ОБРЕЗАНЫ ДЛЯ LLM]"
            logger.warning("text_to_sql_execute_node результат обрезан")

    except Exception as e:
        logger.error(f"text_to_sql_execute_node ошибка SQL error={str(e)}")
        result_str = f"Ошибка выполнения SQL: {str(e)}"

    final_content = f"[SQL_QUERY]\n{query}\n[/SQL_QUERY]\n{result_str}"

    tool_message = ToolMessage(
        content=final_content,
        tool_call_id=tool_call_id,
        name="request_db_query"
    )

    logger.info("text_to_sql_execute_node завершен успешно")

    return {
        "messages": [tool_message],
        "waiting_for_sql_approval": False
    }