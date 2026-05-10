import json
from langchain_core.messages import ToolMessage, AIMessage

from app.config import logger
from app.agents.core.state import AgentState
from app.agents.client import llm
from app.agents.text_to_sql_agent.prompts import TEXT_TO_SQL_PROMPT, TEXT_TO_SQL_REGENERATE_PROMPT
from app.agents.text_to_sql_agent.models import SQLQueryOutput
from app.users_db_interaction import execute_sql_query

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

    llm_instance = llm(temp=0).with_structured_output(SQLQueryOutput)

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

    try:
        response = await llm_instance.ainvoke([{"role": "user", "content": prompt}])
        logger.info("text_to_sql_generate_node LLM выполнен успешно")
    except Exception as e:
        logger.error(f"text_to_sql_generate_node ошибка LLM error={str(e)}")
        return {
            "sql_query": "",
            "waiting_for_sql_approval": True,
            "sql_action": None,
            "sql_feedback": None,
            "origin_agent": origin_agent
        }

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
        if isinstance(msg, AIMessage) and getattr(msg, "tool_calls", None):
            tool_call_id = msg.tool_calls[0]["id"]
            break

    logger.info(f"text_to_sql_execute_node tool_call_id={tool_call_id}")

    try:
        if not creds:
            raise ValueError("Креды для БД отсутствуют.")

        data = await execute_sql_query(creds, query)
        result_str = json.dumps(data, ensure_ascii=False, default=str)

        logger.info(f"text_to_sql_execute_node SQL выполнен rows_size={len(result_str)}")

        if len(result_str) > 15000:
            result_str = result_str[:15000] + "\n...[ДАННЫЕ ОБРЕЗАНЫ ДЛЯ LLM]"
            logger.warning("text_to_sql_execute_node результат обрезан")

    except Exception as e:
        logger.error(f"text_to_sql_execute_node ошибка SQL error={str(e)}")
        result_str = f"Ошибка выполнения SQL: {str(e)}"

    tool_message = ToolMessage(
        content=result_str,
        tool_call_id=tool_call_id,
        name="request_db_query"
    )

    logger.info("text_to_sql_execute_node завершен успешно")

    return {
        "messages": [tool_message],
        "waiting_for_sql_approval": False
    }