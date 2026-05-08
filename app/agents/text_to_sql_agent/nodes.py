import json
from langchain_core.messages import ToolMessage, AIMessage
from app.agents.core.state import AgentState
from app.agents.client import llm
from .prompts import TEXT_TO_SQL_PROMPT, TEXT_TO_SQL_REGENERATE_PROMPT
from .models import SQLQueryOutput
from app.users_db_interaction import execute_sql_query

async def text_to_sql_generate_node(state: AgentState):
    """Узел генерации или перегенерации SQL-запроса."""
    last_message = state["messages"][-1]
    
    # 1. Извлекаем описание запроса. 
    # Если мы пришли сюда впервые, это вызов тула request_db_query от DA/Finance
    query_description = ""
    origin_agent = state.get("origin_agent", "data_analyst_model")
    
    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        query_description = last_message.tool_calls[0]["args"].get("query_description", "")
        # Определяем, кто нас вызвал, чтобы потом вернуть данные именно ему
        origin_agent = "data_analyst_model" if "data_analyst" in state["next_agent"] else "finance_agent_model"
    else:
        # Фолбэк, если восстанавливаемся из стейта без явного tool_call (редкий кейс)
        query_description = "Пожалуйста, сформируй запрос на основе истории сообщений."

    llm_instance = llm(temp=0).with_structured_output(SQLQueryOutput)
    
    # 2. Проверяем, это первый раз или перегенерация
    if state.get("sql_action") == "reject":
        prompt = TEXT_TO_SQL_REGENERATE_PROMPT.format(
            db_schema=json.dumps(state.get("db_schema", {}), ensure_ascii=False, indent=2),
            query_description=query_description,
            previous_sql=state.get("sql_query", ""),
            sql_feedback=state.get("sql_feedback", "Запрос не подошел")
        )
    else:
        prompt = TEXT_TO_SQL_PROMPT.format(
            db_schema=json.dumps(state.get("db_schema", {}), ensure_ascii=False, indent=2),
            query_description=query_description
        )

    response = await llm_instance.ainvoke([{"role": "user", "content": prompt}])
    
    if isinstance(response, dict):
        sql_query = response.get("sql_query", "")
    else:
        sql_query = getattr(response, "sql_query", "")

    return {
        "sql_query": sql_query,
        "waiting_for_sql_approval": True,
        "sql_action": None, # Сбрасываем старые экшены
        "sql_feedback": None,
        "origin_agent": origin_agent
    }

async def text_to_sql_execute_node(state: AgentState):
    """Выполняет подтвержденный SQL запрос и возвращает ToolMessage сабагенту."""
    query = str(state.get("sql_query", ""))
    
    # 1. Очищаем запрос от markdown-разметки (```sql ... ```)
    query = query.strip()
    if query.startswith("```sql"):
        query = query[6:]
    elif query.startswith("```"):
        query = query[3:]
    if query.endswith("```"):
        query = query[:-3]
    query = query.strip()

    creds = state.get("db_credentials")
    
    tool_call_id = "unknown"
    for msg in reversed(state["messages"]):
        if isinstance(msg, AIMessage) and getattr(msg, "tool_calls", None):
            tool_call_id = msg.tool_calls[0]["id"]
            break

    try:
        if not creds:
            raise ValueError("Креды для БД отсутствуют.")
            
        data = await execute_sql_query(creds, query)
        result_str = json.dumps(data, ensure_ascii=False, default=str)
        
        if len(result_str) > 15000:
            result_str = result_str[:15000] + "\n...[ДАННЫЕ ОБРЕЗАНЫ ДЛЯ LLM]"
            
    except Exception as e:
        # Отдаем ошибку агенту, если запрос упал
        result_str = f"Ошибка выполнения SQL: {str(e)}"

    # 2. ОБЯЗАТЕЛЬНО передаем name="request_db_query", чтобы агент понял ответ
    tool_message = ToolMessage(content=result_str, tool_call_id=tool_call_id, name="request_db_query")
    
    return {
        "messages": [tool_message],
        "waiting_for_sql_approval": False # Снимаем паузу
    }