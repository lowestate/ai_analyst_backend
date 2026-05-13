import json
import time
from datetime import datetime, timezone
from langchain_core.messages import ToolMessage, SystemMessage

from app.config import logger
from app.agents.core.state import AgentState
from app.agents.core.utils import get_llm_request_metadata
from app.agents.data_analyst.models import da_agent_instance
from app.agents.data_analyst.prompts import MAIN_SYSTEM_PROMPT, AFTER_TOOL_COMPLETION
from app.users_db_interaction import log_llm_request


async def data_analyst_model_node(state: AgentState):
    """Узел вызова LLM для агента Аналитика Данных"""
    messages = state.get("messages", [])
    logger.info("data_analyst_model_node: старт обработки сообщений")

    clean_messages = [m for m in messages if not isinstance(m, SystemMessage)]
    logger.info("data_analyst_model_node: очищена история сообщений, SystemMessage удалены")

    final_messages = [SystemMessage(content=MAIN_SYSTEM_PROMPT)] + clean_messages
    logger.info(f"data_analyst_model_node: сформирован final_messages, размер={len(final_messages)}")

    # Текст запроса для лога — последнее human-сообщение
    request_text = ""
    for m in reversed(clean_messages):
        if getattr(m, "type", "") == "human":
            request_text = str(m.content)
            break

    created_at = datetime.now(timezone.utc)
    start_ts = time.monotonic()

    try:
        response = await da_agent_instance.llm_with_tools.ainvoke(final_messages)
        logger.info("data_analyst_model_node: LLM ответ получен")
    except Exception as e:
        duration_ms = int((time.monotonic() - start_ts) * 1000)
        logger.error(f"data_analyst_model_node: ошибка LLM error={str(e)}")
        await log_llm_request(
            request_id=None,
            user_id=state.get("user_id"),
            chat_id=state.get("chat_id"),
            request_text=request_text,
            input_tokens=None,
            output_tokens=None,
            request_status=500,
            model=None,
            created_at=created_at,
            duration_ms=duration_ms,
            initiator="data_analyst",
            error_msg=str(e),
        )
        raise

    duration_ms = int((time.monotonic() - start_ts) * 1000)
    meta = get_llm_request_metadata(response)

    await log_llm_request(
        request_id=meta.request_id,
        user_id=state.get("user_id"),
        chat_id=state.get("chat_id"),
        request_text=request_text,
        input_tokens=meta.input_tokens,
        output_tokens=meta.output_tokens,
        request_status=200,
        model=meta.model_name,
        created_at=created_at,
        duration_ms=duration_ms,
        initiator="data_analyst",
        error_msg=None,
    )

    return {
        "messages": [response],
        "charts_payload": state.get("charts_payload", [])
    }


async def data_analyst_tool_node(state: AgentState):
    """Узел исполнения инструментов для агента Аналитика Данных"""
    last_message = state['messages'][-1]
    tool_messages = []
    charts_payload = []

    tool_calls = getattr(last_message, "tool_calls", [])
    logger.info(f"data_analyst_tool_node: найдено tool_calls={len(tool_calls)}")

    for tool_call in tool_calls:
        tool_instance = da_agent_instance.tools_by_name[tool_call["name"]]
        args = tool_call.get("args", {})

        args.pop("chat_id", None)

        config = {"configurable": {"chat_id": state.get("chat_id")}}
        response = await tool_instance.ainvoke(args, config=config)
        logger.info(f"data_analyst_tool_node: выполнен tool={tool_call['name']}")

        try:
            # Тулы возвращают либо JSON-строку (старый формат: {"tool_type": ..., "data": ...})
            # либо словарь (новый формат: {"message": ..., "charts": [...]})
            if isinstance(response, dict):
                parsed_resp = response
            else:
                parsed_resp = json.loads(response)

            # Новый формат: {"message": ..., "charts": [{"type": ..., "data": ...}]}
            if parsed_resp.get("charts") is not None:
                new_charts = parsed_resp.get("charts", [])
                for chart in new_charts:
                    charts_payload.append({
                        "type": chart["type"],
                        "data": chart["data"]
                    })
                content = parsed_resp.get("message", str(response))
                if new_charts:
                    logger.info(f"data_analyst_tool_node: charts format, charts_count={len(new_charts)} tool={tool_call['name']}")
                else:
                    logger.warning(f"data_analyst_tool_node: charts format но пустые charts tool={tool_call['name']}")

            # Старый формат: {"tool_type": ..., "data": ...}
            elif parsed_resp.get("tool_type"):
                charts_payload.append({
                    "type": parsed_resp["tool_type"],
                    "data": parsed_resp["data"]
                })
                data_str = json.dumps(parsed_resp["data"], ensure_ascii=False)
                content = AFTER_TOOL_COMPLETION.format(data_str=data_str)
                logger.info(f"data_analyst_tool_node: обработан structured tool response tool={tool_call['name']}")

            else:
                content = response if isinstance(response, str) else json.dumps(response, ensure_ascii=False)
                logger.warning(f"data_analyst_tool_node: tool вернул неструктурированный ответ tool={tool_call['name']}")

        except json.JSONDecodeError as e:
            content = response
            logger.error(f"data_analyst_tool_node: JSONDecodeError tool={tool_call['name']} error={str(e)}")

        tool_messages.append(ToolMessage(content=str(content), tool_call_id=tool_call["id"]))

    logger.info("data_analyst_tool_node: завершена обработка tool_calls")

    return {
        "messages": tool_messages,
        "charts_payload": list(state.get("charts_payload", [])) + charts_payload,
        "all_charts": charts_payload
    }