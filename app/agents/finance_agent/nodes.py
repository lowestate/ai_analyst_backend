import json
import time
from datetime import datetime, timezone
from langchain_core.messages import ToolMessage, SystemMessage

from app.config import logger
from app.agents.core.state import AgentState
from app.agents.core.utils import get_llm_request_metadata
from app.agents.finance_agent.models import finance_agent_instance
from app.agents.finance_agent.prompts import FINANCE_AFTER_TOOL, FINANCE_SYSTEM_PROMPT
from app.users_db_interaction import log_llm_request


async def finance_agent_model_node(state: AgentState):
    messages = state.get("messages", [])
    clean_messages = [m for m in messages if not isinstance(m, SystemMessage)]
    final_messages = [SystemMessage(content=FINANCE_SYSTEM_PROMPT)] + clean_messages

    # Текст запроса для лога — последнее human-сообщение
    request_text = ""
    for m in reversed(clean_messages):
        if getattr(m, "type", "") == "human":
            request_text = str(m.content)
            break

    created_at = datetime.now(timezone.utc)
    start_ts = time.monotonic()

    try:
        response = await finance_agent_instance.llm_with_tools.ainvoke(final_messages)
        logger.info("finance_agent_model_node: LLM вызван успешно")
    except Exception as e:
        duration_ms = int((time.monotonic() - start_ts) * 1000)
        logger.error(f"finance_agent_model_node: ошибка LLM error={str(e)}")
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
            initiator="finance_agent",
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
        initiator="finance_agent",
        error_msg=None,
    )

    return {
        "messages": [response],
        "charts_payload": state.get("charts_payload", [])
    }


async def finance_agent_tool_node(state: AgentState):
    last_message = state['messages'][-1]
    tool_messages = []
    charts_payload = []

    tool_calls = getattr(last_message, "tool_calls", [])
    logger.info(f"finance_agent_tool_node: получено tool_calls={len(tool_calls)}")

    for tool_call in tool_calls:
        tool_instance = finance_agent_instance.tools_by_name[tool_call["name"]]
        args = tool_call.get("args", {})
        args.pop("chat_id", None)

        config = {"configurable": {"chat_id": state.get("chat_id")}}

        try:
            response = await tool_instance.ainvoke(args, config=config)
            logger.info(f"tool выполнен успешно name={tool_call['name']}")
        except Exception as e:
            logger.error(f"ошибка выполнения tool name={tool_call['name']} error={str(e)}")
            response = str(e)

        try:
            if isinstance(response, dict):
                parsed_resp = response
            else:
                parsed_resp = json.loads(response)

            if parsed_resp.get("tool_type"):
                charts_payload.append({
                    "type": parsed_resp["tool_type"],
                    "data": parsed_resp["data"]
                })
                data_str = json.dumps(parsed_resp["data"], ensure_ascii=False)
                content = FINANCE_AFTER_TOOL.format(data_str=data_str)
                logger.info(f"tool результат обработан tool_type={parsed_resp['tool_type']}")
            else:
                content = response if isinstance(response, str) else json.dumps(response, ensure_ascii=False)
                logger.warning(f"tool вернул неструктурированный ответ name={tool_call['name']}")
        except json.JSONDecodeError:
            content = response
            logger.error(f"JSON decode error в tool name={tool_call['name']}")

        tool_messages.append(ToolMessage(content=content, tool_call_id=tool_call["id"]))

    logger.info("finance_agent_tool_node: обработка tool_calls завершена")

    return {
        "messages": tool_messages,
        "charts_payload": list(state.get("charts_payload", [])) + charts_payload,
        "all_charts": charts_payload
    }