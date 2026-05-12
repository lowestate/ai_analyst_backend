import json
from langchain_core.messages import ToolMessage, SystemMessage

from app.config import logger
from app.agents.core.state import AgentState
from app.agents.data_analyst.models import da_agent_instance
from app.agents.data_analyst.prompts import MAIN_SYSTEM_PROMPT, AFTER_TOOL_COMPLETION

def data_analyst_model_node(state: AgentState):
    """Узел вызова LLM для агента Аналитика Данных"""
    messages = state.get("messages", [])
    logger.info(f"data_analyst_model_node: старт обработки сообщений")

    clean_messages = [m for m in messages if not isinstance(m, SystemMessage)]
    logger.info(f"data_analyst_model_node: очищена история сообщений, SystemMessage удалены")

    final_messages = [SystemMessage(content=MAIN_SYSTEM_PROMPT)] + clean_messages
    logger.info(f"data_analyst_model_node: сформирован final_messages, размер={len(final_messages)}")

    response = da_agent_instance.llm_with_tools.invoke(final_messages)
    logger.info(f"data_analyst_model_node: LLM ответ получен")

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
            parsed_resp = json.loads(response)

            if parsed_resp.get("tool_type"):
                charts_payload.append({
                    "type": parsed_resp["tool_type"],
                    "data": parsed_resp["data"]
                })

                data_str = json.dumps(parsed_resp["data"], ensure_ascii=False)
                content = AFTER_TOOL_COMPLETION.format(data_str=data_str)
                logger.info(f"data_analyst_tool_node: обработан structured tool response tool={tool_call['name']}")
            else:
                content = response
                logger.warning(f"data_analyst_tool_node: tool вернул неструктурированный ответ tool={tool_call['name']}")

        except json.JSONDecodeError as e:
            content = response
            logger.error(f"data_analyst_tool_node: JSONDecodeError tool={tool_call['name']} error={str(e)}")

        tool_messages.append(ToolMessage(content=content, tool_call_id=tool_call["id"]))

    logger.info(f"data_analyst_tool_node: завершена обработка tool_calls")

    return {
        "messages": tool_messages,
        "charts_payload": list(state.get("charts_payload", [])) + charts_payload,
        "all_charts": charts_payload
    }