import json
from langchain_core.messages import ToolMessage, SystemMessage

from app.config import logger
from app.agents.core.state import AgentState
from app.agents.finance_agent.models import finance_agent_instance
from app.agents.finance_agent.prompts import FINANCE_AFTER_TOOL, FINANCE_SYSTEM_PROMPT

def finance_agent_model_node(state: AgentState):
    messages = state.get("messages", [])
    clean_messages = [m for m in messages if not isinstance(m, SystemMessage)]
    final_messages = [SystemMessage(content=FINANCE_SYSTEM_PROMPT)] + clean_messages
    
    response = finance_agent_instance.llm_with_tools.invoke(final_messages)
    logger.info("finance_agent_model_node: LLM вызван успешно")
    
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
                content = response
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