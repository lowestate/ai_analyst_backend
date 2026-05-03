import json
from langchain_core.messages import ToolMessage, SystemMessage

from app.agents.core.state import AgentState
from app.agents.data_analyst.models import da_agent_instance
from app.agents.data_analyst.prompts import MAIN_SYSTEM_PROMPT, AFTER_TOOL_COMPLETION

def data_analyst_model_node(state: AgentState):
    """Узел вызова LLM для агента Аналитика Данных"""
    messages = state.get("messages", [])
    
    # Очищаем историю от предыдущих системных промптов (чтобы они не наслаивались при долгой беседе)
    clean_messages = [m for m in messages if not isinstance(m, SystemMessage)]
    
    # Подставляем актуальный системный промпт именно для Аналитика
    final_messages = [SystemMessage(content=MAIN_SYSTEM_PROMPT)] + clean_messages
    
    # Вызываем модель с привязанными тулами
    response = da_agent_instance.llm_with_tools.invoke(final_messages)
    
    return {
        "messages": [response],
        # Пробрасываем payload дальше, не затирая его
        "charts_payload": state.get("charts_payload", [])
    }


def data_analyst_tool_node(state: AgentState):
    """Узел исполнения инструментов для агента Аналитика Данных"""
    last_message = state['messages'][-1]
    tool_messages = []
    charts_payload = []
    
    # Защита: если тулов нет, просто возвращаем пустой список (хотя роутер графа не должен сюда пустить)
    tool_calls = getattr(last_message, "tool_calls", [])
    
    for tool_call in tool_calls:
        tool_instance = da_agent_instance.tools_by_name[tool_call["name"]]
        args = tool_call.get("args", {})
        
        # Безопасно удаляем chat_id, если он просочился в аргументы из промпта
        args.pop("chat_id", None)
        
        config = {"configurable": {"chat_id": state.get("chat_id")}}
        response = tool_instance.invoke(args, config=config)
        
        try:
            parsed_resp = json.loads(response)
            if parsed_resp.get("tool_type"):
                # Собираем данные для графиков
                charts_payload.append({
                    "type": parsed_resp["tool_type"], 
                    "data": parsed_resp["data"]
                })
                # Формируем ответ для LLM, чтобы она знала, что данные успешно отрисованы
                data_str = json.dumps(parsed_resp["data"], ensure_ascii=False)
                content = AFTER_TOOL_COMPLETION.format(data_str=data_str)
            else:
                content = response
        except json.JSONDecodeError:
            # Если тул вернул просто строку, а не JSON
            content = response
            
        tool_messages.append(ToolMessage(content=content, tool_call_id=tool_call["id"]))
        
    return {
        "messages": tool_messages, 
        # Склеиваем старые графики с новыми, если за один ход было вызвано несколько тулов
        "charts_payload": list(state.get("charts_payload", [])) + charts_payload
    }