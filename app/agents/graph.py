from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import AIMessage

from app.agents.core.state import AgentState
from app.agents.supervisor.node import supervisor_node
from app.agents.data_analyst.nodes import data_analyst_model_node, data_analyst_tool_node
from app.agents.finance_agent.nodes import finance_agent_model_node, finance_agent_tool_node
from app.agents.text_to_sql_agent.nodes import text_to_sql_generate_node, text_to_sql_execute_node

memory = MemorySaver()

def should_continue_agent(state: AgentState):
    """Проверяет вызовы тулов. Маршрутизирует на Text-to-SQL, если вызван нужный тул."""
    last_message = state["messages"][-1]
    
    # Проверяем, что это AIMessage и у него есть tool_calls
    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        if last_message.tool_calls[0]["name"] == "request_db_query":
            return "text_to_sql"
        return "tools"
    return "end"

def entry_router(state: AgentState):
    """Определяет, с какой ноды начать выполнение графа при возобновлении."""
    if state.get("waiting_for_sql_approval"):
        if state.get("sql_action") == "approve":
            return "text_to_sql_execute"
        elif state.get("sql_action") == "reject":
            return "text_to_sql_generate"
    return "supervisor"

def return_to_origin(state: AgentState):
    """Возвращает данные в агент, который их изначально запросил."""
    origin = state.get("origin_agent", "data_analyst_model")
    return origin

def get_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("supervisor", supervisor_node)
    
    # Существующие агенты
    workflow.add_node("data_analyst_model", data_analyst_model_node)
    workflow.add_node("data_analyst_tools", data_analyst_tool_node)
    workflow.add_node("finance_agent_model", finance_agent_model_node)
    workflow.add_node("finance_agent_tools", finance_agent_tool_node)
    
    # НОВЫЕ: Text-to-SQL
    workflow.add_node("text_to_sql_generate", text_to_sql_generate_node)
    workflow.add_node("text_to_sql_execute", text_to_sql_execute_node)

    # Точка входа зависит от того, ждем ли мы аппрува
    workflow.set_conditional_entry_point(
        entry_router,
        {
            "supervisor": "supervisor",
            "text_to_sql_execute": "text_to_sql_execute",
            "text_to_sql_generate": "text_to_sql_generate"
        }
    )
    
    # Маршрутизация от супервизора
    workflow.add_conditional_edges(
        "supervisor",
        lambda state: state.get("next_agent", "data_analyst"),
        {
            "data_analyst": "data_analyst_model",
            "finance_agent": "finance_agent_model"
        }
    )
    
    # DA роутинг
    workflow.add_conditional_edges(
        "data_analyst_model",
        should_continue_agent,
        {
            "tools": "data_analyst_tools",
            "text_to_sql": "text_to_sql_generate",
            "end": END
        }
    )
    
    # Finance роутинг
    workflow.add_conditional_edges(
        "finance_agent_model",
        should_continue_agent,
        {
            "tools": "finance_agent_tools",
            "text_to_sql": "text_to_sql_generate",
            "end": END
        }
    )
    
    workflow.add_edge("data_analyst_tools", "data_analyst_model")
    workflow.add_edge("finance_agent_tools", "finance_agent_model")
    
    # Text-to-SQL прерывается (END) после генерации, чтобы фронт запросил аппрув
    workflow.add_edge("text_to_sql_generate", END)
    
    # После выполнения SQL возвращаемся к целевому агенту
    workflow.add_conditional_edges("text_to_sql_execute", return_to_origin)
    
    return workflow.compile(checkpointer=memory)