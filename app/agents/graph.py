from langgraph.graph import StateGraph, END
from app.agents.core.state import AgentState
from langgraph.checkpoint.memory import MemorySaver

from app.agents.supervisor.node import supervisor_node
from app.agents.data_analyst.nodes import data_analyst_model_node, data_analyst_tool_node
from app.agents.finance_agent.nodes import finance_agent_model_node, finance_agent_tool_node

memory = MemorySaver()

def should_continue_data_analyst(state: AgentState):
    """Проверяет, решила ли модель вызвать тул или дать финальный ответ."""
    last_message = state["messages"][-1]
    if getattr(last_message, "tool_calls", None):
        return "tools"
    return "end"

def get_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("supervisor", supervisor_node)
    workflow.add_node("data_analyst_model", data_analyst_model_node)
    workflow.add_node("data_analyst_tools", data_analyst_tool_node)
    workflow.add_node("finance_agent_model", finance_agent_model_node)
    workflow.add_node("finance_agent_tools", finance_agent_tool_node)

    workflow.set_entry_point("supervisor")
    
    # Условный переход от супервизора к нужному агенту
    workflow.add_conditional_edges(
        "supervisor",
        lambda state: state.get("next_agent", "data_analyst"),
        {
            "data_analyst": "data_analyst_model",
            "finance_agent": "finance_agent_model"
        }
    )
    workflow.add_conditional_edges(
        "data_analyst_model",
        should_continue_data_analyst,
        {
            "tools": "data_analyst_tools",
            "end": END
        }
    )
    workflow.add_conditional_edges(
        "finance_agent_model",
        should_continue_data_analyst,
        {
            "tools": "finance_agent_tools",
            "end": END
        }
    )
    # После выполнения тулов, возвращаемся к модели, чтобы она ответила юзеру
    workflow.add_edge("data_analyst_tools", "data_analyst_model")
    workflow.add_edge("finance_agent_tools", "finance_agent_model")
    
    return workflow.compile(checkpointer=memory)