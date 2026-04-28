from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, END
from langgraph.graph.state import CompiledStateGraph

from app.agent.models import AgentState
from app.agent.nodes import process_model_call, process_tool_call

def should_continue(state: AgentState):
    last_message = state['messages'][-1]
    if getattr(last_message, "tool_calls", []):
        return "continue"
    return "end"

def get_graph() -> CompiledStateGraph:
    workflow = StateGraph(AgentState)
    workflow.add_node("agent", process_model_call)
    workflow.add_node("action", process_tool_call)

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "action",
            "end": END
        }
    )
    workflow.add_edge("action", "agent")

    memory = MemorySaver()
    app_graph = workflow.compile(checkpointer=memory) 

    return app_graph