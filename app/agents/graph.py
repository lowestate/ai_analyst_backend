from langgraph.graph import StateGraph, END
from langchain_core.messages import AIMessage

from app.config import logger
from app.agents.core.state import AgentState
from app.database import CustomAsyncPostgresSaver, pool
from app.agents.supervisor.node import supervisor_node
from app.agents.data_analyst.nodes import data_analyst_model_node, data_analyst_tool_node
from app.agents.finance_agent.nodes import finance_agent_model_node, finance_agent_tool_node
from app.agents.text_to_sql_agent.nodes import text_to_sql_generate_node, text_to_sql_execute_node

def should_continue_agent(state: AgentState):
    """Проверяет вызовы тулов. Маршрутизирует на Text-to-SQL, если вызван нужный тул."""
    last_message = state["messages"][-1]

    logger.info("should_continue_agent вызов")

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        tool_name = last_message.tool_calls[0]["name"]
        logger.info(f"should_continue_agent tool_call найден name={tool_name}")

        if tool_name == "request_db_query":
            logger.info("should_continue_agent маршрут text_to_sql")
            return "text_to_sql"

        logger.info("should_continue_agent маршрут tools")
        return "tools"

    logger.info("should_continue_agent маршрут end")
    return "end"


def entry_router(state: AgentState):
    """Определяет, с какой ноды начать выполнение графа при возобновлении."""
    logger.info("entry_router вызов")

    if state.get("waiting_for_sql_approval"):
        sql_action = state.get("sql_action")
        logger.info(f"entry_router waiting_sql_approval sql_action={sql_action}")

        if sql_action == "approve":
            logger.info("entry_router -> text_to_sql_execute")
            return "text_to_sql_execute"

        if sql_action == "reject":
            logger.info("entry_router -> text_to_sql_generate")
            return "text_to_sql_generate"

    logger.info("entry_router -> supervisor")
    return "supervisor"


def return_to_origin(state: AgentState):
    """Возвращает данные в агент, который их изначально запросил."""
    origin = state.get("origin_agent", "data_analyst_model")
    logger.info(f"return_to_origin origin_agent={origin}")
    return origin


def init_graph():
    logger.info("init_graph старт")

    workflow = StateGraph(AgentState)

    workflow.add_node("supervisor", supervisor_node)

    workflow.add_node("data_analyst_model", data_analyst_model_node)
    workflow.add_node("data_analyst_tools", data_analyst_tool_node)
    workflow.add_node("finance_agent_model", finance_agent_model_node)
    workflow.add_node("finance_agent_tools", finance_agent_tool_node)

    workflow.add_node("text_to_sql_generate", text_to_sql_generate_node)
    workflow.add_node("text_to_sql_execute", text_to_sql_execute_node)

    workflow.set_conditional_entry_point(
        entry_router,
        {
            "supervisor": "supervisor",
            "text_to_sql_execute": "text_to_sql_execute",
            "text_to_sql_generate": "text_to_sql_generate"
        }
    )
    logger.info("init_graph entry_point настроен")

    workflow.add_conditional_edges(
        "supervisor",
        lambda state: state.get("next_agent", "data_analyst"),
        {
            "data_analyst": "data_analyst_model",
            "finance_agent": "finance_agent_model"
        }
    )
    logger.info("init_graph edges supervisor добавлены")

    workflow.add_conditional_edges(
        "data_analyst_model",
        should_continue_agent,
        {
            "tools": "data_analyst_tools",
            "text_to_sql": "text_to_sql_generate",
            "end": END
        }
    )

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

    workflow.add_edge("text_to_sql_generate", END)
    workflow.add_conditional_edges("text_to_sql_execute", return_to_origin)

    memory = CustomAsyncPostgresSaver(pool)
    graph = workflow.compile(checkpointer=memory)

    logger.info("init_graph завершен")

    return graph


app_graph = init_graph()
logger.info("создан инстанс графа app_graph")