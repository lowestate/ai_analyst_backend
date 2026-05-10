from pydantic import BaseModel, Field

from app.config import logger
from app.agents.client import llm
from app.agents.core.state import AgentState
from app.agents.supervisor.prompt import SUPERVISOR_PROMPT

class RouteOutput(BaseModel):
    next_agent: str = Field(
        description="Куда направить запрос. Строго 'data_analyst' или 'finance_agent'"
    )

async def supervisor_node(state: AgentState):
    """Узел супервизора для AI-режима. Определяет следующего агента."""
    llm_instance = llm(temp=0)
    logger.info("supervisor_node старт")

    structured_llm = llm_instance.with_structured_output(RouteOutput)

    last_message = state["messages"][-1].content
    logger.info(f"supervisor_node входное сообщение last_message_len={len(last_message)}")

    system_message = {"role": "system", "content": SUPERVISOR_PROMPT}
    user_message = {"role": "user", "content": last_message}

    try:
        response = await structured_llm.ainvoke([system_message, user_message])
        logger.info("supervisor_node LLM ответ получен")
    except Exception as e:
        logger.error(f"supervisor_node ошибка LLM error={str(e)}")
        return {"next_agent": "data_analyst"}

    if isinstance(response, dict):
        next_agent = response.get("next_agent", "data_analyst")
    else:
        next_agent = getattr(response, "next_agent", "data_analyst")

    logger.info(f"supervisor_node raw next_agent={next_agent}")

    if next_agent not in ["data_analyst", "finance_agent"]:
        logger.warning(f"supervisor_node некорректный next_agent={next_agent} fallback=data_analyst")
        next_agent = "data_analyst"

    logger.info(f"supervisor_node финальный маршрут next_agent={next_agent}")

    return {"next_agent": next_agent}