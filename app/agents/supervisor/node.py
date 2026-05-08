from pydantic import BaseModel, Field

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
    
    structured_llm = llm_instance.with_structured_output(RouteOutput)
    
    last_message = state["messages"][-1].content
    
    system_message = {"role": "system", "content": SUPERVISOR_PROMPT}
    user_message = {"role": "user", "content": last_message}
    
    response = await structured_llm.ainvoke([system_message, user_message])
    
    if isinstance(response, dict):
        next_agent = response.get("next_agent", "data_analyst")
    else:
        next_agent = getattr(response, "next_agent", "data_analyst") # type: ignore
        
    if next_agent not in ["data_analyst", "finance_agent"]:
        next_agent = "data_analyst"
        
    return {"next_agent": next_agent}
