import os
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field

from app.agents.core.state import AgentState
from app.agents.supervisor.prompt import SUPERVISOR_PROMPT

class RouteOutput(BaseModel):
    next_agent: str = Field(
        description="Куда направить запрос. Строго 'data_analyst' или 'finance_agent'"
    )

async def supervisor_node(state: AgentState):
    """Узел супервизора для AI-режима. Определяет следующего агента."""
    llm = ChatGoogleGenerativeAI(
        model=os.getenv("LLM_MODEL", "gemini-2.5-flash"),
        temperature=0, # Нулевая температура для предсказуемой маршрутизации
        api_key=os.getenv("GOOGLE_API_KEY")
    )
    
    # Заставляем LLM вернуть строго JSON по нашей Pydantic схеме
    structured_llm = llm.with_structured_output(RouteOutput)
    
    # Берем последнее сообщение юзера
    last_message = state["messages"][-1].content
    
    system_message = {"role": "system", "content": SUPERVISOR_PROMPT}
    user_message = {"role": "user", "content": last_message}
    
    response = await structured_llm.ainvoke([system_message, user_message])
    
    if isinstance(response, dict):
        next_agent = response.get("next_agent", "data_analyst")
    else:
        # Pydantic v2 возвращает объект
        next_agent = getattr(response, "next_agent", "data_analyst") # type: ignore
        
    # Страховка: если LLM сошла с ума и выдала бред, кидаем в дефолтного агента
    if next_agent not in ["data_analyst", "finance_agent"]:
        next_agent = "data_analyst"
        
    return {"next_agent": next_agent}
