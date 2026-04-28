import os
from typing import Sequence, TypedDict, Dict, Any, Annotated
from pydantic import BaseModel, Field
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

from app.agent.tools import analyze_columns, correlation_matrix


class AgentModel:
    def __init__(self):
        self.llm = ChatGoogleGenerativeAI(
            model=os.getenv("LLM_MODEL", "gemini-2.5-flash"), 
            temperature=float(os.getenv("TEMPERATURE", 0.4)),
            api_key=os.getenv("GOOGLE_API_KEY") # type: ignore
        )
        self.tools = [
            analyze_columns,
            correlation_matrix
        ]
        self.tools_by_name = {t.name: t for t in self.tools}
        self.llm_with_tools = self.llm.bind_tools(self.tools)


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    chat_id: str
    charts_payload: Sequence[Dict[str, Any]]


class InitChatOutput(BaseModel):
    chat_title: str = Field(description="Техническое название датасета на РУССКОМ языке (3-4 слова).")
    initial_message: str = Field(description="Профессиональное краткое сообщение на РУССКОМ языке о результатах загрузки.")