from typing import TypedDict, Annotated, Sequence, Dict, Any
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    chat_id: str
    charts_payload: Sequence[Dict[str, Any]]
    next_agent: str  # Поле для роутинга супервизора