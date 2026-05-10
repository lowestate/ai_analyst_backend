from typing import TypedDict, Annotated, Sequence, Dict, Any, Optional, List
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    chat_id: str
    charts_payload: Sequence[Dict[str, Any]]
    next_agent: str
    data_sample: List[Dict[str, Any]]

    # Поля для БД
    data_source: str # "file" или "db"
    db_schema: Optional[Dict[str, Any]]
    db_credentials: Optional[Dict[str, Any]]
    
    # Состояние Text-to-SQL
    sql_query: Optional[str]
    waiting_for_sql_approval: bool
    sql_action: Optional[str]
    sql_feedback: Optional[str]
    origin_agent: str # Кто запросил данные (data_analyst / finance_agent)