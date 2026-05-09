from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class ChatCreateResponse(BaseModel):
    chat_id: str
    preprocessing_report: str
    dataset_summary: str
    columns: List[str]
    db_schema: Optional[Dict[str, Any]] = None


class ChatRequest(BaseModel):
    chat_id: str
    message: str
    use_ai: bool
    cols_to_remove: list[str] = []
    sql_action: Optional[str] = None
    sql_feedback: Optional[str] = None
    sql_query: Optional[str] = None


class ChartData(BaseModel):
    type: str
    data: Dict[str, Any]


class ChatResponse(BaseModel):
    reply: str
    charts: Optional[List[ChartData]] = []
    sql_query: Optional[str] = None
    is_waiting_for_sql: bool = False


class RefreshSchemaRequest(BaseModel):
    chat_id: str


class ChatSessionDTO(BaseModel):
    id: str
    datasetName: str
    filename: str

class LoadChatResponse(BaseModel):
    db_schema: Optional[Dict[str, Any]] = None
    messages: List[Dict[str, Any]]
    is_waiting_for_sql: bool = False
    sql_query: Optional[str] = None
    charts_payload: List[Dict[str, Any]] = []