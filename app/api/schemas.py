from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class ChatCreateResponse(BaseModel):
    chat_id: str
    preprocessing_report: str
    dataset_summary: str
    columns: List[str]


class ChatRequest(BaseModel):
    chat_id: str
    message: str
    use_ai: bool


class ChartData(BaseModel):
    type: str # 'correlation', 'distribution', 'category_count'
    data: Dict[str, Any]


class ChatResponse(BaseModel):
    reply: str
    charts: Optional[List[ChartData]] = []