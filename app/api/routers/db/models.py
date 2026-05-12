from pydantic import BaseModel


class RefreshSchemaRequest(BaseModel):
    chat_id: str
