from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    username: str
    password: str
    

class UserLogin(BaseModel):
    username: str
    password: str


class SubscriptionChangeRequest(BaseModel):
    user_id: int
    target_plan: str = Field(..., description="Название тарифа: free, pro или ultra")