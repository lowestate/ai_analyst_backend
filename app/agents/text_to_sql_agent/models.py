from pydantic import BaseModel, Field

class SQLQueryOutput(BaseModel):
    sql_query: str = Field(description="Сгенерированный PostgreSQL SELECT запрос. ДОЛЖЕН НАЧИНАТЬСЯ С 'SELECT'")