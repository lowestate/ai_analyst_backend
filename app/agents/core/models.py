from pydantic import BaseModel, Field

class InitChatOutput(BaseModel):
    chat_title: str = Field(description="Техническое название датасета на РУССКОМ языке (3-4 слова).")
    initial_message: str = Field(description="Профессиональное краткое сообщение на РУССКОМ языке о результатах загрузки.")