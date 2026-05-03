import os
from langchain_google_genai import ChatGoogleGenerativeAI

def llm(temp = 0.2) -> ChatGoogleGenerativeAI:
    init_llm = ChatGoogleGenerativeAI(
        model=os.getenv("LLM_MODEL", "gemini-2.5-flash"), 
        temperature=temp, # Низкая температура для генерации саммари
        api_key=os.getenv("GOOGLE_API_KEY")
    )

    return init_llm