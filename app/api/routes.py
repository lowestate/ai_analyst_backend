from fastapi import APIRouter, UploadFile, File
from langchain_core.messages import HumanMessage, AIMessage

from app.api.schemas import (
    ChatCreateResponse,
    ChatRequest,
    ChatResponse
)
from app.services.dataset import process_upload
from app.core.config import logger
from app.agent.graph import get_graph
from app.agent.initial_invoke import generate_initial_metadata, USE_MOCK_ANSWERS
from app.agent.mock_handlers import MOCK_REGISTRY, MockCommands

from app.db.database import redis_client
assert redis_client is not None, "Redis client must be initialized"

router = APIRouter()
app_graph = get_graph()

@router.post("/upload", response_model=ChatCreateResponse)
async def upload_dataset(file: UploadFile = File(...)):
    chat_id, filename, stats, columns = process_upload(file.file, file.filename)
    logger.info("process_upload DONE")
    init_data = await generate_initial_metadata(filename, columns, stats)
    
    config = {
        "configurable": {
            "thread_id": chat_id,
            "chat_id": chat_id
        }
    }
    
    initial_messages = [
        HumanMessage(content=f"Файл {filename} загружен для проведения анализа."),
        AIMessage(content=init_data.initial_message)
    ]
    logger.info(f"initial_messages={initial_messages}")
    
    app_graph.update_state(config, {
        "messages": initial_messages, 
        "chat_id": chat_id, 
        "charts_payload": []
    })
    
    return ChatCreateResponse(
        chat_id=chat_id, 
        preprocessing_report=init_data.initial_message,
        dataset_summary=init_data.chat_title,
        columns=columns
    )

@router.post("/chat", response_model=ChatResponse)
async def chat_interaction(req: ChatRequest):
    config = {
        "configurable": {
            "thread_id": req.chat_id,
            "chat_id": req.chat_id
        }
    }
    app_graph.update_state(config, {"charts_payload": []})
    logger.info("state updated")
    
    user_message = req.message
    
    if USE_MOCK_ANSWERS:
        logger.info("MOCK MODE: Перехват запроса чата")
        
        # .strip() уберет случайные пробелы в начале и конце, если юзер их случайно поставит
        msg_lower = user_message.lower().strip() 
        
        # Проверяем строгое совпадение: есть ли точный ключ в нашем реестре?
        handler_func = MOCK_REGISTRY.get(msg_lower)
        
        if handler_func:
            # Если точное совпадение найдено — вызываем функцию
            try:
                final_message, charts = handler_func(req.chat_id)
            except Exception as e:
                logger.error(f"Ошибка мок-вычисления для '{msg_lower}': {str(e)}")
                final_message = f"Ошибка вычисления инструмента: {str(e)}"
                charts = []
        else:
            # Если юзер написал что-то другое
            final_message = (
                "Это мок-режим 🤖. Я реагирую только на точные команды:\n"
                "- корреляционная матрица\n"
                "- анализ столбцов\n"
                "- аномалии\n"
                "- кросс-зависимости"
                "- тренд"
            )
            charts = []
            
        # Записываем мок-взаимодействие в память графа, чтобы история не порвалась
        app_graph.update_state(config, { # type: ignore
            "messages": [
                HumanMessage(content=user_message),
                AIMessage(content=final_message)
            ],
            "charts_payload": charts
        })
        
        return ChatResponse(reply=final_message, charts=charts)

    inputs = {"messages": [HumanMessage(content=user_message)]}
    final_state = app_graph.invoke(inputs, config=config)
    
    raw_content = final_state["messages"][-1].content
    
    if isinstance(raw_content, list):
        final_message = "".join(
            block["text"] for block in raw_content 
            if isinstance(block, dict) and "text" in block
        )
    else:
        final_message = str(raw_content)
        
    logger.info(f"\ninputs={inputs}\n\nfinal_message={final_message}\n")
    
    if not final_message.strip():
        logger.warning("LLM вернула пустой текст, применяем fallback-сообщение")
        final_message = "Графики успешно построены. Если вам нужна дополнительная текстовая интерпретация, дайте знать."

    charts = final_state.get("charts_payload", [])
    logger.info(f"\ncharts={charts}")
    return ChatResponse(reply=final_message, charts=charts)

@router.get("/available_mock_commands")
async def get_available_mock_commands():
    """
    Возвращает список всех доступных моковых команд из Enum.
    """
    # Достаем все значения (.value) из MockCommands
    return {"commands": [cmd.value for cmd in MockCommands]}