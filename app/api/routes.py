from fastapi import APIRouter, UploadFile, File
from langchain_core.messages import HumanMessage, AIMessage

from app.api.schemas import ChatCreateResponse, ChatRequest, ChatResponse
from app.services.dataset import process_upload
from app.db.database import redis_client
from app.core.config import logger
from app.agent.graph import get_graph
from app.agent.initial_invoke import generate_initial_metadata, USE_MOCK_ANSWERS
from app.agent.utils import get_correlation_data
from app.agent.mock import generate_mock_correlation_report

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
        msg_lower = user_message.lower()
        
        if "корреляц" in msg_lower:
            try:
                # 1. Считаем реальные данные
                corr_data = get_correlation_data(req.chat_id)
                # 2. Формируем красивый мок-текст
                final_message = generate_mock_correlation_report(corr_data)
                charts = [{"type": "correlation", "data": corr_data}]
            except Exception as e:
                final_message = f"Ошибка мок-вычисления: {str(e)}"
                charts = []
        else:
            final_message = "Это мок-режим 🤖. Я умею отвечать только на запросы, содержащие слово 'корреляция'."
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