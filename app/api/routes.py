from fastapi import APIRouter, UploadFile, File
from langchain_core.messages import HumanMessage, AIMessage

from app.api.schemas import (
    ChatCreateResponse,
    ChatRequest,
    ChatResponse
)
from app.config import logger, redis_client
from app.agents.supervisor.mock_router import route_mock_request
assert redis_client is not None, "Redis client must be initialized"
from app.agents.graph import get_graph
from app.agents.core.initial_invoke import generate_initial_metadata
from app.agents.core.utils import process_upload, serialize
from app.agents.data_analyst.mock.mock_handlers import DA_MOCK_REGISTRY, DAMockCommands
from app.agents.finance_agent.mock.mock_handlers import FIN_MOCK_REGISTRY, FinMockCommands


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
    logger.info(f"new request: {req}")
    config = {
        "configurable": {
            "thread_id": req.chat_id,
            "chat_id": req.chat_id
        }
    }
    app_graph.update_state(config, {"charts_payload": []})
    
    user_message = req.message
    
    # --- ОБРАБОТКА МОК-РЕЖИМА ---
    if not req.use_ai:
        logger.info("MOCK MODE: Перехват запроса чата")
        
        # 1. Роутер моков определяет агента и очищает тег [A]/[Ф]
        target_agent, msg_clean = route_mock_request(user_message)
        msg_clean = msg_clean.strip('.?!') 
        
        handler_func = None
        extracted_args = ()
        
        # 2. Выбираем реестр команд в зависимости от тега
        if target_agent == "data_analyst":
            active_registry = DA_MOCK_REGISTRY
        elif target_agent == "finance_agent":
            active_registry = FIN_MOCK_REGISTRY
        else:
            active_registry = DA_MOCK_REGISTRY
            
        # 3. Ищем команду
        for pattern, func in active_registry.items():
            match = pattern.match(msg_clean)
            if match:
                handler_func = func
                extracted_args = match.groups() 
                break
        
        if handler_func:
            try:
                final_message, charts = handler_func(req.chat_id, req.cols_to_remove, *extracted_args)
                charts = serialize(charts)
            except Exception as e:
                logger.error(f"Ошибка мок-вычисления: {str(e)}")
                final_message = f"Ошибка вычисления: {str(e)}"
                charts = []
        else:
            final_message = "Я не знаю такой команды в рамках мок-режима."
            charts = []
            
        # Сохраняем в историю
        app_graph.update_state(config, {
            "messages": [
                HumanMessage(content=user_message),
                AIMessage(content=final_message)
            ],
            "charts_payload": charts
        })
        
        return ChatResponse(reply=final_message, charts=charts)

    # --- ОБРАБОТКА AI-РЕЖИМА ---
    # Граф сам вызовет Супервизора, а затем нужного Агента
    inputs = {"messages": [HumanMessage(content=user_message)]}
    final_state = await app_graph.ainvoke(inputs, config=config)
    
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
    da_commands = [cmd.value for cmd in DAMockCommands]
    fin_commands = [cmd.value for cmd in FinMockCommands]
    return {"commands": da_commands + fin_commands}