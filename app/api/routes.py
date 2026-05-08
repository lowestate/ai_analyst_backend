import json
import uuid
from fastapi import APIRouter, HTTPException, UploadFile, File
from langchain_core.messages import HumanMessage, AIMessage
from fastapi import APIRouter
import asyncpg

from app.users_db_interaction import DBCredentials, extract_schema_from_db
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
    if file.filename and file.filename.endswith(".json"):
        content = await file.read()
        payload = json.loads(content)
        
        if payload.get("type") == "postgresql":
            creds = DBCredentials(**payload["credentials"])
            try:
                db_schema = await extract_schema_from_db(creds)
                all_columns = []
                for table in db_schema["tables"]:
                    all_columns.extend([c["name"] for c in table["columns"]])
                
                unique_suffix = uuid.uuid4().hex[:8]
                chat_id = f"db_chat_{creds.database}_{unique_suffix}"
                
                db_summary = f"База данных PostgreSQL: {creds.database}"
                init_msg = f"Успешное подключение к базе данных `{creds.database}`. Найдено {len(db_schema['tables'])} таблиц. Я готов писать SQL-запросы для проведения финансового анализа."
                
                config = {"configurable": {"thread_id": chat_id, "chat_id": chat_id}}
                initial_messages = [
                    HumanMessage(content=f"Подключена база данных {creds.database}. Используй Text-to-SQL для получения данных."),
                    AIMessage(content=init_msg)
                ]
                
                app_graph.update_state(config, {
                    "messages": initial_messages, 
                    "chat_id": chat_id, 
                    "charts_payload": [],
                    "data_source": "db",
                    "db_schema": db_schema,
                    "db_credentials": creds.model_dump(),
                    "waiting_for_sql_approval": False
                })
                
                return ChatCreateResponse(
                    chat_id=chat_id, preprocessing_report=init_msg,
                    dataset_summary=db_summary, columns=list(set(all_columns)), db_schema=db_schema
                )
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Ошибка подключения к БД: {str(e)}")

    chat_id, filename, stats, columns = process_upload(file.file, file.filename)
    logger.info("process_upload DONE")
    init_data = await generate_initial_metadata(filename, columns, stats)
    
    config = {"configurable": {"thread_id": chat_id, "chat_id": chat_id}}
    
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
        columns=columns,
        db_schema=None
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
    
    # 1. Проверяем, ждет ли граф подтверждения SQL от пользователя
    current_state = app_graph.get_state(config).values
    is_waiting = current_state.get("waiting_for_sql_approval", False)
    
    # ==========================================
    # ВЕТКА A: Ответ на генерацию SQL (HITL)
    # ==========================================
    if is_waiting and req.sql_action:
        logger.info(f"Получен ответ по SQL: {req.sql_action}. Комментарий: {req.sql_feedback}")
        
        updates = {
            "sql_action": req.sql_action,
            "sql_feedback": req.sql_feedback if req.sql_action == "reject" else ""
        }
        
        if req.sql_action == "approve" and req.sql_query:
            updates["sql_query"] = req.sql_query
            
        # ИСПРАВЛЕНИЕ: Передаем updates напрямую в ainvoke. 
        # Это "пнет" граф, заставит его проснуться, обновить стейт 
        # и пойти в entry_router, который перенаправит поток в text_to_sql_execute.
        final_state = await app_graph.ainvoke(updates, config=config)

    # ==========================================
    # ВЕТКА B: Обычный новый запрос в чат
    # ==========================================
    else:
        app_graph.update_state(config, {"charts_payload": []})
        user_message = req.message
        
        # --- МОКОВЫЙ РЕЖИМ ---
        if not req.use_ai:
            logger.info("MOCK MODE: Перехват запроса чата")
            
            target_agent, msg_clean = route_mock_request(user_message)
            msg_clean = msg_clean.strip('.?!') 
            
            handler_func = None
            extracted_args = ()
            
            if target_agent == "data_analyst":
                active_registry = DA_MOCK_REGISTRY
            elif target_agent == "finance_agent":
                active_registry = FIN_MOCK_REGISTRY
            else:
                active_registry = DA_MOCK_REGISTRY
                
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
                
            app_graph.update_state(config, {
                "messages": [
                    HumanMessage(content=user_message),
                    AIMessage(content=final_message)
                ],
                "charts_payload": charts
            })
            
            # В мок-режиме SQL не используется
            return ChatResponse(reply=final_message, charts=charts, is_waiting_for_sql=False)

        # --- AI РЕЖИМ ---
        inputs = {"messages": [HumanMessage(content=user_message)]}
        final_state = await app_graph.ainvoke(inputs, config=config)

    # ==========================================
    # 2. Обработка финального состояния (после графа)
    # ==========================================
    is_waiting_now = final_state.get("waiting_for_sql_approval", False)
    
    if is_waiting_now:
        # Граф остановился, ждет аппрува от юзера
        sql_query = final_state.get("sql_query", "")
        return ChatResponse(
            reply="Чтобы получить данные для анализа, мне нужно выполнить SQL запрос:",
            charts=[],
            sql_query=sql_query,
            is_waiting_for_sql=True
        )
    else:
        # Граф отработал до конца (либо ответил на вопрос, либо выполнил SQL и вернул ответ)
        raw_content = final_state["messages"][-1].content
        
        if isinstance(raw_content, list):
            final_message = "".join(
                block["text"] for block in raw_content 
                if isinstance(block, dict) and "text" in block
            )
        else:
            final_message = str(raw_content)
            
        logger.info(f"\nfinal_message={final_message}\n")
        
        if not final_message.strip():
            logger.warning("LLM вернула пустой текст, применяем fallback-сообщение")
            final_message = "Графики успешно построены. Если вам нужна дополнительная текстовая интерпретация, дайте знать."

        charts = final_state.get("charts_payload", [])
        logger.info(f"\ncharts={charts}")
        
        return ChatResponse(
            reply=final_message, 
            charts=charts,
            sql_query=None,
            is_waiting_for_sql=False
        )

@router.get("/available_mock_commands")
async def get_available_mock_commands():
    """
    Возвращает список всех доступных моковых команд из Enum.
    """
    da_commands = [cmd.value for cmd in DAMockCommands]
    fin_commands = [cmd.value for cmd in FinMockCommands]
    return {"commands": da_commands + fin_commands}

@router.post("/test_connection")
async def test_db_connection(creds: DBCredentials):
    try:
        conn = await asyncpg.connect(
            user=creds.user,
            password=creds.password,
            database=creds.database,
            host=creds.host,
            port=creds.port,
            timeout=5.0
        )
        await conn.close()
        return {"status": "success"}
    except Exception as e:
        # Возвращаем текст ошибки на фронтенд
        return {"status": "error", "message": str(e)}