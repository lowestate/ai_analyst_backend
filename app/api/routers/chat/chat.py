import json
import uuid
from typing import Any
from fastapi import (
    APIRouter,
    HTTPException,
    UploadFile,
    File,
    Depends,
    Form
)
from langchain_core.messages import HumanMessage, AIMessage
from fastapi import APIRouter
import pickle

from app.database import pool
from app.api.dependencies import get_app_graph
from app.users_db_interaction import DBCredentials, extract_schema_from_db, get_user_plan, check_user_rate_limit
from app.api.routers.chat.models import (
    ChatCreateResponse,
    ChatRequest,
    ChatResponse,
    LoadChatResponse,
)
from app.config import logger
from app.agents.core.initial_invoke import generate_initial_metadata
from app.agents.core.utils import process_upload, serialize
from app.agents.data_analyst.mock.mock_handlers import DA_MOCK_REGISTRY, DAMockCommands
from app.agents.finance_agent.mock.mock_handlers import FIN_MOCK_REGISTRY, FinMockCommands
from app.agents.supervisor.mock_router import route_mock_request

router = APIRouter(tags=["Chat"])

@router.post("/upload", response_model=ChatCreateResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    user_id: int = Form(...),
    graph: Any = Depends(get_app_graph)
):
    logger.info(f"upload_dataset user_id={user_id} filename={file.filename}")

    if file.filename and file.filename.endswith(".json"):
        logger.info(f"Обработка JSON подключения filename={file.filename}")

        try:
            content = await file.read()
            payload = json.loads(content)
            logger.info("JSON файл успешно прочитан")

        except Exception as e:
            logger.error(f"Ошибка чтения JSON файла: {str(e)}", exc_info=True)
            raise HTTPException(status_code=400, detail=f"Ошибка JSON файла: {str(e)}")

        if payload.get("type") == "postgresql":
            creds = DBCredentials(**payload["credentials"])

            logger.info(f"Подключение к PostgreSQL database={creds.database}")

            try:
                db_schema = await extract_schema_from_db(creds)

                logger.info(f"Схема БД получена tables={len(db_schema['tables'])}")

                all_columns = []

                for table in db_schema["tables"]:
                    logger.info(f"Обработка таблицы table={table['name']}")

                    all_columns.extend([c["name"] for c in table["columns"]])

                unique_suffix = uuid.uuid4().hex[:8]
                chat_id = f"db_chat_{creds.database}_{unique_suffix}"

                logger.info(f"Создан DB chat_id={chat_id}")

                db_summary = f"База данных PostgreSQL: {creds.database}"
                init_msg = f"Успешное подключение к базе данных `{creds.database}`. Найдено {len(db_schema['tables'])} таблиц. Я готов писать SQL-запросы для проведения финансового анализа."

                async with pool.connection() as conn:
                    await conn.execute(
                        """INSERT INTO chats (chat_id, user_id, chat_desc, filename, dataset_encoded) 
                           VALUES (%s, %s, %s, %s, %s)""",
                        (chat_id, user_id, db_summary, file.filename, None)
                    )

                logger.info(f"DB чат сохранен chat_id={chat_id}")

                config = {"configurable": {"thread_id": chat_id, "chat_id": chat_id}}

                initial_messages = [
                    HumanMessage(content=f"Подключена база данных {creds.database}. Используй Text-to-SQL для получения данных."),
                    AIMessage(content=init_msg)
                ]

                await graph.aupdate_state(config, {
                    "messages": initial_messages,
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "charts_payload": [],
                    "all_charts": [],
                    "data_source": "db",
                    "db_schema": db_schema,
                    "db_credentials": creds.model_dump(),
                    "waiting_for_sql_approval": False,
                    "data_sample": []
                }, as_node="__start__")

                logger.info(f"Состояние графа обновлено chat_id={chat_id}")

                return ChatCreateResponse(
                    chat_id=chat_id,
                    preprocessing_report=init_msg,
                    dataset_summary=db_summary,
                    columns=list(set(all_columns)),
                    db_schema=db_schema
                )

            except Exception as e:
                logger.error(f"Ошибка подключения к БД database={creds.database}: {str(e)}", exc_info=True)
                raise HTTPException(status_code=400, detail=f"Ошибка подключения к БД: {str(e)}")

    try:
        logger.info(f"Запуск process_upload filename={file.filename}")

        # 1. Читаем и чистим файл
        chat_id, filename, stats, columns, df_cleaned = process_upload(file.file, file.filename)

        logger.info(f"Файл обработан chat_id={chat_id} rows={len(df_cleaned)} cols={len(columns)}")

        # 2. Подготавливаем данные
        data_sample = df_cleaned.head(5).fillna("").to_dict(orient="records")
        dataset_bytes = pickle.dumps(df_cleaned)

        logger.info(f"Данные подготовлены chat_id={chat_id}")

    except Exception as e:
        logger.error(f"Ошибка обработки файла filename={file.filename}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Ошибка чтения файла: {str(e)}")

    try:
        logger.info(f"Генерация metadata chat_id={chat_id}")

        init_data = await generate_initial_metadata(filename, columns, stats)

        chat_title = init_data.chat_title
        init_msg = init_data.initial_message

        logger.info(f"Metadata сгенерированы chat_id={chat_id}")

    except Exception as e:
        logger.warning(f"Ошибка generate_initial_metadata chat_id={chat_id}: {str(e)}", exc_info=True)

        chat_title = f"Анализ {filename}"
        init_msg = f"Данные '{filename}' загружены и готовы к работе. (Генерация умного приветствия недоступна: ошибка сети)."

    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO chats (chat_id, user_id, chat_desc, filename, dataset_encoded) 
               VALUES (%s, %s, %s, %s, %s)""",
            (chat_id, user_id, chat_title, filename, dataset_bytes)
        )

    logger.info(f"Чат сохранен chat_id={chat_id}")

    config = {"configurable": {"thread_id": chat_id, "chat_id": chat_id}}

    initial_messages = [
        HumanMessage(content=f"Файл {filename} загружен для проведения анализа."),
        AIMessage(content=init_msg)
    ]

    await graph.aupdate_state(config, {
        "messages": initial_messages,
        "chat_id": chat_id,
        "user_id": user_id,
        "charts_payload": [],
        "all_charts": [],
        "data_sample": data_sample
    }, as_node="__start__")

    logger.info(f"Граф инициализирован chat_id={chat_id}")

    return ChatCreateResponse(
        chat_id=chat_id,
        preprocessing_report=init_msg,
        dataset_summary=chat_title,
        columns=columns,
        db_schema=None
    )


@router.post("/chat", response_model=ChatResponse)
async def chat_interaction(
    req: ChatRequest,
    graph: Any = Depends(get_app_graph)
):
    logger.info(f"chat_interaction chat_id={req.chat_id} user_id={req.user_id} use_ai={req.use_ai}")

    # 1. ПРОВЕРКА ДОСТУПА И ПОЛУЧЕНИЕ ДАННЫХ
    # Проверяем, существует ли чат и принадлежит ли он юзеру
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT user_id, dataset_encoded FROM chats WHERE chat_id = %s",
                (req.chat_id,)
            )
            row = await cur.fetchone()

    if not row:
        logger.warning(f"Чат не найден chat_id={req.chat_id}")
        raise HTTPException(status_code=404, detail="Чат не найден")

    owner_id, dataset_bytes = row

    # Проверка прав (Вася может писать только в свои чаты)
    if owner_id != req.user_id:
        logger.warning(f"Запрещен доступ chat_id={req.chat_id} user_id={req.user_id}")
        raise HTTPException(status_code=403, detail="Доступ запрещен")

    plan_id, _ = await get_user_plan(req.user_id)
    
    if req.use_ai:
        if plan_id == 1:
            raise HTTPException(status_code=403, detail="Использование AI недоступно на тарифе free")
        elif plan_id == 2:
            is_allowed = await check_user_rate_limit(req.user_id)
            if not is_allowed:
                raise HTTPException(status_code=429, detail="Превышен лимит запросов в минуту (5) для тарифа pro")

    config = {
        "configurable": {
            "thread_id": req.chat_id,
            "chat_id": req.chat_id
        }
    }

    # 2. Проверяем состояние графа (HITL)
    state_snap = await graph.aget_state(config)
    current_state = state_snap.values
    is_waiting = current_state.get("waiting_for_sql_approval", False)

    logger.info(f"Состояние графа получено chat_id={req.chat_id} waiting_for_sql={is_waiting}")

    # ==========================================
    # ВЕТКА A: Ответ на генерацию SQL (HITL)
    # ==========================================
    if is_waiting and req.sql_action:
        logger.info(f"SQL action={req.sql_action} chat_id={req.chat_id}")

        updates = {
            "sql_action": req.sql_action,
            "sql_feedback": req.sql_feedback if req.sql_action == "reject" else ""
        }

        if req.sql_action == "approve" and req.sql_query:
            logger.info(f"SQL query approved chat_id={req.chat_id}")
            updates["sql_query"] = req.sql_query

        final_state = await graph.ainvoke(updates, config=config)

        logger.info(f"SQL ветка завершена chat_id={req.chat_id}")

    # ==========================================
    # ВЕТКА B: Обычный новый запрос в чат
    # ==========================================
    else:
        # Сбрасываем графики перед новым ходом
        await graph.aupdate_state(config, {"charts_payload": []}, as_node="__start__")

        logger.info(f"charts_payload очищен chat_id={req.chat_id}")

        user_message = req.message

        # --- МОКОВЫЙ РЕЖИМ (Исправляем ошибку "Данные устарели") ---
        if not req.use_ai:
            logger.info(f"MOCK режим chat_id={req.chat_id}")

            # Если это чат по файлу, восстанавливаем DF из байтов
            df = None

            if dataset_bytes:
                try:
                    df = pickle.loads(dataset_bytes)
                    logger.info(f"Dataset восстановлен chat_id={req.chat_id}")

                except Exception as e:
                    logger.error(f"Ошибка десериализации chat_id={req.chat_id}: {str(e)}", exc_info=True)

            target_agent, msg_clean = route_mock_request(user_message)
            msg_clean = msg_clean.strip('.?!')

            logger.info(f"Mock routing target_agent={target_agent}")

            active_registry = FIN_MOCK_REGISTRY if target_agent == "finance_agent" else DA_MOCK_REGISTRY

            handler_func = None
            extracted_args = ()

            for pattern, func in active_registry.items():
                match = pattern.match(msg_clean)

                if match:
                    handler_func = func
                    extracted_args = match.groups()

                    logger.info(f"Найден mock handler={func.__name__}")

                    break

            if handler_func:
                try:
                    # ВАЖНО: Твои handler_func теперь должны уметь принимать df
                    # Либо мы меняем логику внутри них, чтобы они не лезли в кэш.
                    # Здесь я передаю df первым аргументом вместо chat_id, если ты обновишь функции.
                    # Если функции старые, передавай chat_id, но внутри них вызывай загрузку из БД.
                    final_message, charts = handler_func(df, req.cols_to_remove, *extracted_args)

                    logger.info(f"Mock handler выполнен handler={handler_func.__name__}")

                    charts = serialize(charts)

                    logger.info(f"Charts сериализованы count={len(charts)}")

                except Exception as e:
                    logger.error(f"Ошибка вычисления handler={handler_func.__name__}: {str(e)}", exc_info=True)

                    final_message = f"Ошибка вычисления: {str(e)}"
                    charts = []

            else:
                logger.warning(f"Mock handler не найден chat_id={req.chat_id}")

                final_message = "Я не знаю такой команды в рамках мок-режима. Воспользуйтесь коммандами"
                charts = []

            # Обновляем стейт графа (CustomAsyncPostgresSaver сам сохранит это в chat_checkpoints)
            current_all_charts = current_state.get("all_charts", [])
            final_state_values = {
                "messages": [
                    HumanMessage(content=msg_clean),
                    AIMessage(content=final_message, additional_kwargs={"charts": charts})
                ],
                "charts_payload": charts,
                "all_charts": current_all_charts + charts
            }

            await graph.aupdate_state(config, final_state_values, as_node="__start__")

            logger.info(f"Mock state обновлен chat_id={req.chat_id}")

            return ChatResponse(reply=final_message, charts=charts, is_waiting_for_sql=False)

        # --- AI РЕЖИМ ---
        inputs = {"messages": [HumanMessage(content=user_message)]}

        logger.info(f"Запуск AI graph chat_id={req.chat_id}")

        # LangGraph сам подтянет последний чекпоинт из БД через наш Custom Saver
        final_state = await graph.ainvoke(inputs, config=config)

        logger.info(f"AI graph завершен chat_id={req.chat_id}")

    # ==========================================
    # 3. Обработка финального состояния
    # ==========================================
    # Вытаскиваем значения из финального стейта (после ainvoke)
    # Если мы были в ветке А, final_state уже есть. В ветке B он тоже есть.
    state_values = final_state if isinstance(final_state, dict) else final_state.get("values", {})

    is_waiting_now = state_values.get("waiting_for_sql_approval", False)

    logger.info(f"Обработка final_state chat_id={req.chat_id} waiting_for_sql={is_waiting_now}")

    if is_waiting_now:
        logger.info(f"Ожидание SQL подтверждения chat_id={req.chat_id}")

        return ChatResponse(
            reply="Мне нужно выполнить SQL запрос для получения данных:",
            charts=[],
            sql_query=state_values.get("sql_query", ""),
            is_waiting_for_sql=True
        )

    # Получаем последнее сообщение
    messages = state_values.get("messages", [])
    charts_payload = state_values.get("charts_payload", [])

    # === ИНЪЕКЦИЯ ГРАФИКОВ В AIMESSAGE ===
    if charts_payload and messages and isinstance(messages[-1], AIMessage):
        last_msg = messages[-1]
        if "charts" not in last_msg.additional_kwargs:
            last_msg.additional_kwargs["charts"] = charts_payload
            try:
                await graph.aupdate_state(config, {"messages": [last_msg]}, as_node="__start__")
                logger.info(f"Графики привязаны к AIMessage id={last_msg.id}")
            except Exception as e:
                logger.error(f"Не удалось обновить сообщение с графиками: {e}")

    if not messages:
        logger.warning(f"Пустой messages state chat_id={req.chat_id}")
        final_message = "Нет ответа от агента."

    else:
        raw_content = messages[-1].content

        if isinstance(raw_content, list):
            # Используем .get("text", ""), чтобы избежать KeyError, если ключа нет
            final_message = "".join(b.get("text", "") for b in raw_content if isinstance(b, dict))

        else:
            final_message = str(raw_content)

    logger.info(f"Ответ подготовлен chat_id={req.chat_id}")

    return ChatResponse(
        reply=final_message or "Графики построены.",
        charts=state_values.get("charts_payload", []),
        is_waiting_for_sql=False
    )


@router.get("/sessions")
async def get_sessions(user_id: int):
    """Отдает список чатов только для конкретного пользователя"""
    logger.info(f"Получение списка чатов user_id={user_id}")

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT chat_id, chat_desc, filename FROM chats WHERE user_id = %s AND deleted = FALSE ORDER BY created_at ASC",
                (user_id,)
            )
            rows = await cur.fetchall()

    logger.info(f"Список чатов получен user_id={user_id} count={len(rows)}")

    return [{"id": r[0], "datasetName": r[1], "filename": r[2]} for r in rows]

@router.get("/chat/{chat_id}", response_model=LoadChatResponse)
async def get_chat_history(
    chat_id: str,
    user_id: int,
    graph: Any = Depends(get_app_graph)
):
    """Отдает историю сообщений и поднимает датасет из БД при открытии старого чата"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT user_id, dataset_encoded FROM chats WHERE chat_id = %s", (chat_id,))
            row = await cur.fetchone()
            
    # Защита: нельзя читать чужие чаты
    if not row or row[0] != user_id:
        raise HTTPException(status_code=404, detail="Чат не найден или доступ запрещен")

    dataset_bytes = row[1]
    
    # "Оживляем" датафрейм, чтобы взять 5 строк для превью
    data_sample = []
    if dataset_bytes:
        try:
            df = pickle.loads(dataset_bytes)
            data_sample = df.head(5).fillna("").to_dict(orient="records")
        except Exception as e:
            logger.error(f"Ошибка при десериализации data_sample: {e}")

    # Достаем сообщения и метаданные из LangGraph
    config = {"configurable": {"thread_id": chat_id, "chat_id": chat_id}}
    state_snap = await graph.aget_state(config)
    
    messages_formatted = []
    charts_payload = []
    is_waiting = False
    db_schema = None
    
    if state_snap and state_snap.values:
        state_vals = state_snap.values
        raw_messages = state_vals.get("messages", [])
        
        charts_payload = state_vals.get("all_charts", [])
        db_schema = state_vals.get("db_schema")
        is_waiting = state_vals.get("waiting_for_sql_approval", False)

        for i, m in enumerate(raw_messages):
            m_type = getattr(m, "type", "") if not isinstance(m, dict) else m.get("type", "")
            
            # 1. ОБРАБОТКА ИСТОРИЧЕСКИХ (ВЫПОЛНЕННЫХ) SQL ЗАПРОСОВ
            if m_type in ["tool", "function"] or m.__class__.__name__ in ["ToolMessage", "FunctionMessage"]:
                content = str(getattr(m, "content", "") if not isinstance(m, dict) else m.get("content", ""))
                
                # Если в ответе базы зашит наш SQL-запрос, достаем его и рисуем плашку
                if "[SQL_QUERY]" in content:
                    sql_part = content.split("[/SQL_QUERY]")[0].replace("[SQL_QUERY]", "").strip()
                    messages_formatted.append({
                        "id": str(id(m)) + "_sql", 
                        "sender": "agent", 
                        # ИСПРАВЛЕНИЕ: Добавляем сам текст SQL запроса и флаг статуса, 
                        # чтобы фронт покрасил его в зеленый и отрисовал текст
                        "text": f"Мне нужно выполнить SQL запрос:\n\n```sql\n{sql_part}\n```\n\n[STATUS: approve]",
                        "isSqlWaiting": False
                    })
                # Сам сырой JSON из базы на фронтенд не отправляем
                continue

            sender = "user" if m_type == "human" else "agent"
            text = getattr(m, "content", "") if not isinstance(m, dict) else m.get("content", "")
            
            if isinstance(text, list):
                # Безопасное извлечение текста
                text = "".join(b.get("text", "") for b in text if isinstance(b, dict))
                
            text = str(text).strip()

            # 2. ОБРАБОТКА ТЕКУЩЕГО ОЖИДАЮЩЕГО SQL ЗАПРОСА
            tool_calls = getattr(m, "tool_calls", []) if not isinstance(m, dict) else m.get("tool_calls", [])
            if sender == "agent" and tool_calls:
                is_current_waiting = is_waiting and (i == len(raw_messages) - 1)
                
                # Если мы сейчас стоим на паузе, берем свежий SQL напрямую из стейта
                if is_current_waiting:
                    actual_sql = state_vals.get("sql_query", "")
                    messages_formatted.append({
                        "id": str(id(m)), 
                        "sender": sender, 
                        "text": f"Мне нужно выполнить SQL запрос:\n\n```sql\n{actual_sql}\n```",
                        "isSqlWaiting": True
                    })
                # Пропускаем системный блок с tool_calls, чтобы он не висел пустым пузырем
                continue 

            # 3. ДОБАВЛЕНИЕ ОБЫЧНЫХ СООБЩЕНИЙ
            # Фильтруем пустые технические сообщения (например, пробросы агентов)
            if sender == "agent" and not text:
                continue

            # === ИСПРАВЛЕНИЕ: Скрываем системные промпты, вшитые под видом пользователя ===
            # Проверяем маркеры стартовых технических сообщений (как для БД, так и для файлов)
            if sender == "user":
                is_db_prompt = text.startswith("Подключена база данных") and "Используй Text-to-SQL" in text
                # Если у тебя есть аналогичное стартовое сообщение для файлов, можешь раскомментировать строку ниже
                # is_file_prompt = text.startswith("Загружен файл") or "Вот первые строки" in text
                
                if is_db_prompt: # or is_file_prompt:
                    continue

            add_kwargs = getattr(m, "additional_kwargs", {}) if not isinstance(m, dict) else m.get("additional_kwargs", {})
            msg_charts = add_kwargs.get("charts", []) if sender == "agent" else []

            messages_formatted.append({
                "id": str(id(m)), 
                "sender": sender, 
                "text": text,
                "charts": msg_charts
            })

    return {
        "chat_id": chat_id,
        "messages": messages_formatted,
        "data_sample": data_sample,
        "charts_payload": charts_payload,
        "db_schema": db_schema
    }


@router.get("/available_mock_commands")
async def get_available_mock_commands():
    """
    Возвращает список всех доступных моковых команд из Enum.
    """
    logger.info("Получение списка mock команд")

    da_commands = [cmd.value for cmd in DAMockCommands]
    fin_commands = [cmd.value for cmd in FinMockCommands]

    logger.info(f"Mock команды загружены count={len(da_commands) + len(fin_commands)}")

    return {"commands": da_commands + fin_commands}

@router.delete("/chat/{chat_id}")
async def delete_chat(chat_id: str, user_id: int):
    """Удаляет чат пользователя. Стейты LangGraph удалятся каскадом (ON DELETE CASCADE)"""
    logger.info(f"Удаление чата chat_id={chat_id} user_id={user_id}")

    async with pool.connection() as conn:
        # Проверяем, что чат принадлежит юзеру перед удалением
        await conn.execute(
            "DELETE FROM chats WHERE chat_id = %s AND user_id = %s",
            (chat_id, user_id)
        )

    logger.info(f"Чат удален chat_id={chat_id}")

    return {"status": "success", "message": "Чат удален"}
