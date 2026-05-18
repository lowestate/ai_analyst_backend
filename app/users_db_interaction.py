import asyncpg
from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel

from app.config import logger

class DBCredentials(BaseModel):
    host: str
    port: str
    database: str
    user: str
    password: str


async def get_user_plan(user_id: int) -> tuple[int, str, str]:
    """Возвращает plan_id, plan_name и role пользователя из базы данных."""
    from app.database import pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT u.plan_id, p.plan_name, u.role
                    FROM users u
                    LEFT JOIN plans p ON u.plan_id = p.plan_id
                    WHERE u.user_id = %s
                    """, (user_id,)
                )
                row = await cur.fetchone()
                if row and row[0] is not None:
                    return row[0], row[1] or "free", row[2] or "user"
                return 1, "free", "user"  # По умолчанию free, user
    except Exception as e:
        logger.error(f"Ошибка при получении плана пользователя user_id={user_id}: {str(e)}")
        return 1, "free", "user"

async def get_llm_requests_data(limit: int, offset: int, sort_col: str, sort_order: str, filter_col: str = None, filter_val: str = None) -> tuple[list[dict], int]:
    """Возвращает данные таблицы llm_requests для админ-панели."""
    from app.database import pool
    
    # Защита от SQL-инъекций через белые списки
    allowed_cols = ["request_id", "user_id", "chat_id", "input_text", "input_tokens", "output_tokens", "request_status", "model_name", "created_at", "duration_ms", "initiator", "error_message"]
    
    if sort_col not in allowed_cols:
        sort_col = "created_at"
    
    if sort_order.upper() not in ["ASC", "DESC"]:
        sort_order = "DESC"
        
    where_clause = ""
    params = []
    
    if filter_col and filter_col in allowed_cols and filter_val:
        where_clause = f"WHERE CAST({filter_col} AS TEXT) ILIKE %s"
        params.append(f"%{filter_val}%")
        
    query = f"SELECT * FROM llm_requests {where_clause} ORDER BY {sort_col} {sort_order} LIMIT %s OFFSET %s"
    count_query = f"SELECT COUNT(*) FROM llm_requests {where_clause}"
    
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                # Получаем общее количество
                await cur.execute(count_query, params)
                count_row = await cur.fetchone()
                total_count = count_row[0] if count_row else 0
                
                # Получаем данные
                data_params = params + [limit, offset]
                await cur.execute(query, data_params)
                
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = await cur.fetchall()
                data = [dict(zip(columns, row)) for row in rows] if rows else []
                
                return data, total_count
    except Exception as e:
        logger.error(f"Ошибка при получении llm_requests: {str(e)}")
        return [], 0

async def check_user_rate_limit(user_id: int) -> bool:
    """
    Возвращает True, если лимит не превышен, и False, если превышен (>= 5 запросов в минуту).
    """
    from app.database import pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT COUNT(*) FROM llm_requests 
                    WHERE user_id = %s 
                      AND initiator = 'user' 
                      AND created_at >= NOW() - INTERVAL '1 minute'
                    """,
                    (user_id,)
                )
                row = await cur.fetchone()
                count = row[0] if row else 0
                return count < 5
    except Exception as e:
        logger.error(f"Ошибка при проверке лимитов запросов user_id={user_id}: {str(e)}")
        return True

async def extract_schema_from_db(creds: DBCredentials) -> Dict[str, Any]:
    logger.info(f"Начало выполнения функции extract_schema_from_db для базы данных: {creds.database}")
    
    try:
        # Устанавливаем подключение
        conn = await asyncpg.connect(
            user=creds.user,
            password=creds.password,
            database=creds.database,
            host=creds.host,
            port=creds.port
        )
        logger.info("Успешное подключение к базе данных в функции extract_schema_from_db")
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных в функции extract_schema_from_db: {str(e)}")
        raise

    try:
        # 1. Получаем Foreign Keys (Внешние ключи и связи между таблицами)
        logger.info("Выполнение запроса на получение внешних ключей (fk_query)")
        fk_query = """
            SELECT
                tc.table_name AS source_table,
                kcu.column_name AS source_column,
                ccu.table_name AS target_table
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';
        """
        fk_records = await conn.fetch(fk_query)
        logger.info(f"Успешно получено {len(fk_records)} записей внешних ключей (fk_records)")
        
        relations = []
        fk_set = set() # Сет для быстрого поиска: (table_name, column_name)
        
        for row in fk_records:
            fk_set.add((row["source_table"], row["source_column"]))
            # Добавляем связь для линий в графе
            rel = {"sourceTable": row["source_table"], "targetTable": row["target_table"]}
            if rel not in relations: # Избегаем дубликатов линий (например, при составных ключах)
                relations.append(rel)

        # 2. Получаем Primary Keys (Первичные ключи)
        logger.info("Выполнение запроса на получение первичных ключей (pk_query)")
        pk_query = """
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tco.constraint_name AND kcu.constraint_schema = tco.constraint_schema
            WHERE tco.constraint_type = 'PRIMARY KEY' AND tco.table_schema = 'public';
        """
        pk_records = await conn.fetch(pk_query)
        pk_set = set((row["table_name"], row["column_name"]) for row in pk_records)
        logger.info(f"Успешно получено {len(pk_records)} записей первичных ключей (pk_records)")

        # 3. Получаем все таблицы и их столбцы
        logger.info("Выполнение запроса на получение всех таблиц и столбцов (cols_query)")
        cols_query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """
        cols_records = await conn.fetch(cols_query)
        logger.info(f"Успешно получено {len(cols_records)} записей информации о столбцах (cols_records)")

        tables_dict = {}
        for row in cols_records:
            t_name = row["table_name"]
            c_name = row["column_name"]
            
            if t_name not in tables_dict:
                tables_dict[t_name] = {"name": t_name, "columns": []}
                
            col_info = {
                "name": c_name,
                "type": row["data_type"]
            }
            
            # Развешиваем бейджики PK и FK
            if (t_name, c_name) in pk_set:
                col_info["isPk"] = True
            if (t_name, c_name) in fk_set:
                col_info["isFk"] = True
                
            tables_dict[t_name]["columns"].append(col_info)

        result = {
            "tables": list(tables_dict.values()),
            "relations": relations
        }
        logger.info(f"Успешное завершение extract_schema_from_db. Сформирована схема: таблиц - {len(result['tables'])}, связей - {len(result['relations'])}")
        return result

    except Exception as e:
        logger.error(f"Ошибка во время извлечения схемы базы данных в функции extract_schema_from_db: {str(e)}")
        raise

    finally:
        # Всегда закрываем соединение!
        await conn.close()
        logger.info("Соединение с базой данных закрыто в блоке finally функции extract_schema_from_db")


async def execute_sql_query(creds_dict: dict, query: str) -> list[dict]:
    """Выполняет подтвержденный SQL запрос к БД. Разрешены только SELECT."""
    logger.info(f"Начало выполнения функции execute_sql_query. Длина запроса query: {len(query)} символов")
    
    if not query.strip().upper().startswith("SELECT"):
        error_msg = "Разрешены только SELECT запросы в целях безопасности."
        logger.warning(f"Заблокирована попытка выполнения небезопасного запроса в execute_sql_query. Запрос: {query}. Ошибка: {error_msg}")
        raise ValueError(error_msg)
        
    try:
        conn = await asyncpg.connect(
            user=creds_dict["user"],
            password=creds_dict["password"],
            database=creds_dict["database"],
            host=creds_dict["host"],
            port=creds_dict["port"]
        )
        logger.info("Успешное подключение к базе данных в функции execute_sql_query")
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных в функции execute_sql_query: {str(e)}")
        raise
        
    try:
        # Выполняем запрос
        logger.info("Выполнение SQL запроса (query)")
        records = await conn.fetch(query)
        
        # Преобразуем записи в список словарей
        result = [dict(r) for r in records]
        logger.info(f"Успешное выполнение execute_sql_query. Получено {len(result)} строк результата (result)")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении SQL запроса в функции execute_sql_query: {str(e)}. Проблемный запрос: {query}")
        raise
        
    finally:
        await conn.close()
        logger.info("Соединение с базой данных закрыто в блоке finally функции execute_sql_query")


async def log_llm_request(
    request_id: Optional[str],
    user_id: Optional[int],
    chat_id: Optional[str],
    request_text: str,
    input_tokens: Optional[int],
    output_tokens: Optional[int],
    request_status: int,
    model: Optional[str],
    created_at: datetime,
    duration_ms: int,
    initiator: str,
    error_msg: Optional[str] = None,
) -> None:
    """Записывает метаданные LLM-запроса в таблицу llm_requests."""
    from app.database import pool  # Отложенный импорт во избежание циклов

    query = """
        INSERT INTO llm_requests (
            request_id, user_id, chat_id, input_text,
            input_tokens, output_tokens, request_status,
            model_name, created_at, duration_ms, initiator, error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (request_id) DO NOTHING
    """
    try:
        async with pool.connection() as conn:
            await conn.execute(
                query,
                (
                    request_id,
                    user_id,
                    chat_id,
                    request_text[:4000] if request_text else None,
                    input_tokens,
                    output_tokens,
                    request_status,
                    model,
                    created_at,
                    duration_ms,
                    initiator,
                    error_msg,
                ),
            )
        logger.info(f"log_llm_request записан request_id={request_id} initiator={initiator} duration_ms={duration_ms}")
    except Exception as e:
        logger.error(f"log_llm_request ошибка записи request_id={request_id}: {e}")


async def get_users_data(
    limit: int,
    offset: int,
    sort_col: str,
    sort_order: str,
    filter_col: Optional[str] = None,
    filter_val: Optional[str] = None
) -> tuple[list[dict], int]:
    """Возвращает данные таблицы users (без паролей) для админ-панели."""
    from app.database import pool
    
    # Разрешенные колонки для сортировки/фильтрации
    allowed_cols = ["user_id", "username", "plan_id", "is_active", "role"]
    
    if sort_col not in allowed_cols:
        sort_col = "user_id"
        
    if sort_order.upper() not in ["ASC", "DESC"]:
        sort_order = "ASC"
        
    where_clause = ""
    params = []
    
    if filter_col and filter_col in allowed_cols and filter_val:
        # Для булевой колонки is_active приводим к тексту, чтобы работал ILIKE
        where_clause = f"WHERE CAST({filter_col} AS TEXT) ILIKE %s"
        params.append(f"%{filter_val}%")
        
    query = f"SELECT user_id, username, plan_id, is_active, role FROM users {where_clause} ORDER BY {sort_col} {sort_order} LIMIT %s OFFSET %s"
    count_query = f"SELECT COUNT(*) FROM users {where_clause}"
    
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                # Получаем общее количество
                await cur.execute(count_query, params)
                count_row = await cur.fetchone()
                total_count = count_row[0] if count_row else 0
                
                # Получаем данные
                data_params = params + [limit, offset]
                await cur.execute(query, data_params)
                
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = await cur.fetchall()
                data = [dict(zip(columns, row)) for row in rows] if rows else []
                
                return data, total_count
    except Exception as e:
        logger.error(f"Ошибка при получении пользователей: {str(e)}")
        return [], 0


async def get_plans_list() -> list[dict]:
    """Возвращает список всех тарифных планов (id и название) из таблицы plans."""
    from app.database import pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT plan_id, plan_name FROM plans ORDER BY plan_id ASC")
                rows = await cur.fetchall()
                if rows:
                    return [{"plan_id": r[0], "plan_name": r[1]} for r in rows]
                return []
    except Exception as e:
        logger.error(f"Ошибка при получении списка планов: {str(e)}")
        return []


async def update_user_admin(target_user_id: int, role: str, plan_id: int, is_active: bool) -> bool:
    """Обновляет роль, план и статус активности пользователя (запрос от администратора)."""
    from app.database import pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                # Проверяем, существует ли план
                await cur.execute("SELECT plan_id FROM plans WHERE plan_id = %s", (plan_id,))
                if not await cur.fetchone():
                    logger.warning(f"План plan_id={plan_id} не найден при обновлении юзера")
                    return False
                    
                # Обновляем роль, план и статус активности
                await cur.execute(
                    "UPDATE users SET role = %s, plan_id = %s, is_active = %s WHERE user_id = %s RETURNING user_id",
                    (role, plan_id, is_active, target_user_id)
                )
                row = await cur.fetchone()
                return row is not None
    except Exception as e:
        logger.error(f"Ошибка при обновлении пользователя target_user_id={target_user_id}: {str(e)}")
        return False