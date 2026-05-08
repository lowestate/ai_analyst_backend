import asyncpg
from typing import Dict, Any
from pydantic import BaseModel


class DBCredentials(BaseModel):
    host: str
    port: str
    database: str
    user: str
    password: str


async def extract_schema_from_db(creds: DBCredentials) -> Dict[str, Any]:
    # Устанавливаем подключение
    conn = await asyncpg.connect(
        user=creds.user,
        password=creds.password,
        database=creds.database,
        host=creds.host,
        port=creds.port
    )
    
    try:
        # 1. Получаем Foreign Keys (Внешние ключи и связи между таблицами)
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
        
        relations = []
        fk_set = set() # Сет для быстрого поиска: (table_name, column_name)
        
        for row in fk_records:
            fk_set.add((row["source_table"], row["source_column"]))
            # Добавляем связь для линий в графе
            rel = {"sourceTable": row["source_table"], "targetTable": row["target_table"]}
            if rel not in relations: # Избегаем дубликатов линий (например, при составных ключах)
                relations.append(rel)

        # 2. Получаем Primary Keys (Первичные ключи)
        pk_query = """
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tco.constraint_name AND kcu.constraint_schema = tco.constraint_schema
            WHERE tco.constraint_type = 'PRIMARY KEY' AND tco.table_schema = 'public';
        """
        pk_records = await conn.fetch(pk_query)
        pk_set = set((row["table_name"], row["column_name"]) for row in pk_records)

        # 3. Получаем все таблицы и их столбцы
        cols_query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """
        cols_records = await conn.fetch(cols_query)

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

        return {
            "tables": list(tables_dict.values()),
            "relations": relations
        }

    finally:
        # Всегда закрываем соединение!
        await conn.close()

async def execute_sql_query(creds_dict: dict, query: str) -> list[dict]:
    """Выполняет подтвержденный SQL запрос к БД. Разрешены только SELECT."""
    if not query.strip().upper().startswith("SELECT"):
        raise ValueError("Разрешены только SELECT запросы в целях безопасности.")
        
    conn = await asyncpg.connect(
        user=creds_dict["user"],
        password=creds_dict["password"],
        database=creds_dict["database"],
        host=creds_dict["host"],
        port=creds_dict["port"]
    )
    
    try:
        # Выполняем запрос
        records = await conn.fetch(query)
        # Преобразуем записи в список словарей
        return [dict(r) for r in records]
    finally:
        await conn.close()