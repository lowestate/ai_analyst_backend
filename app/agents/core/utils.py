import math
import pickle
import pandas as pd
import numpy as np
from functools import wraps
import warnings
from datetime import datetime, date
import uuid
from dataclasses import dataclass
from langchain_core.messages import AIMessage

from app.database import pool
from app.config import logger

def process_upload(file_obj, filename: str) -> tuple[str, str, dict, list, pd.DataFrame]:
    logger.info(f"Начата обработка файла filename={filename}")

    file_ext = filename.split('.')[-1]
    chat_id = str(uuid.uuid4())

    logger.info(f"Создан chat_id={chat_id}")

    if file_ext in ['xlsx', 'xls']:
        df = pd.read_excel(file_obj)
        logger.info(f"Excel файл прочитан filename={filename}")

    else:
        df = pd.read_csv(file_obj)
        logger.info(f"CSV файл прочитан filename={filename}")

    initial_shape = df.shape

    logger.info(f"Исходный dataframe rows={initial_shape[0]} cols={initial_shape[1]}")

    cols_to_drop = []

    for col in df.columns:
        col_lower = str(col).lower()

        if col_lower == 'id' or 'unnamed:' in col_lower:
            cols_to_drop.append(col)

    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True)
        logger.info(f"Удалены служебные столбцы count={len(cols_to_drop)}")

    duplicates_count = int(df.duplicated().sum())

    logger.info(f"Найдено дубликатов count={duplicates_count}")

    df.drop_duplicates(inplace=True)
    logger.info("Дубликаты удалены")

    missing_count = int(df.isna().sum().sum())

    logger.info(f"Найдено пропусков count={missing_count}")

    df.ffill(inplace=True)
    logger.info("Выполнен ffill")

    df.bfill(inplace=True)
    logger.info("Выполнен bfill")

    final_shape = df.shape

    logger.info(f"Финальный dataframe rows={final_shape[0]} cols={final_shape[1]}")

    # redis_client.set(...) удален

    stats = {
        "initial_rows": initial_shape[0],
        "duplicates_removed": duplicates_count,
        "missing_values_filled": missing_count,
        "final_rows": final_shape[0],
        "final_columns": final_shape[1]
    }

    logger.info(f"Статистика обработки подготовлена chat_id={chat_id}")

    # ВАЖНО: Возвращаем очищенный df, чтобы эндпоинт /upload сохранял в БД именно его!
    return chat_id, filename, stats, list(df.columns), df

def serialize(obj):
    """
    Рекурсивно обходит объект и конвертирует все Numpy и Pandas типы
    в стандартные типы Python (int, float, list, str, None).
    """
    # Если это словарь - рекурсивно обрабатываем ключи и значения
    if isinstance(obj, dict):
        logger.info("serialize обработка dict")
        return {str(k): serialize(v) for k, v in obj.items()}

    # Если это список, кортеж или множество - рекурсивно обрабатываем элементы
    elif isinstance(obj, (list, tuple, set)):
        logger.info("serialize обработка iterable")
        return [serialize(v) for v in obj]

    # Если это Numpy массив - превращаем в список и прогоняем через сериализатор
    elif isinstance(obj, np.ndarray):
        logger.info("serialize обработка ndarray")
        return serialize(obj.tolist())

    # Если это скалярный тип Numpy (np.float64, np.int64, np.bool_ и т.д.)
    elif isinstance(obj, np.generic):
        item = obj.item()

        # Если это NaN (Not a Number), превращаем в None (null в JSON)
        if isinstance(item, float) and math.isnan(item):
            logger.warning("serialize обнаружен numpy NaN")
            return None

        return item

    # Если случайно прилетел DataFrame или Series
    elif isinstance(obj, pd.DataFrame):
        logger.warning("serialize получил DataFrame")
        return serialize(obj.to_dict(orient='records'))

    elif isinstance(obj, pd.Series):
        logger.warning("serialize получил Series")
        return serialize(obj.tolist())

    # Обработка дат
    elif isinstance(obj, (datetime, date)):
        logger.info("serialize обработка datetime")
        return obj.isoformat()

    # Обработка NaN из стандартного Python
    elif isinstance(obj, float) and math.isnan(obj):
        logger.warning("serialize обнаружен float NaN")
        return None

    # Обработка специфичных Pandas пропусков (pd.NA, NaT)
    elif pd.isna(obj):
        logger.warning("serialize обнаружен pandas NA")
        return None

    # Возвращаем стандартные типы (int, str, bool, обычный float) как есть
    return obj

def filter_dataframe(df: pd.DataFrame, cols_to_remove: list = None) -> pd.DataFrame:
    """Вспомогательная функция (замена get_df_from_redis): отсекает ненужные столбцы"""
    logger.info("Запуск filter_dataframe")

    if df is None or not isinstance(df, pd.DataFrame):
        logger.error("Некорректный dataframe в filter_dataframe")
        raise ValueError("Данные не загружены или имеют неверный формат.")

    df_filtered = df.copy()
    logger.info(f"Создана копия dataframe rows={len(df_filtered)} cols={len(df_filtered.columns)}")

    # ИСКЛЮЧАЕМ СТОЛБЦЫ: Удаляем только те, что реально есть в датафрейме
    if cols_to_remove:
        existing_cols = [c for c in cols_to_remove if c in df_filtered.columns]
        logger.info(f"Найдено столбцов для удаления count={len(existing_cols)}")

        if existing_cols:
            df_filtered = df_filtered.drop(columns=existing_cols)
            logger.info(f"Столбцы удалены count={len(existing_cols)}")

    return df_filtered

async def aget_df_from_db(chat_id: str) -> pd.DataFrame:
    """Асинхронно достает датасет из PostgreSQL и конвертирует обратно в DataFrame"""
    logger.info(f"Загрузка dataset из БД chat_id={chat_id}")

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT dataset_encoded FROM chats WHERE chat_id = %s",
                (chat_id,)
            )

            row = await cur.fetchone()
            logger.info(f"Запрос dataset выполнен chat_id={chat_id}")

    if not row or not row[0]:
        logger.error(f"Dataset не найден chat_id={chat_id}")
        raise ValueError("Данные не найдены в базе данных для этого чата.")

    df = pickle.loads(row[0])
    logger.info(f"Dataset десериализован chat_id={chat_id} rows={len(df)}")

    return df

@dataclass
class LLMRequestMetadata:
    request_id: str
    input_tokens: int
    output_tokens: int
    model_name: str

def get_llm_request_metadata(response: AIMessage) -> LLMRequestMetadata:
    request_id = response.id

    input_tokens = 0
    output_tokens = 0

    if hasattr(response, 'usage_metadata') and response.usage_metadata:
        input_tokens = response.usage_metadata.get('input_tokens', 0)
        output_tokens = response.usage_metadata.get('output_tokens', 0)
    elif hasattr(response, 'response_metadata') and response.response_metadata:
        token_usage = response.response_metadata.get('token_usage', {})
        input_tokens = token_usage.get('prompt_tokens', 0)
        output_tokens = token_usage.get('completion_tokens', 0)

    resp_meta = getattr(response, 'response_metadata', {}) or {}
    model_name = (
        resp_meta.get('model_name')
        or resp_meta.get('model_version')
        or resp_meta.get('model')
        or 'unknown'
    )

    return LLMRequestMetadata(
        request_id=request_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        model_name=model_name
    )

def remove_outliers_iqr(columns=None):
    """Декоратор: Удаляет выбросы по методу Тьюки (IQR)"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"remove_outliers_iqr func={func.__name__}")

            df = func(*args, **kwargs)
            logger.info(f"DataFrame получен func={func.__name__}")

            if not isinstance(df, pd.DataFrame):
                logger.error(f"Некорректный тип данных func={func.__name__}")
                raise TypeError("Декоратор remove_outliers_iqr ожидает функцию, возвращающую DataFrame")

            df_clean = df.copy()
            logger.info(f"Создана копия dataframe func={func.__name__}")

            cols_to_clean = columns if columns else df_clean.select_dtypes(include=['number']).columns
            logger.info(f"Столбцы для очистки count={len(cols_to_clean)}")

            for col in cols_to_clean:
                q1 = df_clean[col].quantile(0.25)
                q3 = df_clean[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                df_clean = df_clean[
                    (df_clean[col] >= lower_bound) &
                    (df_clean[col] <= upper_bound)
                ]

                logger.info(f"Выбросы удалены column={col}")

            return df_clean

        return wrapper

    return decorator

def remove_datetime_columns(func):
    """Декоратор: Очищает датафрейм от колонок с датами"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"remove_datetime_columns func={func.__name__}")

        df = func(*args, **kwargs)
        logger.info(f"DataFrame получен func={func.__name__}")

        if not isinstance(df, pd.DataFrame):
            logger.error(f"Некорректный тип данных func={func.__name__}")
            raise TypeError("Декоратор remove_datetime_columns ожидает функцию, возвращающую DataFrame")

        df_filtered = df.copy()
        logger.info(f"Создана копия dataframe func={func.__name__}")

        # 1. Удаляем колонки с явным типом datetime
        datetime_cols = df_filtered.select_dtypes(include=['datetime', 'datetimetz']).columns

        df_filtered = df_filtered.drop(columns=datetime_cols)
        logger.info(f"Удалены datetime столбцы count={len(datetime_cols)}")

        # 2. Ищем "скрытые" даты в текстовых колонках
        object_cols = df_filtered.select_dtypes(include=['object', 'string']).columns
        cols_to_drop = []

        logger.info(f"Проверка object столбцов count={len(object_cols)}")

        for col in object_cols:
            non_null_series = df_filtered[col].dropna()

            if len(non_null_series) == 0:
                logger.warning(f"Пустой столбец column={col}")
                continue

            sample = non_null_series.sample(min(100, len(non_null_series)))

            if pd.to_numeric(sample, errors='coerce').notna().mean() > 0.8:
                logger.info(f"Столбец пропущен как numeric column={col}")
                continue

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                converted = pd.to_datetime(
                    sample,
                    errors='coerce',
                    format='mixed'
                )

            if converted.notna().mean() > 0.8:
                cols_to_drop.append(col)
                logger.info(f"Обнаружен datetime столбец column={col}")

        result = df_filtered.drop(columns=cols_to_drop)
        logger.info(f"Удалены скрытые datetime столбцы count={len(cols_to_drop)}")

        return result

    return wrapper

@remove_outliers_iqr()
@remove_datetime_columns
def remove_outliers_and_dates(
    df: pd.DataFrame,
    cols_to_remove: list = []
) -> pd.DataFrame:
    logger.info("Запуск remove_outliers_and_dates")

    result = filter_dataframe(df, cols_to_remove)
    logger.info("remove_outliers_and_dates завершен")

    return result

@remove_datetime_columns
def remove_dates(
    df: pd.DataFrame,
    cols_to_remove: list = []
) -> pd.DataFrame:
    logger.info("Запуск remove_dates")

    result = filter_dataframe(df, cols_to_remove)
    logger.info("remove_dates завершен")

    return result

@remove_outliers_iqr()
def remove_outliers(
    df: pd.DataFrame,
    cols_to_remove: list = []
) -> pd.DataFrame:
    logger.info("Запуск remove_outliers")

    result = filter_dataframe(df, cols_to_remove)
    logger.info("remove_outliers завершен")

    return result