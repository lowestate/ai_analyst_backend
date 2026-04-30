import pandas as pd
from io import StringIO

from app.db.database import redis_client

def remove_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает датафрейм от колонок с датами (явных и скрытых в строках)."""
    df_filtered = df.copy()
    
    # 1. Удаляем колонки с явным типом datetime
    datetime_cols = df_filtered.select_dtypes(include=['datetime', 'datetimetz']).columns
    df_filtered = df_filtered.drop(columns=datetime_cols)
    
    # 2. Ищем "скрытые" даты в текстовых колонках
    object_cols = df_filtered.select_dtypes(include=['object', 'string']).columns
    cols_to_drop = []
    
    for col in object_cols:
        non_null_series = df_filtered[col].dropna()
        if len(non_null_series) == 0:
            continue
            
        # Защита: проверяем, не являются ли данные числами в виде строк
        # (чтобы '123' не интерпретировалось как миллисекунды даты)
        sample = non_null_series.sample(min(100, len(non_null_series)))
        if pd.to_numeric(sample, errors='coerce').notna().mean() > 0.8:
            continue
            
        # Пытаемся конвертировать сэмпл в даты
        # Если более 80% значений распознались как дата — считаем колонку датой
        converted = pd.to_datetime(sample, errors='coerce')
        if converted.notna().mean() > 0.8:
            cols_to_drop.append(col)
            
    return df_filtered.drop(columns=cols_to_drop)

def get_df_from_redis(chat_id: str) -> pd.DataFrame:
    """Вспомогательная функция для извлечения датасета из Redis"""
    data_str = redis_client.get(f"dataset:{chat_id}")
    if not data_str:
        raise ValueError("Данные устарели или не найдены в кэше.")
    return pd.read_json(StringIO(data_str), orient='split')
