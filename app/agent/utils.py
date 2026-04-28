import pandas as pd
from io import StringIO

from app.db.database import redis_client

def get_df_from_redis(chat_id: str) -> pd.DataFrame:
    """Вспомогательная функция для извлечения датасета из Redis"""
    data_str = redis_client.get(f"dataset:{chat_id}")
    if not data_str:
        raise ValueError("Данные устарели или не найдены в кэше.")
    return pd.read_json(StringIO(data_str), orient='split')

def get_correlation_data(chat_id: str) -> dict:
    """Чистая бизнес-логика вычисления корреляции, независимая от LangGraph"""
    df = get_df_from_redis(chat_id)
    df_encoded = df.copy()
    for col in df_encoded.select_dtypes(exclude=['number']).columns:
        df_encoded[col] = pd.factorize(df_encoded[col])[0]
    return df_encoded.corr().round(3).to_dict()
