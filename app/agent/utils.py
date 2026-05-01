import pandas as pd
from io import StringIO
from functools import wraps
import warnings

from app.db.database import redis_client

def remove_datetime_columns(func):
    """Декоратор: Очищает датафрейм от колонок с датами"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Декоратор remove_datetime_columns ожидает функцию, возвращающую DataFrame")
            
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
                
            sample = non_null_series.sample(min(100, len(non_null_series)))
            if pd.to_numeric(sample, errors='coerce').notna().mean() > 0.8:
                continue
                
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                # format='mixed' помогает в pandas 2.0+ работать быстрее без ошибок
                converted = pd.to_datetime(sample, errors='coerce', format='mixed')
                
            if converted.notna().mean() > 0.8:
                cols_to_drop.append(col)
                
        return df_filtered.drop(columns=cols_to_drop)
    return wrapper

def get_df_from_redis(chat_id: str, cols_to_remove: list = None) -> pd.DataFrame:
    """Вспомогательная функция для извлечения датасета из Redis"""
    data_str = redis_client.get(f"dataset:{chat_id}")
    if not data_str:
        raise ValueError("Данные устарели или не найдены в кэше.")
    
    df = pd.read_json(StringIO(data_str), orient='split')
    
    # ИСКЛЮЧАЕМ СТОЛБЦЫ: Удаляем только те, что реально есть в датафрейме
    if cols_to_remove:
        existing_cols = [c for c in cols_to_remove if c in df.columns]
        if existing_cols:
            df = df.drop(columns=existing_cols)
            
    return df

def remove_outliers_iqr(columns=None):
    """Декоратор: Удаляет выбросы по методу Тьюки (IQR)"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Получаем датафрейм из обернутой функции
            df = func(*args, **kwargs)
            
            if not isinstance(df, pd.DataFrame):
                raise TypeError("Декоратор remove_outliers_iqr ожидает функцию, возвращающую DataFrame")
                
            df_clean = df.copy()
            cols_to_clean = columns if columns else df_clean.select_dtypes(include=['number']).columns
            
            for col in cols_to_clean:
                q1 = df_clean[col].quantile(0.25)
                q3 = df_clean[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                df_clean = df_clean[(df_clean[col] >= lower_bound) & (df_clean[col] <= upper_bound)]
            
            return df_clean
        return wrapper
    return decorator

@remove_outliers_iqr()
@remove_datetime_columns
def remove_outliers_and_dates(chat_id: str, cols_to_remove: list = []) -> pd.DataFrame:
    return get_df_from_redis(chat_id, cols_to_remove)

@remove_datetime_columns
def remove_dates(chat_id: str, cols_to_remove: list = []) -> pd.DataFrame:
    return get_df_from_redis(chat_id, cols_to_remove)

@remove_outliers_iqr()
def remove_outliers(chat_id: str, cols_to_remove: list = []) -> pd.DataFrame:
    return get_df_from_redis(chat_id, cols_to_remove)