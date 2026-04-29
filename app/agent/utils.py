import numpy as np
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

def get_column_stats_data(chat_id: str) -> dict:
    df = get_df_from_redis(chat_id)
    numeric_cols = df.select_dtypes(include=['number']).columns
    cat_cols = df.select_dtypes(exclude=['number']).columns
    
    numeric_stats = df[numeric_cols].describe().round(2).to_dict() if len(numeric_cols) > 0 else {}
    
    categorical_charts = {}
    categorical_uniform = {}
    
    for col in cat_cols:
        counts = df[col].value_counts()
        num_categories = len(counts)
        if num_categories == 0: continue
        min_c, max_c = counts.min(), counts.max()
        if (max_c - min_c) <= max(2, max_c * 0.05):
            categorical_uniform[col] = {"unique_count": num_categories, "approx_val": int(counts.mean())}
        else:
            if num_categories > 6:
                top_n = counts.iloc[:5]
                other_sum = counts.iloc[5:].sum()
                stats = top_n.to_dict()
                stats["Другие"] = int(other_sum)
            else:
                stats = counts.to_dict()
            categorical_charts[col] = stats
            
    # НОВОЕ: Считаем распределение (гистограммы) для числовых данных
    numeric_charts = {}
    for col in numeric_cols:
        vals = df[col].dropna()
        if len(vals) > 0:
            counts, bin_edges = np.histogram(vals, bins='auto')
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2 # Центры столбцов для графика
            numeric_charts[col] = {
                "x": np.round(bin_centers, 2).tolist(),
                "y": counts.tolist()
            }
            
    return {
        "numeric": numeric_stats,
        "categorical_charts": categorical_charts,
        "categorical_uniform": categorical_uniform,
        "numeric_charts": numeric_charts # <-- Передаем новые графики
    }

def get_outliers_data(chat_id: str) -> dict:
    df = get_df_from_redis(chat_id)
    numeric_cols = df.select_dtypes(include=['number']).columns
    
    stats = {}
    charts = []
    
    for col in numeric_cols:
        s = df[col].dropna()
        if len(s) == 0: continue
            
        q1 = s.quantile(0.25)
        q3 = s.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = s[(s < lower_bound) | (s > upper_bound)]
        outliers_count = len(outliers)
        
        stats[col] = {
            "total": len(s),
            "outliers_count": outliers_count,
            "outliers_percent": round((outliers_count / len(s)) * 100, 2),
            "lower": round(lower_bound, 2),
            "upper": round(upper_bound, 2)
        }
        
        if outliers_count > 0:
            charts.append({
                "type": "outliers",
                "data": {
                    "column_name": col,
                    "y": s.tolist() # Отправляем массив для Boxplot
                }
            })
            
    return {"stats": stats, "charts": charts}

def get_cross_dependencies_data(chat_id: str) -> dict:
    df = get_df_from_redis(chat_id)
    cols = df.columns
    
    # Хак для смешанных данных: факторизуем категории в числа и считаем ранговую корреляцию
    df_numeric = pd.DataFrame()
    for c in cols:
        if df[c].dtype == 'object' or df[c].dtype.name == 'category':
            df_numeric[c] = df[c].factorize()[0]
        else:
            df_numeric[c] = df[c]
            
    # Спирмен отлично ловит нелинейные и категориальные ассоциации
    corr = df_numeric.corr(method='spearman').abs().fillna(0).round(2)
    
    x_vals, y_vals, scores = [], [], []
    for c1 in corr.columns:
        for c2 in corr.columns:
            x_vals.append(c1)
            y_vals.append(c2)
            scores.append(float(corr.loc[c1, c2]))
            
    return {
        "x": x_vals, "y": y_vals, "scores": scores, "matrix": corr.to_dict()
    }

def get_trend_data(chat_id: str) -> dict:
    df = get_df_from_redis(chat_id).copy()

    # --- 1. ПОИСК КОЛОНКИ С ДАТОЙ ---
    date_col = None
    dt_cols = df.select_dtypes(include=['datetime', 'datetimetz']).columns
    
    if len(dt_cols) > 0:
        date_col = dt_cols[0]
    else:
        keywords = ['date', 'time', 'дата', 'день', 'месяц', 'год', 'period']
        for col in df.columns:
            if any(kw in str(col).lower() for kw in keywords):
                date_col = col
                break
        
        if not date_col:
            date_col = df.columns[0]
            
    # --- 2. БРОНЕБОЙНАЯ КОНВЕРТАЦИЯ ---
    # Проверяем, не является ли колонка УЖЕ форматом времени
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        # Пытаемся перевести все строки в числа (например, '41470.166' станет float)
        numeric_vals = pd.to_numeric(df[date_col], errors='coerce')
        
        # Находим те ячейки, которые попадают в диапазон дат Excel (примерно 1927 - 2173 года)
        is_excel = (numeric_vals > 10000) & (numeric_vals < 100000)
        
        # Создаем базовую маску: пытаемся распарсить как обычный текст/timestamp
        parsed_dates = pd.to_datetime(df[date_col].astype(str), errors='coerce')
        
        # Точечно (векторизованно) перезаписываем те ячейки, которые оказались Excel-числами
        if is_excel.any():
            parsed_dates.loc[is_excel] = pd.to_datetime(numeric_vals[is_excel], unit='D', origin='1899-12-30')
            
        df[date_col] = parsed_dates

    # --- 3. ОЧИСТКА И ФОРМАТИРОВАНИЕ ---
    # Выкидываем пустые значения (NaT), из-за которых график может упасть
    df = df.dropna(subset=[date_col])
    df = df.sort_values(by=date_col)

    numeric_cols = [c for c in df.select_dtypes(include=['number']).columns if c != date_col]

    if not numeric_cols:
        raise ValueError("В датасете нет числовых признаков для построения трендов.")

    # Форматируем в идеальную строку. 
    # Фронтенд (календарики От и До) ожидает строгий формат, поэтому отдаем ему четкие данные.
    x_data = df[date_col].dt.strftime('%Y-%m-%d %H:%M:%S').tolist() 
    y_data = {col: df[col].fillna(0).tolist() for col in numeric_cols}

    return {
        "date_col": date_col,
        "numeric_cols": numeric_cols,
        "x": x_data,
        "y": y_data
    }