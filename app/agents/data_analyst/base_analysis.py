import numpy as np
import pandas as pd
from scipy.cluster import hierarchy
from scipy.spatial.distance import squareform

from app.agents.core.utils import (
    get_df_from_redis,
    remove_outliers_and_dates,
    remove_dates,
    remove_outliers
)

def get_correlation_data(chat_id: str, cols_to_remove: list[str] = []) -> dict:
    """Чистая бизнес-логика вычисления корреляции, независимая от LangGraph"""
    # Используем провайдер: получаем уже очищенный от дат и выбросов датафрейм
    df_encoded = remove_dates(chat_id, cols_to_remove)
    
    for col in df_encoded.select_dtypes(exclude=['number']).columns:
        df_encoded[col] = pd.factorize(df_encoded[col])[0]
        
    # fillna(0) страхует от колонок с нулевой дисперсией, где корреляция дает NaN
    return df_encoded.corr().fillna(0).round(3).to_dict()

def get_column_stats_data(chat_id: str, cols_to_remove: list[str] = []) -> dict:
    # Используем провайдер: получаем уже очищенный датафрейм
    df = remove_outliers_and_dates(chat_id, cols_to_remove)
    
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
            
    # Распределение (гистограммы) для числовых данных
    numeric_charts = {}
    for col in numeric_cols:
        vals = df[col].dropna()
        if len(vals) > 0:
            counts, bin_edges = np.histogram(vals, bins='auto')
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2 
            numeric_charts[col] = {
                "x": np.round(bin_centers, 2).tolist(),
                "y": counts.tolist()
            }
            
    return {
        "numeric": numeric_stats,
        "categorical_charts": categorical_charts,
        "categorical_uniform": categorical_uniform,
        "numeric_charts": numeric_charts
    }

def get_outliers_data(chat_id: str, cols_to_remove: list[str] = []) -> dict:
    # ЗДЕСЬ ОСТАВЛЯЕМ СЫРОЙ РЕДИС: если убрать аномалии, детекция найдет 0 аномалий
    df = get_df_from_redis(chat_id, cols_to_remove) 
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
                    "y": s.tolist() 
                }
            })
            
    return {"stats": stats, "charts": charts}

def get_trend_data(chat_id: str, cols_to_remove: list[str] = []) -> dict:
    # Используем провайдер: даты остаются, выбросы убираются
    df = remove_outliers(chat_id, cols_to_remove)
    
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
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        numeric_vals = pd.to_numeric(df[date_col], errors='coerce')
        is_excel = (numeric_vals > 10000) & (numeric_vals < 100000)
        parsed_dates = pd.to_datetime(df[date_col].astype(str), errors='coerce')
        
        if is_excel.any():
            parsed_dates.loc[is_excel] = pd.to_datetime(numeric_vals[is_excel], unit='D', origin='1899-12-30')
            
        df[date_col] = parsed_dates

    # --- 3. ОЧИСТКА И ФОРМАТИРОВАНИЕ ---
    # Ручной вызов remove_outliers_iqr отсюда убран, так как он отработал в remove_outliers
    df = df.dropna(subset=[date_col])
    df = df.sort_values(by=date_col)

    numeric_cols = [c for c in df.select_dtypes(include=['number']).columns if c != date_col]

    if not numeric_cols:
        raise ValueError("В датасете нет числовых признаков для построения трендов.")

    x_data = df[date_col].dt.strftime('%Y-%m-%d %H:%M:%S').tolist() 
    y_data = {col: df[col].fillna(0).tolist() for col in numeric_cols}

    return {
        "date_col": date_col,
        "numeric_cols": numeric_cols,
        "x": x_data,
        "y": y_data
    }

def get_dependency_data(chat_id: str, col1: str, col2: str, cols_to_remove: list[str] = []) -> dict:
    # Используем провайдер: убираем даты, чтобы они не мешали графикам рассеяния
    df = remove_dates(chat_id, cols_to_remove)
    
    col_map = {c.lower(): c for c in df.columns}
    
    real_col1 = col_map.get(col1.lower())
    real_col2 = col_map.get(col2.lower())

    if not real_col1 or not real_col2:
        raise ValueError(f"Колонки '{col1}' или '{col2}' не найдены в датасете.")

    df = df.dropna(subset=[col1, col2])

    def is_categorical(series):
        return pd.api.types.is_object_dtype(series) or series.nunique() < 15

    col1_is_cat = is_categorical(df[col1])
    col2_is_cat = is_categorical(df[col2])

    result = {"col1": col1, "col2": col2}

    if not col1_is_cat and not col2_is_cat:
        if len(df) > 2000:
            df = df.sample(2000, random_state=42)
        
        result["sub_type"] = "scatter"
        result["x"] = df[col2].tolist() 
        result["y"] = df[col1].tolist() 

    elif col1_is_cat and col2_is_cat:
        ct = pd.crosstab(df[col1], df[col2])
        result["sub_type"] = "heatmap"
        result["x"] = ct.columns.astype(str).tolist()
        result["y"] = ct.index.astype(str).tolist()
        result["z"] = ct.values.tolist()

    else:
        cat_col = col1 if col1_is_cat else col2
        num_col = col2 if col1_is_cat else col1

        top_cats = df[cat_col].value_counts().nlargest(12).index
        df = df[df[cat_col].isin(top_cats)]

        grouped = df.groupby(cat_col)[num_col].apply(list).to_dict()
        result["sub_type"] = "box"
        result["cat_col"] = cat_col
        result["num_col"] = num_col
        result["categories"] = list(grouped.keys())
        result["values"] = list(grouped.values())

    return result

def get_pairplot_data(chat_id: str, cols_to_remove: list[str] = []) -> dict:
    # ЗДЕСЬ ОСТАВЛЯЕМ СЫРОЙ РЕДИС: для Pairplot (матрицы рассеяния) выбросы важны визуально
    df = get_df_from_redis(chat_id, cols_to_remove).copy()
    
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    if len(num_cols) < 2:
        raise ValueError("Для матрицы рассеяния нужно минимум 2 числовых признака.")
        
    if len(num_cols) > 5:
        default_cols = df[num_cols].var().nlargest(5).index.tolist()
    else:
        default_cols = num_cols

    # ИСПРАВЛЕНИЕ 4: ВМЕСТО df.dropna(subset=num_cols), который удалял 
    # абсолютно все строки из-за одного пропуска, заполняем пропуски медианой!
    for col in num_cols:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].median())

    if len(df) > 500:
        df = df.sample(n=500, random_state=42)

    dimensions = []
    for col in num_cols:
        dimensions.append({
            "label": col,
            "values": df[col].tolist()
        })

    return {
        "dimensions": dimensions,
        "all_columns": num_cols,
        "default_columns": default_cols
    }

def get_all_relationships_data(chat_id: str, cols_to_remove: list[str] = []) -> list[dict]:
    """
    Агрегирует данные сразу для трех графиков взаимосвязей.
    """
    charts = []
    
    try:
        corr_data = get_correlation_data(chat_id, cols_to_remove)
        charts.append({"type": "correlation", "data": corr_data})
    except Exception as e:
        print(f"Ошибка при генерации correlation: {e}")

    try:
        feature_tree = get_feature_tree(chat_id, cols_to_remove)
        charts.append({"type": "feature_tree", "data": feature_tree})
    except Exception as e:
        print(f"Ошибка при генерации feature_tree: {e}")

    try:
        pairplot_data = get_pairplot_data(chat_id, cols_to_remove)
        charts.append({"type": "pairplot", "data": pairplot_data})
    except Exception as e:
        print(f"Ошибка при генерации pairplot: {e}")

    return charts

def get_feature_importances(chat_id: str, target_col: str, cols_to_remove: list[str] = []) -> dict:
    """Вычисляет важность признаков для указанной целевой переменной с помощью Random Forest."""
    # Используем провайдер очищенных данных (без дат, без пропусков)
    df = remove_dates(chat_id, cols_to_remove)
    
    col_map = {c.lower(): c for c in df.columns}
    real_target = col_map.get(target_col.lower())

    if not real_target:
        raise ValueError(f"Целевая колонка '{target_col}' не найдена в датасете.")

    # Очищаем датафрейм только от тех строк, где пустой САМ таргет (без него не обучить)
    df = df.dropna(subset=[real_target])
    
    y = df[real_target]
    X = df.drop(columns=[real_target])

    # УМНОЕ ЗАПОЛНЕНИЕ ПРОПУСКОВ (вместо dropna)
    # Числа заполняем медианой, категории - отдельным классом 'Unknown'
    for col in X.columns:
        if pd.api.types.is_numeric_dtype(X[col]):
            X[col] = X[col].fillna(X[col].median())
        else:
            X[col] = X[col].fillna('Unknown')

    if len(X) < 10:
        raise ValueError("Недостаточно данных для обучения модели (меньше 10 валидных строк).")

    # Факторизуем категориальные фичи
    for col in X.select_dtypes(include=['object', 'category', 'string']).columns:
        X[col] = pd.factorize(X[col])[0]

    # Определяем тип задачи
    is_numeric = pd.api.types.is_numeric_dtype(y)
    
    if is_numeric and y.nunique() > 5:
        from sklearn.ensemble import RandomForestRegressor
        model = RandomForestRegressor(n_estimators=50, random_state=42)
    else:
        from sklearn.ensemble import RandomForestClassifier
        if not is_numeric:
            y = pd.factorize(y)[0]
        model = RandomForestClassifier(n_estimators=50, random_state=42)

    # Обучаем модель
    model.fit(X, y)
    importances = model.feature_importances_

    imp_df = pd.DataFrame({
        'feature': X.columns,
        'importance': importances
    })

    imp_df = imp_df.sort_values(by='importance', ascending=False).head(7)
    imp_df = imp_df.iloc[::-1]

    return {
        "target": real_target,
        "features": imp_df['feature'].tolist(),
        "importances": np.round(imp_df['importance'], 4).tolist()
    }

def get_feature_tree(chat_id: str, cols_to_remove: list[str] = None) -> dict:
    """Строит иерархическое дерево признаков на основе корреляции"""
    # Берем данные без дат (с ними корреляция не работает)
    df = remove_dates(chat_id, cols_to_remove)
    
    # Факторизуем категориальные переменные (переводим в числа)
    for col in df.select_dtypes(exclude=['number']).columns:
        df[col] = pd.factorize(df[col])[0]
        
    # Заполняем пропуски медианой, чтобы не терять строки
    for col in df.columns:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].median())
            
    # Вычисляем матрицу корреляций
    corr = df.corr().fillna(0)
    
    # Превращаем корреляцию в "дистанцию" (1 - abs(corr)). 
    # Чем сильнее связь (неважно, прямая или обратная), тем ближе признаки.
    distances = 1 - corr.abs().values
    np.fill_diagonal(distances, 0)
    distances = np.clip(distances, 0, 1) # Защита от микроскопических погрешностей < 0
    
    # Сжимаем матрицу в 1D массив для scipy
    dist_array = squareform(distances)
    
    # Строим дерево методом Уорда (Ward variance minimization)
    Z = hierarchy.linkage(dist_array, method='ward')
    
    # Генерируем координаты для отрисовки графиков без самого рисования matplotlib (no_plot=True)
    dendro = hierarchy.dendrogram(Z, labels=corr.columns, no_plot=True)
    
    return {
        "icoord": dendro['icoord'], # X координаты линий
        "dcoord": dendro['dcoord'], # Y координаты линий
        "ivl": dendro['ivl'],       # Имена признаков (листьев) в правильном порядке
    }