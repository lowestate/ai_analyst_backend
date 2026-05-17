import numpy as np
import pandas as pd
from scipy.cluster import hierarchy
from scipy.spatial.distance import squareform

from app.config import logger
from app.agents.core.utils import (
    filter_dataframe,
    remove_outliers_and_dates,
    remove_dates,
    remove_outliers
)

def get_correlation_data(df: pd.DataFrame, cols_to_remove: list[str] = []) -> dict:
    """Чистая бизнес-логика вычисления корреляции, независимая от LangGraph"""
    df_encoded = remove_dates(df, cols_to_remove)
    logger.info(f"Datetime колонки удалены correlation cols={len(df_encoded.columns)}")

    for col in df_encoded.select_dtypes(exclude=['number']).columns:
        df_encoded[col] = pd.factorize(df_encoded[col])[0]
    logger.info("Категориальные колонки факторизованы correlation")

    corr_result = df_encoded.corr().fillna(0).round(3).to_dict()
    logger.info(f"Correlation рассчитана cols={len(corr_result)}")

    return corr_result


def get_column_stats_data(df: pd.DataFrame, cols_to_remove: list[str] = []) -> dict:
    df = remove_outliers_and_dates(df, cols_to_remove)
    logger.info(f"DataFrame очищен stats shape={df.shape}")

    numeric_cols = df.select_dtypes(include=['number']).columns
    cat_cols = df.select_dtypes(exclude=['number']).columns
    logger.info(f"Колонки определены numeric={len(numeric_cols)} categorical={len(cat_cols)}")

    numeric_stats = df[numeric_cols].describe().round(2).to_dict() if len(numeric_cols) > 0 else {}
    logger.info(f"Numeric stats рассчитаны cols={len(numeric_stats)}")

    categorical_charts = {}
    categorical_uniform = {}

    for col in cat_cols:
        counts = df[col].value_counts()
        num_categories = len(counts)

        if num_categories == 0:
            continue

        min_c, max_c = counts.min(), counts.max()

        if (max_c - min_c) <= max(2, max_c * 0.05):
            categorical_uniform[col] = {
                "unique_count": num_categories,
                "approx_val": int(counts.mean())
            }
        else:
            if num_categories > 6:
                top_n = counts.iloc[:5]
                other_sum = counts.iloc[5:].sum()

                stats = top_n.to_dict()
                stats["Другие"] = int(other_sum)
            else:
                stats = counts.to_dict()

            categorical_charts[col] = stats

    logger.info(
        f"Categorical stats рассчитаны uniform={len(categorical_uniform)} charts={len(categorical_charts)}"
    )

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

    logger.info(f"Histogram данные рассчитаны charts={len(numeric_charts)}")

    return {
        "numeric": numeric_stats,
        "categorical_charts": categorical_charts,
        "categorical_uniform": categorical_uniform,
        "numeric_charts": numeric_charts
    }


def get_outliers_data(df: pd.DataFrame, cols_to_remove: list[str] = []) -> dict:
    df = filter_dataframe(df, cols_to_remove)
    logger.info(f"DataFrame отфильтрован outliers shape={df.shape}")

    numeric_cols = df.select_dtypes(include=['number']).columns
    logger.info(f"Numeric колонки получены outliers cols={len(numeric_cols)}")

    stats = {}
    charts = []

    for col in numeric_cols:
        s = df[col].dropna()

        if len(s) == 0:
            continue

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

    logger.info(f"Outliers рассчитаны stats={len(stats)} charts={len(charts)}")

    return {"stats": stats, "charts": charts}


def get_trend_data(df: pd.DataFrame, cols_to_remove: list[str] = []) -> dict:
    df = remove_outliers(df, cols_to_remove)
    logger.info(f"Outliers удалены trends shape={df.shape}")

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

    logger.info(f"Date колонка определена col={date_col}")

    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        numeric_vals = pd.to_numeric(df[date_col], errors='coerce')
        is_excel = (numeric_vals > 10000) & (numeric_vals < 100000)

        parsed_dates = pd.to_datetime(df[date_col].astype(str), errors='coerce')

        if is_excel.any():
            parsed_dates.loc[is_excel] = pd.to_datetime(
                numeric_vals[is_excel],
                unit='D',
                origin='1899-12-30'
            )

        df[date_col] = parsed_dates
        logger.info(f"Date колонка сконвертирована col={date_col}")

    df = df.dropna(subset=[date_col])
    df = df.sort_values(by=date_col)
    logger.info(f"Trend данные отсортированы rows={len(df)}")

    numeric_cols = [
        c for c in df.select_dtypes(include=['number']).columns
        if c != date_col
    ]

    if not numeric_cols:
        logger.error("Нет numeric колонок для trend")
        raise ValueError("В датасете нет числовых признаков для построения трендов.")

    x_data = df[date_col].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
    y_data = {
        col: df[col].fillna(0).tolist()
        for col in numeric_cols
    }

    logger.info(f"Trend данные подготовлены metrics={len(numeric_cols)}")

    return {
        "date_col": date_col,
        "numeric_cols": numeric_cols,
        "x": x_data,
        "y": y_data
    }

def get_dependency_data(df: pd.DataFrame, col1: str, col2: str, cols_to_remove: list[str] = []) -> dict:
    df = remove_dates(df, cols_to_remove)
    logger.info(f"Dates удалены dependency shape={df.shape}")

    col_map = {c.lower(): c for c in df.columns}

    real_col1 = col_map.get(col1.lower())
    real_col2 = col_map.get(col2.lower())

    if not real_col1 or not real_col2:
        logger.error(f"Колонки не найдены col1={col1} col2={col2}")
        raise ValueError(f"Колонки '{col1}' или '{col2}' не найдены в датасете.")

    df = df.dropna(subset=[real_col1, real_col2])
    logger.info(f"NaN удалены dependency rows={len(df)}")

    def is_categorical(series):
        return pd.api.types.is_object_dtype(series) or series.nunique() < 15

    col1_is_cat = is_categorical(df[real_col1])
    col2_is_cat = is_categorical(df[real_col2])

    result = {"col1": col1, "col2": col2}

    if not col1_is_cat and not col2_is_cat:
        if len(df) > 2000:
            df = df.sample(2000, random_state=42)

        result["sub_type"] = "scatter"
        result["x"] = df[real_col2].tolist()
        result["y"] = df[real_col1].tolist()
        logger.info(f"Dependency scatter prepared rows={len(df)}")

    elif col1_is_cat and col2_is_cat:
        ct = pd.crosstab(df[real_col1], df[real_col2])

        result["sub_type"] = "heatmap"
        result["x"] = ct.columns.astype(str).tolist()
        result["y"] = ct.index.astype(str).tolist()
        result["z"] = ct.values.tolist()
        logger.info(f"Dependency heatmap prepared shape={ct.shape}")

    else:
        cat_col = real_col1 if col1_is_cat else real_col2
        num_col = real_col2 if col1_is_cat else real_col1

        top_cats = df[cat_col].value_counts().nlargest(12).index
        df = df[df[cat_col].isin(top_cats)]

        grouped = df.groupby(cat_col)[num_col].apply(list).to_dict()

        result["sub_type"] = "box"
        result["cat_col"] = cat_col
        result["num_col"] = num_col
        result["categories"] = list(grouped.keys())
        result["values"] = list(grouped.values())

        logger.info(f"Dependency box prepared cats={len(grouped)}")

    return result


def get_pairplot_data(df: pd.DataFrame, cols_to_remove: list[str] = []) -> dict:
    df = filter_dataframe(df, cols_to_remove).copy()
    logger.info(f"Pairplot dataframe prepared shape={df.shape}")

    num_cols = df.select_dtypes(include=['number']).columns.tolist()

    if len(num_cols) < 2:
        logger.error("Недостаточно числовых колонок pairplot")
        raise ValueError("Для матрицы рассеяния нужно минимум 2 числовых признака.")

    if len(num_cols) > 5:
        default_cols = df[num_cols].var().nlargest(5).index.tolist()
    else:
        default_cols = num_cols

    for col in num_cols:
        if bool(df[col].isnull().any()):
            df[col] = df[col].fillna(df[col].median())

    logger.info(f"NaN заполнены pairplot cols={len(num_cols)}")

    if len(df) > 500:
        df = df.sample(n=500, random_state=42)
        logger.info("Pairplot sampled to 500 rows")

    dimensions = []

    for col in num_cols:
        dimensions.append({
            "label": col,
            "values": df[col].tolist()
        })

    logger.info(f"Pairplot prepared dims={len(dimensions)}")

    return {
        "dimensions": dimensions,
        "all_columns": num_cols,
        "default_columns": default_cols
    }


def get_all_relationships_data(df: pd.DataFrame, cols_to_remove: list[str] = []) -> list[dict]:
    charts = []

    try:
        corr_data = get_correlation_data(df, cols_to_remove)
        charts.append({"type": "correlation", "data": corr_data})
        logger.info("Correlation added to all_relationships")
    except Exception as e:
        logger.error(f"Correlation error: {e}")

    try:
        feature_tree = get_feature_tree(df, cols_to_remove)
        charts.append({"type": "feature_tree", "data": feature_tree})
        logger.info("Feature_tree added to all_relationships")
    except Exception as e:
        logger.error(f"Feature_tree error: {e}")

    try:
        pairplot_data = get_pairplot_data(df, cols_to_remove)
        charts.append({"type": "pairplot", "data": pairplot_data})
        logger.info("Pairplot added to all_relationships")
    except Exception as e:
        logger.error(f"Pairplot error: {e}")

    logger.info(f"All relationships prepared charts={len(charts)}")
    return charts


def get_feature_importances(df: pd.DataFrame, target_col: str, cols_to_remove: list[str] = []) -> dict:
    df = remove_dates(df, cols_to_remove)
    logger.info(f"Dates removed feature_importance shape={df.shape}")

    col_map = {c.lower(): c for c in df.columns}
    real_target = col_map.get(target_col.lower())

    if not real_target:
        logger.error(f"Target not found {target_col}")
        raise ValueError(f"Целевая колонка '{target_col}' не найдена в датасете.")

    df = df.dropna(subset=[real_target])
    logger.info(f"Target NaN dropped rows={len(df)}")

    y = df[real_target]
    X = df.drop(columns=[real_target])

    for col in X.columns:
        if pd.api.types.is_numeric_dtype(X[col]):
            X[col] = X[col].fillna(X[col].median())
        else:
            X[col] = X[col].fillna('Unknown')

    if len(X) < 10:
        logger.error("Not enough data for ML")
        raise ValueError("Недостаточно данных для обучения модели (меньше 10 валидных строк).")

    for col in X.select_dtypes(include=['object', 'category', 'string']).columns:
        X[col] = pd.factorize(X[col])[0]

    is_numeric = pd.api.types.is_numeric_dtype(y)

    if is_numeric and y.nunique() > 5:
        from sklearn.ensemble import RandomForestRegressor
        model = RandomForestRegressor(n_estimators=50, random_state=42)
    else:
        from sklearn.ensemble import RandomForestClassifier
        if not is_numeric:
            y = pd.factorize(y)[0]
        model = RandomForestClassifier(n_estimators=50, random_state=42)

    model.fit(X, y)
    importances = model.feature_importances_

    imp_df = pd.DataFrame({
        'feature': X.columns,
        'importance': importances
    }).sort_values(by='importance', ascending=False).head(7).iloc[::-1]

    logger.info("Feature importance model trained")

    return {
        "target": real_target,
        "features": imp_df['feature'].tolist(),
        "importances": np.round(imp_df['importance'], 4).tolist()
    }


def get_feature_tree(df: pd.DataFrame, cols_to_remove: list[str] = None) -> dict:
    df = remove_dates(df, cols_to_remove)
    logger.info(f"Dates removed feature_tree shape={df.shape}")

    for col in df.select_dtypes(exclude=['number']).columns:
        df[col] = pd.factorize(df[col])[0]

    for col in df.columns:
        if bool(df[col].isnull().any()):
            df[col] = df[col].fillna(df[col].median())

    corr = df.corr().fillna(0)
    logger.info("Correlation computed for feature_tree")

    distances = 1 - corr.abs().values
    np.fill_diagonal(distances, 0)
    distances = np.clip(distances, 0, 1)

    dist_array = squareform(distances)
    Z = hierarchy.linkage(dist_array, method='ward')

    dendro = hierarchy.dendrogram(Z, labels=corr.columns, no_plot=True)
    logger.info("Dendrogram computed")

    return {
        "icoord": dendro['icoord'],
        "dcoord": dendro['dcoord'],
        "ivl": dendro['ivl'],
    }