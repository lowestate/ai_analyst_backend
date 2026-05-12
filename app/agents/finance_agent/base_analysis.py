import numpy as np
import pandas as pd

from app.config import logger   

def calc_cash_flow(df: pd.DataFrame, date_col: str, amount_col: str, freq: str = 'M') -> dict:
    if date_col not in df.columns or amount_col not in df.columns:
        raise ValueError(f"Отсутствуют нужные колонки ({date_col}, {amount_col}). Доступны: {list(df.columns)}")

    df_clean = df.copy()
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    df_clean = df_clean.dropna(subset=[date_col])
    
    if df_clean.empty:
        raise ValueError("После очистки дат датафрейм оказался пустым.")

    # Универсальная адаптация алиасов частоты (решает проблему M vs ME в Pandas 2.2+)
    try:
        modern_freq = freq
        if modern_freq == 'M': modern_freq = 'ME'
        elif modern_freq == 'Y': modern_freq = 'YE'
        elif modern_freq == 'Q': modern_freq = 'QE'
        
        # Пытаемся сгруппировать по современному стандарту
        cf = df_clean.groupby(pd.Grouper(key=date_col, freq=modern_freq))[amount_col].sum().reset_index()
    except ValueError:
        # Если Pandas старый (< 2.2) и выбросил ошибку на 'ME', откатываемся к исходному 'M'
        cf = df_clean.groupby(pd.Grouper(key=date_col, freq=freq))[amount_col].sum().reset_index()

    cf['date_str'] = cf[date_col].dt.strftime('%Y-%m-%d')
    
    logger.info(f"calc_cash_flow completed rows={len(cf)}")
    return {
        "tool_type": "cash_flow_chart",
        "data": {
            "labels": cf['date_str'].tolist(),
            "values": cf[amount_col].tolist()
        }
    }


def calc_pnl(df: pd.DataFrame, amount_col: str) -> dict:
    if amount_col not in df.columns:
        raise ValueError(f"Колонка {amount_col} не найдена. Доступны: {list(df.columns)}")
        
    df_clean = df.copy()
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    
    income = df_clean[df_clean[amount_col] > 0][amount_col].sum()
    expense = df_clean[df_clean[amount_col] < 0][amount_col].sum()
    
    # Защита: Если SQL вернул только положительные числа, но это очевидный запрос расходов
    if income > 0 and expense == 0 and 'exp' in df.columns.str.lower().tolist():
        expense = -income
        income = 0
        
    net_profit = income + expense 
    margin = (net_profit / income * 100) if income > 0 else 0
    
    logger.info(f"calc_pnl completed income={income} expense={expense} net_profit={net_profit}")
    return {
        "tool_type": "pnl_report",
        "data": {
            "total_income": float(income),
            "total_expense": float(abs(expense)),
            "net_profit": float(net_profit),
            "margin_percent": float(round(margin, 2))
        }
    }


def expense_structure(df: pd.DataFrame, category_col: str, amount_col: str) -> dict:
    if category_col not in df.columns or amount_col not in df.columns:
        raise ValueError(f"Колонки {category_col} или {amount_col} не найдены. Доступны: {list(df.columns)}")
        
    df_clean = df.copy()
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    
    # Умная фильтрация: если SQL УЖЕ отфильтровал расходы и сделал их положительными (например, через ABS())
    if (df_clean[amount_col] >= 0).all():
        expenses = df_clean
    else:
        expenses = df_clean[df_clean[amount_col] < 0]
        
    if expenses.empty:
        raise ValueError("Не найдено данных о расходах (отрицательных значений).")
        
    grouped = expenses.groupby(category_col)[amount_col].sum().abs().reset_index()
    grouped = grouped.sort_values(by=amount_col, ascending=False).head(10)
    
    logger.info(f"expense_structure completed categories={len(grouped)}")
    return {
        "tool_type": "expense_pie_chart",
        "data": {
            "categories": grouped[category_col].astype(str).tolist(),
            "amounts": grouped[amount_col].tolist()
        }
    }

def calc_abc_analysis(df: pd.DataFrame, category_col: str, amount_col: str) -> dict:
    if category_col not in df.columns or amount_col not in df.columns:
        raise ValueError(f"Колонки {category_col} или {amount_col} не найдены. Доступны: {list(df.columns)}")
        
    df_clean = df.copy()
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    df_clean = df_clean[df_clean[amount_col] > 0]
    
    if df_clean.empty:
        raise ValueError("Нет положительных транзакций для ABC-анализа.")
        
    grouped = df_clean.groupby(category_col)[amount_col].sum().sort_values(ascending=False).reset_index()
    
    grouped['cumsum'] = grouped[amount_col].cumsum()
    total_sum = grouped[amount_col].sum()
    grouped['cum_percent'] = (grouped['cumsum'] / total_sum * 100).round(2)
    
    logger.info(f"calc_abc_analysis completed categories={len(grouped)}")
    return {
        "tool_type": "abc_analysis",
        "data": {
            "categories": grouped[category_col].astype(str).tolist(),
            "amounts": grouped[amount_col].tolist(),
            "cum_percent": grouped['cum_percent'].tolist()
        }
    }



def calc_unit_economics(df: pd.DataFrame, source_col: str, amount_col: str, cac_col: str, user_col: str = None) -> dict:
    df_clean = df.copy()

    # 1. Защита: проверяем наличие базовых колонок
    if source_col not in df_clean.columns or amount_col not in df_clean.columns:
        raise ValueError(f"Необходимы колонки {source_col} и {amount_col}. Доступны: {list(df_clean.columns)}")

    # 2. Защита: если колонки CAC нет (например, органический трафик), создаем нулевую
    if cac_col not in df_clean.columns:
        df_clean[cac_col] = 0

    df_clean = df_clean[pd.to_numeric(df_clean[amount_col], errors='coerce') > 0].copy()
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    df_clean[cac_col] = pd.to_numeric(df_clean[cac_col], errors='coerce').fillna(0)
    
    # 3. Умная агрегация: проверяем, есть ли колонка пользователя
    if user_col and user_col in df_clean.columns:
        # Сырые данные: собираем честный LTV по юзерам
        user_ltv = df_clean.groupby(user_col).agg({
            amount_col: 'sum',
            cac_col: 'first',
            source_col: 'first'
        }).reset_index()
        grouped = user_ltv.groupby(source_col).agg({amount_col: 'mean', cac_col: 'mean'}).reset_index()
    else:
        # Данные уже сагрегированы SQL-запросом по каналам или нет user_id
        grouped = df_clean.groupby(source_col).agg({amount_col: 'mean', cac_col: 'mean'}).reset_index()
    
    grouped.rename(columns={amount_col: 'arpu', cac_col: 'cac'}, inplace=True)
    
    valid_romi = [((r['arpu'] - r['cac']) / r['cac'] * 100) for _, r in grouped.iterrows() if r['cac'] > 0]
    max_romi = max(valid_romi) if valid_romi else 0
    organic_y = max(max_romi * 1.1, 100.0) 
    
    romi_values = []
    romi_text = []
    
    for _, row in grouped.iterrows():
        if row['cac'] > 0:
            val = round((row['arpu'] - row['cac']) / row['cac'] * 100, 1)
            romi_values.append(val)
            romi_text.append(f"{val}%")
        elif row['cac'] == 0 and row['arpu'] > 0:
            romi_values.append(round(organic_y, 1))
            romi_text.append("∞")
        else:
            romi_values.append(0)
            romi_text.append("0%")
    
    logger.info(f"calc_unit_economics completed sources={len(grouped)}")
    return {
        "tool_type": "unit_economics",
        "data": {
            "sources": grouped[source_col].astype(str).tolist(), # astype(str) страхует от пустых значений
            "arpu": grouped['arpu'].round(2).tolist(),
            "cac": grouped['cac'].round(2).tolist(),
            "romi": romi_values,
            "romi_text": romi_text
        }
    }


def calc_revenue_forecast(df: pd.DataFrame, date_col: str, amount_col: str, forecast_periods: int = 3) -> dict:
    if date_col not in df.columns or amount_col not in df.columns:
        raise ValueError(f"Колонки {date_col} или {amount_col} не найдены. Доступны: {list(df.columns)}")
        
    df_clean = df.copy()
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    df_clean = df_clean[df_clean[amount_col] > 0]
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
    df_clean = df_clean.dropna(subset=[date_col])
    
    # Группировка по месяцам (используем 'ME')
    monthly = df_clean.groupby(pd.Grouper(key=date_col, freq='ME'))[amount_col].sum().reset_index()
    
    if len(monthly) < 2:
        raise ValueError(f"Для прогноза нужно хотя бы 2 исторических периода (месяца). Найдено: {len(monthly)}")
    
    x_hist = np.arange(len(monthly), dtype=float)
    y_hist = monthly[amount_col].astype(float).values
    
    forecast_y = []
    forecast_y_upper = []
    forecast_y_lower = []
    forecast_dates = []
    
    slope, intercept = np.polyfit(x_hist, y_hist, 1) # type: ignore
    
    residuals = y_hist - (slope * x_hist + intercept)
    std_dev = np.std(residuals) if len(residuals) > 0 else 0
    
    last_hist_x = x_hist[-1]
    last_hist_y = y_hist[-1]
    last_date = monthly[date_col].iloc[-1]
    
    forecast_y.append(round(last_hist_y, 2))
    forecast_y_upper.append(round(last_hist_y, 2))
    forecast_y_lower.append(round(last_hist_y, 2))
    forecast_dates.append(last_date.strftime('%Y-%m-%d'))

    for i in range(1, forecast_periods + 1):
        fut_y = slope * (last_hist_x + i) + intercept
        forecast_y.append(max(0, round(fut_y, 2)))
        
        uncertainty = std_dev * (0.5 + 0.5 * i)
        forecast_y_upper.append(round(fut_y + uncertainty, 2))
        forecast_y_lower.append(max(0, round(fut_y - uncertainty, 2)))
        
        forecast_dates.append((last_date + pd.DateOffset(months=i)).strftime('%Y-%m-%d'))

    logger.info(f"calc_revenue_forecast completed points={len(forecast_dates)}")
    return {
        "tool_type": "revenue_forecast",
        "data": {
            "hist_dates": monthly[date_col].dt.strftime('%Y-%m-%d').tolist(),
            "hist_values": y_hist.tolist(),
            "forecast_dates": forecast_dates,
            "forecast_values": forecast_y,
            "forecast_upper": forecast_y_upper,
            "forecast_lower": forecast_y_lower
        }
    }


def calc_cohort_analysis(df: pd.DataFrame, date_col: str, user_col: str) -> dict:
    if date_col not in df.columns or user_col not in df.columns:
        raise ValueError(f"Для когортного анализа нужны дата и ID пользователя ({date_col}, {user_col}). Убедитесь, что SQL не сагрегировал базу заранее.")
        
    df_clean = df.copy()
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
    df_clean = df_clean.dropna(subset=[date_col, user_col])
    
    if df_clean.empty:
        raise ValueError("Датафрейм пуст после парсинга дат.")

    df_clean['Order_Month'] = df_clean[date_col].dt.to_period('M').dt.to_timestamp()
    df_clean['Cohort'] = df_clean.groupby(user_col)['Order_Month'].transform('min')

    df_clean['Period'] = (df_clean['Order_Month'].dt.year - df_clean['Cohort'].dt.year) * 12 + \
                         (df_clean['Order_Month'].dt.month - df_clean['Cohort'].dt.month)

    cohort_data = df_clean.groupby(['Cohort', 'Period'])[user_col].nunique().reset_index()
    
    if cohort_data.empty:
        raise ValueError("Не удалось сформировать когорты из предоставленных данных.")
        
    cohort_pivot = cohort_data.pivot(index='Cohort', columns='Period', values=user_col)
    
    cohort_sizes = cohort_pivot.iloc[:, 0]
    retention = cohort_pivot.divide(cohort_sizes, axis=0) * 100
    retention = retention.round(1)

    cohort_labels = [d.strftime('%Y-%m') for d in retention.index]
    periods = [str(p) for p in retention.columns]
    
    z_data = []
    text_data = []
    
    for row in retention.values:
        z_row = []
        text_row = []
        for val in row:
            if pd.isna(val):
                z_row.append(None)
                text_row.append("")
            else:
                z_row.append(val)
                text_row.append(f"{val}%")
        z_data.append(z_row)
        text_data.append(text_row)

    logger.info(f"calc_cohort_analysis completed cohorts={len(cohort_labels)}")
    return {
        "tool_type": "cohort_analysis",
        "data": {
            "cohorts": cohort_labels,
            "periods": periods,
            "z": z_data,
            "text": text_data
        }
    }