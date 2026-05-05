import numpy as np
import pandas as pd

def calc_cash_flow(df: pd.DataFrame, date_col: str, amount_col: str, freq: str = 'M') -> dict:
    """Группирует денежный поток по временным периодам (по умолчанию по месяцам)."""
    df_clean = df.copy()
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
    df_clean = df_clean.dropna(subset=[date_col, amount_col])
    
    # Группировка сумм по выбранной частоте
    cf = df_clean.groupby(pd.Grouper(key=date_col, freq=freq))[amount_col].sum().reset_index()
    cf['date_str'] = cf[date_col].dt.strftime('%Y-%m-%d')
    
    return {
        "tool_type": "cash_flow_chart",
        "data": {
            "labels": cf['date_str'].tolist(),
            "values": cf[amount_col].tolist()
        }
    }

def calc_pnl(df: pd.DataFrame, amount_col: str) -> dict:
    """Считает базовый P&L: Доходы, Расходы, Чистую прибыль и Маржинальность."""
    df_clean = df.copy()
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    
    income = df_clean[df_clean[amount_col] > 0][amount_col].sum()
    expense = df_clean[df_clean[amount_col] < 0][amount_col].sum() # Будет отрицательным
    net_profit = income + expense 
    
    margin = (net_profit / income * 100) if income > 0 else 0
    
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
    """Группирует только расходы по категориям для пай-чарта."""
    df_clean = df.copy()
    df_clean[amount_col] = pd.to_numeric(df_clean[amount_col], errors='coerce').fillna(0)
    
    # Берем только отрицательные суммы (расходы)
    expenses = df_clean[df_clean[amount_col] < 0]
    
    grouped = expenses.groupby(category_col)[amount_col].sum().abs().reset_index()
    grouped = grouped.sort_values(by=amount_col, ascending=False).head(10) # Топ-10 категорий
    
    return {
        "tool_type": "expense_pie_chart",
        "data": {
            "categories": grouped[category_col].tolist(),
            "amounts": grouped[amount_col].tolist()
        }
    }

def calc_abc_analysis(df: pd.DataFrame, category_col: str, amount_col: str) -> dict:
    """ABC-анализ (Парето) по выручке."""
    df_clean = df[pd.to_numeric(df[amount_col], errors='coerce') > 0].copy()
    grouped = df_clean.groupby(category_col)[amount_col].sum().sort_values(ascending=False).reset_index()
    
    grouped['cumsum'] = grouped[amount_col].cumsum()
    total_sum = grouped[amount_col].sum()
    grouped['cum_percent'] = (grouped['cumsum'] / total_sum * 100).round(2)
    
    return {
        "tool_type": "abc_analysis",
        "data": {
            "categories": grouped[category_col].tolist(),
            "amounts": grouped[amount_col].tolist(),
            "cum_percent": grouped['cum_percent'].tolist()
        }
    }

def calc_unit_economics(df: pd.DataFrame, source_col: str, amount_col: str, cac_col: str, user_col: str) -> dict:
    """Сравнение LTV (выручки за всё время) и CAC (затрат) по каналам с обработкой органики."""
    df_clean = df[pd.to_numeric(df[amount_col], errors='coerce') > 0].copy()
    df_clean[cac_col] = pd.to_numeric(df_clean[cac_col], errors='coerce').fillna(0)
    
    # 1. Считаем LTV каждого отдельного клиента за всё время
    # Берем сумму всех его покупок, а источник трафика и CAC считаем по его первой транзакции
    user_ltv = df_clean.groupby(user_col).agg({
        amount_col: 'sum',   # LTV: сумма всех покупок клиента
        cac_col: 'first',    # CAC: стоимость его привлечения
        source_col: 'first'  # Источник, из которого он пришел
    }).reset_index()
    
    # 2. Усредняем LTV и CAC в разрезе каналов трафика
    grouped = user_ltv.groupby(source_col).agg({amount_col: 'mean', cac_col: 'mean'}).reset_index()
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
    
    return {
        "tool_type": "unit_economics",
        "data": {
            "sources": grouped[source_col].tolist(),
            "arpu": grouped['arpu'].round(2).tolist(),
            "cac": grouped['cac'].round(2).tolist(),
            "romi": romi_values,
            "romi_text": romi_text
        }
    }

def calc_revenue_forecast(df: pd.DataFrame, date_col: str, amount_col: str, forecast_periods: int = 3) -> dict:
    """Прогноз выручки с конусом неопределенности и без разрывов графика."""
    df_clean = df[pd.to_numeric(df[amount_col], errors='coerce') > 0].copy()
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
    df_clean = df_clean.dropna(subset=[date_col])
    
    monthly = df_clean.groupby(pd.Grouper(key=date_col, freq='M'))[amount_col].sum().reset_index()
    
    x_hist = np.arange(len(monthly), dtype=float)
    y_hist = monthly[amount_col].astype(float).values
    
    forecast_y = []
    forecast_y_upper = []
    forecast_y_lower = []
    forecast_dates = []
    
    if len(x_hist) > 1:
        slope, intercept = np.polyfit(x_hist, y_hist, 1) # type: ignore
        
        # Считаем стандартное отклонение для ширины "треугольника"
        residuals = y_hist - (slope * x_hist + intercept)
        std_dev = np.std(residuals) if len(residuals) > 0 else 0
        
        # ИСПРАВЛЕНИЕ РАЗРЫВА: берем ПОСЛЕДНЮЮ точку факта как ПЕРВУЮ точку прогноза
        last_hist_x = x_hist[-1]
        last_hist_y = y_hist[-1]
        last_date = monthly[date_col].iloc[-1]
        
        forecast_y.append(round(last_hist_y, 2))
        forecast_y_upper.append(round(last_hist_y, 2))
        forecast_y_lower.append(round(last_hist_y, 2))
        forecast_dates.append(last_date.strftime('%Y-%m-%d'))

        # Строим прогноз в будущее
        for i in range(1, forecast_periods + 1):
            fut_y = slope * (last_hist_x + i) + intercept
            forecast_y.append(max(0, round(fut_y, 2)))
            
            # Конус расширяется с каждым шагом
            uncertainty = std_dev * (0.5 + 0.5 * i) 
            forecast_y_upper.append(round(fut_y + uncertainty, 2))
            forecast_y_lower.append(max(0, round(fut_y - uncertainty, 2)))
            
            forecast_dates.append((last_date + pd.DateOffset(months=i)).strftime('%Y-%m-%d'))

    return {
        "tool_type": "revenue_forecast",
        "data": {
            "hist_dates": monthly[date_col].dt.strftime('%Y-%m-%d').tolist(),
            "hist_values": y_hist.tolist(),
            "forecast_dates": forecast_dates,
            "forecast_values": forecast_y,
            "forecast_upper": forecast_y_upper, # Для верхнего края площади
            "forecast_lower": forecast_y_lower  # Для нижнего края площади
        }
    }

def calc_cohort_analysis(df: pd.DataFrame, date_col: str, user_col: str) -> dict:
    """Когортный анализ удержания (Retention Rate)."""
    df_clean = df.copy()
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
    df_clean = df_clean.dropna(subset=[date_col, user_col])

    # Приводим даты к началу месяца для создания когорт
    df_clean['Order_Month'] = df_clean[date_col].dt.to_period('M').dt.to_timestamp()
    
    # Определяем когорту: месяц ПЕРВОЙ покупки для каждого пользователя
    df_clean['Cohort'] = df_clean.groupby(user_col)['Order_Month'].transform('min')

    # Считаем "возраст" клиента в месяцах на момент каждой покупки (0, 1, 2...)
    df_clean['Period'] = (df_clean['Order_Month'].dt.year - df_clean['Cohort'].dt.year) * 12 + \
                         (df_clean['Order_Month'].dt.month - df_clean['Cohort'].dt.month)

    # Считаем уникальных юзеров по когортам и периодам
    cohort_data = df_clean.groupby(['Cohort', 'Period'])[user_col].nunique().reset_index()
    cohort_pivot = cohort_data.pivot(index='Cohort', columns='Period', values=user_col)
    
    # 100% — это количество людей в нулевой месяц (когда они пришли)
    cohort_sizes = cohort_pivot.iloc[:, 0]
    retention = cohort_pivot.divide(cohort_sizes, axis=0) * 100
    retention = retention.round(1)

    # Подготовка данных для Plotly Heatmap
    cohort_labels = [d.strftime('%Y-%m') for d in retention.index]
    periods = [str(p) for p in retention.columns]
    
    # --- ИСПРАВЛЕННЫЙ БЛОК ---
    z_data = []
    text_data = []
    
    for row in retention.values:
        z_row = []
        text_row = []
        for val in row:
            if pd.isna(val): # Надежно отлавливаем NaN
                z_row.append(None)
                text_row.append("") # Оставляем ячейку пустой
            else:
                z_row.append(val)
                text_row.append(f"{val}%")
        z_data.append(z_row)
        text_data.append(text_row)
    # -------------------------

    return {
        "tool_type": "cohort_analysis",
        "data": {
            "cohorts": cohort_labels,
            "periods": periods,
            "z": z_data,
            "text": text_data
        }
    }