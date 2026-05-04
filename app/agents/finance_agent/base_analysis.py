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

def calc_unit_economics(df: pd.DataFrame, source_col: str, amount_col: str, cac_col: str) -> dict:
    """Сравнение среднего чека (ARPU) и стоимости привлечения (CAC) по каналам."""
    df_clean = df[pd.to_numeric(df[amount_col], errors='coerce') > 0].copy()
    df_clean[cac_col] = pd.to_numeric(df_clean[cac_col], errors='coerce').fillna(0)
    
    grouped = df_clean.groupby(source_col).agg({amount_col: 'mean', cac_col: 'mean'}).reset_index()
    grouped.rename(columns={amount_col: 'arpu', cac_col: 'cac'}, inplace=True)
    
    # Считаем ROMI (Return on Marketing Investment)
    grouped['romi'] = np.where(grouped['cac'] > 0, ((grouped['arpu'] - grouped['cac']) / grouped['cac'] * 100), 0)
    
    return {
        "tool_type": "unit_economics",
        "data": {
            "sources": grouped[source_col].tolist(),
            "arpu": grouped['arpu'].round(2).tolist(),
            "cac": grouped['cac'].round(2).tolist(),
            "romi": grouped['romi'].round(1).tolist()
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