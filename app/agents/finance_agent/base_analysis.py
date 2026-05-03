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