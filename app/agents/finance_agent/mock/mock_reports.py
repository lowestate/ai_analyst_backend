def mock_cash_flow_report(data: dict) -> str:
    """Генерация текстового отчета по реальному Cash Flow"""
    labels = data.get("labels", [])
    values = data.get("values", [])
    
    if not labels:
        return "Не удалось рассчитать денежный поток. Проверьте правильность указания колонок."

    total_flow = sum(values)
    pos_months = sum(1 for v in values if v > 0)
    neg_months = sum(1 for v in values if v < 0)

    report = "**Анализ движения денежных средств (Cash Flow)**\n\n"
    report += f"Проанализировано периодов (месяцев): **{len(labels)}**.\n"
    report += f"Общий денежный поток за период: **{total_flow:,.2f}**.\n\n"

    if total_flow > 0:
        report += "🟢 **Итог:** В целом динамика положительная (поступления превышают оттоки).\n"
    elif total_flow < 0:
        report += "🔴 **Итог:** Наблюдается отрицательный денежный поток (оттоки превышают поступления). Возможен кассовый разрыв.\n"
    else:
        report += "⚪ **Итог:** Денежный поток нулевой.\n"

    report += f"\n- Периодов с профицитом (+): {pos_months}\n"
    report += f"- Периодов с дефицитом (-): {neg_months}\n"

    return report

def mock_pnl_report(data: dict) -> str:
    """Генерация текстового отчета по реальному P&L"""
    income = data.get("total_income", 0)
    expense = data.get("total_expense", 0)
    profit = data.get("net_profit", 0)
    margin = data.get("margin_percent", 0)

    report = "**Отчет о прибылях и убытках (P&L)**\n\n"
    report += f"- **Общие доходы:** {income:,.2f}\n"
    report += f"- **Общие расходы:** {expense:,.2f}\n"
    report += f"- **Чистая прибыль:** **{profit:,.2f}**\n"
    report += f"- **Маржинальность:** {margin}%\n\n"

    if profit > 0:
        report += "🟢 Бизнес работает в плюс. Выручка полностью покрывает текущие затраты."
    elif profit < 0:
        report += "🔴 **Внимание:** Бизнес убыточен в анализируемом периоде! Затраты превышают выручку."
    else:
        report += "Бизнес работает ровно 'в ноль'."

    return report

def mock_expense_report(data: dict) -> str:
    """Генерация текстового отчета по структуре расходов"""
    categories = data.get("categories", [])
    amounts = data.get("amounts", [])

    if not categories:
        return "Не удалось найти расходы (отрицательные суммы) для анализа структуры."

    total_expenses = sum(amounts)
    top_cat = categories[0]
    top_amt = amounts[0]
    top_pct = (top_amt / total_expenses * 100) if total_expenses > 0 else 0

    report = "**Структура расходов**\n\n"
    report += f"Общая сумма учтенных затрат: **{total_expenses:,.2f}**.\n\n"
    
    report += f"🔥 Основная статья расходов — **'{top_cat}'** ({top_amt:,.2f}, что составляет **{top_pct:.1f}%** от всех расходов бюджета).\n\n"
    
    report += "**Топ статей расходов:**\n"
    for cat, amt in zip(categories[:5], amounts[:5]):
        report += f"- {cat}: {amt:,.2f}\n"

    return report