def generate_mock_correlation_report(corr_data: dict) -> str:
    """Генерация текстового отчета на основе реальных цифр для мок-режима"""
    strong_positive = []
    strong_negative = []
    moderate = []

    seen = set()
    for col1, row in corr_data.items():
        for col2, val in row.items():
            if col1 == col2: continue
            
            # Избегаем дубликатов пар (A-B и B-A)
            pair = tuple(sorted([col1, col2]))
            if pair in seen: continue
            seen.add(pair)

            if val > 0.7:
                strong_positive.append((pair, val))
            elif val < -0.7:
                strong_negative.append((pair, val))
            elif 0.4 < abs(val) <= 0.7:
                moderate.append((pair, val))

    report = "Корреляционный анализ показал следующее:\n\n"
    
    if strong_positive:
        report += "Сильная прямая корреляция (0.7 < R):\n"
        for pair, val in strong_positive:
            report += f"- {pair[0]} и {pair[1]} (R = {val})\n"
        report += "\n"

    if strong_negative:
        report += "Сильная обратная корреляция (R < -0.7):\n"
        for pair, val in strong_negative:
            report += f"- {pair[0]} и {pair[1]} (R = {val})\n"
        report += "\n"

    if moderate:
        report += "Также стоит отметить умеренные корреляции:\n"
        # Показываем только топ-3 умеренных, чтобы не спамить
        for pair, val in sorted(moderate, key=lambda x: abs(x[1]), reverse=True)[:3]:
            report += f"- {pair[0]} и {pair[1]} (R = {val})\n"

    if not strong_positive and not strong_negative and not moderate:
        report += "Значимых корреляций между признаками не выявлено."

    return report.strip()