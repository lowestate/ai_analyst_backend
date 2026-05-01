def mock_correlation_report(corr_data: dict) -> str:
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
        report += "Умеренные корреляции:\n"
        # Показываем только топ-3 умеренных, чтобы не спамить
        for pair, val in sorted(moderate, key=lambda x: abs(x[1]), reverse=True)[:3]:
            report += f"- {pair[0]} и {pair[1]} (R = {val})\n"

    if not strong_positive and not strong_negative and not moderate:
        report += "Значимых корреляций между признаками не выявлено."

    return report.strip()

def mock_column_report(stats_data: dict) -> str:
    report = "Анализ столбцов успешно завершен.\n\n"
    
    numeric = stats_data.get("numeric", {})
    if numeric:
        report += "### Числовые признаки\n"
        cols = list(numeric.keys())
        if cols:
            metrics = list(numeric[cols[0]].keys())
            header = "| Признак | " + " | ".join(metrics) + " |"
            separator = "|---|" + "|".join(["---"] * len(metrics)) + "|"
            report += header + "\n" + separator + "\n"
            for col in cols:
                row_vals = [str(numeric[col].get(m, 0)) for m in metrics]
                report += f"| **{col}** | " + " | ".join(row_vals) + " |\n"
        report += "\n"

    cat_uniform = stats_data.get("categorical_uniform", {})
    if cat_uniform:
        report += "### Категориальные (Равномерные)\n"
        for col, info in cat_uniform.items():
            report += f"- **{col}**: {info['unique_count']} уник. категорий, примерно по {info['approx_val']} значений в каждой (график не строится, так как распределение равномерное).\n"
        report += "\n"

    cat_charts = stats_data.get("categorical_charts", {})
    if cat_charts:
        report += "### Категориальные (С явными лидерами)\n"
        for col, counts in cat_charts.items():
            top_cat = list(counts.keys())[0] if counts else "Нет"
            has_others = "Другие" in counts
            report += f"- **{col}**: Самая частая категория — '{top_cat}' ({counts.get(top_cat, 0)} шт.). {'Мелкие категории сгруппированы в «Другие».' if has_others else ''}\n"

    return report.strip()

def mock_outliers_report(data: dict) -> str:
    stats = data.get("stats", {})
    report = "Анализ аномалий и выбросов завершен (метод межквартильного размаха IQR).\n\n"
    report += "| Признак | Всего непустых | Выбросов (шт) | Выбросов (%) | Норм. диапазон |\n"
    report += "|---|---|---|---|---|\n"
    
    for col, info in stats.items():
        if info["outliers_count"] > 0:
            report += f"| **{col}** | {info['total']} | {info['outliers_count']} | {info['outliers_percent']}% | от {info['lower']} до {info['upper']} |\n"
            
    if not any(info["outliers_count"] > 0 for info in stats.values()):
        report += "Выбросов в числовых данных не обнаружено!\n"
        
    return report

def mock_trend_report(data: dict) -> str:
    date_col = data.get("date_col", "Неизвестно")
    cols = data.get("numeric_cols", [])
    
    report = f"**Анализ временных трендов (Time-Series) завершен.**\n\n"
    report += f"Ось времени (X) привязана к признаку: `{date_col}`.\n"
    report += f"Всего проанализировано числовых метрик: **{len(cols)}**.\n\n"
    
    report += "**Краткие выводы по динамике:**\n"
    
    # Даем словесную оценку только первым 3 признакам, чтобы не спамить
    for col in cols[:3]:
        report += f"- **{col}**: наблюдается стабильная дисперсия, резких разрывов в данных (gaps) не обнаружено. Присутствуют локальные всплески.\n"
        
    if len(cols) > 3:
        report += f"- *(...и еще {len(cols) - 3} признак(ов).)*\n\n"
        
    report += "*Подсказка:* В раскрытом виде графика справа вы можете включать и отключать отображение конкретных линий с помощью панели чекбоксов."
    
    return report

def mock_dependency_report(data: dict) -> str:
    c1, c2 = data['col1'], data['col2']
    report = f"**Анализ зависимости: `{c1}` от `{c2}`**\n\n"

    if data["sub_type"] == "scatter":
        report += f"Оба признака являются числовыми. Построен **График рассеяния (Scatter Plot)**.\nОн помогает визуально оценить наличие линейной или нелинейной связи между `{c1}` и `{c2}`, а также выявить возможные выбросы (аномалии) в данных."
    elif data["sub_type"] == "heatmap":
        report += f"Оба признака — категориальные. Построена **Тепловая матрица сопряженности (Crosstab Heatmap)**.\nОна показывает частоту совместного появления различных категорий. Чем темнее ячейка, тем чаще эта комбинация встречается в датасете."
    elif data["sub_type"] == "box":
        report += f"Смешанные типы данных: категориальный (`{data['cat_col']}`) и числовой (`{data['num_col']}`). Построен **Ящик с усами (Box Plot)**.\nЭтот график позволяет сравнить распределение числового признака, его медиану и разброс внутри каждой отдельной категории."

    return report

def mock_feature_importances_report(data: dict) -> str:
    target = data["target"]
    # Переворачиваем обратно для текста (чтобы самый важный был первым)
    features = data["features"][::-1] 
    importances = data["importances"][::-1]

    if not features:
        return f"Не удалось определить важные признаки для '{target}'."

    top_feature = features[0]
    top_score = importances[0]

    msg = "Я обучил ML-модель (Случайный лес) на ваших данных, чтобы выявить скрытые закономерности. "
    msg += f"Ниже представлены параметры, которые сильнее всего влияют на {target}.\n\n"
    
    msg += f"**Признак с самым сильным влиянием:** `{top_feature}` (вклад: {int(top_score * 100)}%).\n\n"

    msg += "**Топ признаков:**\n"
    for f, imp in zip(features, importances):
        msg += f"- **{f}**: {int(imp * 100)}%\n"

    return msg

def mock_feature_tree_report(data: dict) -> str:
    msg = (
        "**Дендрограмма признаков (Кластеризация)**\n\n"
        "Этот график показывает, как параметры группируются на основе их взаимной связи. "
        "Чем ниже по вертикальной оси сливаются ветви двух признаков, тем сильнее они похожи (дублируют друг друга). "
        "Используйте эту информацию, чтобы удалить лишние переменные перед построением ML-моделей или выявить неочевидные смысловые кластеры в данных."
    )
    return msg