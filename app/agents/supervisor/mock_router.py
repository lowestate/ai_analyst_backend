def route_mock_request(user_message: str) -> tuple[str, str]:
    """
    Парсит мок-запрос. Если есть тег [A] или [Ф], возвращает нужного агента
    и очищенное от тега сообщение. По умолчанию кидает в data_analyst.
    """
    msg_clean = user_message.strip()
    
    # Проверяем теги (русские и английские буквы для надежности)
    if msg_clean.startswith("[A]") or msg_clean.startswith("[А]"):
        return "data_analyst", msg_clean[3:].strip()
    
    elif msg_clean.startswith("[F]") or msg_clean.startswith("[Ф]"):
        return "finance_agent", msg_clean[3:].strip()
    
    # Fallback, если тег забыли
    return "data_analyst", msg_clean