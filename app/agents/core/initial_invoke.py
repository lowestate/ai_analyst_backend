import os

from app.agents.client import llm
from app.agents.core.models import InitChatOutput
from app.config import logger
from app.agents.core.prompt import INITIAL_METADATA_PROMPT_TEMPLATE

USE_MOCK_ANSWERS = bool(os.getenv("USE_MOCK_ANSWERS", "1"))

async def generate_initial_metadata(filename: str, columns: list, stats: dict) -> InitChatOutput:
    if USE_MOCK_ANSWERS:
        logger.info("MOCK MODE: Генерируем детерминированное приветствие")
        title = f"Анализ {filename}"
        
        msg = (
            f"Загружен датасет '{filename}'.\n\n"
            f"Исходно было {stats['initial_rows']} записей и {stats['final_columns']} столбцов, "
            f"после очистки данных осталось {stats['final_rows']} записей. "
            f"Удалено дубликатов: {stats['duplicates_removed']}, "
            f"заполнено пропусков: {stats['missing_values_filled']}.\n\n"
            f"Предлагаю построить корреляционную матрицу для выявления взаимосвязей между показателями "
            f"и провести анализ распределения ключевых метрик."
        )
        return InitChatOutput(chat_title=title, initial_message=msg)

    prompt = INITIAL_METADATA_PROMPT_TEMPLATE.format(
        filename=filename, 
        columns=', '.join(columns), 
        stats=stats
    )
    llm_instance = llm()
    structured_llm = llm_instance.with_structured_output(InitChatOutput)
    result = await structured_llm.ainvoke(prompt)
    return result # type: ignore
