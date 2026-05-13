import os
import time
from datetime import datetime, timezone

from app.agents.client import llm
from app.agents.core.models import InitChatOutput
from app.agents.core.utils import get_llm_request_metadata
from app.config import logger
from app.agents.core.prompt import INITIAL_METADATA_PROMPT_TEMPLATE
from app.users_db_interaction import log_llm_request

USE_MOCK_ANSWERS = bool(os.getenv("USE_MOCK_ANSWERS", "1"))


async def generate_initial_metadata(
    filename: str,
    columns: list,
    stats: dict
) -> InitChatOutput:
    logger.info(f"Запуск generate_initial_metadata filename={filename}")

    if USE_MOCK_ANSWERS:
        logger.info(f"MOCK режим generate_initial_metadata filename={filename}")

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

        logger.info(f"MOCK metadata сгенерированы filename={filename}")

        return InitChatOutput(
            chat_title=title,
            initial_message=msg
        )

    try:
        prompt = INITIAL_METADATA_PROMPT_TEMPLATE.format(
            filename=filename,
            columns=', '.join(columns),
            stats=stats
        )
        logger.info(f"Prompt сформирован filename={filename}")

        llm_instance = llm()
        logger.info("LLM instance создан")

        # include_raw=True — чтобы получить AIMessage с usage_metadata
        structured_llm = llm_instance.with_structured_output(InitChatOutput, include_raw=True)
        logger.info("Structured output настроен")

        created_at = datetime.now(timezone.utc)
        start_ts = time.monotonic()

        raw_result = await structured_llm.ainvoke(prompt)
        logger.info(f"Metadata успешно сгенерированы filename={filename}")

        duration_ms = int((time.monotonic() - start_ts) * 1000)

        raw_ai_message = raw_result.get("raw")
        result = raw_result.get("parsed")

        meta = get_llm_request_metadata(raw_ai_message) if raw_ai_message else None

        await log_llm_request(
            request_id=meta.request_id if meta else None,
            user_id=None,  # user_id недоступен на этапе загрузки файла
            chat_id=None,
            request_text=f"generate_initial_metadata: {filename}",
            input_tokens=meta.input_tokens if meta else None,
            output_tokens=meta.output_tokens if meta else None,
            request_status=200,
            model=meta.model_name if meta else None,
            created_at=created_at,
            duration_ms=duration_ms,
            initiator="user",
            error_msg=raw_result.get("parsing_error"),
        )

        return result  # type: ignore

    except Exception as e:
        logger.error(f"Ошибка generate_initial_metadata filename={filename}: {str(e)}", exc_info=True)
        raise