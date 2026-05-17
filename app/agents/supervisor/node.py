import time
from datetime import datetime, timezone
from typing import Any, Dict
from pydantic import BaseModel, Field

from app.config import logger
from app.agents.client import llm
from app.agents.core.state import AgentState
from app.agents.core.utils import get_llm_request_metadata
from app.agents.supervisor.prompt import SUPERVISOR_PROMPT
from app.users_db_interaction import log_llm_request


class RouteOutput(BaseModel):
    next_agent: str = Field(
        description="Куда направить запрос. Строго 'data_analyst' или 'finance_agent'"
    )


async def supervisor_node(state: AgentState):
    """Узел супервизора для AI-режима. Определяет следующего агента."""
    llm_instance = llm(temp=0)
    logger.info("supervisor_node старт")

    # include_raw=True — получаем AIMessage с usage_metadata для логирования
    structured_llm = llm_instance.with_structured_output(RouteOutput, include_raw=True)

    last_message = state["messages"][-1].content
    logger.info(f"supervisor_node входное сообщение last_message_len={len(last_message)}")

    system_message = {"role": "system", "content": SUPERVISOR_PROMPT}
    user_message = {"role": "user", "content": last_message}

    created_at = datetime.now(timezone.utc)
    start_ts = time.monotonic()

    try:
        raw_result: Dict[str, Any] = await structured_llm.ainvoke([system_message, user_message])  # type: ignore[assignment]
        logger.info("supervisor_node LLM ответ получен")
    except Exception as e:
        duration_ms = int((time.monotonic() - start_ts) * 1000)
        logger.error(f"supervisor_node ошибка LLM error={str(e)}")
        await log_llm_request(
            request_id=None,
            user_id=state.get("user_id"),
            chat_id=state.get("chat_id"),
            request_text=str(last_message),
            input_tokens=None,
            output_tokens=None,
            request_status=500,
            model=None,
            created_at=created_at,
            duration_ms=duration_ms,
            initiator="supervisor",
            error_msg=str(e),
        )
        return {"next_agent": "data_analyst"}

    duration_ms = int((time.monotonic() - start_ts) * 1000)

    raw_ai_message = raw_result.get("raw")
    response = raw_result.get("parsed")
    meta = get_llm_request_metadata(raw_ai_message) if raw_ai_message else None

    await log_llm_request(
        request_id=meta.request_id if meta else None,
        user_id=state.get("user_id"),
        chat_id=state.get("chat_id"),
        request_text=str(last_message),
        input_tokens=meta.input_tokens if meta else None,
        output_tokens=meta.output_tokens if meta else None,
        request_status=200,
        model=meta.model_name if meta else None,
        created_at=created_at,
        duration_ms=duration_ms,
        initiator="supervisor",
        error_msg=raw_result.get("parsing_error"),
    )

    if isinstance(response, dict):
        next_agent = response.get("next_agent", "data_analyst")
    else:
        next_agent = getattr(response, "next_agent", "data_analyst")

    logger.info(f"supervisor_node raw next_agent={next_agent}")

    if next_agent not in ["data_analyst", "finance_agent"]:
        logger.warning(f"supervisor_node некорректный next_agent={next_agent} fallback=data_analyst")
        next_agent = "data_analyst"

    logger.info(f"supervisor_node финальный маршрут next_agent={next_agent}")

    return {"next_agent": next_agent}