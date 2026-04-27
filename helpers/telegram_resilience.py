import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from typing import TypeVar

from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut


logger = logging.getLogger(__name__)
T = TypeVar("T")


def _env_int(name: str, default: int, *, minimum: int = 1, maximum: int = 10) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


def _env_float(name: str, default: float, *, minimum: float = 0.1, maximum: float = 10.0) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        value = float(str(raw_value).strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


def is_stale_callback_query_error(error: BaseException) -> bool:
    if not isinstance(error, BadRequest):
        return False
    message = str(error).lower()
    return (
        "query is too old" in message
        or "response timeout expired" in message
        or "query id is invalid" in message
    )


async def safe_answer_callback_query(query, *args, action: str = "callback_query.answer", **kwargs) -> bool:
    """Answer a callback query without breaking the handler on stale Telegram callbacks."""
    if query is None:
        return False

    try:
        await query.answer(*args, **kwargs)
        return True
    except BadRequest as exc:
        if is_stale_callback_query_error(exc):
            logger.info("Ignored stale callback query in %s: %s", action, exc)
            return False
        raise
    except (NetworkError, TimedOut) as exc:
        logger.warning("Could not answer callback query in %s: %s", action, exc)
        return False


async def telegram_api_call(
    call_factory: Callable[[], Awaitable[T]],
    *,
    action: str,
    attempts: int | None = None,
    base_delay: float | None = None,
) -> T:
    """Retry short-lived Telegram API calls that fail due to transient network issues."""
    max_attempts = attempts or _env_int("BOT_TELEGRAM_API_RETRY_ATTEMPTS", 3, minimum=1, maximum=6)
    retry_delay = base_delay or _env_float("BOT_TELEGRAM_API_RETRY_DELAY", 0.8, minimum=0.1, maximum=5.0)
    last_error: BaseException | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await call_factory()
        except RetryAfter as exc:
            last_error = exc
            delay = max(float(getattr(exc, "retry_after", 1.0)), retry_delay)
            logger.warning(
                "Telegram API rate-limited %s (attempt %s/%s); retrying in %.1fs",
                action,
                attempt,
                max_attempts,
                delay,
            )
        except (NetworkError, TimedOut) as exc:
            last_error = exc
            delay = retry_delay * attempt
            logger.warning(
                "Telegram API transient failure in %s (attempt %s/%s): %s",
                action,
                attempt,
                max_attempts,
                exc,
            )

        if attempt < max_attempts:
            await asyncio.sleep(delay)

    assert last_error is not None
    raise last_error
