from __future__ import annotations

import time
from typing import Callable, TypeVar

from shared.logging import get_logger

T = TypeVar("T")

log = get_logger(__name__)


def retry(
    fn: Callable[[], T],
    *,
    name: str,
    max_attempts: int = 20,
    initial_sleep_s: float = 0.5,
    max_sleep_s: float = 10.0,
) -> T:
    sleep_s = initial_sleep_s
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 (intentional retry wrapper)
            last_exc = e
            if attempt >= max_attempts:
                break
            log.info(
                "%s failed; retrying",
                name,
                extra={"attempt": attempt, "max_attempts": max_attempts, "sleep_s": sleep_s},
            )
            time.sleep(sleep_s)
            sleep_s = min(max_sleep_s, sleep_s * 1.7)

    assert last_exc is not None
    raise last_exc

