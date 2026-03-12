from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Mapping, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def milliseconds_between(start_iso: str, end_iso: str) -> Optional[int]:
    try:
        start = datetime.fromisoformat(str(start_iso).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(end_iso).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), default=str)


def truncate_text(value: str, limit: int = 32000) -> str:
    txt = str(value or "")
    if len(txt) <= limit:
        return txt
    return txt[: limit - 3] + "..."


def mask_sensitive(value: Any) -> Any:
    if isinstance(value, Mapping):
        masked: dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if lowered in {"secret", "password", "api_key", "apikey", "token", "cst", "x-security-token"}:
                masked[str(key)] = "***"
            else:
                masked[str(key)] = mask_sensitive(item)
        return masked
    if isinstance(value, list):
        return [mask_sensitive(item) for item in value]
    return value


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, safe_json_dumps({"event": event, **fields}))
