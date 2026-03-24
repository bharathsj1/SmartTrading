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


def summarize_payload(value: Any) -> Any:
    masked = mask_sensitive(value)
    if not isinstance(masked, Mapping):
        return masked
    keys = (
        "event",
        "action",
        "side",
        "instrument",
        "ticker",
        "strategy",
        "account",
        "quantity",
        "qty_percent",
        "price",
        "sl",
        "tp1",
        "tp2",
        "tp3",
        "reason",
        "tf",
        "bar_time",
        "comment",
        "deal_id",
        "dealId",
    )
    return {key: masked[key] for key in keys if key in masked}


def summarize_result(value: Any) -> Any:
    masked = mask_sensitive(value)
    if not isinstance(masked, Mapping):
        return masked
    summary: dict[str, Any] = {}
    keys = (
        "ok",
        "error",
        "message",
        "mode",
        "dealId",
        "dealReference",
        "closed_size",
        "remaining_size",
    )
    for key in keys:
        if key in masked:
            summary[key] = masked[key]
    details = masked.get("details")
    if isinstance(details, list):
        summary["details_count"] = len(details)
    opened = masked.get("opened")
    if isinstance(opened, Mapping):
        summary["opened"] = {
            key: opened.get(key)
            for key in ("ok", "dealId", "dealReference", "message")
            if key in opened
        }
    closed_opposites = masked.get("closed_opposites")
    if isinstance(closed_opposites, list):
        summary["closed_opposites_count"] = len(closed_opposites)
    timing = masked.get("timing")
    if isinstance(timing, Mapping):
        summary["timing"] = {
            key: timing.get(key)
            for key in (
                "queue_latency_ms",
                "execution_duration_ms",
                "end_to_end_latency_ms",
            )
            if key in timing
        }
    return summary


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, safe_json_dumps({"event": event, **fields}))
