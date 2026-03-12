from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

import azure.functions as func

from shared.capital_service import CapitalTradingService
from shared.config import get_settings
from shared.helpers import (
    log_event,
    mask_sensitive,
    milliseconds_between,
    safe_json_dumps,
    utc_now_iso,
)
from shared.models import NormalizedWebhook, QueueEnvelope
from shared.storage import QueuePublisher, TradingStateStore, ensure_runtime_infrastructure

SETTINGS = get_settings()
LOGGER = logging.getLogger("trading.azure_functions")
try:
    ensure_runtime_infrastructure(SETTINGS, logger=LOGGER)
except Exception as exc:
    LOGGER.warning("storage infrastructure bootstrap failed: %s", exc)
STATE_STORE = TradingStateStore(SETTINGS)
QUEUE_PUBLISHER = QueuePublisher(SETTINGS)
CAPITAL_SERVICE = CapitalTradingService(SETTINGS, logger=LOGGER)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def _json_response(body: dict[str, Any], status_code: int) -> func.HttpResponse:
    return func.HttpResponse(
        safe_json_dumps(body),
        status_code=status_code,
        mimetype="application/json",
    )


@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health(req: func.HttpRequest) -> func.HttpResponse:
    return _json_response(
        {
            "ok": True,
            "status": "healthy",
            "queue": SETTINGS.webhook_queue_name,
            "poison_queue": SETTINGS.worker_poison_queue_name,
            "state_table": SETTINGS.trading_state_table_name,
            "secret_configured": bool(SETTINGS.tradingview_webhook_secret),
        },
        200,
    )


@app.route(route="webhook/tradingview", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def tradingview_webhook(req: func.HttpRequest) -> func.HttpResponse:
    request_id = str(uuid.uuid4())
    received_at = utc_now_iso()

    try:
        body = req.get_json()
    except ValueError as exc:
        log_event(LOGGER, logging.WARNING, "webhook.invalid_json", request_id=request_id, error=str(exc))
        return _json_response(
            {"ok": False, "error": "invalid_json", "message": "Request body must be valid JSON."},
            400,
        )

    webhook = NormalizedWebhook.from_dict(body)
    if SETTINGS.tradingview_webhook_secret and webhook.secret != SETTINGS.tradingview_webhook_secret:
        log_event(
            LOGGER,
            logging.WARNING,
            "webhook.unauthorized",
            request_id=request_id,
            payload=mask_sensitive(webhook.raw),
        )
        return _json_response(
            {"ok": False, "error": "unauthorized", "message": "Invalid webhook secret."},
            401,
        )

    dedupe_key = webhook.dedupe_key()
    envelope = QueueEnvelope(
        request_id=request_id,
        dedupe_key=dedupe_key,
        received_at=received_at,
        payload=webhook,
    )

    if not STATE_STORE.reserve_webhook(envelope):
        log_event(
            LOGGER,
            logging.INFO,
            "webhook.duplicate_ignored",
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=mask_sensitive(webhook.raw),
        )
        return _json_response(
            {
                "ok": True,
                "message": "Webhook accepted",
                "request_id": request_id,
                "duplicate": True,
            },
            200,
        )

    try:
        QUEUE_PUBLISHER.enqueue(envelope)
        STATE_STORE.mark_enqueued(dedupe_key)
    except Exception as exc:
        STATE_STORE.mark_enqueue_failed(dedupe_key, str(exc))
        log_event(
            LOGGER,
            logging.ERROR,
            "webhook.enqueue_failed",
            request_id=request_id,
            dedupe_key=dedupe_key,
            error=str(exc),
        )
        return _json_response(
            {"ok": False, "error": "queue_unavailable", "message": "Failed to enqueue webhook."},
            500,
        )

    log_event(
        LOGGER,
        logging.INFO,
        "webhook.accepted",
        request_id=request_id,
        dedupe_key=dedupe_key,
        payload=mask_sensitive(webhook.raw),
    )
    return _json_response(
        {"ok": True, "message": "Webhook accepted", "request_id": request_id},
        200,
    )

@app.queue_trigger(
    arg_name="msg",
    queue_name=SETTINGS.webhook_queue_name,
    connection="AzureWebJobsStorage",
)
def trading_worker(msg: func.QueueMessage) -> None:
    raw_body = msg.get_body().decode("utf-8")
    envelope = QueueEnvelope.from_dict(json.loads(raw_body))
    processing_started_at = utc_now_iso()
    queue_latency_ms = milliseconds_between(envelope.received_at, processing_started_at)
    existing = STATE_STORE.get(envelope.dedupe_key)
    if existing and str(existing.get("Status") or "").lower() == "completed":
        log_event(
            LOGGER,
            logging.INFO,
            "worker.skip_completed",
            request_id=envelope.request_id,
            dedupe_key=envelope.dedupe_key,
            queue_latency_ms=queue_latency_ms,
        )
        return

    dequeue_count = getattr(msg, "dequeue_count", 1) or 1
    STATE_STORE.mark_processing(envelope.dedupe_key, int(dequeue_count))
    log_event(
        LOGGER,
        logging.INFO,
        "worker.processing_started",
        request_id=envelope.request_id,
        dedupe_key=envelope.dedupe_key,
        dequeue_count=int(dequeue_count),
        queue_latency_ms=queue_latency_ms,
    )

    try:
        started = time.perf_counter()
        result = CAPITAL_SERVICE.execute_webhook(
            envelope.payload,
            request_id=envelope.request_id,
            dedupe_key=envelope.dedupe_key,
        )
        execution_duration_ms = int((time.perf_counter() - started) * 1000)
        completed_at = utc_now_iso()
        end_to_end_latency_ms = milliseconds_between(envelope.received_at, completed_at)
        result["timing"] = {
            "received_at": envelope.received_at,
            "processing_started_at": processing_started_at,
            "completed_at": completed_at,
            "queue_latency_ms": queue_latency_ms,
            "execution_duration_ms": execution_duration_ms,
            "end_to_end_latency_ms": end_to_end_latency_ms,
        }
        if not result.get("ok"):
            STATE_STORE.mark_failed(envelope.dedupe_key, safe_json_dumps(result))
            log_event(
                LOGGER,
                logging.ERROR,
                "worker.execution_timing",
                request_id=envelope.request_id,
                dedupe_key=envelope.dedupe_key,
                queue_latency_ms=queue_latency_ms,
                execution_duration_ms=execution_duration_ms,
                end_to_end_latency_ms=end_to_end_latency_ms,
                result=mask_sensitive(result),
            )
            raise RuntimeError(safe_json_dumps(result))
        STATE_STORE.mark_completed(envelope.dedupe_key, result)
        log_event(
            LOGGER,
            logging.INFO,
            "worker.execution_timing",
            request_id=envelope.request_id,
            dedupe_key=envelope.dedupe_key,
            queue_latency_ms=queue_latency_ms,
            execution_duration_ms=execution_duration_ms,
            end_to_end_latency_ms=end_to_end_latency_ms,
            result=mask_sensitive(result),
        )
    except Exception as exc:
        STATE_STORE.mark_failed(envelope.dedupe_key, str(exc))
        log_event(
            LOGGER,
            logging.ERROR,
            "worker.execution_failed",
            request_id=envelope.request_id,
            dedupe_key=envelope.dedupe_key,
            error=str(exc),
        )
        raise


@app.queue_trigger(
    arg_name="msg",
    queue_name=SETTINGS.worker_poison_queue_name,
    connection="AzureWebJobsStorage",
)
def trading_worker_poison(msg: func.QueueMessage) -> None:
    raw_body = msg.get_body().decode("utf-8")
    envelope = QueueEnvelope.from_dict(json.loads(raw_body))
    dequeue_count = getattr(msg, "dequeue_count", 0) or 0
    STATE_STORE.mark_poisoned(envelope.dedupe_key, "Moved to poison queue after repeated failures.", int(dequeue_count))
    log_event(
        LOGGER,
        logging.ERROR,
        "worker.poison_message",
        request_id=envelope.request_id,
        dedupe_key=envelope.dedupe_key,
        payload=mask_sensitive(envelope.payload.raw),
    )
