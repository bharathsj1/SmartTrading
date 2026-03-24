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
    milliseconds_between,
    safe_json_dumps,
    summarize_payload,
    summarize_result,
    utc_now_iso,
)
from shared.models import NormalizedWebhook, QueueEnvelope
from shared.storage import QueuePublisher, TradingStateStore, ensure_runtime_infrastructure

SETTINGS = get_settings()
LOGGER = logging.getLogger("trading.azure_functions")
STATE_STORE = TradingStateStore(SETTINGS, logger=LOGGER)
QUEUE_PUBLISHER = QueuePublisher(SETTINGS, logger=LOGGER)
CAPITAL_SERVICE = CapitalTradingService(SETTINGS, logger=LOGGER)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
OPEN_EVENTS = {"entry", "open", "long", "short", "buy", "sell", "reversal", "reverse", "flip"}
CLOSE_EVENTS = {"close", "exit"}


def _json_response(body: dict[str, Any], status_code: int) -> func.HttpResponse:
    return func.HttpResponse(
        safe_json_dumps(body),
        status_code=status_code,
        mimetype="application/json",
    )


def _normalize_trade_side(value: str) -> str:
    side = str(value or "").strip().upper()
    if side in {"BUY", "LONG"}:
        return "BUY"
    if side in {"SELL", "SHORT"}:
        return "SELL"
    return side


def _route_event(webhook: NormalizedWebhook) -> str:
    event = str(webhook.event or "").strip().lower()
    if event:
        return event
    if webhook.action in {"BUY", "SELL"} or webhook.side in {"BUY", "SELL", "LONG", "SHORT"}:
        return "entry"
    return "close"


def _trade_side_for_webhook(webhook: NormalizedWebhook) -> str:
    route_event = _route_event(webhook)
    if route_event in OPEN_EVENTS:
        return _normalize_trade_side(webhook.action or webhook.side or route_event)
    return _normalize_trade_side(webhook.side or webhook.action)


def _active_trade_key_for_webhook(webhook: NormalizedWebhook) -> str:
    side = _trade_side_for_webhook(webhook)
    if not side or not webhook.instrument:
        return ""
    return STATE_STORE.make_active_trade_key(
        strategy=webhook.strategy,
        instrument=webhook.instrument,
        side=side,
        account=webhook.account,
    )


def _payload_with_deal_id(webhook: NormalizedWebhook, deal_id: str) -> NormalizedWebhook:
    if not deal_id or webhook.deal_id:
        return webhook
    payload = dict(webhook.raw)
    payload["deal_id"] = deal_id
    return NormalizedWebhook.from_dict(payload)


def _extract_opened_deal_id(result: dict[str, Any]) -> str:
    direct = str(result.get("dealId") or "").strip()
    if direct:
        return direct
    opened = result.get("opened") or {}
    return str(opened.get("dealId") or "").strip()


def _deal_closed_fully(result: dict[str, Any], deal_id: str) -> bool:
    target_deal_id = str(deal_id or "").strip()
    if not target_deal_id:
        return False
    for detail in result.get("details") or []:
        if str(detail.get("dealId") or "").strip() != target_deal_id:
            continue
        return str(detail.get("mode") or "").strip().lower() == "full"
    return False


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
    log_event(
        LOGGER,
        logging.INFO,
        "webhook.received",
        request_id=request_id,
        instrument=webhook.instrument,
        webhook_event=webhook.event,
        action=webhook.action,
        side=webhook.side,
        quantity=webhook.quantity,
        quantity_percent=webhook.quantity_percent,
        strategy=webhook.strategy,
        bar_time=webhook.bar_time,
        comment=webhook.comment,
        payload_summary=summarize_payload(webhook.raw),
    )
    if SETTINGS.tradingview_webhook_secret and webhook.secret != SETTINGS.tradingview_webhook_secret:
        log_event(
            LOGGER,
            logging.WARNING,
            "webhook.unauthorized",
            request_id=request_id,
            instrument=webhook.instrument,
            webhook_event=webhook.event,
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

    try:
        reserved = STATE_STORE.reserve_webhook(envelope)
        if not reserved:
            existing = STATE_STORE.get(dedupe_key) or {}
            log_event(
                LOGGER,
                logging.INFO,
                "webhook.duplicate_ignored",
                request_id=request_id,
                dedupe_key=dedupe_key,
                existing_status=str(existing.get("Status") or "").lower(),
                instrument=webhook.instrument,
                webhook_event=webhook.event,
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
        QUEUE_PUBLISHER.enqueue(envelope)
    except Exception as exc:
        STATE_STORE.mark_enqueue_failed(dedupe_key, str(exc))
        log_event(
            LOGGER,
            logging.ERROR,
            "webhook.enqueue_failed",
            request_id=request_id,
            dedupe_key=dedupe_key,
            instrument=webhook.instrument,
            webhook_event=webhook.event,
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
        webhook_event=webhook.event,
        action=webhook.action,
        side=webhook.side,
        instrument=webhook.instrument,
        quantity=webhook.quantity,
        quantity_percent=webhook.quantity_percent,
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
    payload = envelope.payload
    processing_started_at = utc_now_iso()
    queue_latency_ms = milliseconds_between(envelope.received_at, processing_started_at)

    existing = STATE_STORE.get(envelope.dedupe_key)
    if existing and str(existing.get("Status") or "").lower() == "completed":
        log_event(
            LOGGER,
            logging.INFO,
            "worker.duplicate_skipped",
            request_id=envelope.request_id,
            dedupe_key=envelope.dedupe_key,
            status="completed",
            queue_latency_ms=queue_latency_ms,
        )
        return

    dequeue_count = getattr(msg, "dequeue_count", 1) or 1
    log_event(
        LOGGER,
        logging.INFO,
        "worker.message_received",
        request_id=envelope.request_id,
        dedupe_key=envelope.dedupe_key,
        queue_latency_ms=queue_latency_ms,
        existing_status=str((existing or {}).get("Status") or "").lower(),
        dequeue_count=int(dequeue_count),
        instrument=envelope.payload.instrument,
        webhook_event=envelope.payload.event,
        action=envelope.payload.action,
        side=envelope.payload.side,
    )
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
        trade_key = _active_trade_key_for_webhook(payload)
        route_event = _route_event(payload)
        if route_event in CLOSE_EVENTS and not payload.deal_id and trade_key:
            active_trade = STATE_STORE.get_active_trade(trade_key)
            active_deal_id = str((active_trade or {}).get("DealId") or "").strip()
            if active_deal_id:
                payload = _payload_with_deal_id(payload, active_deal_id)
                log_event(
                    LOGGER,
                    logging.INFO,
                    "worker.active_trade_resolved",
                    request_id=envelope.request_id,
                    dedupe_key=envelope.dedupe_key,
                    trade_key=trade_key,
                    deal_id=active_deal_id,
                    instrument=payload.instrument,
                    side=_trade_side_for_webhook(payload),
                )

        started = time.perf_counter()
        result = CAPITAL_SERVICE.execute_webhook(
            payload,
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
                result_summary=summarize_result(result),
            )
            raise RuntimeError(safe_json_dumps(result))

        STATE_STORE.mark_completed(envelope.dedupe_key, result)
        if route_event in OPEN_EVENTS and trade_key:
            opened_deal_id = _extract_opened_deal_id(result)
            if opened_deal_id:
                trade_side = _trade_side_for_webhook(payload)
                STATE_STORE.upsert_active_trade(
                    trade_key=trade_key,
                    deal_id=opened_deal_id,
                    strategy=payload.strategy,
                    instrument=payload.instrument,
                    side=trade_side,
                    account=payload.account,
                    epic=payload.epic,
                )
                opposite_side = "SELL" if trade_side == "BUY" else "BUY" if trade_side == "SELL" else ""
                if opposite_side:
                    opposite_trade_key = STATE_STORE.make_active_trade_key(
                        strategy=payload.strategy,
                        instrument=payload.instrument,
                        side=opposite_side,
                        account=payload.account,
                    )
                    STATE_STORE.clear_active_trade(opposite_trade_key)
        elif route_event in CLOSE_EVENTS and trade_key and _deal_closed_fully(result, payload.deal_id):
            STATE_STORE.clear_active_trade(trade_key)
        log_event(
            LOGGER,
            logging.INFO,
            "worker.execution_timing",
            request_id=envelope.request_id,
            dedupe_key=envelope.dedupe_key,
            queue_latency_ms=queue_latency_ms,
            execution_duration_ms=execution_duration_ms,
            end_to_end_latency_ms=end_to_end_latency_ms,
            result_summary=summarize_result(result),
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
    existing = STATE_STORE.get(envelope.dedupe_key) or {}
    previous_error = str(existing.get("LastError") or "").strip()
    poison_reason = "Moved to poison queue after repeated failures."
    STATE_STORE.mark_poisoned(
        envelope.dedupe_key,
        poison_reason,
        int(dequeue_count),
        previous_error=previous_error,
    )
    log_event(
        LOGGER,
        logging.ERROR,
        "worker.poison_message",
        request_id=envelope.request_id,
        dedupe_key=envelope.dedupe_key,
        previous_error=previous_error,
        poison_reason=poison_reason,
        payload_summary=summarize_payload(envelope.payload.raw),
    )
