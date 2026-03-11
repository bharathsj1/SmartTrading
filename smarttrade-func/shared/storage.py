from __future__ import annotations

import base64
import json
from typing import Any, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode
from azure.storage.queue import QueueServiceClient

from shared.config import Settings
from shared.helpers import truncate_text, utc_now_iso
from shared.models import QueueEnvelope

STATE_PARTITION_KEY = "tradingview"


class QueuePublisher:
    """Azure Functions change: explicit Queue SDK use gives clearer enqueue error handling and testability."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._queue_service: Optional[QueueServiceClient] = None
        self._queue_client = None
        self._initialized = False

    def _ensure_queue(self) -> None:
        if self._initialized:
            return
        if not self._settings.azure_webjobs_storage:
            raise RuntimeError("AzureWebJobsStorage is missing.")
        if self._queue_service is None:
            self._queue_service = QueueServiceClient.from_connection_string(
                self._settings.azure_webjobs_storage
            )
        if self._queue_client is None:
            self._queue_client = self._queue_service.get_queue_client(
                self._settings.webhook_queue_name
            )
        try:
            self._queue_client.create_queue()
        except ResourceExistsError:
            pass
        self._initialized = True

    def enqueue(self, envelope: QueueEnvelope) -> None:
        self._ensure_queue()
        encoded = base64.b64encode(envelope.to_json().encode("utf-8")).decode("utf-8")
        self._queue_client.send_message(encoded)


class TradingStateStore:
    """Stores webhook idempotency and execution status in Azure Table Storage."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service: Optional[TableServiceClient] = None
        self._table = None

    def _table_client(self):
        if self._table is not None:
            return self._table
        if not self._settings.azure_webjobs_storage:
            raise RuntimeError("AzureWebJobsStorage is missing.")
        if self._service is None:
            self._service = TableServiceClient.from_connection_string(
                self._settings.azure_webjobs_storage
            )
        self._table = self._service.create_table_if_not_exists(
            self._settings.trading_state_table_name
        )
        return self._table

    def _base_entity(self, dedupe_key: str) -> dict[str, Any]:
        return {"PartitionKey": STATE_PARTITION_KEY, "RowKey": dedupe_key}

    def get(self, dedupe_key: str) -> Optional[dict[str, Any]]:
        try:
            return self._table_client().get_entity(STATE_PARTITION_KEY, dedupe_key)
        except ResourceNotFoundError:
            return None

    def reserve_webhook(self, envelope: QueueEnvelope) -> bool:
        entity = {
            **self._base_entity(envelope.dedupe_key),
            "Status": "accepted",
            "RequestId": envelope.request_id,
            "ReceivedAt": envelope.received_at,
            "UpdatedAt": utc_now_iso(),
            "Instrument": envelope.payload.instrument,
            "EventType": envelope.payload.event,
            "Side": envelope.payload.action or envelope.payload.side,
            "Strategy": envelope.payload.strategy,
            "BarTime": envelope.payload.bar_time,
            "Comment": envelope.payload.comment,
            "Attempts": 0,
        }
        try:
            self._table_client().create_entity(entity)
            return True
        except ResourceExistsError:
            existing = self.get(envelope.dedupe_key)
            if not existing:
                return False
            status = str(existing.get("Status") or "").lower()
            if status == "enqueue_failed":
                existing["Status"] = "accepted"
                existing["RequestId"] = envelope.request_id
                existing["ReceivedAt"] = envelope.received_at
                existing["UpdatedAt"] = utc_now_iso()
                self._table_client().upsert_entity(existing, mode=UpdateMode.MERGE)
                return True
            return False

    def mark_enqueued(self, dedupe_key: str) -> None:
        self._table_client().upsert_entity(
            {
                **self._base_entity(dedupe_key),
                "Status": "enqueued",
                "UpdatedAt": utc_now_iso(),
            },
            mode=UpdateMode.MERGE,
        )

    def mark_enqueue_failed(self, dedupe_key: str, error: str) -> None:
        self._table_client().upsert_entity(
            {
                **self._base_entity(dedupe_key),
                "Status": "enqueue_failed",
                "LastError": truncate_text(error),
                "UpdatedAt": utc_now_iso(),
            },
            mode=UpdateMode.MERGE,
        )

    def mark_processing(self, dedupe_key: str, dequeue_count: int) -> None:
        entity = self.get(dedupe_key) or self._base_entity(dedupe_key)
        attempts = int(entity.get("Attempts") or 0) + 1
        self._table_client().upsert_entity(
            {
                **entity,
                "Status": "processing",
                "Attempts": attempts,
                "LastDequeueCount": int(dequeue_count),
                "UpdatedAt": utc_now_iso(),
            },
            mode=UpdateMode.MERGE,
        )

    def mark_completed(self, dedupe_key: str, result: dict[str, Any]) -> None:
        self._table_client().upsert_entity(
            {
                **self._base_entity(dedupe_key),
                "Status": "completed",
                "ResultJson": truncate_text(json.dumps(result, ensure_ascii=True, default=str)),
                "CompletedAt": utc_now_iso(),
                "UpdatedAt": utc_now_iso(),
            },
            mode=UpdateMode.MERGE,
        )

    def mark_failed(self, dedupe_key: str, error: str) -> None:
        self._table_client().upsert_entity(
            {
                **self._base_entity(dedupe_key),
                "Status": "failed",
                "LastError": truncate_text(error),
                "UpdatedAt": utc_now_iso(),
            },
            mode=UpdateMode.MERGE,
        )

    def mark_poisoned(self, dedupe_key: str, error: str, dequeue_count: int) -> None:
        self._table_client().upsert_entity(
            {
                **self._base_entity(dedupe_key),
                "Status": "poisoned",
                "LastError": truncate_text(error),
                "LastDequeueCount": int(dequeue_count),
                "UpdatedAt": utc_now_iso(),
            },
            mode=UpdateMode.MERGE,
        )
