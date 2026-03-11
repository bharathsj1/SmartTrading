from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    capital_base_url: str
    capital_api_key: str
    capital_password: str
    capital_identifier: str
    capital_api_version: str
    tradingview_webhook_secret: str
    azure_webjobs_storage: str
    webhook_queue_name: str
    worker_poison_queue_name: str
    trading_state_table_name: str
    default_instrument: str
    capital_request_timeout_seconds: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    webhook_queue_name = os.getenv("WEBHOOK_QUEUE_NAME", "tradingview-webhooks").strip()
    return Settings(
        capital_base_url=os.getenv(
            "CAPITAL_BASE_URL", "https://demo-api-capital.backend-capital.com"
        ).rstrip("/"),
        capital_api_key=os.getenv("CAPITAL_API_KEY", "").strip(),
        capital_password=os.getenv("CAPITAL_PASSWORD", "").strip(),
        capital_identifier=os.getenv("CAPITAL_IDENTIFIER", "").strip(),
        capital_api_version=os.getenv("CAPITAL_API_VERSION", "1").strip(),
        tradingview_webhook_secret=os.getenv("TRADINGVIEW_WEBHOOK_SECRET", "").strip(),
        azure_webjobs_storage=os.getenv("AzureWebJobsStorage", "").strip(),
        webhook_queue_name=webhook_queue_name,
        worker_poison_queue_name=os.getenv(
            "WORKER_POISON_QUEUE_NAME", f"{webhook_queue_name}-poison"
        ).strip(),
        trading_state_table_name=os.getenv(
            "TRADING_STATE_TABLE_NAME", "TradingWebhookState"
        ).strip(),
        default_instrument=os.getenv("DEFAULT_INSTRUMENT", "forex_eurusd").strip(),
        capital_request_timeout_seconds=int(
            os.getenv("CAPITAL_REQUEST_TIMEOUT_SECONDS", "25").strip()
        ),
    )
