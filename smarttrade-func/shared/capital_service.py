from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from shared.config import Settings
from shared.helpers import log_event, mask_sensitive
from shared.models import NormalizedWebhook

INSTRUMENT_ALIASES = {
    "GOLD": "metal_gold_spot",
    "XAUUSD": "metal_gold_spot",
    "SILVER": "metal_silver_spot",
    "XAGUSD": "metal_silver_spot",
    "EURUSD": "forex_eurusd",
    "GBPUSD": "forex_gbpusd",
    "USDJPY": "forex_usdjpy",
    "AAPL": "stock_aapl",
    "MSFT": "stock_msft",
    "TSLA": "stock_tsla",
}

INSTRUMENTS: Dict[str, Dict[str, str]] = {
    "forex_eurusd": {
        "group": "forex",
        "label": "EUR/USD",
        "search_term": "EUR/USD",
        "preferred_epic": "",
    },
    "forex_gbpusd": {
        "group": "forex",
        "label": "GBP/USD",
        "search_term": "GBP/USD",
        "preferred_epic": "",
    },
    "forex_usdjpy": {
        "group": "forex",
        "label": "USD/JPY",
        "search_term": "USD/JPY",
        "preferred_epic": "",
    },
    "metal_gold_spot": {
        "group": "metals",
        "label": "Gold Spot",
        "search_term": "Gold",
        "preferred_epic": "",
    },
    "metal_silver_spot": {
        "group": "metals",
        "label": "Silver Spot",
        "search_term": "Silver",
        "preferred_epic": "",
    },
    "metal_gld": {
        "group": "metals",
        "label": "Gold ETF (GLD)",
        "search_term": "GLD",
        "preferred_epic": "",
    },
    "metal_slv": {
        "group": "metals",
        "label": "Silver ETF (SLV)",
        "search_term": "SLV",
        "preferred_epic": "",
    },
    "stock_aapl": {
        "group": "stocks",
        "label": "Apple (AAPL)",
        "search_term": "AAPL",
        "preferred_epic": "",
    },
    "stock_msft": {
        "group": "stocks",
        "label": "Microsoft (MSFT)",
        "search_term": "MSFT",
        "preferred_epic": "",
    },
    "stock_tsla": {
        "group": "stocks",
        "label": "Tesla (TSLA)",
        "search_term": "TSLA",
        "preferred_epic": "",
    },
}


class CapitalTradingService:
    """Refactor of the original `automation.py` service for Azure Functions background execution."""

    def __init__(self, settings: Settings, logger: Optional[logging.Logger] = None) -> None:
        self._settings = settings
        self._logger = logger or logging.getLogger(__name__)
        self._lock = RLock()
        self._http = requests.Session()
        self._configure_session()

        self._api_key = settings.capital_api_key
        self._password = settings.capital_password
        self._identifier = settings.capital_identifier

        self._cst = ""
        self._security_token = ""
        self._market_cache: Dict[str, Dict[str, Any]] = {}
        self._market_by_epic_cache: Dict[str, Dict[str, Any]] = {}
        self._account_preferences_cache: Dict[str, Any] = {}

        self._apply_env_overrides()

    def _apply_env_overrides(self) -> None:
        preferred_map = {
            "forex_eurusd": "EPIC_FOREX_EURUSD",
            "forex_gbpusd": "EPIC_FOREX_GBPUSD",
            "forex_usdjpy": "EPIC_FOREX_USDJPY",
            "metal_gold_spot": "EPIC_METAL_GOLD_SPOT",
            "metal_silver_spot": "EPIC_METAL_SILVER_SPOT",
            "metal_gld": "EPIC_METAL_GLD",
            "metal_slv": "EPIC_METAL_SLV",
            "stock_aapl": "EPIC_STOCK_AAPL",
            "stock_msft": "EPIC_STOCK_MSFT",
            "stock_tsla": "EPIC_STOCK_TSLA",
        }
        for key, env_name in preferred_map.items():
            value = os.getenv(env_name, "").strip()
            if value:
                INSTRUMENTS[key]["preferred_epic"] = value

    def _configure_session(self) -> None:
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "DELETE"),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._http.mount("https://", adapter)
        self._http.mount("http://", adapter)

    def _safe_float(self, value: Any) -> float:
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _clean_message(self, text: str) -> str:
        if not text:
            return ""
        txt = text.replace("<br>", " ").replace("<br/>", " ")
        txt = re.sub(r"<[^>]+>", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _instrument_spec(self, instrument_key: str) -> Dict[str, str]:
        key = str(instrument_key or "").strip()
        key = INSTRUMENT_ALIASES.get(key.upper(), key)
        spec = INSTRUMENTS.get(key)
        if not spec:
            raise ValueError("Unknown instrument selected.")
        return spec

    def _normalize_instrument_key(self, instrument_key: str) -> str:
        key = str(instrument_key or "").strip()
        if ":" in key:
            key = key.split(":", 1)[1].strip()
        return INSTRUMENT_ALIASES.get(key.upper(), key)

    def _resolve_order_quantity(self, webhook: NormalizedWebhook) -> float:
        quantity = webhook.quantity if webhook.quantity > 0 else 1.0
        instrument_candidates = (
            webhook.instrument,
            webhook.ticker,
            self._settings.default_instrument,
        )
        normalized_keys = {
            self._normalize_instrument_key(candidate) for candidate in instrument_candidates if candidate
        }
        if "metal_gold_spot" in normalized_keys:
            return 10.0
        if "metal_silver_spot" in normalized_keys:
            return 1000.0
        return quantity

    def _opposite_action(self, action: str) -> str:
        normalized = self._normalize_action(action)
        if normalized == "BUY":
            return "SELL"
        if normalized == "SELL":
            return "BUY"
        return ""

    def _hedging_mode_enabled(self) -> bool:
        cached = self._account_preferences_cache
        now_ts = datetime.now(timezone.utc).timestamp()
        if cached and now_ts < float(cached.get("expires_at", 0)):
            return bool((cached.get("value") or {}).get("hedgingMode"))
        preferences = self._request("GET", "/api/v1/accounts/preferences")
        self._account_preferences_cache = {
            "value": preferences,
            "expires_at": now_ts + 30,
        }
        return bool((preferences or {}).get("hedgingMode"))

    def _close_single_position(
        self,
        deal_id: str,
        epic: str,
        direction: str,
        current_size: float,
        quantity: float = 0.0,
    ) -> dict[str, Any]:
        full_close = quantity <= 0 or quantity >= current_size - 1e-9
        if full_close:
            result = self._request("DELETE", f"/api/v1/positions/{deal_id}")
            return {
                "ok": True,
                "mode": "full",
                "dealId": deal_id,
                "closed_size": current_size,
                "dealReference": result.get("dealReference"),
            }

        if self._hedging_mode_enabled():
            return {
                "ok": False,
                "error": "partial_close_unsupported",
                "message": "Partial close requires hedgingMode to be disabled on the Capital.com account.",
            }

        reduce_direction = self._opposite_action(direction)
        if not reduce_direction:
            return {"ok": False, "error": "invalid_action", "message": "Unable to determine close direction."}

        payload = {
            "epic": epic,
            "direction": reduce_direction,
            "size": float(quantity),
            "guaranteedStop": False,
        }
        create = self._request("POST", "/api/v1/positions", payload=payload)
        deal_reference = str(create.get("dealReference", "")).strip()
        confirm = self._confirm_by_reference(deal_reference) if deal_reference else {}
        return {
            "ok": True,
            "mode": "partial",
            "dealId": deal_id,
            "closed_size": float(quantity),
            "remaining_size": max(current_size - float(quantity), 0.0),
            "dealReference": deal_reference,
            "confirm": confirm,
            "sent": payload,
        }

    def _must_have_credentials(self) -> None:
        if not self._api_key:
            raise RuntimeError("CAPITAL_API_KEY is missing.")
        if not self._password:
            raise RuntimeError("CAPITAL_PASSWORD is missing.")
        if not self._identifier:
            raise RuntimeError("CAPITAL_IDENTIFIER is missing.")

    def _apply_runtime_credentials(self, identifier: str = "") -> None:
        if identifier and identifier != self._identifier:
            self._identifier = identifier
            self._cst = ""
            self._security_token = ""

    def _error_text_from_response(self, response: requests.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        message = (
            payload.get("errorCode")
            or payload.get("errorMessage")
            or payload.get("message")
            or response.text
            or f"HTTP {response.status_code}"
        )
        return self._clean_message(str(message))

    def _login(self) -> None:
        self._must_have_credentials()
        log_event(
            self._logger,
            logging.INFO,
            "capital.login.start",
            base_url=self._settings.capital_base_url,
            has_identifier=bool(self._identifier),
            has_api_key=bool(self._api_key),
        )
        response = self._http.post(
            f"{self._settings.capital_base_url}/api/v1/session",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-CAP-API-KEY": self._api_key,
            },
            json={"identifier": self._identifier, "password": self._password},
            timeout=self._settings.capital_request_timeout_seconds,
        )
        if response.status_code >= 400:
            error_text = self._error_text_from_response(response)
            log_event(
                self._logger,
                logging.ERROR,
                "capital.login.failed",
                base_url=self._settings.capital_base_url,
                status_code=response.status_code,
                error=error_text,
            )
            raise RuntimeError(f"Capital.com login failed: {error_text}")
        self._cst = response.headers.get("CST", "")
        self._security_token = response.headers.get("X-SECURITY-TOKEN", "")
        if not self._cst or not self._security_token:
            raise RuntimeError("Capital.com login failed: Missing auth tokens.")
        log_event(
            self._logger,
            logging.INFO,
            "capital.login.succeeded",
            base_url=self._settings.capital_base_url,
        )

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        payload: Optional[dict[str, Any]] = None,
        retry_on_auth: bool = True,
    ) -> dict[str, Any]:
        if not self._cst or not self._security_token:
            self._login()

        response = self._http.request(
            method,
            f"{self._settings.capital_base_url}{path}",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Version": self._settings.capital_api_version,
                "X-CAP-API-KEY": self._api_key,
                "CST": self._cst,
                "X-SECURITY-TOKEN": self._security_token,
            },
            params=params,
            json=payload,
            timeout=self._settings.capital_request_timeout_seconds,
        )
        if response.status_code in {401, 403} and retry_on_auth:
            self._login()
            return self._request(method, path, params=params, payload=payload, retry_on_auth=False)
        if response.status_code >= 400:
            raise RuntimeError(self._error_text_from_response(response))
        if not response.text:
            return {}
        try:
            return response.json()
        except ValueError:
            return {}

    def _normalize_action(self, action: str) -> str:
        value = str(action or "").strip().upper()
        if value in {"BUY", "LONG"}:
            return "BUY"
        if value in {"SELL", "SHORT"}:
            return "SELL"
        return value

    def _confirm_by_reference(self, deal_reference: str) -> dict[str, Any]:
        for _ in range(12):
            try:
                data = self._request("GET", f"/api/v1/confirms/{deal_reference}")
            except RuntimeError:
                data = {}
            if data:
                return data
            time.sleep(0.4)
        return {}

    def _pick_market(self, markets: List[dict[str, Any]], preferred_epic: str) -> Optional[dict[str, Any]]:
        if not markets:
            return None
        if preferred_epic:
            for item in markets:
                if item.get("epic") == preferred_epic:
                    return item
        for item in markets:
            status = str(item.get("snapshot", {}).get("marketStatus", "")).upper()
            if status == "TRADEABLE":
                return item
        return markets[0]

    def _resolve_market(self, instrument_key: str) -> dict[str, Any]:
        cached = self._market_cache.get(instrument_key)
        if cached and datetime.now(timezone.utc).timestamp() < cached["expires_at"]:
            return cached["value"]

        spec = self._instrument_spec(instrument_key)
        search_data = self._request(
            "GET",
            "/api/v1/markets",
            params={"searchTerm": spec["search_term"]},
        )
        markets = search_data.get("markets", []) or []
        pick = self._pick_market(markets, spec.get("preferred_epic", ""))
        if not pick or not str(pick.get("epic", "")).strip():
            raise RuntimeError(f"No Capital.com market found for {spec['label']}.")

        epic = str(pick["epic"]).strip()
        details = self._request("GET", f"/api/v1/markets/{epic}")
        market = {
            "epic": epic,
            "instrument": details.get("instrument", {}) or {},
            "snapshot": details.get("snapshot", {}) or {},
            "dealingRules": details.get("dealingRules", {}) or {},
        }
        self._market_cache[instrument_key] = {
            "value": market,
            "expires_at": datetime.now(timezone.utc).timestamp() + 180,
        }
        return market

    def _market_for_epic(self, epic: str) -> dict[str, Any]:
        cached = self._market_by_epic_cache.get(epic)
        if cached and datetime.now(timezone.utc).timestamp() < cached["expires_at"]:
            return cached["value"]
        details = self._request("GET", f"/api/v1/markets/{epic}")
        market = {
            "epic": epic,
            "instrument": details.get("instrument", {}) or {},
            "snapshot": details.get("snapshot", {}) or {},
            "dealingRules": details.get("dealingRules", {}) or {},
        }
        self._market_by_epic_cache[epic] = {
            "value": market,
            "expires_at": datetime.now(timezone.utc).timestamp() + 30,
        }
        return market

    def _resolve_epic(self, instrument_key: str = "", epic: str = "") -> str:
        if epic:
            return str(epic).strip()
        if not instrument_key:
            raise ValueError("Provide instrument key or epic.")
        market = self._resolve_market(instrument_key)
        return str(market.get("epic", "")).strip()

    def _positions(self) -> List[dict[str, Any]]:
        data = self._request("GET", "/api/v1/positions")
        return data.get("positions", []) or []

    def _positions_for_epic(self, epic: str) -> List[dict[str, Any]]:
        out: List[dict[str, Any]] = []
        for row in self._positions():
            market = row.get("market", {}) or {}
            position = row.get("position", {}) or {}
            if str(market.get("epic", "")).strip() != epic:
                continue
            deal_id = position.get("dealId")
            if not deal_id:
                continue
            out.append(
                {
                    "dealId": str(deal_id),
                    "direction": str(position.get("direction", "")).upper(),
                    "size": self._safe_float(position.get("size")),
                }
            )
        return out

    def _extract_open_dealid_from_confirm(self, confirm: dict[str, Any]) -> str:
        for row in confirm.get("affectedDeals") or []:
            if str(row.get("status", "")).upper() in {"OPEN", "OPENED"}:
                deal_id = str(row.get("dealId") or "").strip()
                if deal_id:
                    return deal_id
        return str(confirm.get("dealId") or "").strip()

    def _format_price(self, value: float) -> str:
        if value <= 0:
            return "0"
        return f"{value:.5f}".rstrip("0").rstrip(".")

    def _distance_to_price(self, rule: Any, market: dict[str, Any], reference_price: float) -> float:
        if isinstance(rule, dict):
            raw_value = rule.get("value")
            unit = str(rule.get("unit", "")).strip().upper()
        else:
            raw_value = rule
            unit = ""
        value = self._safe_float(raw_value)
        if value <= 0:
            return 0.0
        if unit in {"", "PRICE", "AMOUNT"}:
            return value
        if unit == "PERCENTAGE" and reference_price > 0:
            return reference_price * (value / 100.0)
        if unit == "POINTS":
            scaling = self._safe_float((market.get("instrument", {}) or {}).get("scalingFactor"))
            if scaling > 0:
                return value / scaling
            return value
        return 0.0

    def _validate_protective_levels(
        self,
        action: str,
        market: dict[str, Any],
        sl: Optional[float],
        tp: Optional[float],
    ) -> Optional[dict[str, Any]]:
        rules = market.get("dealingRules", {}) or {}
        snapshot = market.get("snapshot", {}) or {}
        min_rule = rules.get("minNormalStopOrLimitDistance") or {}
        bid = self._safe_float(snapshot.get("bid"))
        offer = self._safe_float(snapshot.get("offer"))
        action = self._normalize_action(action)

        def error_result(code: str, level_name: str, submitted: float, comparator: str, required: float) -> dict[str, Any]:
            return {
                "ok": False,
                "error": code,
                "message": (
                    f"{level_name} {self._format_price(submitted)} is invalid for {action} on {market.get('epic', '')}. "
                    f"It must be {comparator} {self._format_price(required)} based on current market rules."
                ),
                "details": {
                    "epic": market.get("epic", ""),
                    "submitted": submitted,
                    "required": required,
                    "bid": bid,
                    "offer": offer,
                },
            }

        if tp is not None and tp > 0:
            reference = offer if action == "BUY" else bid
            distance = self._distance_to_price(min_rule, market, reference)
            if reference > 0 and distance > 0:
                required = reference + distance if action == "BUY" else reference - distance
                if action == "BUY" and tp < required:
                    return error_result("invalid_takeprofit", "Take-profit", tp, ">=", required)
                if action == "SELL" and tp > required:
                    return error_result("invalid_takeprofit", "Take-profit", tp, "<=", required)

        if sl is not None and sl > 0:
            reference = bid if action == "BUY" else offer
            distance = self._distance_to_price(min_rule, market, reference)
            if reference > 0 and distance > 0:
                required = reference - distance if action == "BUY" else reference + distance
                if action == "BUY" and sl > required:
                    return error_result("invalid_stoploss", "Stop-loss", sl, "<=", required)
                if action == "SELL" and sl < required:
                    return error_result("invalid_stoploss", "Stop-loss", sl, ">=", required)
        return None

    def place_market_order_epic(
        self,
        action: str,
        quantity: float,
        epic: str,
        identifier: str = "",
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        guaranteed_stop: bool = False,
        attach_if_missing: bool = True,
    ) -> dict[str, Any]:
        action = self._normalize_action(action)
        epic = str(epic or "").strip()
        if action not in {"BUY", "SELL"}:
            return {"ok": False, "error": "invalid_action", "message": "Action must be BUY or SELL."}
        if not epic:
            return {"ok": False, "error": "invalid_epic", "message": "epic is required."}
        if quantity <= 0:
            return {"ok": False, "error": "invalid_quantity", "message": "Quantity must be greater than zero."}

        with self._lock:
            self._apply_runtime_credentials(identifier)
            log_event(
                self._logger,
                logging.INFO,
                "capital.order.submit_start",
                epic=epic,
                action=action,
                quantity=float(quantity),
                sl=self._safe_float(sl) if sl is not None else None,
                tp=self._safe_float(tp) if tp is not None else None,
            )
            if sl is not None or tp is not None:
                market = self._market_for_epic(epic)
                validation_error = self._validate_protective_levels(action, market, sl, tp)
                if validation_error:
                    log_event(
                        self._logger,
                        logging.ERROR,
                        "capital.order.validation_failed",
                        epic=epic,
                        action=action,
                        result=mask_sensitive(validation_error),
                    )
                    return validation_error

            payload: dict[str, Any] = {
                "epic": epic,
                "direction": action,
                "size": float(quantity),
                "orderType": "MARKET",
                "guaranteedStop": bool(guaranteed_stop),
            }
            sl_value = self._safe_float(sl)
            tp_value = self._safe_float(tp)
            if sl is not None and sl_value > 0:
                payload["stopLevel"] = sl_value
            if tp is not None and tp_value > 0:
                payload["profitLevel"] = tp_value

            try:
                create = self._request("POST", "/api/v1/positions", payload=payload)
            except Exception as exc:
                log_event(
                    self._logger,
                    logging.ERROR,
                    "capital.order.submit_failed",
                    epic=epic,
                    action=action,
                    quantity=float(quantity),
                    error=self._clean_message(str(exc)),
                )
                return {"ok": False, "error": "order_rejected", "message": self._clean_message(str(exc))}

            deal_reference = str(create.get("dealReference", "")).strip()
            confirm = self._confirm_by_reference(deal_reference) if deal_reference else {}
            opened_deal_id = self._extract_open_dealid_from_confirm(confirm) if confirm else ""

            attach_result: Optional[dict[str, Any]] = None
            if attach_if_missing and confirm and tp_value > 0:
                has_profit = confirm.get("profitLevel") is not None
                if not has_profit:
                    deal_id = self._extract_open_dealid_from_confirm(confirm)
                    if deal_id:
                        try:
                            attach_payload: dict[str, Any] = {}
                            if sl_value > 0:
                                attach_payload["stopLevel"] = sl_value
                            if tp_value > 0:
                                attach_payload["profitLevel"] = tp_value
                            if attach_payload:
                                update = self._request(
                                    "PUT",
                                    f"/api/v1/positions/{deal_id}",
                                    payload=attach_payload,
                                )
                                attach_result = {
                                    "ok": True,
                                    "dealReference": update.get("dealReference"),
                                    "sent": attach_payload,
                                }
                        except Exception as exc:
                            attach_result = {
                                "ok": False,
                                "message": self._clean_message(str(exc)),
                                "sent": {"stopLevel": sl_value, "profitLevel": tp_value},
                            }

            result: dict[str, Any] = {
                "ok": True,
                "message": "Market order submitted.",
                "dealId": opened_deal_id,
                "dealReference": deal_reference,
                "confirm": confirm,
                "sent": payload,
            }
            log_event(
                self._logger,
                logging.INFO,
                "capital.order.submit_succeeded",
                epic=epic,
                action=action,
                quantity=float(quantity),
                deal_reference=deal_reference,
                confirmed=bool(confirm),
            )
            if attach_result is not None:
                result["attach"] = attach_result
            return result

    def close_positions(
        self,
        side: str = "",
        epic: str = "",
        deal_id: str = "",
        instrument_key: str = "",
        identifier: str = "",
        quantity: float = 0.0,
        quantity_percent: float = 0.0,
    ) -> dict[str, Any]:
        with self._lock:
            self._apply_runtime_credentials(identifier)
            side = self._normalize_action(side) if side else ""
            if side and side not in {"BUY", "SELL"}:
                return {"ok": False, "error": "invalid_action", "message": "side must be BUY/SELL when provided."}
            close_quantity = self._safe_float(quantity)
            close_percent = self._safe_float(quantity_percent)
            if close_percent < 0:
                return {"ok": False, "error": "invalid_quantity_percent", "message": "quantity_percent must be non-negative."}
            if close_percent > 100:
                return {"ok": False, "error": "invalid_quantity_percent", "message": "quantity_percent cannot exceed 100."}

            resolved_epic = ""
            if epic or instrument_key:
                try:
                    resolved_epic = self._resolve_epic(instrument_key=instrument_key, epic=epic)
                except Exception:
                    resolved_epic = str(epic or "").strip()

            if deal_id:
                try:
                    details = self._request("GET", f"/api/v1/positions/{str(deal_id).strip()}")
                    position = details.get("position", {}) or {}
                    market = details.get("market", {}) or {}
                    close_result = self._close_single_position(
                        deal_id=str(deal_id).strip(),
                        epic=str(market.get("epic", "")).strip(),
                        direction=str(position.get("direction", "")).upper(),
                        current_size=self._safe_float(position.get("size")),
                        quantity=(
                            self._safe_float(position.get("size")) * close_percent / 100.0
                            if close_percent > 0
                            else close_quantity
                        ),
                    )
                    if close_result.get("ok"):
                        return {
                            "ok": True,
                            "message": "Position close request sent.",
                            "closed_deals": [str(deal_id).strip()],
                            "details": [close_result],
                        }
                    return close_result
                except Exception as exc:
                    return {"ok": False, "error": "close_failed", "message": self._clean_message(str(exc))}

            targets: List[dict[str, Any]] = []
            for row in self._positions():
                position = row.get("position", {}) or {}
                market = row.get("market", {}) or {}
                if side and str(position.get("direction", "")).upper() != side:
                    continue
                if resolved_epic and str(market.get("epic", "")).strip() != resolved_epic:
                    continue
                if position.get("dealId"):
                    targets.append(
                        {
                            "dealId": str(position["dealId"]),
                            "direction": str(position.get("direction", "")).upper(),
                            "size": self._safe_float(position.get("size")),
                            "epic": str(market.get("epic", "")).strip(),
                        }
                    )

            if not targets:
                return {"ok": True, "message": "No matching open positions found.", "closed_deals": []}

            closed: List[str] = []
            errors: List[str] = []
            details: List[dict[str, Any]] = []
            remaining_to_close = close_quantity
            for target in targets:
                size_to_close = 0.0
                if close_percent > 0:
                    size_to_close = target["size"] * close_percent / 100.0
                elif close_quantity > 0:
                    if remaining_to_close <= 0:
                        break
                    size_to_close = min(target["size"], remaining_to_close)
                try:
                    close_result = self._close_single_position(
                        deal_id=target["dealId"],
                        epic=target["epic"],
                        direction=target["direction"],
                        current_size=target["size"],
                        quantity=size_to_close,
                    )
                    if not close_result.get("ok"):
                        errors.append(
                            f"{target['dealId']}: {self._clean_message(str(close_result.get('message') or close_result.get('error') or 'close_failed'))}"
                        )
                        continue
                    details.append(close_result)
                    closed.append(target["dealId"])
                    if close_percent <= 0 and close_quantity > 0:
                        remaining_to_close = max(remaining_to_close - float(close_result.get("closed_size") or 0.0), 0.0)
                except Exception as exc:
                    errors.append(f"{target['dealId']}: {self._clean_message(str(exc))}")

            if errors and not closed:
                return {"ok": False, "error": "close_failed", "message": "; ".join(errors), "closed_deals": []}
            if errors:
                return {
                    "ok": True,
                    "message": "Some positions were closed, some failed.",
                    "closed_deals": closed,
                    "details": details,
                    "errors": errors,
                }
            return {"ok": True, "message": "Position close request sent.", "closed_deals": closed, "details": details}

    def close_opposites_then_open(
        self,
        desired_action: str,
        quantity: float,
        instrument_key: str = "",
        epic: str = "",
        identifier: str = "",
        sl: Optional[float] = None,
        tp: Optional[float] = None,
    ) -> dict[str, Any]:
        desired_action = self._normalize_action(desired_action)
        if desired_action not in {"BUY", "SELL"}:
            return {"ok": False, "error": "invalid_action", "message": "Action must be BUY or SELL."}
        if quantity <= 0:
            return {"ok": False, "error": "invalid_quantity", "message": "Quantity must be greater than zero."}

        with self._lock:
            self._apply_runtime_credentials(identifier)
            try:
                resolved_epic = self._resolve_epic(instrument_key=instrument_key, epic=epic)
            except Exception as exc:
                return {"ok": False, "error": "invalid_payload", "message": self._clean_message(str(exc))}

            existing = self._positions_for_epic(resolved_epic)
            opposites = [
                row["dealId"] for row in existing if row.get("direction") and row["direction"] != desired_action
            ]
            log_event(
                self._logger,
                logging.INFO,
                "capital.reverse.positions_scanned",
                desired_action=desired_action,
                epic=resolved_epic,
                open_positions=len(existing),
                opposite_positions=len(opposites),
            )

            close_errors: List[str] = []
            for deal_id in opposites:
                try:
                    log_event(
                        self._logger,
                        logging.INFO,
                        "capital.reverse.close_attempt",
                        deal_id=deal_id,
                        epic=resolved_epic,
                    )
                    self._request("DELETE", f"/api/v1/positions/{deal_id}")
                except Exception as exc:
                    close_errors.append(self._clean_message(str(exc)))

            if close_errors and opposites:
                return {
                    "ok": False,
                    "error": "close_failed",
                    "message": "Failed closing opposite position(s): " + "; ".join(close_errors),
                    "closed_attempts": opposites,
                }

            opened = self.place_market_order_epic(
                action=desired_action,
                quantity=quantity,
                epic=resolved_epic,
                identifier=identifier,
                sl=sl,
                tp=tp,
            )
            if not opened.get("ok"):
                opened["closed_opposites"] = opposites
                return opened
            return {
                "ok": True,
                "message": "Flip executed (closed opposites first, then opened new).",
                "closed_opposites": opposites,
                "opened": opened,
            }

    def execute_webhook(self, webhook: NormalizedWebhook, request_id: str, dedupe_key: str) -> dict[str, Any]:
        # Azure Functions change: secret validation moved to the HTTP ingress function.
        started = time.perf_counter()
        identifier = webhook.effective_identifier()
        event = webhook.event
        action = webhook.action
        side = webhook.side
        requested_quantity = webhook.quantity if webhook.quantity > 0 else 0.0
        requested_quantity_percent = webhook.quantity_percent if webhook.quantity_percent and webhook.quantity_percent > 0 else 0.0
        instrument_key = webhook.instrument or self._settings.default_instrument
        epic = webhook.epic
        deal_id = webhook.deal_id
        sl = webhook.sl
        tp = webhook.tp

        if not event:
            if action in {"BUY", "SELL"} or side in {"BUY", "SELL", "LONG", "SHORT"}:
                event = "entry"
            else:
                event = "close"

        open_events = {"entry", "open", "long", "short", "buy", "sell", "reversal", "reverse", "flip"}
        close_events = {"close", "exit"}
        quantity = self._resolve_order_quantity(webhook) if event in open_events else requested_quantity

        log_event(
            self._logger,
            logging.INFO,
            "capital.execution.start",
            request_id=request_id,
            dedupe_key=dedupe_key,
            route_event=event,
            action=action,
            side=side,
            instrument=instrument_key,
            quantity=quantity,
            quantity_percent=requested_quantity_percent,
            payload=mask_sensitive(webhook.raw),
        )

        if webhook.price is not None:
            log_event(
                self._logger,
                logging.INFO,
                "capital.market_price_ignored",
                request_id=request_id,
                dedupe_key=dedupe_key,
                input_price=webhook.price,
                instrument=instrument_key,
                route_event=event,
            )

        if event in open_events:
            open_action = action or side
            if not open_action and event in {"long", "buy"}:
                open_action = "BUY"
            if not open_action and event in {"short", "sell"}:
                open_action = "SELL"
            result = self.close_opposites_then_open(
                desired_action=open_action,
                quantity=quantity,
                instrument_key=instrument_key,
                epic=epic,
                identifier=identifier,
                sl=sl,
                tp=tp,
            )
        elif event in close_events:
            close_side = side or action
            if close_side == "LONG":
                close_side = "BUY"
            elif close_side == "SHORT":
                close_side = "SELL"
            result = self.close_positions(
                side=close_side,
                epic=epic,
                deal_id=deal_id,
                instrument_key=instrument_key,
                identifier=identifier,
                quantity=quantity,
                quantity_percent=requested_quantity_percent,
            )
        else:
            result = {"ok": False, "error": "invalid_payload", "message": f"Unknown event: {event}"}

        log_event(
            self._logger,
            logging.INFO if result.get("ok") else logging.ERROR,
            "capital.execution.finish",
            request_id=request_id,
            dedupe_key=dedupe_key,
            duration_ms=int((time.perf_counter() - started) * 1000),
            result=mask_sensitive(result),
        )
        return result
