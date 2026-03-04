"""
Capital.com TradingView Webhook Bridge (NETTING-SAFE)

✅ Fixes your issue where a BUY after a SELL just closes the short (netting),
   by doing: CLOSE opposite positions FIRST → then OPEN new position.

✅ Accepts TradingView JSON payload with:
   - secret, identifier
   - event: "entry"/"close" (or omitted)
   - action/side: BUY/SELL/LONG/SHORT
   - instrument (your key like "metal_gold_spot") OR epic (Capital market epic)
   - quantity/qty
   - sl/tp (levels) OR stopLevel/limitLevel (levels)

✅ Places MARKET order and (optionally) attaches stopLevel/limitLevel.

IMPORTANT SECURITY:
- You pasted real credentials (API key + password) in chat.
  Rotate them immediately and move them to environment variables.
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

# ---------------------------
# Config (use env vars!)
# ---------------------------
CAPITAL_BASE_URL = os.getenv(
    "CAPITAL_BASE_URL", "https://demo-api-capital.backend-capital.com"
).rstrip("/")
CAPITAL_API_KEY = os.getenv("CAPITAL_API_KEY", "Jm8K70gi5PNGHWpb")
CAPITAL_PASSWORD = os.getenv("CAPITAL_PASSWORD", "Livepresent@26")
CAPITAL_IDENTIFIER = os.getenv("CAPITAL_IDENTIFIER", "bharath.sj1@gmail.com")
CAPITAL_API_VERSION = "1"

WEB_HOST = os.getenv("WEB_HOST", "127.0.0.1").strip()
WEB_PORT = int(os.getenv("WEB_PORT", "8080").strip())

UI_DIR = Path(__file__).resolve().parent / "ui"
DEFAULT_INSTRUMENT = os.getenv("DEFAULT_INSTRUMENT", "forex_eurusd").strip()

TRADINGVIEW_WEBHOOK_SECRET = os.getenv("TRADINGVIEW_WEBHOOK_SECRET", "Smartconnect4u").strip()

INSTRUMENTS: Dict[str, Dict[str, str]] = {
    "forex_eurusd": {
        "group": "forex",
        "label": "EUR/USD",
        "search_term": "EUR/USD",
        "preferred_epic": os.getenv("EPIC_FOREX_EURUSD", "").strip(),
    },
    "forex_gbpusd": {
        "group": "forex",
        "label": "GBP/USD",
        "search_term": "GBP/USD",
        "preferred_epic": os.getenv("EPIC_FOREX_GBPUSD", "").strip(),
    },
    "forex_usdjpy": {
        "group": "forex",
        "label": "USD/JPY",
        "search_term": "USD/JPY",
        "preferred_epic": os.getenv("EPIC_FOREX_USDJPY", "").strip(),
    },
    "metal_gold_spot": {
        "group": "metals",
        "label": "Gold Spot",
        "search_term": "Gold",
        "preferred_epic": os.getenv("EPIC_METAL_GOLD_SPOT", "").strip(),
    },
    "metal_silver_spot": {
        "group": "metals",
        "label": "Silver Spot",
        "search_term": "Silver",
        "preferred_epic": os.getenv("EPIC_METAL_SILVER_SPOT", "").strip(),
    },
    "metal_gld": {
        "group": "metals",
        "label": "Gold ETF (GLD)",
        "search_term": "GLD",
        "preferred_epic": os.getenv("EPIC_METAL_GLD", "").strip(),
    },
    "metal_slv": {
        "group": "metals",
        "label": "Silver ETF (SLV)",
        "search_term": "SLV",
        "preferred_epic": os.getenv("EPIC_METAL_SLV", "").strip(),
    },
    "stock_aapl": {
        "group": "stocks",
        "label": "Apple (AAPL)",
        "search_term": "AAPL",
        "preferred_epic": os.getenv("EPIC_STOCK_AAPL", "").strip(),
    },
    "stock_msft": {
        "group": "stocks",
        "label": "Microsoft (MSFT)",
        "search_term": "MSFT",
        "preferred_epic": os.getenv("EPIC_STOCK_MSFT", "").strip(),
    },
    "stock_tsla": {
        "group": "stocks",
        "label": "Tesla (TSLA)",
        "search_term": "TSLA",
        "preferred_epic": os.getenv("EPIC_STOCK_TSLA", "").strip(),
    },
}

if DEFAULT_INSTRUMENT not in INSTRUMENTS:
    DEFAULT_INSTRUMENT = "forex_eurusd"


# ---------------------------
# Capital.com Service
# ---------------------------
class CapitalTradingService:
    def __init__(self) -> None:
        self._lock = Lock()
        self._http = requests.Session()
        self._api_key = CAPITAL_API_KEY
        self._password = CAPITAL_PASSWORD
        self._identifier = CAPITAL_IDENTIFIER
        self._cst = ""
        self._security_token = ""
        self._market_cache: Dict[str, Dict[str, Any]] = {}
        self._market_failures: Dict[str, Dict[str, Any]] = {}

    def _safe_float(self, value) -> float:
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
        spec = INSTRUMENTS.get(instrument_key)
        if not spec:
            raise ValueError("Unknown instrument selected.")
        return spec

    def _must_have_credentials(self) -> None:
        if not self._api_key:
            raise RuntimeError("CAPITAL_API_KEY is missing.")
        if not self._password:
            raise RuntimeError("CAPITAL_PASSWORD is missing.")
        if not self._identifier:
            raise RuntimeError("CAPITAL_IDENTIFIER is missing.")

    def _apply_runtime_credentials(self, identifier: str = "") -> None:
        if identifier is not None and str(identifier).strip():
            value = str(identifier).strip()
            if value != self._identifier:
                self._identifier = value
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
        url = f"{CAPITAL_BASE_URL}/api/v1/session"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-CAP-API-KEY": self._api_key,
        }
        payload = {"identifier": self._identifier, "password": self._password}
        response = self._http.post(url, headers=headers, json=payload, timeout=25)
        if response.status_code >= 400:
            raise RuntimeError(
                f"Capital.com login failed: {self._error_text_from_response(response)}"
            )

        self._cst = response.headers.get("CST", "")
        self._security_token = response.headers.get("X-SECURITY-TOKEN", "")
        if not self._cst or not self._security_token:
            raise RuntimeError("Capital.com login failed: Missing auth tokens.")

    def _request(
        self,
        method: str,
        path: str,
        params=None,
        payload=None,
        retry_on_auth: bool = True,
    ):
        if not self._cst or not self._security_token:
            self._login()

        url = f"{CAPITAL_BASE_URL}{path}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": CAPITAL_API_VERSION,
            "X-CAP-API-KEY": self._api_key,
            "CST": self._cst,
            "X-SECURITY-TOKEN": self._security_token,
        }

        response = self._http.request(
            method,
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=25,
        )
        if response.status_code in {401, 403} and retry_on_auth:
            self._login()
            return self._request(
                method, path, params=params, payload=payload, retry_on_auth=False
            )

        if response.status_code >= 400:
            raise RuntimeError(self._error_text_from_response(response))

        if not response.text:
            return {}
        try:
            return response.json()
        except ValueError:
            return {}

    def _cache_market_failure(self, key: str, message: str) -> None:
        self._market_failures[key] = {
            "message": message,
            "retry_after": datetime.now(timezone.utc).timestamp() + 120,
        }

    def _cached_market_failure(self, key: str) -> Optional[str]:
        row = self._market_failures.get(key)
        if not row:
            return None
        if datetime.now(timezone.utc).timestamp() < row["retry_after"]:
            return row["message"]
        self._market_failures.pop(key, None)
        return None

    def _pick_market(self, markets: List[dict], preferred_epic: str) -> Optional[dict]:
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

    def _resolve_market(self, instrument_key: str) -> Dict[str, Any]:
        cached = self._market_cache.get(instrument_key)
        if cached and datetime.now(timezone.utc).timestamp() < cached["expires_at"]:
            return cached["value"]

        cached_failure = self._cached_market_failure(instrument_key)
        if cached_failure:
            raise RuntimeError(cached_failure)

        spec = self._instrument_spec(instrument_key)
        search_data = self._request(
            "GET",
            "/api/v1/markets",
            params={"searchTerm": spec["search_term"]},
        )
        markets = search_data.get("markets", []) or []
        pick = self._pick_market(markets, spec.get("preferred_epic", ""))
        if not pick:
            msg = f"No Capital.com market found for {spec['label']}."
            self._cache_market_failure(instrument_key, msg)
            raise RuntimeError(msg)

        epic = str(pick.get("epic", "")).strip()
        if not epic:
            msg = f"No valid epic found for {spec['label']}."
            self._cache_market_failure(instrument_key, msg)
            raise RuntimeError(msg)

        details = self._request("GET", f"/api/v1/markets/{epic}")
        market = {
            "epic": epic,
            "instrument": details.get("instrument", {}) or {},
            "snapshot": details.get("snapshot", {}) or {},
        }

        self._market_cache[instrument_key] = {
            "value": market,
            "expires_at": datetime.now(timezone.utc).timestamp() + 180,
        }
        self._market_failures.pop(instrument_key, None)
        return market

    def _market_status(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        status = str(snapshot.get("marketStatus", "")).upper()
        if not status:
            return {"is_open": None, "message": "Market status unavailable."}
        if status == "TRADEABLE":
            return {"is_open": True, "message": "Market is open and tradeable."}
        return {"is_open": False, "message": f"Market is {status}."}

    def _normalize_action(self, action: str) -> str:
        value = str(action or "").strip().upper()
        if value in {"BUY", "LONG"}:
            return "BUY"
        if value in {"SELL", "SHORT"}:
            return "SELL"
        return value

    def _confirm_by_reference(self, deal_reference: str) -> Dict[str, Any]:
        for _ in range(12):
            try:
                data = self._request("GET", f"/api/v1/confirms/{deal_reference}")
            except RuntimeError:
                data = {}
            if data:
                return data
            time.sleep(0.4)
        return {}

    # ---- position helpers (for NETTING-safe flip) ----
    def _positions(self) -> List[Dict[str, Any]]:
        data = self._request("GET", "/api/v1/positions")
        return data.get("positions", []) or []

    def _positions_for_epic(self, epic: str) -> List[Dict[str, Any]]:
        epic = str(epic or "").strip()
        if not epic:
            return []
        out: List[Dict[str, Any]] = []
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

    def _resolve_epic(self, instrument_key: str = "", epic: str = "") -> str:
        epic = str(epic or "").strip()
        if epic:
            return epic
        instrument_key = str(instrument_key or "").strip()
        if not instrument_key:
            raise ValueError("Provide instrument key or epic.")
        market = self._resolve_market(instrument_key)
        return str(market.get("epic", "")).strip()

    def place_market_order_epic(
        self,
        action: str,
        quantity: float,
        epic: str,
        identifier: str = "",
        sl: Optional[float] = None,
        tp: Optional[float] = None,
    ) -> Dict[str, Any]:
        action = self._normalize_action(action)
        epic = str(epic or "").strip()
        if action not in {"BUY", "SELL"}:
            return {"ok": False, "error": "invalid_action", "message": "Action must be BUY or SELL."}
        if not epic:
            return {"ok": False, "error": "invalid_epic", "message": "epic is required."}
        if quantity <= 0:
            return {"ok": False, "error": "invalid_quantity", "message": "Quantity must be greater than zero."}

        with self._lock:
            self._apply_runtime_credentials(identifier=identifier)

            # Check market status
            details = self._request("GET", f"/api/v1/markets/{epic}")
            snapshot = details.get("snapshot", {}) or {}
            mstat = self._market_status(snapshot)
            if mstat.get("is_open") is False:
                return {"ok": False, "error": "market_closed", "message": mstat["message"], "market": mstat}

            payload: Dict[str, Any] = {
                "epic": epic,
                "direction": action,
                "size": float(quantity),
                "orderType": "MARKET",
            }

            sl_v = self._safe_float(sl)
            tp_v = self._safe_float(tp)
            if sl is not None and sl_v > 0:
                payload["stopLevel"] = sl_v
            if tp is not None and tp_v > 0:
                payload["limitLevel"] = tp_v

            try:
                create = self._request("POST", "/api/v1/positions", payload=payload)
            except Exception as exc:
                msg = self._clean_message(str(exc))
                return {"ok": False, "error": "order_rejected", "message": msg}

            deal_reference = str(create.get("dealReference", "")).strip()
            confirm = self._confirm_by_reference(deal_reference) if deal_reference else {}

            if confirm:
                deal_status = str(confirm.get("dealStatus", "")).upper()
                reason = self._clean_message(
                    str(confirm.get("reason") or confirm.get("statusReason") or confirm.get("errorCode") or "")
                )
                if deal_status and deal_status != "ACCEPTED":
                    return {"ok": False, "error": "order_rejected", "message": reason or f"Order rejected ({deal_status}).", "confirm": confirm}

            return {"ok": True, "message": "Market order submitted.", "dealReference": deal_reference, "confirm": confirm, "sent": payload}

    def close_positions(
        self,
        side: str = "",
        epic: str = "",
        deal_id: str = "",
        instrument_key: str = "",
        identifier: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            self._apply_runtime_credentials(identifier=identifier)
            side = self._normalize_action(side) if side else ""
            if side and side not in {"BUY", "SELL"}:
                return {"ok": False, "error": "invalid_action", "message": "side must be BUY/SELL/LONG/SHORT when provided."}

            try:
                resolved_epic = self._resolve_epic(instrument_key=instrument_key, epic=epic) if (epic or instrument_key) else ""
            except Exception:
                resolved_epic = str(epic or "").strip()

            closed: List[str] = []

            if deal_id:
                deal = str(deal_id).strip()
                try:
                    self._request("DELETE", f"/api/v1/positions/{deal}")
                    closed.append(deal)
                except Exception as exc:
                    return {"ok": False, "error": "close_failed", "message": self._clean_message(str(exc))}
                return {"ok": True, "message": "Position close request sent.", "closed_deals": closed}

            targets: List[str] = []
            for row in self._positions():
                position = row.get("position", {}) or {}
                market = row.get("market", {}) or {}
                if side and str(position.get("direction", "")).upper() != side:
                    continue
                if resolved_epic and str(market.get("epic", "")).strip() != resolved_epic:
                    continue
                if position.get("dealId"):
                    targets.append(str(position.get("dealId")))

            if not targets:
                return {"ok": True, "message": "No matching open positions found.", "closed_deals": []}

            errors: List[str] = []
            for target in targets:
                try:
                    self._request("DELETE", f"/api/v1/positions/{target}")
                    closed.append(target)
                except Exception as exc:
                    errors.append(f"{target}: {self._clean_message(str(exc))}")

            if errors and not closed:
                return {"ok": False, "error": "close_failed", "message": "; ".join(errors), "closed_deals": []}
            if errors:
                return {"ok": True, "message": "Some positions were closed, some failed.", "closed_deals": closed, "errors": errors}
            return {"ok": True, "message": "Position close request sent.", "closed_deals": closed}

    def close_opposites_then_open(
        self,
        desired_action: str,
        quantity: float,
        instrument_key: str = "",
        epic: str = "",
        identifier: str = "",
        sl: Optional[float] = None,
        tp: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        NETTING-safe "flip":
        - If there is an opposite open position for the epic, close it first
        - Then open the new position
        """
        desired_action = self._normalize_action(desired_action)
        if desired_action not in {"BUY", "SELL"}:
            return {"ok": False, "error": "invalid_action", "message": "Action must be BUY or SELL."}
        if quantity <= 0:
            return {"ok": False, "error": "invalid_quantity", "message": "Quantity must be greater than zero."}

        with self._lock:
            self._apply_runtime_credentials(identifier=identifier)

            try:
                resolved_epic = self._resolve_epic(instrument_key=instrument_key, epic=epic)
            except Exception as exc:
                return {"ok": False, "error": "invalid_payload", "message": self._clean_message(str(exc))}

            # 1) close opposite positions for this epic
            existing = self._positions_for_epic(resolved_epic)
            opposites = [p["dealId"] for p in existing if p.get("direction") and p["direction"] != desired_action]

            close_errors: List[str] = []
            for deal_id in opposites:
                try:
                    self._request("DELETE", f"/api/v1/positions/{deal_id}")
                except Exception as exc:
                    close_errors.append(self._clean_message(str(exc)))

            if close_errors and opposites:
                # If you want strict correctness, you can abort open when close fails:
                return {
                    "ok": False,
                    "error": "close_failed",
                    "message": "Failed closing opposite position(s): " + "; ".join(close_errors),
                    "closed_attempts": opposites,
                }

            # 2) open new position (by epic)
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


SERVICE = CapitalTradingService()


# ---------------------------
# HTTP Handler
# ---------------------------
class TradingRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_file(self, filename: str, content_type: str) -> None:
        path = UI_DIR / filename
        if not path.exists():
            self._send_json(
                {"ok": False, "message": f"Missing UI file: {filename}"},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _mask_sensitive(self, value):
        if isinstance(value, dict):
            masked = {}
            for k, v in value.items():
                key = str(k).lower()
                if key in {"secret", "password", "api_key", "apikey", "token", "cst", "x-security-token"}:
                    masked[k] = "***"
                else:
                    masked[k] = self._mask_sensitive(v)
            return masked
        if isinstance(value, list):
            return [self._mask_sensitive(item) for item in value]
        return value

    def _log_webhook_result(self, body, result, status: int) -> None:
        if status < HTTPStatus.BAD_REQUEST:
            return
        safe_body = self._mask_sensitive(body)
        safe_result = self._mask_sensitive(result)
        print(
            "[webhook] "
            f"status={int(status)} "
            f"error={safe_result.get('error', '')} "
            f"message={safe_result.get('message', '')} "
            f"payload={json.dumps(safe_body, ensure_ascii=True)}"
        )

    def _read_json_body(self) -> Tuple[Optional[dict], Optional[str], str]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8", errors="replace") if length else ""
        print(f"TV RAW BODY: {raw!r}")
        try:
            return json.loads(raw), None, raw
        except json.JSONDecodeError as exc:
            msg = f"Invalid JSON payload: {exc.msg} at line {exc.lineno}, column {exc.colno}."
            return None, msg, raw

    def _parse_sl_tp(self, body: dict) -> Tuple[Optional[float], Optional[float]]:
        # Accept SL/TP in multiple key names
        sl = body.get("sl", None)
        tp = body.get("tp", None)
        if sl is None:
            sl = body.get("stopLevel", None)
        if tp is None:
            tp = body.get("limitLevel", None)

        try:
            sl = float(sl) if sl is not None else None
        except (TypeError, ValueError):
            sl = None
        try:
            tp = float(tp) if tp is not None else None
        except (TypeError, ValueError):
            tp = None
        return sl, tp

    def _execute_tradingview_webhook(self, body: dict) -> Tuple[dict, int]:
        # Secret check
        secret = str(body.get("secret", "")).strip()
        if TRADINGVIEW_WEBHOOK_SECRET and secret != TRADINGVIEW_WEBHOOK_SECRET:
            return {"ok": False, "error": "unauthorized", "message": "Invalid webhook secret."}, HTTPStatus.UNAUTHORIZED

        identifier = str(body.get("identifier", "")).strip()

        event = str(body.get("event", "")).strip().lower()
        action = str(body.get("action", "")).strip().upper()
        side = str(body.get("side", "")).strip().upper()

        quantity = SERVICE._safe_float(body.get("quantity"))
        if quantity <= 0:
            quantity = SERVICE._safe_float(body.get("qty"))
        if quantity <= 0:
            quantity = 1.0

        instrument_key = str(body.get("instrument", "")).strip()
        epic = str(body.get("epic", "")).strip()
        deal_id = str(body.get("deal_id") or body.get("dealId") or "").strip()

        sl, tp = self._parse_sl_tp(body)

        open_events = {"entry", "open", "long", "short", "buy", "sell"}
        close_events = {"close", "exit"}

        # If event omitted, infer.
        if not event:
            if action in {"BUY", "SELL"} or side in {"BUY", "SELL", "LONG", "SHORT"}:
                event = "entry"
            else:
                event = "close"

        if event in open_events:
            open_action = action or side
            if not open_action and event in {"long", "buy"}:
                open_action = "BUY"
            if not open_action and event in {"short", "sell"}:
                open_action = "SELL"

            # NETTING-SAFE flip: close opposite then open
            result = SERVICE.close_opposites_then_open(
                desired_action=open_action,
                quantity=quantity,
                instrument_key=instrument_key,
                epic=epic,
                identifier=identifier,
                sl=sl,
                tp=tp,
            )

            if result.get("ok"):
                return result, HTTPStatus.OK
            if result.get("error") in {"market_closed", "order_rejected"}:
                return result, HTTPStatus.CONFLICT
            return result, HTTPStatus.BAD_REQUEST

        if event in close_events:
            close_side = side or action
            if close_side == "LONG":
                close_side = "BUY"
            elif close_side == "SHORT":
                close_side = "SELL"

            result = SERVICE.close_positions(
                side=close_side,
                epic=epic,
                deal_id=deal_id,
                instrument_key=instrument_key,
                identifier=identifier,
            )
            if result.get("ok"):
                return result, HTTPStatus.OK
            return result, HTTPStatus.BAD_REQUEST

        return {"ok": False, "error": "invalid_payload", "message": f"Unknown event: {event}"}, HTTPStatus.BAD_REQUEST

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/":
            # optional UI
            if (UI_DIR / "index.html").exists():
                self._send_file("index.html", "text/html; charset=utf-8")
            else:
                self._send_json({"ok": True, "message": "Server is running."}, HTTPStatus.OK)
            return
        if path == "/styles.css":
            self._send_file("styles.css", "text/css; charset=utf-8")
            return
        if path == "/app.js":
            self._send_file("app.js", "application/javascript; charset=utf-8")
            return

        self._send_json({"ok": False, "message": "Not found"}, HTTPStatus.NOT_FOUND)

    def do_POST(self):
        path = urlparse(self.path).path
        if path != "/webhook/tradingview":
            self._send_json({"ok": False, "message": "Not found"}, HTTPStatus.NOT_FOUND)
            return

        body, err, raw = self._read_json_body()
        if err:
            print(f"[webhook] status=400 error=invalid_json message={err} raw={raw[:500]!r}")
            self._send_json({"ok": False, "error": "invalid_json", "message": err}, HTTPStatus.BAD_REQUEST)
            return

        result, status = self._execute_tradingview_webhook(body or {})
        self._log_webhook_result(body or {}, result, status)
        self._send_json(result, status)

    def log_message(self, format, *args):
        return


def main():
    server = HTTPServer((WEB_HOST, WEB_PORT), TradingRequestHandler)
    print(f"Server started at http://{WEB_HOST}:{WEB_PORT}")
    print(f"Capital.com base URL: {CAPITAL_BASE_URL}")
    print("Using Capital.com API v1 session auth.")
    print("TradingView webhook endpoint: /webhook/tradingview")
    if not TRADINGVIEW_WEBHOOK_SECRET:
        print("WARNING: TRADINGVIEW_WEBHOOK_SECRET is empty. Set it for safety.")
    if not CAPITAL_API_KEY or not CAPITAL_PASSWORD or not CAPITAL_IDENTIFIER:
        print("WARNING: Missing Capital credentials in env vars. Set CAPITAL_API_KEY / CAPITAL_PASSWORD / CAPITAL_IDENTIFIER.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()