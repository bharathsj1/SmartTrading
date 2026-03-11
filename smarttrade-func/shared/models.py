from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional

from shared.helpers import safe_json_dumps, sha256_hex


@dataclass(frozen=True)
class NormalizedWebhook:
    secret: str
    event: str
    action: str
    side: str
    instrument: str
    epic: str
    deal_id: str
    identifier: str
    account: str
    quantity: float
    price: Optional[float]
    sl: Optional[float]
    tp: Optional[float]
    tp1: Optional[float]
    tp2: Optional[float]
    tp3: Optional[float]
    ticker: str
    exchange: str
    strategy: str
    reason: str
    setup: str
    regime: str
    tf: str
    bar_time: str
    comment: str
    raw: dict[str, Any]

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def from_dict(cls, body: Mapping[str, Any]) -> "NormalizedWebhook":
        payload = {str(key): value for key, value in dict(body).items()}
        instrument = str(payload.get("instrument") or payload.get("symbol") or "").strip()
        ticker = str(payload.get("ticker") or "").strip()
        if not instrument and ":" in ticker:
            instrument = ticker.split(":", 1)[1].strip()
        quantity = cls._to_float(payload.get("quantity"))
        if quantity is None or quantity <= 0:
            quantity = cls._to_float(payload.get("qty"))
        if quantity is None or quantity <= 0:
            quantity = 1.0

        tp1 = cls._to_float(payload.get("tp1"))
        tp = tp1
        if tp is None:
            tp = cls._to_float(payload.get("tp"))
        if tp is None:
            tp = cls._to_float(payload.get("profitLevel"))

        sl = cls._to_float(payload.get("sl"))
        if sl is None:
            sl = cls._to_float(payload.get("stopLevel"))

        return cls(
            secret=str(payload.get("secret") or "").strip(),
            event=str(payload.get("event") or "").strip().lower(),
            action=str(payload.get("action") or "").strip().upper(),
            side=str(payload.get("side") or "").strip().upper(),
            instrument=instrument,
            epic=str(payload.get("epic") or "").strip(),
            deal_id=str(payload.get("deal_id") or payload.get("dealId") or "").strip(),
            identifier=str(payload.get("identifier") or "").strip(),
            account=str(payload.get("account") or "").strip(),
            quantity=quantity,
            price=cls._to_float(payload.get("price")),
            sl=sl,
            tp=tp,
            tp1=tp1,
            tp2=cls._to_float(payload.get("tp2")),
            tp3=cls._to_float(payload.get("tp3")),
            ticker=ticker,
            exchange=str(payload.get("exchange") or "").strip(),
            strategy=str(payload.get("strategy") or "").strip(),
            reason=str(payload.get("reason") or "").strip(),
            setup=str(payload.get("setup") or "").strip(),
            regime=str(payload.get("regime") or "").strip(),
            tf=str(payload.get("tf") or "").strip(),
            bar_time=str(payload.get("bar_time") or "").strip(),
            comment=str(payload.get("comment") or "").strip(),
            raw=payload,
        )

    def effective_identifier(self) -> str:
        return self.identifier or self.account

    def dedupe_material(self) -> str:
        normalized_side = self.action or self.side or self.event
        parts = [
            self.event,
            normalized_side,
            self.instrument,
            self.strategy,
            self.bar_time,
            self.comment,
            str(self.quantity),
        ]
        return "|".join(part.strip().lower() for part in parts)

    def dedupe_key(self) -> str:
        return sha256_hex(self.dedupe_material())

    def to_public_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("raw", None)
        return data


@dataclass(frozen=True)
class QueueEnvelope:
    request_id: str
    dedupe_key: str
    received_at: str
    payload: NormalizedWebhook

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "dedupe_key": self.dedupe_key,
            "received_at": self.received_at,
            "payload": self.payload.to_public_dict(),
            "raw": self.payload.raw,
        }

    def to_json(self) -> str:
        return safe_json_dumps(self.to_dict())

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "QueueEnvelope":
        merged_payload = dict(payload.get("payload") or {})
        raw_payload = payload.get("raw")
        if isinstance(raw_payload, Mapping):
            merged_payload.update(dict(raw_payload))
        webhook = NormalizedWebhook.from_dict(merged_payload)
        return cls(
            request_id=str(payload.get("request_id") or "").strip(),
            dedupe_key=str(payload.get("dedupe_key") or webhook.dedupe_key()).strip(),
            received_at=str(payload.get("received_at") or "").strip(),
            payload=webhook,
        )
