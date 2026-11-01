"""Phase 16: the authenticated message protocol between the Python engine and the MQL5 executor EA.

**Wire format.** One message per line: ``<mac> <json>\\n``.
- ``<json>`` is a JSON object written by the sender in ASCII (non-ASCII characters escaped).
- ``<mac>`` is HMAC-SHA256(secret, ``<json>`` bytes) in lower-case hex, computed over the exact text sent. Neither
  side re-serialises before checking it, so there is no canonicalisation problem between Python and MQL5.

**Envelope fields** (in every message):

| Field | Meaning |
|---|---|
| ``v`` | protocol version (1) |
| ``type`` | message type |
| ``seq`` | strictly increasing per sender and session |
| ``ts`` | sender UTC time in ms |
| ``session`` | the Python engine's session id, echoed by the EA |
| ``sender`` | ``py`` or ``ea`` |

**Messages Python → EA.** They are **flat**: string and number values only, so the EA's parser stays minimal.
- ``HELLO``;
- ``SYMBOL_INFO_REQ``;
- ``POSITIONS_REQ``;
- ``HEARTBEAT``;
- ``ORDER_NEW`` {key, symbol, side, volume, sl, tp, deviation};
- ``ORDER_MODIFY`` {key, position, sl, tp};
- ``ORDER_CLOSE`` {key, position, volume};
- ``FLATTEN_ALL`` {reason}.

**Messages EA → Python.**
- ``HELLO`` {account, trade_mode, margin_mode, server, currency, balance, equity, build};
- ``SYMBOL_INFO`` {…};
- ``ACK`` {ref, key, status};
- ``REJECT`` {ref, key, retcode, reason};
- ``TXN`` {key, kind, order, deal, position, entry, side, volume, price, reason, retcode, time_msc};
- ``POSITIONS`` {positions: […], orders: […], balance, equity};
- ``HEARTBEAT`` {bid, ask, spread_points, balance, equity, time_msc}.

**HELLO** starts a new incarnation of its sender: its ``seq`` may restart (an EA or engine restart), but MAC, session and
clock checks still apply. The EA adopts the engine's session from an authenticated HELLO and ignores every other
message until it has one.

**A receiver refuses** a bad MAC, a non-increasing ``seq``, a ``ts`` outside ``max_skew_ms``, a wrong session, an
unknown type, and missing or mistyped fields. Each refusal is audited and the message is not acted on.

**Idempotency.** The idempotency key is ``gf`` + the first 16 hex characters of SHA-256(signal_id). It goes into the
MT5 order comment as ``gf:<key>``, together with the magic number. A retry re-sends the same key, and the EA refuses to
open a second position for a key already present among its positions, orders or recent history.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import Any

VERSION = 1
PY_TYPES = {"HELLO", "SYMBOL_INFO_REQ", "POSITIONS_REQ", "HEARTBEAT", "ORDER_NEW", "ORDER_MODIFY", "ORDER_CLOSE", "FLATTEN_ALL"}
EA_TYPES = {"HELLO", "SYMBOL_INFO", "ACK", "REJECT", "TXN", "POSITIONS", "HEARTBEAT"}
_S, _I, _N = (str,), (int,), (int, float)
FIELDS: dict[tuple[str, str], dict[str, tuple[type, ...]]] = {
    ("py", "ORDER_NEW"): {"key": _S, "symbol": _S, "side": _S, "volume": _S, "sl": _S, "tp": _S, "deviation": _I},
    ("py", "ORDER_MODIFY"): {"key": _S, "position": _I, "sl": _S, "tp": _S},
    ("py", "ORDER_CLOSE"): {"key": _S, "position": _I, "volume": _S},
    ("py", "FLATTEN_ALL"): {"reason": _S},
    ("py", "SYMBOL_INFO_REQ"): {"symbol": _S},
    ("ea", "HELLO"): {
        "account": _I,
        "trade_mode": _S,
        "margin_mode": _S,
        "server": _S,
        "currency": _S,
        "balance": _N,
        "equity": _N,
    },
    ("ea", "SYMBOL_INFO"): {
        "symbol": _S,
        "digits": _I,
        "point": _N,
        "tick_size": _N,
        "tick_value": _N,
        "contract_size": _N,
        "volume_min": _N,
        "volume_max": _N,
        "volume_step": _N,
        "stops_level": _I,
        "trade_mode": _S,
    },  # fmt: skip
    ("ea", "ACK"): {"ref": _I, "key": _S, "status": _S},
    ("ea", "REJECT"): {"ref": _I, "key": _S, "retcode": _I, "reason": _S},
    ("ea", "TXN"): {"key": _S, "kind": _S, "position": _I, "volume": _N, "price": _N, "time_msc": _I},
    ("ea", "POSITIONS"): {"positions": (list,), "orders": (list,), "balance": _N, "equity": _N},
    ("ea", "HEARTBEAT"): {"bid": _N, "ask": _N, "spread_points": _N, "balance": _N, "equity": _N, "time_msc": _I},
}
ENVELOPE = {"v": _I, "type": _S, "seq": _I, "ts": _I, "session": _S, "sender": _S}


class ProtocolError(ValueError):
    """A message that must not be acted on (bad MAC, replay, skew, schema)."""


def idempotency_key(signal_id: str) -> str:
    return "gf" + hashlib.sha256(signal_id.encode()).hexdigest()[:16]


def mac(secret: bytes, text: str) -> str:
    return hmac.new(secret, text.encode("ascii"), hashlib.sha256).hexdigest()


def encode(secret: bytes, msg: dict[str, Any]) -> str:
    text = json.dumps(msg, separators=(",", ":"), ensure_ascii=True)
    if "\n" in text:  # pragma: no cover - json.dumps escapes newlines
        raise ProtocolError("newline in message")
    return f"{mac(secret, text)} {text}\n"


@dataclass
class Endpoint:
    """One side of the channel: signs what it sends, checks what it receives."""

    secret: bytes
    me: str  # "py" | "ea"
    session: str
    max_skew_ms: int = 5000
    seq_out: int = 0
    seq_in: int = 0
    refused: list[dict[str, Any]] = field(default_factory=list)

    def make(self, mtype: str, now_ms: int, **body: Any) -> tuple[dict[str, Any], str]:
        allowed = PY_TYPES if self.me == "py" else EA_TYPES
        if mtype not in allowed:
            raise ProtocolError(f"{self.me} may not send {mtype}")
        self.seq_out += 1
        msg = {
            "v": VERSION,
            "type": mtype,
            "seq": self.seq_out,
            "ts": int(now_ms),
            "session": self.session,
            "sender": self.me,
            **body,
        }
        _check_fields(msg, self.me)
        return msg, encode(self.secret, msg)

    def parse(self, line: str, now_ms: int) -> dict[str, Any]:
        try:
            m, _, text = line.rstrip("\n").partition(" ")
            if len(m) != 64 or not hmac.compare_digest(m, mac(self.secret, text)):
                raise ProtocolError("bad MAC")
            msg = json.loads(text)
            if not isinstance(msg, dict):
                raise ProtocolError("not an object")
            other = "ea" if self.me == "py" else "py"
            if msg.get("sender") != other:
                raise ProtocolError(f"sender {msg.get('sender')!r} is not {other!r}")
            _check_fields(msg, other)
            if msg["v"] != VERSION:
                raise ProtocolError(f"protocol version {msg['v']} != {VERSION}")
            # the EA adopts the engine's session from an authenticated, timely HELLO (assigned only after every check)
            sess = msg["session"] if (msg["type"] == "HELLO" and self.me == "ea") else self.session
            if msg["session"] != sess:
                raise ProtocolError("wrong session")
            if msg["seq"] <= self.seq_in and msg["type"] != "HELLO":  # a HELLO starts a new incarnation of the sender
                raise ProtocolError(f"replayed or reordered seq {msg['seq']} <= {self.seq_in}")
            if abs(int(msg["ts"]) - now_ms) > self.max_skew_ms:
                raise ProtocolError(f"clock skew {int(msg['ts']) - now_ms} ms")
        except (ProtocolError, ValueError, UnicodeEncodeError, KeyError, TypeError) as e:
            self.refused.append({"reason": str(e), "line": line[:300], "at": now_ms})
            raise ProtocolError(str(e)) from None
        self.session = sess
        self.seq_in = int(msg["seq"])
        return msg


def _check_fields(msg: dict[str, Any], sender: str) -> None:
    for k, t in ENVELOPE.items():
        if not isinstance(msg.get(k), t) or isinstance(msg.get(k), bool):
            raise ProtocolError(f"envelope field {k!r} missing or not {t[0].__name__}")
    types = PY_TYPES if sender == "py" else EA_TYPES
    if msg["type"] not in types:
        raise ProtocolError(f"unknown message type {msg['type']!r} from {sender}")
    for k, ft in FIELDS.get((sender, msg["type"]), {}).items():
        v = msg.get(k)
        if not isinstance(v, ft) or isinstance(v, bool):
            raise ProtocolError(f"{msg['type']}.{k} missing or not {'/'.join(x.__name__ for x in ft)}")
    if sender == "py":
        for k, v in msg.items():
            if isinstance(v, (dict, list)):
                raise ProtocolError(f"Python -> EA messages are flat; {k!r} is nested")


def hmac_test_vector() -> dict[str, str]:
    """A fixed vector the EA checks in OnInit, so its HMAC-SHA256 implementation is proven equal to Python's."""
    key = b"goldflow-test-key"
    text = '{"v":1,"type":"HEARTBEAT","seq":1,"ts":0,"session":"selftest","sender":"py"}'
    return {"key": key.decode(), "text": text, "mac": mac(key, text)}
