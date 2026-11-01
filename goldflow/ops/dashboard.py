"""Phase 17: the read-only operations dashboard.

It is a standard-library HTTP server (no new dependency on the trading PC), bound to a **loopback** address. If the
environment variable ``GOLDFLOW_DASHBOARD_TOKEN`` is set, every request must carry it, either as
``Authorization: Bearer <token>`` or as ``?token=``. The page has no control actions. The emergency stop stays a CLI
command (``emergency-stop``), so a browser tab can never trade.

| Endpoint | Content |
|---|---|
| ``/api/status`` | the full status snapshot (``status.collect``); every value carries its source file |
| ``/api/alerts`` | open alerts and the last 200 alert transitions |
| ``/api/health`` | ``{"ok": bool, "critical": n}``, for supervisors and uptime checks |
| ``/`` | ``static/dashboard.html``: plain HTML/JS that renders the snapshot every ``refresh_s`` seconds |

Each status request re-evaluates the alert rules, so alerts open and close even when no one runs ``ops-check``.
"""

from __future__ import annotations

import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .alerts import AlertManager, evaluate, notifiers_for, ops_settings
from .status import collect

STATIC = Path(__file__).with_name("static")


class Dashboard:
    def __init__(self, cfg: Any, port: int | None = None) -> None:
        self.cfg = cfg
        self.ops = ops_settings(cfg)
        self.alerts = AlertManager(Path(cfg.data_root) / "ops", notifiers_for(self.ops))
        self.lock = threading.Lock()
        self.token = os.environ.get("GOLDFLOW_DASHBOARD_TOKEN") or None
        self.server = ThreadingHTTPServer(
            (self.ops["dashboard_host"], int(self.ops["dashboard_port"] if port is None else port)), self._handler()
        )
        self.port = self.server.server_address[1]

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            now = int(time.time() * 1000)
            st = collect(self.cfg, now)
            self.alerts.update(evaluate(st, self.ops), now)
            st["alerts_open"] = list(self.alerts.open.values())
            st["refresh_s"] = self.ops["refresh_s"]
            return st

    def recent_alerts(self) -> list[dict[str, Any]]:
        p = Path(self.cfg.data_root) / "ops" / "alerts.jsonl"
        if not p.exists():
            return []
        return [json.loads(x) for x in p.read_text().splitlines()[-200:] if x.strip()]

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        dash = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:  # quiet: requests are not operational events
                return

            def _send(self, code: int, body: bytes, ctype: str) -> None:
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
                )
                self.end_headers()
                self.wfile.write(body)

            def _authorised(self, q: dict[str, list[str]]) -> bool:
                if dash.token is None:
                    return True
                got = (self.headers.get("Authorization") or "").removeprefix("Bearer ").strip() or (q.get("token") or [""])[0]
                return hmac.compare_digest(got, dash.token)

            def do_GET(self) -> None:  # noqa: N802 - http.server API
                u = urlparse(self.path)
                q = parse_qs(u.query)
                if not self._authorised(q):
                    self._send(401, b'{"error": "token required"}', "application/json")
                    return
                try:
                    if u.path == "/api/status":
                        self._send(200, json.dumps(dash.snapshot(), default=str).encode(), "application/json")
                    elif u.path == "/api/alerts":
                        self._send(200, json.dumps({"open": list(dash.alerts.open.values()), "recent": dash.recent_alerts()}, default=str).encode(),
                                   "application/json")  # fmt: skip
                    elif u.path == "/api/health":
                        st = dash.snapshot()
                        crit = sum(1 for a in st["alerts_open"] if a["severity"] == "critical")
                        self._send(
                            200 if crit == 0 else 503,
                            json.dumps({"ok": crit == 0, "critical": crit}).encode(),
                            "application/json",
                        )
                    elif u.path in ("/", "/index.html"):
                        self._send(200, (STATIC / "dashboard.html").read_bytes(), "text/html; charset=utf-8")
                    else:
                        self._send(404, b'{"error": "not found"}', "application/json")
                except Exception as e:  # noqa: BLE001 - report the failure instead of dropping the connection
                    self._send(500, json.dumps({"error": repr(e)}).encode(), "application/json")

        return H

    def serve(self) -> None:
        self.server.serve_forever(poll_interval=0.5)

    def start_background(self) -> threading.Thread:
        t = threading.Thread(target=self.serve, daemon=True)
        t.start()
        return t

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
