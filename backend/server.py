"""AGCN LIVE: one-page HTTP/SSE host for the preserved notebook runtime.

Run with `python -m backend.server`. The same process serves the frontend and
the Python capture modules; no browser executes a Worker or consumes a queue.
"""

from __future__ import annotations

import json
import mimetypes
import os
import re
import secrets
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .runtime import MissingCaptureDependency, load_runtime

DIST = Path(__file__).resolve().parents[1] / "dist"
MAX_SESSIONS = int(os.environ.get("AGCN_MAX_SESSIONS", "8"))
ALLOWED_ORIGINS = {x.strip().rstrip("/") for x in os.environ.get("AGCN_ALLOWED_ORIGINS", "").split(",") if x.strip()}
_sessions = {}
_registry_lock = threading.Lock()


class Session:
    def __init__(self):
        self.runtime = load_runtime()
        self.lock = threading.RLock()
        self.touched = time.monotonic()


def _session(identifier, create=True):
    with _registry_lock:
        session = _sessions.get(identifier) if identifier else None
        if session:
            session.touched = time.monotonic()
            return identifier, session
        if not create:
            return None, None
        # A stale session no longer holds a Chromium process indefinitely.
        for sid, old in list(_sessions.items()):
            if time.monotonic() - old.touched > 60 * 60:
                old.runtime.stop()
                del _sessions[sid]
        if len(_sessions) >= MAX_SESSIONS:
            raise ValueError("Limite de sessões simultâneas atingido. Tente novamente em alguns minutos.")
        identifier = secrets.token_urlsafe(24)
        session = Session()
        _sessions[identifier] = session
        return identifier, session


class Handler(BaseHTTPRequestHandler):
    server_version = "AGCNLive/1.0"

    def log_message(self, fmt, *args):
        # Internal diagnostics stay off the user-facing page.
        entry = re.sub(r"sid=[A-Za-z0-9_-]+", "sid=[redacted]", fmt % args)
        print(f"[http] {self.address_string()} {entry}", flush=True)

    def _origin(self):
        origin = self.headers.get("Origin", "").rstrip("/")
        if not origin:
            return ""
        hostname = self.headers.get("Host", "")
        if origin in {f"http://{hostname}", f"https://{hostname}"} or origin in ALLOWED_ORIGINS:
            return origin
        return None

    def _headers(self, code=200, content_type="application/json; charset=utf-8", session_id=None):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        origin = self._origin()
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-AGCN-Session")
            self.send_header("Access-Control-Expose-Headers", "X-AGCN-Session")
            self.send_header("Vary", "Origin")
        if session_id:
            self.send_header("X-AGCN-Session", session_id)
        self.end_headers()

    def _json(self, obj, code=200, session_id=None):
        encoded = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self._headers(code, session_id=session_id)
        self.wfile.write(encoded)

    def _sid(self, url):
        return self.headers.get("X-AGCN-Session") or parse_qs(url.query).get("sid", [None])[0]

    def do_OPTIONS(self):
        if self._origin() is None:
            self._json({"ok": False, "message": "Origem não autorizada."}, 403)
            return
        self._headers(204)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path.startswith("/api/") and self._origin() is None:
            self._json({"ok": False, "message": "Origem não autorizada."}, 403)
            return
        if url.path == "/api/health":
            self._json({"ok": True, "name": "AGCN LIVE", "transport": "sse"})
            return
        if url.path == "/api/state":
            try:
                sid, session = _session(self._sid(url))
                with session.lock:
                    state = session.runtime.status()
                self._json(state, session_id=sid)
            except Exception as exc:
                self._error(exc)
            return
        if url.path == "/api/events":
            sid, session = _session(self._sid(url), create=False)
            if not session:
                self._json({"ok": False, "message": "Sessão expirada. Atualize a página."}, 401)
                return
            self._headers(200, "text/event-stream; charset=utf-8", session_id=sid)
            try:
                while True:
                    session.touched = time.monotonic()
                    with session.lock:
                        state = session.runtime.status()
                    event = json.dumps(state, ensure_ascii=False, default=str)
                    self.wfile.write(f"event: state\ndata: {event}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(1)
            except (BrokenPipeError, ConnectionResetError):
                return
            return
        path = (DIST / url.path.lstrip("/")).resolve() if url.path != "/" else DIST / "index.html"
        if DIST not in path.parents or not path.is_file():
            self._json({"ok": False, "message": "Página não encontrada."}, 404)
            return
        data = path.read_bytes()
        self._headers(200, mimetypes.guess_type(path.name)[0] or "application/octet-stream")
        self.wfile.write(data)

    def do_POST(self):
        url = urlparse(self.path)
        if self._origin() is None:
            self._json({"ok": False, "message": "Origem não autorizada."}, 403)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 32_768 or length <= 0:
                raise ValueError("Requisição vazia ou muito grande.")
            data = json.loads(self.rfile.read(length))
            if not isinstance(data, dict):
                raise ValueError("Envie um objeto JSON.")
            sid, session = _session(self._sid(url), create=False)
            if not session:
                raise ValueError("Sessão expirada. Atualize a página.")
            with session.lock:
                result = self._command(session.runtime, url.path, data)
                state = session.runtime.status()
            self._json({"ok": result.get("ok", True), "result": result, "state": state}, session_id=sid)
        except MissingCaptureDependency as exc:
            self._json({"ok": False, "message": str(exc)}, 503)
        except (ValueError, TypeError, KeyError) as exc:
            self._json({"ok": False, "message": str(exc)}, 400)
        except Exception as exc:
            self._error(exc)

    def _command(self, runtime, path, data):
        if path == "/api/start":
            facts = data.get("facts") or {}
            if not isinstance(facts, dict):
                raise ValueError("Os fatos estruturados devem formar um objeto JSON.")
            platform = data.get("platform")
            return runtime.start(platform, data.get("value"), price=str(data.get("price") or "")[:100], info=str(data.get("info") or "")[:4000], facts=facts, style=data.get("style", "equilibrado"))
        if path == "/api/stop":
            return runtime.stop()
        if path == "/api/alert-mode":
            return runtime.alert_mode(data.get("mode"))
        if path == "/api/sales-style":
            return runtime.sales_style(data.get("style"))
        if path == "/api/product-info":
            facts = data.get("facts") or {}
            if not isinstance(facts, dict):
                raise ValueError("Os fatos estruturados devem formar um objeto JSON.")
            return runtime.product_info(price=str(data.get("price") or "")[:100], info=str(data.get("info") or "")[:4000], facts=facts)
        if path == "/api/product-link":
            return runtime.product_link(data.get("url"))
        raise ValueError("Comando desconhecido.")

    def _error(self, exc):
        traceback.print_exc()
        self._json({"ok": False, "message": f"Erro interno: {type(exc).__name__}."}, 500)


def main():
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    print(f"AGCN LIVE disponível na porta {port}.", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for session in list(_sessions.values()):
            session.runtime.stop()
        server.server_close()


if __name__ == "__main__":
    main()
