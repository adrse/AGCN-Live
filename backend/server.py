"""AGCN LIVE HTTP + SSE server - V2.1.

Mantem a interface atual compativel e expoe:
- monitoramento Shopee/TikTok;
- Live Coach;
- Product Context V1;
- Sales Coach V2;
- Live History V1.

Historico sem login:
- o frontend gera um owner_key anonimo;
- fetch usa X-AGCN-Owner;
- EventSource pode usar ?owner=...;
- o owner_key separa o historico de cada navegador.
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
from http.server import (
    BaseHTTPRequestHandler,
    ThreadingHTTPServer,
)
from pathlib import Path
from urllib.parse import (
    parse_qs,
    urlparse,
)

from .runtime import (
    MissingCaptureDependency,
    load_runtime,
)


DIST = (
    Path(__file__).resolve().parents[1]
    / "dist"
)

MAX_SESSIONS = int(
    os.environ.get(
        "AGCN_MAX_SESSIONS",
        "8",
    )
)

SESSION_TTL_SECONDS = int(
    os.environ.get(
        "AGCN_SESSION_TTL_SECONDS",
        "3600",
    )
)

MAX_REQUEST_BYTES = int(
    os.environ.get(
        "AGCN_MAX_REQUEST_BYTES",
        "65536",
    )
)

ALLOWED_ORIGINS = {
    value.strip().rstrip("/")
    for value in os.environ.get(
        "AGCN_ALLOWED_ORIGINS",
        "",
    ).split(",")
    if value.strip()
}

OWNER_RE = re.compile(
    r"^[A-Za-z0-9_-]{16,160}$"
)

_sessions = {}
_registry_lock = threading.Lock()


# ============================================================
# SESSION
# ============================================================

class Session:
    def __init__(self):
        self.runtime = load_runtime()
        self.lock = threading.RLock()
        self.touched = time.monotonic()
        self.history_owner = None


def _cleanup_stale_sessions():
    now = time.monotonic()

    for sid, old in list(
        _sessions.items()
    ):
        if (
            now - old.touched
            <= SESSION_TTL_SECONDS
        ):
            continue

        try:
            old.runtime.stop()
        except Exception:
            pass

        _sessions.pop(
            sid,
            None,
        )


def _session(
    identifier,
    create=True,
):
    with _registry_lock:
        session = (
            _sessions.get(
                identifier
            )
            if identifier
            else None
        )

        if session:
            session.touched = (
                time.monotonic()
            )
            return (
                identifier,
                session,
            )

        if not create:
            return (
                None,
                None,
            )

        _cleanup_stale_sessions()

        if (
            len(_sessions)
            >= MAX_SESSIONS
        ):
            raise ValueError(
                "Limite de sessoes simultaneas atingido. "
                "Tente novamente em alguns minutos."
            )

        identifier = (
            secrets.token_urlsafe(
                24
            )
        )

        session = Session()

        _sessions[
            identifier
        ] = session

        return (
            identifier,
            session,
        )


# ============================================================
# HTTP
# ============================================================

class Handler(
    BaseHTTPRequestHandler
):
    server_version = (
        "AGCNLive/2.1"
    )

    # --------------------------------------------------------
    # LOG
    # --------------------------------------------------------

    def log_message(
        self,
        fmt,
        *args,
    ):
        entry = fmt % args

        entry = re.sub(
            r"sid=[A-Za-z0-9_-]+",
            "sid=[redacted]",
            entry,
        )

        entry = re.sub(
            r"owner=[A-Za-z0-9_-]+",
            "owner=[redacted]",
            entry,
        )

        print(
            f"[http] "
            f"{self.address_string()} "
            f"{entry}",
            flush=True,
        )

    # --------------------------------------------------------
    # ORIGIN
    # --------------------------------------------------------

    def _origin(self):
        origin = (
            self.headers.get(
                "Origin",
                "",
            )
            .rstrip("/")
        )

        if not origin:
            return ""

        hostname = (
            self.headers.get(
                "Host",
                "",
            )
        )

        same_origin = {
            f"http://{hostname}",
            f"https://{hostname}",
        }

        if (
            origin in same_origin
            or origin in ALLOWED_ORIGINS
        ):
            return origin

        return None

    # --------------------------------------------------------
    # HEADERS
    # --------------------------------------------------------

    def _headers(
        self,
        code=200,
        content_type=(
            "application/json; "
            "charset=utf-8"
        ),
        session_id=None,
    ):
        self.send_response(
            code
        )

        self.send_header(
            "Content-Type",
            content_type,
        )

        self.send_header(
            "Cache-Control",
            "no-store",
        )

        self.send_header(
            "X-Content-Type-Options",
            "nosniff",
        )

        self.send_header(
            "Referrer-Policy",
            "no-referrer",
        )

        self.send_header(
            "X-Frame-Options",
            "DENY",
        )

        origin = self._origin()

        if origin:
            self.send_header(
                "Access-Control-Allow-Origin",
                origin,
            )

            self.send_header(
                "Access-Control-Allow-Methods",
                "GET, POST, OPTIONS",
            )

            self.send_header(
                "Access-Control-Allow-Headers",
                (
                    "Content-Type, "
                    "X-AGCN-Session, "
                    "X-AGCN-Owner"
                ),
            )

            self.send_header(
                "Access-Control-Expose-Headers",
                "X-AGCN-Session",
            )

            self.send_header(
                "Vary",
                "Origin",
            )

        if session_id:
            self.send_header(
                "X-AGCN-Session",
                session_id,
            )

        self.end_headers()

    def _json(
        self,
        obj,
        code=200,
        session_id=None,
    ):
        encoded = json.dumps(
            obj,
            ensure_ascii=False,
            default=str,
        ).encode(
            "utf-8"
        )

        self._headers(
            code,
            session_id=session_id,
        )

        self.wfile.write(
            encoded
        )

    # --------------------------------------------------------
    # IDS
    # --------------------------------------------------------

    def _sid(
        self,
        url,
    ):
        return (
            self.headers.get(
                "X-AGCN-Session"
            )
            or parse_qs(
                url.query
            ).get(
                "sid",
                [None],
            )[0]
        )

    def _owner(
        self,
        url,
        *,
        required=False,
    ):
        owner = (
            self.headers.get(
                "X-AGCN-Owner"
            )
            or parse_qs(
                url.query
            ).get(
                "owner",
                [None],
            )[0]
        )

        if owner is None:
            if required:
                raise ValueError(
                    "Identificador de historico ausente."
                )
            return None

        owner = str(
            owner
        ).strip()

        if not OWNER_RE.fullmatch(
            owner
        ):
            raise ValueError(
                "Identificador de historico invalido."
            )

        return owner

    def _bind_owner(
        self,
        session,
        url,
        *,
        required=False,
    ):
        owner = self._owner(
            url,
            required=required,
        )

        if owner is None:
            return None

        if (
            session.history_owner
            == owner
        ):
            return owner

        result = (
            session.runtime.set_history_owner(
                owner
            )
        )

        session.history_owner = (
            result.get(
                "owner_key"
            )
            or owner
        )

        return session.history_owner

    # --------------------------------------------------------
    # BODY
    # --------------------------------------------------------

    def _read_json_body(self):
        try:
            length = int(
                self.headers.get(
                    "Content-Length",
                    "0",
                )
            )
        except ValueError:
            raise ValueError(
                "Content-Length invalido."
            ) from None

        if length <= 0:
            raise ValueError(
                "Requisicao vazia."
            )

        if (
            length
            > MAX_REQUEST_BYTES
        ):
            raise ValueError(
                "Requisicao muito grande."
            )

        raw = self.rfile.read(
            length
        )

        try:
            data = json.loads(
                raw
            )
        except json.JSONDecodeError:
            raise ValueError(
                "JSON invalido."
            ) from None

        if not isinstance(
            data,
            dict,
        ):
            raise ValueError(
                "Envie um objeto JSON."
            )

        return data

    # --------------------------------------------------------
    # OPTIONS
    # --------------------------------------------------------

    def do_OPTIONS(self):
        if self._origin() is None:
            self._json(
                {
                    "ok": False,
                    "message":
                        "Origem nao autorizada.",
                },
                403,
            )
            return

        self._headers(
            204
        )

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    def do_GET(self):
        url = urlparse(
            self.path
        )

        if (
            url.path.startswith(
                "/api/"
            )
            and self._origin()
            is None
        ):
            self._json(
                {
                    "ok": False,
                    "message":
                        "Origem nao autorizada.",
                },
                403,
            )
            return

        # ====================================================
        # HEALTH
        # ====================================================

        if url.path == "/api/health":
            self._json({
                "ok":
                    True,
                "name":
                    "AGCN LIVE",
                "runtime":
                    "v2.1",
                "transport":
                    "sse",
                "sales_coach":
                    "v2",
                "product_context":
                    "v1",
                "live_history":
                    "v1",
            })
            return

        # ====================================================
        # FULL STATE
        # ====================================================

        if url.path == "/api/state":
            try:
                sid, session = (
                    _session(
                        self._sid(
                            url
                        )
                    )
                )

                with session.lock:
                    self._bind_owner(
                        session,
                        url,
                        required=False,
                    )

                    state = (
                        session.runtime.status()
                    )

                self._json(
                    state,
                    session_id=sid,
                )

            except (
                ValueError,
                TypeError,
                KeyError,
            ) as exc:
                self._json(
                    {
                        "ok": False,
                        "message":
                            str(
                                exc
                            ),
                    },
                    400,
                )

            except Exception as exc:
                self._error(
                    exc
                )

            return

        # ====================================================
        # PRODUCT CONTEXT
        # ====================================================

        if (
            url.path
            == "/api/product-context"
        ):
            try:
                sid, session = (
                    _session(
                        self._sid(
                            url
                        )
                    )
                )

                with session.lock:
                    self._bind_owner(
                        session,
                        url,
                        required=False,
                    )

                    product_context = (
                        session.runtime
                        .product_context_state()
                    )

                self._json(
                    {
                        "ok":
                            True,
                        "product_context":
                            product_context,
                    },
                    session_id=sid,
                )

            except (
                ValueError,
                TypeError,
                KeyError,
            ) as exc:
                self._json(
                    {
                        "ok": False,
                        "message":
                            str(
                                exc
                            ),
                    },
                    400,
                )

            except Exception as exc:
                self._error(
                    exc
                )

            return

        # ====================================================
        # HISTORY SUMMARY
        #
        # GET /api/history/summary?days=7
        # GET /api/history/summary?days=30
        # ====================================================

        if (
            url.path
            == "/api/history/summary"
        ):
            try:
                query = parse_qs(
                    url.query
                )

                days = (
                    query.get(
                        "days",
                        ["7"],
                    )[0]
                )

                sid, session = (
                    _session(
                        self._sid(
                            url
                        )
                    )
                )

                with session.lock:
                    self._bind_owner(
                        session,
                        url,
                        required=True,
                    )

                    summary = (
                        session.runtime
                        .history_summary(
                            days=days
                        )
                    )

                self._json(
                    {
                        "ok":
                            True,
                        "summary":
                            summary,
                    },
                    session_id=sid,
                )

            except (
                ValueError,
                TypeError,
                KeyError,
            ) as exc:
                self._json(
                    {
                        "ok": False,
                        "message":
                            str(
                                exc
                            ),
                    },
                    400,
                )

            except Exception as exc:
                self._error(
                    exc
                )

            return

        # ====================================================
        # HISTORY RECENT
        #
        # GET /api/history/recent?limit=20
        # ====================================================

        if (
            url.path
            == "/api/history/recent"
        ):
            try:
                query = parse_qs(
                    url.query
                )

                limit = (
                    query.get(
                        "limit",
                        ["20"],
                    )[0]
                )

                sid, session = (
                    _session(
                        self._sid(
                            url
                        )
                    )
                )

                with session.lock:
                    self._bind_owner(
                        session,
                        url,
                        required=True,
                    )

                    items = (
                        session.runtime
                        .history_recent(
                            limit=limit
                        )
                    )

                self._json(
                    {
                        "ok":
                            True,
                        "items":
                            items,
                    },
                    session_id=sid,
                )

            except (
                ValueError,
                TypeError,
                KeyError,
            ) as exc:
                self._json(
                    {
                        "ok": False,
                        "message":
                            str(
                                exc
                            ),
                    },
                    400,
                )

            except Exception as exc:
                self._error(
                    exc
                )

            return

        # ====================================================
        # LAST FINISHED LIVE
        #
        # O summary ja devolve last_live global do owner.
        # ====================================================

        if (
            url.path
            == "/api/history/last"
        ):
            try:
                sid, session = (
                    _session(
                        self._sid(
                            url
                        )
                    )
                )

                with session.lock:
                    self._bind_owner(
                        session,
                        url,
                        required=True,
                    )

                    summary = (
                        session.runtime
                        .history_summary(
                            days=7
                        )
                    )

                    last_live = (
                        summary.get(
                            "last_live"
                        )
                    )

                self._json(
                    {
                        "ok":
                            True,
                        "last_live":
                            last_live,
                    },
                    session_id=sid,
                )

            except (
                ValueError,
                TypeError,
                KeyError,
            ) as exc:
                self._json(
                    {
                        "ok": False,
                        "message":
                            str(
                                exc
                            ),
                    },
                    400,
                )

            except Exception as exc:
                self._error(
                    exc
                )

            return

        # ====================================================
        # SSE
        #
        # EventSource nao aceita header customizado.
        # A Interface V9 usara:
        # /api/events?sid=...&owner=...
        # ====================================================

        if url.path == "/api/events":
            try:
                sid, session = (
                    _session(
                        self._sid(
                            url
                        ),
                        create=False,
                    )
                )

                if not session:
                    self._json(
                        {
                            "ok": False,
                            "message":
                                "Sessao expirada. "
                                "Atualize a pagina.",
                        },
                        401,
                    )
                    return

                with session.lock:
                    self._bind_owner(
                        session,
                        url,
                        required=False,
                    )

                self._headers(
                    200,
                    (
                        "text/event-stream; "
                        "charset=utf-8"
                    ),
                    session_id=sid,
                )

                while True:
                    session.touched = (
                        time.monotonic()
                    )

                    with session.lock:
                        state = (
                            session.runtime.status()
                        )

                    event = json.dumps(
                        state,
                        ensure_ascii=False,
                        default=str,
                    )

                    self.wfile.write(
                        (
                            "event: state\n"
                            f"data: {event}\n\n"
                        ).encode(
                            "utf-8"
                        )
                    )

                    self.wfile.flush()

                    time.sleep(
                        1
                    )

            except (
                BrokenPipeError,
                ConnectionResetError,
            ):
                return

            except (
                ValueError,
                TypeError,
                KeyError,
            ) as exc:
                try:
                    self._json(
                        {
                            "ok": False,
                            "message":
                                str(
                                    exc
                                ),
                        },
                        400,
                    )
                except Exception:
                    pass

            except Exception as exc:
                self._error(
                    exc
                )

            return

        # ====================================================
        # STATIC FRONTEND
        # ====================================================

        path = (
            (
                DIST
                / url.path.lstrip("/")
            ).resolve()
            if url.path != "/"
            else (
                DIST
                / "index.html"
            )
        )

        if (
            DIST not in path.parents
            or not path.is_file()
        ):
            self._json(
                {
                    "ok": False,
                    "message":
                        "Pagina nao encontrada.",
                },
                404,
            )
            return

        data = (
            path.read_bytes()
        )

        self._headers(
            200,
            (
                mimetypes.guess_type(
                    path.name
                )[0]
                or "application/octet-stream"
            ),
        )

        self.wfile.write(
            data
        )

    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------

    def do_POST(self):
        url = urlparse(
            self.path
        )

        if self._origin() is None:
            self._json(
                {
                    "ok": False,
                    "message":
                        "Origem nao autorizada.",
                },
                403,
            )
            return

        try:
            data = (
                self._read_json_body()
            )

            sid, session = (
                _session(
                    self._sid(
                        url
                    ),
                    create=False,
                )
            )

            if not session:
                raise ValueError(
                    "Sessao expirada. "
                    "Atualize a pagina."
                )

            session.touched = (
                time.monotonic()
            )

            with session.lock:
                # Fundamental: o owner precisa estar definido
                # ANTES de /api/start criar o registro.
                self._bind_owner(
                    session,
                    url,
                    required=False,
                )

                result = self._command(
                    session.runtime,
                    url.path,
                    data,
                )

                state = (
                    session.runtime.status()
                )

            self._json(
                {
                    "ok":
                        result.get(
                            "ok",
                            True,
                        ),
                    "result":
                        result,
                    "state":
                        state,
                },
                session_id=sid,
            )

        except MissingCaptureDependency as exc:
            self._json(
                {
                    "ok": False,
                    "message":
                        str(
                            exc
                        ),
                },
                503,
            )

        except (
            ValueError,
            TypeError,
            KeyError,
        ) as exc:
            self._json(
                {
                    "ok": False,
                    "message":
                        str(
                            exc
                        ),
                },
                400,
            )

        except Exception as exc:
            self._error(
                exc
            )

    # --------------------------------------------------------
    # COMMAND ROUTER
    # --------------------------------------------------------

    def _command(
        self,
        runtime,
        path,
        data,
    ):
        # ====================================================
        # LIVE
        # ====================================================

        if path == "/api/start":
            facts = (
                data.get(
                    "facts"
                )
                or {}
            )

            if not isinstance(
                facts,
                dict,
            ):
                raise ValueError(
                    "Os fatos estruturados "
                    "devem formar um objeto JSON."
                )

            return runtime.start(
                data.get(
                    "platform"
                ),
                data.get(
                    "value"
                ),
                price=str(
                    data.get(
                        "price"
                    )
                    or ""
                )[:100],
                info=str(
                    data.get(
                        "info"
                    )
                    or ""
                )[:4000],
                facts=facts,
                style=data.get(
                    "style"
                ),
            )

        if path == "/api/stop":
            return runtime.stop()

        # ====================================================
        # LIVE COACH
        # ====================================================

        if path == "/api/alert-mode":
            return runtime.alert_mode(
                data.get(
                    "mode"
                )
            )

        # ====================================================
        # PRODUCT CONTEXT V1
        # ====================================================

        if (
            path
            == "/api/product-context"
        ):
            product = (
                data.get(
                    "product"
                )
                if isinstance(
                    data.get(
                        "product"
                    ),
                    dict,
                )
                else data
            )

            allowed = {
                "name",
                "regular_price",
                "current_price",
                "discount",
                "description",
                "additional_info",
                "mode",
            }

            payload = {
                key: value
                for key, value
                in product.items()
                if key in allowed
            }

            replace = bool(
                data.get(
                    "replace",
                    False,
                )
            )

            return (
                runtime
                .product_context_update(
                    payload,
                    replace=replace,
                )
            )

        if (
            path
            == "/api/product-context/activate"
        ):
            return (
                runtime
                .product_context_activate(
                    mode=data.get(
                        "mode"
                    )
                )
            )

        if (
            path
            == "/api/product-context/deactivate"
        ):
            return (
                runtime
                .product_context_deactivate()
            )

        if (
            path
            == "/api/product-context/clear"
        ):
            return (
                runtime
                .product_context_clear()
            )

        # ====================================================
        # SALES MODE
        # ====================================================

        if path == "/api/sales-mode":
            return runtime.sales_mode(
                data.get(
                    "mode"
                )
            )

        # ====================================================
        # LEGACY COMPATIBILITY
        # ====================================================

        if path == "/api/sales-style":
            value = (
                data.get(
                    "mode"
                )
                or data.get(
                    "style"
                )
            )

            return runtime.sales_style(
                value
            )

        if path == "/api/product-info":
            facts = (
                data.get(
                    "facts"
                )
                or {}
            )

            if not isinstance(
                facts,
                dict,
            ):
                raise ValueError(
                    "Os fatos estruturados "
                    "devem formar um objeto JSON."
                )

            return runtime.product_info(
                price=str(
                    data.get(
                        "price"
                    )
                    or ""
                )[:100],
                info=str(
                    data.get(
                        "info"
                    )
                    or ""
                )[:4000],
                facts=facts,
            )

        if path == "/api/product-link":
            return runtime.product_link(
                data.get(
                    "url"
                )
            )

        raise ValueError(
            "Comando desconhecido."
        )

    # --------------------------------------------------------
    # INTERNAL ERROR
    # --------------------------------------------------------

    def _error(
        self,
        exc,
    ):
        traceback.print_exc()

        self._json(
            {
                "ok": False,
                "message": (
                    "Erro interno: "
                    f"{type(exc).__name__}."
                ),
            },
            500,
        )


# ============================================================
# MAIN
# ============================================================

def main():
    port = int(
        os.environ.get(
            "PORT",
            "8000",
        )
    )

    server = (
        ThreadingHTTPServer(
            (
                "0.0.0.0",
                port,
            ),
            Handler,
        )
    )

    server.daemon_threads = (
        True
    )

    print(
        (
            "AGCN LIVE disponivel "
            f"na porta {port}."
        ),
        flush=True,
    )

    try:
        server.serve_forever(
            poll_interval=0.5
        )

    except KeyboardInterrupt:
        pass

    finally:
        for session in list(
            _sessions.values()
        ):
            try:
                session.runtime.stop()
            except Exception:
                pass

        server.server_close()


if __name__ == "__main__":
    main()
