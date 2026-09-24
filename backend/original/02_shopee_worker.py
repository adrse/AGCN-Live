"""
AGCN LIVE - SHOPEE WORKER V2.0

Objetivo:
- resolver link curto da Shopee LIVE;
- capturar dados reais da sessao;
- preservar cookies/contexto obtidos durante a resolucao;
- reutilizar headers observados no navegador quando necessario;
- tentar caminhos de fallback sem inventar dados;
- falhar de forma explicita em vez de ficar em "CONECTANDO..." para sempre;
- manter a API publica esperada pelo Live Engine e pela Interface V8.

IMPORTANTE:
- nenhum dado de metricas e fabricado;
- nenhuma informacao comercial e inferida;
- o Worker apenas coleta a LIVE;
- Product Context e Sales Coach V2 continuam separados.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from collections import deque
from datetime import datetime
from urllib.parse import (
    parse_qs,
    unquote,
    urlparse,
)
from zoneinfo import ZoneInfo

import requests

from playwright.async_api import (
    async_playwright,
)

from IPython.display import (
    clear_output,
)


# ============================================================
# ENTRADA
# ============================================================

LINK_CURTO = None


# ============================================================
# CONFIGURACOES
# ============================================================

DURACAO = 300
MAX_COMENTARIOS = 10

TIMEZONE = ZoneInfo(
    "America/Araguaina"
)

SHOPEE_MOBILE_UA = (
    "Mozilla/5.0 "
    "(Linux; Android 14; Pixel 7) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/140.0.0.0 "
    "Mobile Safari/537.36"
)

SHOPEE_RESOLVE_TIMEOUT_MS = 30000
SHOPEE_SESSION_TIMEOUT_MS = 12000

# Antes de existir a primeira metrica, nao deixamos a interface
# presa para sempre em "Conectando".
SHOPEE_MAX_INITIAL_FAILURES = 3

# Depois de ja ter conectado, falhas transitorias podem ocorrer.
SHOPEE_MAX_POST_CONNECT_FAILURES = 12


# ============================================================
# ESTADO DA LIVE
# ============================================================

estado = {
    "sessionId": None,
    "chatroomId": None,
    "loja": None,
    "username": None,
    "titulo": None,
    "viewers": None,
    "likes": None,
    "shares": None,
    "products": None,
    "memberCnt": None,
    "status": None,
    "ultimaMetrica": None,
    "ultimoChat": None,
    "erro": None,
    "urlExpandida": None,

    # Diagnostico operacional.
    "metodoSessao": None,
    "sessionHttpStatus": None,
    "tentativasSessao": 0,
    "ultimaFalhaSessao": None,
}


comentarios = deque(
    maxlen=MAX_COMENTARIOS
)

comentarios_ids = set()

encerrar = False

LIVE_URL = None
SESSION_ID = None
ENDPOINT_SESSION = None

# Estado obtido pelo navegador durante a resolucao do link curto.
# Reutilizar este contexto e importante porque a Shopee pode
# depender de cookies/tokens criados nessa primeira navegacao.
SHOPEE_STORAGE_STATE = None

# Somente headers selecionados. Nunca sao impressos nos logs.
SHOPEE_SESSION_HEADERS = {}


# ============================================================
# HELPERS DE URL
# ============================================================

def host_permitido(
    host,
):
    host = (
        host
        or ""
    ).lower().rstrip(".")

    return (
        host == "br.shp.ee"
        or host.endswith(
            ".shp.ee"
        )
        or host == "shopee.com.br"
        or host.endswith(
            ".shopee.com.br"
        )
    )


def validar_url_shopee(
    url,
):
    parsed = urlparse(
        str(
            url
            or ""
        ).strip()
    )

    if parsed.scheme != "https":
        raise ValueError(
            "Somente HTTPS e permitido."
        )

    if not host_permitido(
        parsed.hostname
    ):
        raise ValueError(
            "Dominio da Shopee nao permitido."
        )


def extrair_session_id(
    url,
):
    try:
        query = parse_qs(
            urlparse(
                str(
                    url
                    or ""
                )
            ).query
        )

        value = query.get(
            "session"
        )

        if not value:
            return None

        return str(
            value[0]
        )

    except Exception:
        return None


def detectar_live(
    url,
):
    if not url:
        return None

    candidate = str(
        url
    )

    for _ in range(3):
        decoded = unquote(
            candidate
        )

        if decoded == candidate:
            break

        candidate = decoded

    try:
        parsed = urlparse(
            candidate
        )
    except Exception:
        return None

    if (
        parsed.hostname
        != "live.shopee.com.br"
    ):
        return None

    session_id = (
        extrair_session_id(
            candidate
        )
    )

    if not session_id:
        return None

    return {
        "url":
            candidate,
        "sessionId":
            session_id,
    }


def _session_endpoint_url():
    if not SESSION_ID:
        return None

    return (
        "https://live.shopee.com.br"
        f"/api/v1/session/{SESSION_ID}"
    )


def _is_session_url(
    url,
):
    if not ENDPOINT_SESSION:
        return False

    try:
        path = (
            urlparse(
                str(
                    url
                    or ""
                )
            ).path
            or ""
        ).rstrip("/")

        target = (
            str(
                ENDPOINT_SESSION
            ).rstrip("/")
        )

        return path == target

    except Exception:
        return False


# ============================================================
# HEADERS / CONTEXTO
# ============================================================

def _filtrar_headers_sessao(
    headers,
):
    if not isinstance(
        headers,
        dict,
    ):
        return {}

    allowed = {
        "accept",
        "accept-language",
        "client-info",
        "referer",
        "user-agent",
        "x-livestreaming-source",
        "x-ls-sz-token",
    }

    result = {}

    for key, value in (
        headers.items()
    ):
        normalized = str(
            key
        ).strip().lower()

        if (
            normalized in allowed
            and value is not None
        ):
            result[
                normalized
            ] = str(
                value
            )

    return result


def _base_session_headers():
    headers = {
        "accept":
            "application/json, text/plain, */*",
        "accept-language":
            "pt-BR,pt;q=0.9,en;q=0.8",
        "referer":
            str(
                LIVE_URL
                or "https://live.shopee.com.br/"
            ),
        "user-agent":
            SHOPEE_MOBILE_UA,
        "x-livestreaming-source":
            "shopee",
    }

    headers.update(
        _filtrar_headers_sessao(
            SHOPEE_SESSION_HEADERS
        )
    )

    return headers


async def _guardar_contexto(
    context,
):
    global SHOPEE_STORAGE_STATE

    try:
        SHOPEE_STORAGE_STATE = (
            await context.storage_state()
        )
    except Exception:
        SHOPEE_STORAGE_STATE = None


def _guardar_headers_request(
    request,
):
    global SHOPEE_SESSION_HEADERS

    try:
        if not _is_session_url(
            request.url
        ):
            return

        headers = (
            request.headers
            if isinstance(
                request.headers,
                dict,
            )
            else {}
        )

        selected = (
            _filtrar_headers_sessao(
                headers
            )
        )

        if selected:
            SHOPEE_SESSION_HEADERS.update(
                selected
            )

    except Exception:
        pass


# ============================================================
# RESOLVER LINK CURTO
# ============================================================

async def resolver_link_mobile(
    browser,
    link,
):
    """
    Resolve https://br.shp.ee/... dentro de um navegador mobile.

    Alem do URL final, preserva storage_state do contexto para que a
    consulta da sessao possa reutilizar cookies criados pela Shopee.
    """

    validar_url_shopee(
        link
    )

    host_inicial = (
        urlparse(
            link
        ).hostname
        or ""
    ).lower()

    if (
        host_inicial
        != "br.shp.ee"
    ):
        raise ValueError(
            "Informe um link curto no formato "
            "https://br.shp.ee/..."
        )

    context = (
        await browser.new_context(
            user_agent=
                SHOPEE_MOBILE_UA,
            viewport={
                "width": 390,
                "height": 844,
            },
            screen={
                "width": 390,
                "height": 844,
            },
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
            locale="pt-BR",
            timezone_id=
                "America/Araguaina",
        )
    )

    page = await context.new_page()
    urls_vistas = set()

    def registrar_request(
        request,
    ):
        try:
            urls_vistas.add(
                request.url
            )
        except Exception:
            pass

    page.on(
        "request",
        registrar_request,
    )

    try:
        try:
            await page.goto(
                link,
                wait_until=
                    "domcontentloaded",
                timeout=
                    SHOPEE_RESOLVE_TIMEOUT_MS,
            )
        except Exception:
            # Redirect pode ter acontecido antes do timeout.
            pass

        for _ in range(60):
            achou = detectar_live(
                page.url
            )

            if achou:
                validar_url_shopee(
                    achou[
                        "url"
                    ]
                )
                await _guardar_contexto(
                    context
                )
                return achou

            for url in list(
                urls_vistas
            ):
                achou = detectar_live(
                    url
                )

                if achou:
                    validar_url_shopee(
                        achou[
                            "url"
                        ]
                    )
                    await _guardar_contexto(
                        context
                    )
                    return achou

            try:
                links = (
                    await page.eval_on_selector_all(
                        "a[href]",
                        """
                        els => els
                            .map(e => e.href)
                            .filter(Boolean)
                        """,
                    )
                )

                for url in links:
                    achou = detectar_live(
                        url
                    )

                    if achou:
                        validar_url_shopee(
                            achou[
                                "url"
                            ]
                        )
                        await _guardar_contexto(
                            context
                        )
                        return achou

            except Exception:
                pass

            try:
                candidates = (
                    await page.evaluate(
                        """
                        () => {
                            const result = [];

                            const canonical =
                                document.querySelector(
                                    'link[rel="canonical"]'
                                );

                            if (canonical?.href) {
                                result.push(
                                    canonical.href
                                );
                            }

                            const og =
                                document.querySelector(
                                    'meta[property="og:url"]'
                                );

                            if (og?.content) {
                                result.push(
                                    og.content
                                );
                            }

                            return result;
                        }
                        """
                    )
                )

                for url in candidates:
                    achou = detectar_live(
                        url
                    )

                    if achou:
                        validar_url_shopee(
                            achou[
                                "url"
                            ]
                        )
                        await _guardar_contexto(
                            context
                        )
                        return achou

            except Exception:
                pass

            await asyncio.sleep(
                0.5
            )

        raise RuntimeError(
            "Nao consegui resolver o link curto "
            "para uma LIVE com session=..."
        )

    finally:
        await context.close()


# ============================================================
# UTILITARIOS
# ============================================================

def agora():
    return datetime.now(
        TIMEZONE
    ).strftime(
        "%H:%M:%S"
    )


def tempo_desde(
    ts,
):
    if ts is None:
        return "aguardando"

    seconds = (
        time.time()
        - ts
    )

    if seconds < 1:
        return "agora"

    return (
        f"{seconds:.1f}s atras"
    )


def _extract_session(
    payload,
):
    if not isinstance(
        payload,
        dict,
    ):
        return None

    data = (
        payload.get(
            "data"
        )
        or {}
    )

    if not isinstance(
        data,
        dict,
    ):
        return None

    session = data.get(
        "session"
    )

    if not isinstance(
        session,
        dict,
    ):
        session = data

    if not isinstance(
        session,
        dict,
    ):
        return None

    if not (
        "viewer_count" in session
        or "like_cnt" in session
        or "chatroom_id" in session
        or "member_cnt" in session
        or "status" in session
    ):
        return None

    return session


def _apply_session(
    session,
    *,
    method,
    http_status=200,
):
    if not isinstance(
        session,
        dict,
    ):
        return False

    estado[
        "chatroomId"
    ] = (
        session.get(
            "chatroom_id"
        )
        or estado[
            "chatroomId"
        ]
    )

    estado[
        "loja"
    ] = (
        session.get(
            "nickname"
        )
        or estado[
            "loja"
        ]
    )

    estado[
        "username"
    ] = (
        session.get(
            "username"
        )
        or estado[
            "username"
        ]
    )

    estado[
        "titulo"
    ] = (
        session.get(
            "title"
        )
        or estado[
            "titulo"
        ]
    )

    estado[
        "viewers"
    ] = session.get(
        "viewer_count",
        estado[
            "viewers"
        ],
    )

    estado[
        "likes"
    ] = session.get(
        "like_cnt",
        estado[
            "likes"
        ],
    )

    estado[
        "shares"
    ] = session.get(
        "share_cnt",
        estado[
            "shares"
        ],
    )

    estado[
        "products"
    ] = session.get(
        "items_cnt",
        estado[
            "products"
        ],
    )

    estado[
        "memberCnt"
    ] = session.get(
        "member_cnt",
        estado[
            "memberCnt"
        ],
    )

    estado[
        "status"
    ] = session.get(
        "status",
        estado[
            "status"
        ],
    )

    estado[
        "ultimaMetrica"
    ] = time.time()

    estado[
        "metodoSessao"
    ] = method

    estado[
        "sessionHttpStatus"
    ] = http_status

    estado[
        "ultimaFalhaSessao"
    ] = None

    estado[
        "erro"
    ] = None

    return True


def _safe_json_loads(
    text,
):
    try:
        return json.loads(
            text
        )
    except Exception:
        return None


def _failure_message(
    label,
    detail=None,
):
    label = str(
        label
        or "Falha desconhecida"
    ).strip()

    detail = str(
        detail
        or ""
    ).strip()

    if detail:
        return (
            label
            + ": "
            + detail[:300]
        )

    return label[:350]


# ============================================================
# CAPTURA DA SESSAO - CAMINHO 1
# RESPOSTA NATIVA DA PAGINA
# ============================================================

async def _session_from_native_response(
    page,
):
    try:
        async with page.expect_response(
            lambda response:
                _is_session_url(
                    response.url
                ),
            timeout=
                SHOPEE_SESSION_TIMEOUT_MS,
        ) as info:
            try:
                await page.goto(
                    LIVE_URL,
                    wait_until=
                        "domcontentloaded",
                    timeout=
                        SHOPEE_SESSION_TIMEOUT_MS,
                )
            except Exception:
                # A resposta da API pode chegar mesmo com timeout parcial.
                pass

        response = await info.value

    except Exception as exc:
        detail = str(
            exc
            or ""
        )

        if "Timeout" in type(
            exc
        ).__name__ or "timeout" in detail.lower():
            return (
                False,
                "A pagina nao chamou o endpoint da sessao no tempo esperado.",
            )

        return (
            False,
            _failure_message(
                "Falha observando a requisicao nativa",
                exc,
            ),
        )

    try:
        _guardar_headers_request(
            response.request
        )
    except Exception:
        pass

    status = int(
        response.status
    )

    estado[
        "sessionHttpStatus"
    ] = status

    if status != 200:
        return (
            False,
            f"Endpoint de sessao respondeu HTTP {status}.",
        )

    try:
        payload = await response.json()
    except Exception as exc:
        return (
            False,
            _failure_message(
                "Resposta HTTP 200 sem JSON valido",
                exc,
            ),
        )

    session = _extract_session(
        payload
    )

    if session is None:
        return (
            False,
            "Resposta da sessao nao trouxe metricas reconhecidas.",
        )

    return (
        _apply_session(
            session,
            method="native_response",
            http_status=status,
        ),
        None,
    )


# ============================================================
# CAPTURA DA SESSAO - CAMINHO 2
# FETCH EXECUTADO DENTRO DA PAGINA
# ============================================================

async def _session_from_page_fetch(
    page,
):
    endpoint = (
        _session_endpoint_url()
    )

    if not endpoint:
        return (
            False,
            "Session ID indisponivel.",
        )

    headers = (
        _base_session_headers()
    )

    # Headers que o browser nao permite definir via fetch.
    browser_headers = {
        key: value
        for key, value in headers.items()
        if key not in {
            "user-agent",
            "referer",
        }
    }

    try:
        result = await page.evaluate(
            """
            async ({url, headers}) => {
                try {
                    const response = await fetch(
                        url,
                        {
                            method: 'GET',
                            credentials: 'include',
                            cache: 'no-store',
                            headers
                        }
                    );

                    return {
                        ok: response.ok,
                        status: response.status,
                        text: await response.text()
                    };
                } catch (error) {
                    return {
                        ok: false,
                        status: 0,
                        text: '',
                        error: String(error)
                    };
                }
            }
            """,
            {
                "url": endpoint,
                "headers":
                    browser_headers,
            },
        )

    except Exception as exc:
        return (
            False,
            _failure_message(
                "Fetch dentro da pagina falhou",
                exc,
            ),
        )

    if not isinstance(
        result,
        dict,
    ):
        return (
            False,
            "Fetch da pagina retornou resultado invalido.",
        )

    status = int(
        result.get(
            "status"
        )
        or 0
    )

    estado[
        "sessionHttpStatus"
    ] = status

    if status != 200:
        detail = (
            result.get(
                "error"
            )
            or ""
        )

        message = (
            f"Fetch da pagina respondeu HTTP {status}."
        )

        if detail:
            message += (
                " "
                + str(
                    detail
                )[:200]
            )

        return (
            False,
            message,
        )

    payload = _safe_json_loads(
        result.get(
            "text"
        )
        or ""
    )

    session = _extract_session(
        payload
    )

    if session is None:
        return (
            False,
            "Fetch da pagina recebeu JSON sem metricas reconhecidas.",
        )

    return (
        _apply_session(
            session,
            method="page_fetch",
            http_status=status,
        ),
        None,
    )


# ============================================================
# CAPTURA DA SESSAO - CAMINHO 3
# APIREQUESTCONTEXT COMPARTILHANDO COOKIES
# ============================================================

async def _session_from_context_request(
    context,
):
    endpoint = (
        _session_endpoint_url()
    )

    if not endpoint:
        return (
            False,
            "Session ID indisponivel.",
        )

    try:
        response = await context.request.get(
            endpoint,
            headers=
                _base_session_headers(),
            timeout=
                SHOPEE_SESSION_TIMEOUT_MS,
        )

    except Exception as exc:
        return (
            False,
            _failure_message(
                "Requisicao direta no contexto falhou",
                exc,
            ),
        )

    status = int(
        response.status
    )

    estado[
        "sessionHttpStatus"
    ] = status

    if status != 200:
        return (
            False,
            f"Requisicao no contexto respondeu HTTP {status}.",
        )

    try:
        payload = await response.json()
    except Exception as exc:
        return (
            False,
            _failure_message(
                "Requisicao HTTP 200 sem JSON valido",
                exc,
            ),
        )

    session = _extract_session(
        payload
    )

    if session is None:
        return (
            False,
            "Requisicao no contexto recebeu JSON sem metricas reconhecidas.",
        )

    return (
        _apply_session(
            session,
            method="context_request",
            http_status=status,
        ),
        None,
    )


# ============================================================
# SHOPEE SESSION READER
# ============================================================

async def ler_sessao(
    browser,
):
    context = None
    errors = []

    try:
        context_kwargs = {
            "user_agent":
                SHOPEE_MOBILE_UA,
            "locale":
                "pt-BR",
            "viewport": {
                "width": 390,
                "height": 844,
            },
            "screen": {
                "width": 390,
                "height": 844,
            },
            "device_scale_factor":
                3,
            "is_mobile":
                True,
            "has_touch":
                True,
            "timezone_id":
                "America/Araguaina",
        }

        if isinstance(
            SHOPEE_STORAGE_STATE,
            dict,
        ):
            context_kwargs[
                "storage_state"
            ] = SHOPEE_STORAGE_STATE

        context = (
            await browser.new_context(
                **context_kwargs
            )
        )

        page = await context.new_page()

        # Captura headers da requisicao de sessao quando a pagina
        # realmente fizer essa chamada.
        page.on(
            "request",
            _guardar_headers_request,
        )

        async def filtrar(
            route,
        ):
            resource_type = (
                route
                .request
                .resource_type
            )

            if resource_type in {
                "image",
                "media",
                "font",
            }:
                await route.abort()
            else:
                await route.continue_()

        await page.route(
            "**/*",
            filtrar,
        )

        # ----------------------------------------------------
        # 1. Caminho original: observar a propria Shopee.
        # ----------------------------------------------------

        success, error = (
            await _session_from_native_response(
                page
            )
        )

        if success:
            await _guardar_contexto(
                context
            )
            return True

        if error:
            errors.append(
                error
            )

        # A navegacao pode nao ter concluido quando expect_response
        # expirou. Tentamos manter a pagina da LIVE aberta.
        try:
            if (
                not page.url
                or "live.shopee.com.br"
                not in page.url
            ):
                await page.goto(
                    LIVE_URL,
                    wait_until=
                        "domcontentloaded",
                    timeout=
                        SHOPEE_SESSION_TIMEOUT_MS,
                )
        except Exception:
            pass

        await _guardar_contexto(
            context
        )

        # ----------------------------------------------------
        # 2. Fetch no proprio browser.
        # ----------------------------------------------------

        success, error = (
            await _session_from_page_fetch(
                page
            )
        )

        if success:
            await _guardar_contexto(
                context
            )
            return True

        if error:
            errors.append(
                error
            )

        # ----------------------------------------------------
        # 3. Request API do proprio BrowserContext.
        # Compartilha cookies com o contexto.
        # ----------------------------------------------------

        success, error = (
            await _session_from_context_request(
                context
            )
        )

        if success:
            await _guardar_contexto(
                context
            )
            return True

        if error:
            errors.append(
                error
            )

        detail = (
            errors[-1]
            if errors
            else "Falha desconhecida consultando a sessao."
        )

        estado[
            "ultimaFalhaSessao"
        ] = detail

        return False

    except Exception as exc:
        estado[
            "ultimaFalhaSessao"
        ] = (
            _failure_message(
                "Falha no leitor de sessao",
                exc,
            )
        )

        return False

    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass


# ============================================================
# WORKER CONTINUO DE METRICAS
# ============================================================

async def metricas_worker(
    browser,
):
    global encerrar

    consecutive_failures = 0
    ever_connected = False

    while not encerrar:
        success = await ler_sessao(
            browser
        )

        estado[
            "tentativasSessao"
        ] = (
            int(
                estado.get(
                    "tentativasSessao"
                )
                or 0
            )
            + 1
        )

        if success:
            consecutive_failures = 0
            ever_connected = True

            await asyncio.sleep(
                0.8
            )

            continue

        consecutive_failures += 1

        detail = (
            estado.get(
                "ultimaFalhaSessao"
            )
            or "Falha sem detalhe."
        )

        print(
            (
                "[Shopee] tentativa de sessao falhou "
                f"({consecutive_failures}): "
                f"{detail}"
            ),
            flush=True,
        )

        limit = (
            SHOPEE_MAX_POST_CONNECT_FAILURES
            if ever_connected
            else SHOPEE_MAX_INITIAL_FAILURES
        )

        if (
            consecutive_failures
            >= limit
        ):
            if ever_connected:
                estado[
                    "erro"
                ] = (
                    "Conexao com a Shopee foi perdida. "
                    f"Ultimo erro: {detail}"
                )
            else:
                estado[
                    "erro"
                ] = (
                    "Nao foi possivel conectar a Shopee. "
                    f"Ultimo erro: {detail}"
                )

            print(
                (
                    "[Shopee] monitoramento encerrado: "
                    + estado[
                        "erro"
                    ]
                ),
                flush=True,
            )

            encerrar = True
            break

        await asyncio.sleep(
            1.5
        )


# ============================================================
# CHAT
# ============================================================

def requisicao_chat(
    chatroom_id,
    chat_uuid,
    cursor,
):
    url = (
        "https://chatroom-live.shopee.com.br"
        f"/api/v1/fetch/chatroom/"
        f"{chatroom_id}"
        f"/message"
    )

    return requests.get(
        url,
        params={
            "uuid":
                chat_uuid,
            "timestamp":
                cursor,
            "version":
                "v2",
        },
        headers={
            "Accept":
                "application/json,text/plain,*/*",
            "User-Agent":
                SHOPEE_MOBILE_UA,
            "X-Livestreaming-Source":
                "shopee",
        },
        timeout=15,
    )


async def chat_worker():
    global encerrar

    chat_uuid = str(
        uuid.uuid4()
    )

    cursor = (
        int(
            time.time()
        )
        - 10
    )

    while (
        not estado[
            "chatroomId"
        ]
        and not encerrar
    ):
        await asyncio.sleep(
            0.5
        )

    while not encerrar:
        chatroom = estado[
            "chatroomId"
        ]

        if not chatroom:
            await asyncio.sleep(
                1
            )
            continue

        try:
            response = (
                await asyncio.to_thread(
                    requisicao_chat,
                    chatroom,
                    chat_uuid,
                    cursor,
                )
            )

            if (
                response.status_code
                != 200
            ):
                await asyncio.sleep(
                    2
                )
                continue

            payload = (
                response.json()
            )

            data = (
                payload.get(
                    "data"
                )
                or {}
            )

            if (
                data.get(
                    "timestamp"
                )
                is not None
            ):
                cursor = int(
                    data[
                        "timestamp"
                    ]
                )

            for group in (
                data.get(
                    "message"
                )
                or []
            ):
                for message in group.get(
                    "msgs",
                    [],
                ):
                    message_id = str(
                        message.get(
                            "id",
                            "",
                        )
                    )

                    if (
                        message_id
                        and message_id
                        in comentarios_ids
                    ):
                        continue

                    if message_id:
                        comentarios_ids.add(
                            message_id
                        )

                    name = (
                        message.get(
                            "display_name"
                        )
                        or message.get(
                            "nickname"
                        )
                        or "Usuario"
                    )

                    raw_content = (
                        message.get(
                            "content"
                        )
                    )

                    text = None

                    if isinstance(
                        raw_content,
                        str,
                    ):
                        try:
                            parsed_content = (
                                json.loads(
                                    raw_content
                                )
                            )

                            text = (
                                parsed_content.get(
                                    "content_v2"
                                )
                                or parsed_content.get(
                                    "content"
                                )
                            )

                        except Exception:
                            text = raw_content

                    if text:
                        comentarios.append({
                            "hora":
                                agora(),
                            "usuario":
                                name,
                            "texto":
                                text,
                        })

            estado[
                "ultimoChat"
            ] = time.time()

            interval = data.get(
                "poll_interval",
                3,
            )

            try:
                interval = float(
                    interval
                )
            except Exception:
                interval = 3

            await asyncio.sleep(
                max(
                    2,
                    interval,
                )
            )

        except Exception:
            await asyncio.sleep(
                2
            )


# ============================================================
# PAINEL ORIGINAL
# ============================================================

async def painel():
    global encerrar

    start = time.time()

    while (
        not encerrar
        and time.time()
        - start
        < DURACAO
    ):
        clear_output(
            wait=True
        )

        shop = (
            estado[
                "loja"
            ]
            or estado[
                "username"
            ]
            or "Carregando..."
        )

        print(
            "=" * 60
        )
        print(
            "AGCN SHOPEE LIVE"
        )
        print(
            "=" * 60
        )
        print(
            shop
        )

        if estado[
            "titulo"
        ]:
            print(
                estado[
                    "titulo"
                ]
            )

        print()
        print(
            "ESPECTADORES :",
            (
                estado[
                    "viewers"
                ]
                if estado[
                    "viewers"
                ]
                is not None
                else "-"
            ),
        )
        print(
            "LIKES        :",
            (
                estado[
                    "likes"
                ]
                if estado[
                    "likes"
                ]
                is not None
                else "-"
            ),
        )
        print(
            "COMPART.     :",
            (
                estado[
                    "shares"
                ]
                if estado[
                    "shares"
                ]
                is not None
                else "-"
            ),
        )
        print(
            "PRODUTOS     :",
            (
                estado[
                    "products"
                ]
                if estado[
                    "products"
                ]
                is not None
                else "-"
            ),
        )
        print()
        print(
            "Metricas:",
            tempo_desde(
                estado[
                    "ultimaMetrica"
                ]
            ),
        )
        print()

        if estado[
            "erro"
        ]:
            print(
                "ERRO:",
                estado[
                    "erro"
                ],
            )

        await asyncio.sleep(
            0.5
        )

    encerrar = True


# ============================================================
# EXECUCAO PRINCIPAL
# ============================================================

async def main():
    global encerrar
    global LIVE_URL
    global SESSION_ID
    global ENDPOINT_SESSION
    global SHOPEE_STORAGE_STATE
    global SHOPEE_SESSION_HEADERS

    encerrar = False

    # Sempre limpar artefatos da LIVE anterior.
    SHOPEE_STORAGE_STATE = None
    SHOPEE_SESSION_HEADERS = {}

    estado[
        "metodoSessao"
    ] = None

    estado[
        "sessionHttpStatus"
    ] = None

    estado[
        "tentativasSessao"
    ] = 0

    estado[
        "ultimaFalhaSessao"
    ] = None

    estado[
        "erro"
    ] = None

    if (
        not LINK_CURTO
        or "COLE_AQUI"
        in str(
            LINK_CURTO
        )
    ):
        estado[
            "erro"
        ] = (
            "Informe um link valido da Shopee LIVE."
        )

        print(
            estado[
                "erro"
            ],
            flush=True,
        )

        return

    async with async_playwright() as playwright:
        browser = (
            await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
        )

        clear_output(
            wait=True
        )

        print(
            "=" * 60
        )
        print(
            "AGCN SHOPEE"
        )
        print(
            "=" * 60
        )
        print()
        print(
            "Link recebido:"
        )
        print(
            LINK_CURTO
        )
        print()
        print(
            "Resolvendo link automaticamente..."
        )

        try:
            resolvido = (
                await resolver_link_mobile(
                    browser,
                    str(
                        LINK_CURTO
                    ).strip(),
                )
            )

            LIVE_URL = (
                resolvido[
                    "url"
                ]
            )

            SESSION_ID = (
                resolvido[
                    "sessionId"
                ]
            )

            ENDPOINT_SESSION = (
                f"/api/v1/session/"
                f"{SESSION_ID}"
            )

            estado[
                "sessionId"
            ] = SESSION_ID

            estado[
                "urlExpandida"
            ] = LIVE_URL

        except Exception as exc:
            estado[
                "erro"
            ] = (
                "Nao foi possivel resolver a LIVE da Shopee. "
                + _failure_message(
                    "Detalhe",
                    exc,
                )
            )

            print(
                estado[
                    "erro"
                ],
                flush=True,
            )

            await browser.close()
            return

        print()
        print(
            "Link resolvido com sucesso."
        )
        print(
            "Session ID:",
            SESSION_ID,
        )
        print(
            "Iniciando Shopee Worker..."
        )

        await asyncio.sleep(
            0.5
        )

        tarefa_metricas = (
            asyncio.create_task(
                metricas_worker(
                    browser
                )
            )
        )

        tarefa_chat = (
            asyncio.create_task(
                chat_worker()
            )
        )

        tarefa_painel = (
            asyncio.create_task(
                painel()
            )
        )

        try:
            await tarefa_painel

        except asyncio.CancelledError:
            pass

        finally:
            encerrar = True

            tarefa_metricas.cancel()
            tarefa_chat.cancel()

            try:
                await tarefa_metricas
            except BaseException:
                pass

            try:
                await tarefa_chat
            except BaseException:
                pass

            await browser.close()

    clear_output(
        wait=True
    )

    print(
        "Monitoramento Shopee encerrado.",
        flush=True,
    )


# ============================================================
# IMPORTANTE
#
# NAO COLOCAR:
# await main()
#
# A Interface / Runtime inicia o Worker.
# ============================================================

print(
    "AGCN Shopee Worker V2.0 carregado."
)
