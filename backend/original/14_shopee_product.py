# ============================================================
# AGCN LIVE - WORKER SHOPEE PRODUTO V1.0
#
# Responsabilidade unica:
# - receber o link da LIVE Shopee;
# - resolver/expandir o link;
# - localizar o session_id;
# - ler o snapshot oficial da sessao;
# - observar data.show_item;
# - detectar quando o produto atual muda;
# - emitir eventos proprios para o futuro fluxo do Coach Vendas.
#
# IMPORTANTE:
# - NAO altera o Worker Shopee atual.
# - NAO usa Live Engine.
# - NAO usa Dispatcher / Coaches atuais.
# - NAO decide o que vender nem o que falar.
# - NAO extrai beneficios.
# - NAO faz compras, ATC ou qualquer mutacao.
#
# Esta celula apenas CARREGA o modulo.
# Quem inicia o monitoramento sera a Interface ou uma celula de teste.
# ============================================================

import asyncio
import copy
import time
from collections import deque
from datetime import datetime
from urllib.parse import (
    parse_qs,
    unquote,
    urlparse,
)
from zoneinfo import ZoneInfo

from playwright.async_api import async_playwright


# ============================================================
# 1. VERSAO / CONFIGURACOES
# ============================================================

WORKER_SHOPEE_PRODUCT_VERSION = "1.0"

SHOPEE_PRODUCT_TIMEZONE = ZoneInfo(
    "America/Araguaina"
)

# Intervalo adicional entre leituras.
# A propria abertura/consulta da pagina tambem consome tempo,
# entao o ciclo real tende a ser maior que este valor.
SHOPEE_PRODUCT_POLL_SUCCESS_SECONDS = 0.8
SHOPEE_PRODUCT_POLL_ERROR_SECONDS = 1.5

SHOPEE_PRODUCT_RESPONSE_TIMEOUT_MS = 20000
SHOPEE_PRODUCT_NAVIGATION_TIMEOUT_MS = 20000
SHOPEE_PRODUCT_RESOLVE_TIMEOUT_STEPS = 60
SHOPEE_PRODUCT_RESOLVE_STEP_SECONDS = 0.5

SHOPEE_PRODUCT_HISTORY_MAXLEN = 200


# ============================================================
# 2. ESTADO INDEPENDENTE
# ============================================================

shopee_product_state = {
    "version": WORKER_SHOPEE_PRODUCT_VERSION,
    "running": False,
    "input_url": None,
    "live_url": None,
    "session_id": None,
    "endpoint_session": None,

    "current_key": None,
    "current_product": None,
    "previous_product": None,

    "last_read_at": None,
    "last_success_at": None,
    "last_change_at": None,

    "reads": 0,
    "successful_reads": 0,
    "changes": 0,
    "clears": 0,

    "last_http_status": None,
    "last_error": None,
}

_shopee_product_stop_requested = False
_shopee_product_seq = 0

# Fila exclusiva para o proximo modulo do fluxo futuro.
# Regra: apenas um consumidor downstream devera consumir esta fila.
shopee_product_output_queue = asyncio.Queue()

# Diagnostico/interface consultam historico, sem roubar eventos da fila.
shopee_product_event_history = deque(
    maxlen=SHOPEE_PRODUCT_HISTORY_MAXLEN
)


# ============================================================
# 3. UTILITARIOS
# ============================================================

def _shopee_product_now_iso():
    return datetime.now(
        SHOPEE_PRODUCT_TIMEZONE
    ).isoformat()


def _shopee_product_key(product):
    if not isinstance(product, dict):
        return None

    item_id = product.get("item_id")
    shop_id = product.get("shop_id")

    if item_id is None or shop_id is None:
        return None

    return (
        str(item_id),
        str(shop_id),
    )


def _shopee_product_safe_copy(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _shopee_product_emit(
    event_type,
    product=None,
    previous_product=None,
):
    global _shopee_product_seq

    _shopee_product_seq += 1

    current = (
        product
        if isinstance(product, dict)
        else None
    )
    previous = (
        previous_product
        if isinstance(previous_product, dict)
        else None
    )

    event = {
        "seq": _shopee_product_seq,
        "type": event_type,
        "platform": "shopee",
        "session_id": shopee_product_state.get(
            "session_id"
        ),
        "observed_at": _shopee_product_now_iso(),

        # Identidade minima para roteamento/dedupe.
        "item_id": (
            current.get("item_id")
            if current
            else None
        ),
        "shop_id": (
            current.get("shop_id")
            if current
            else None
        ),

        # O Worker nao interpreta o produto.
        # O Product Extractor recebera o objeto bruto.
        "raw_product": _shopee_product_safe_copy(
            current
        ),

        "previous_item_id": (
            previous.get("item_id")
            if previous
            else None
        ),
        "previous_shop_id": (
            previous.get("shop_id")
            if previous
            else None
        ),
    }

    shopee_product_event_history.append(
        _shopee_product_safe_copy(event)
    )

    try:
        shopee_product_output_queue.put_nowait(
            _shopee_product_safe_copy(event)
        )
    except Exception:
        pass

    return event


# ============================================================
# 4. VALIDACAO DE URL
# ============================================================

def shopee_product_host_allowed(host):
    host = (
        host
        or ""
    ).lower().rstrip(".")

    return (
        host == "br.shp.ee"
        or host.endswith(".shp.ee")
        or host == "shopee.com.br"
        or host.endswith(".shopee.com.br")
    )


def shopee_product_validate_url(url):
    parsed = urlparse(
        str(url or "").strip()
    )

    if parsed.scheme != "https":
        raise ValueError(
            "Somente URLs HTTPS da Shopee sao permitidas."
        )

    if not shopee_product_host_allowed(
        parsed.hostname
    ):
        raise ValueError(
            f"Dominio Shopee nao permitido: {parsed.hostname}"
        )


def shopee_product_extract_session_id(url):
    try:
        query = parse_qs(
            urlparse(url).query
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


def shopee_product_detect_live(url):
    if not url:
        return None

    candidate = str(
        url
    )

    # URLs podem aparecer percent-encoded em navegacao interna.
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
        shopee_product_extract_session_id(
            candidate
        )
    )

    if not session_id:
        return None

    return {
        "url": candidate,
        "session_id": session_id,
    }


# ============================================================
# 5. RESOLVER / EXPANDIR LINK
#
# Baseado no metodo ja validado no Worker Shopee atual.
# O codigo e independente: nao chama o Worker antigo.
# ============================================================

async def shopee_product_resolve_live(
    browser,
    link,
):
    link = str(
        link
        or ""
    ).strip()

    if not link:
        raise ValueError(
            "Informe o link da LIVE Shopee."
        )

    shopee_product_validate_url(
        link
    )

    # --------------------------------------------------------
    # Se o usuario ja enviou a URL final da LIVE,
    # nao e necessario expandir novamente.
    # --------------------------------------------------------
    direct_live = (
        shopee_product_detect_live(
            link
        )
    )

    if direct_live:
        return direct_live

    initial_host = (
        urlparse(
            link
        ).hostname
        or ""
    ).lower()

    # O resolvedor automatico foi validado para br.shp.ee.
    if (
        initial_host
        != "br.shp.ee"
    ):
        raise ValueError(
            "Use o link curto https://br.shp.ee/... "
            "ou a URL final https://live.shopee.com.br/..."
            "?session=..."
        )

    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 "
            "(iPhone; CPU iPhone OS 18_0 like Mac OS X) "
            "AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) "
            "Version/18.0 "
            "Mobile/15E148 "
            "Safari/604.1"
        ),
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
        timezone_id="America/Araguaina",
    )

    page = await context.new_page()
    seen_urls = set()

    def register_request(request):
        try:
            seen_urls.add(
                request.url
            )
        except Exception:
            pass

    page.on(
        "request",
        register_request,
    )

    try:
        try:
            await page.goto(
                link,
                wait_until="domcontentloaded",
                timeout=30000,
            )
        except Exception:
            # Mesmo com timeout parcial, a navegacao pode
            # ter revelado a LIVE nos requests.
            pass

        for _ in range(
            SHOPEE_PRODUCT_RESOLVE_TIMEOUT_STEPS
        ):
            # 1. URL principal.
            found = (
                shopee_product_detect_live(
                    page.url
                )
            )

            if found:
                shopee_product_validate_url(
                    found["url"]
                )
                return found

            # 2. Requests vistos pelo navegador.
            for seen_url in list(
                seen_urls
            ):
                found = (
                    shopee_product_detect_live(
                        seen_url
                    )
                )

                if found:
                    shopee_product_validate_url(
                        found["url"]
                    )
                    return found

            # 3. Links no DOM.
            try:
                links = await page.eval_on_selector_all(
                    "a[href]",
                    """
                    els => els
                        .map(e => e.href)
                        .filter(Boolean)
                    """,
                )

                for dom_url in links:
                    found = (
                        shopee_product_detect_live(
                            dom_url
                        )
                    )

                    if found:
                        shopee_product_validate_url(
                            found["url"]
                        )
                        return found
            except Exception:
                pass

            # 4. Canonical / og:url.
            try:
                candidates = await page.evaluate(
                    """
                    () => {
                        const r = [];

                        const canonical =
                            document.querySelector(
                                'link[rel="canonical"]'
                            );

                        if (canonical?.href) {
                            r.push(canonical.href);
                        }

                        const og =
                            document.querySelector(
                                'meta[property="og:url"]'
                            );

                        if (og?.content) {
                            r.push(og.content);
                        }

                        return r;
                    }
                    """
                )

                for metadata_url in candidates:
                    found = (
                        shopee_product_detect_live(
                            metadata_url
                        )
                    )

                    if found:
                        shopee_product_validate_url(
                            found["url"]
                        )
                        return found
            except Exception:
                pass

            await asyncio.sleep(
                SHOPEE_PRODUCT_RESOLVE_STEP_SECONDS
            )

        raise RuntimeError(
            "Nao consegui resolver o link para uma "
            "LIVE Shopee com session=..."
        )

    finally:
        await context.close()


# ============================================================
# 6. LER SNAPSHOT ATUAL DA SESSAO
#
# Mesma estrategia validada no Worker Shopee:
# abre a LIVE e observa a propria pagina requisitar
# /api/v1/session/{session_id}.
#
# Nao copiamos tokens nem fabricamos autenticacao.
# ============================================================

async def shopee_product_read_snapshot(
    browser,
):
    context = None

    live_url = shopee_product_state.get(
        "live_url"
    )
    endpoint_session = shopee_product_state.get(
        "endpoint_session"
    )

    if not live_url or not endpoint_session:
        raise RuntimeError(
            "LIVE ainda nao foi resolvida."
        )

    try:
        context = await browser.new_context(
            locale="pt-BR",
            viewport={
                "width": 1024,
                "height": 720,
            },
        )

        page = await context.new_page()

        # Bloqueia apenas recursos pesados.
        # Scripts/XHR continuam livres para a propria Shopee
        # gerar a chamada oficial da sessao.
        async def filter_route(route):
            resource_type = (
                route.request.resource_type
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
            filter_route,
        )

        async with page.expect_response(
            lambda response:
                urlparse(
                    response.url
                ).path
                ==
                endpoint_session,
            timeout=(
                SHOPEE_PRODUCT_RESPONSE_TIMEOUT_MS
            ),
        ) as response_info:
            try:
                await page.goto(
                    live_url,
                    wait_until="commit",
                    timeout=(
                        SHOPEE_PRODUCT_NAVIGATION_TIMEOUT_MS
                    ),
                )
            except Exception:
                # O response observado e o que importa.
                pass

        response = await response_info.value

        shopee_product_state[
            "last_http_status"
        ] = response.status

        if response.status != 200:
            raise RuntimeError(
                f"Resposta da sessao: HTTP {response.status}"
            )

        try:
            payload = await response.json()
        except Exception as exc:
            raise RuntimeError(
                "A resposta da sessao nao trouxe JSON valido."
            ) from exc

        data = (
            payload.get("data")
            if isinstance(payload, dict)
            else None
        )

        if not isinstance(
            data,
            dict,
        ):
            raise RuntimeError(
                "Resposta sem objeto data."
            )

        # O produto atualmente destacado esta neste campo.
        show_item = data.get(
            "show_item"
        )

        if show_item is not None and not isinstance(
            show_item,
            dict,
        ):
            # Valor inesperado: tratamos como ausencia,
            # sem tentar interpretar.
            show_item = None

        return {
            "show_item": (
                _shopee_product_safe_copy(
                    show_item
                )
            ),
            "session": (
                _shopee_product_safe_copy(
                    data.get("session")
                )
                if isinstance(
                    data.get("session"),
                    dict,
                )
                else None
            ),
        }

    finally:
        if context:
            try:
                await context.close()
            except Exception:
                pass


# ============================================================
# 7. PROCESSAR PRODUTO ATUAL
# ============================================================

def shopee_product_process_show_item(
    show_item,
):
    now = time.time()

    shopee_product_state[
        "last_read_at"
    ] = now

    previous = shopee_product_state.get(
        "current_product"
    )
    previous_key = shopee_product_state.get(
        "current_key"
    )

    new_product = (
        _shopee_product_safe_copy(
            show_item
        )
        if isinstance(
            show_item,
            dict,
        )
        else None
    )

    new_key = (
        _shopee_product_key(
            new_product
        )
    )

    # --------------------------------------------------------
    # Sem produto destacado.
    # Se antes havia produto, registra que saiu da tela.
    # --------------------------------------------------------
    if new_key is None:
        if previous_key is not None:
            shopee_product_state[
                "previous_product"
            ] = _shopee_product_safe_copy(
                previous
            )
            shopee_product_state[
                "current_product"
            ] = None
            shopee_product_state[
                "current_key"
            ] = None
            shopee_product_state[
                "last_change_at"
            ] = now
            shopee_product_state[
                "clears"
            ] += 1

            return _shopee_product_emit(
                "product_cleared",
                product=None,
                previous_product=previous,
            )

        return None

    # --------------------------------------------------------
    # Mesmo produto.
    # Atualiza o snapshot bruto porque preco/estoque/promocao
    # podem mudar, mas NAO emite product_changed.
    # --------------------------------------------------------
    if new_key == previous_key:
        shopee_product_state[
            "current_product"
        ] = new_product
        return None

    # --------------------------------------------------------
    # Primeiro produto observado.
    # --------------------------------------------------------
    if previous_key is None:
        shopee_product_state[
            "previous_product"
        ] = None
        shopee_product_state[
            "current_product"
        ] = new_product
        shopee_product_state[
            "current_key"
        ] = new_key
        shopee_product_state[
            "last_change_at"
        ] = now

        return _shopee_product_emit(
            "product_started",
            product=new_product,
            previous_product=None,
        )

    # --------------------------------------------------------
    # Troca real de produto.
    # --------------------------------------------------------
    shopee_product_state[
        "previous_product"
    ] = _shopee_product_safe_copy(
        previous
    )
    shopee_product_state[
        "current_product"
    ] = new_product
    shopee_product_state[
        "current_key"
    ] = new_key
    shopee_product_state[
        "last_change_at"
    ] = now
    shopee_product_state[
        "changes"
    ] += 1

    return _shopee_product_emit(
        "product_changed",
        product=new_product,
        previous_product=previous,
    )


# ============================================================
# 8. UMA LEITURA COMPLETA
# ============================================================

async def shopee_product_read_once(
    browser,
):
    shopee_product_state[
        "reads"
    ] += 1

    snapshot = await shopee_product_read_snapshot(
        browser
    )

    shopee_product_state[
        "successful_reads"
    ] += 1
    shopee_product_state[
        "last_success_at"
    ] = time.time()
    shopee_product_state[
        "last_error"
    ] = None

    event = (
        shopee_product_process_show_item(
            snapshot.get("show_item")
        )
    )

    return {
        "snapshot": snapshot,
        "event": event,
    }


# ============================================================
# 9. RESET
# ============================================================

def shopee_product_reset(
    clear_history=True,
    clear_queue=True,
):
    global _shopee_product_stop_requested
    global _shopee_product_seq

    _shopee_product_stop_requested = False
    _shopee_product_seq = 0

    shopee_product_state.update({
        "version": WORKER_SHOPEE_PRODUCT_VERSION,
        "running": False,
        "input_url": None,
        "live_url": None,
        "session_id": None,
        "endpoint_session": None,

        "current_key": None,
        "current_product": None,
        "previous_product": None,

        "last_read_at": None,
        "last_success_at": None,
        "last_change_at": None,

        "reads": 0,
        "successful_reads": 0,
        "changes": 0,
        "clears": 0,

        "last_http_status": None,
        "last_error": None,
    })

    if clear_history:
        shopee_product_event_history.clear()

    if clear_queue:
        try:
            while True:
                shopee_product_output_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass


# ============================================================
# 10. PARAR
# ============================================================

def shopee_product_stop():
    global _shopee_product_stop_requested
    _shopee_product_stop_requested = True


# ============================================================
# 11. EXECUCAO PRINCIPAL
#
# Esta e a funcao que, futuramente, a Interface chamara com
# o mesmo link enviado ao Worker Shopee atual.
# ============================================================

async def executar_worker_shopee_produto(
    live_link,
    duration=None,
):
    global _shopee_product_stop_requested

    shopee_product_reset(
        clear_history=True,
        clear_queue=True,
    )

    live_link = str(
        live_link
        or ""
    ).strip()

    if not live_link:
        raise ValueError(
            "Informe o link da LIVE Shopee."
        )

    shopee_product_state[
        "input_url"
    ] = live_link
    shopee_product_state[
        "running"
    ] = True

    started_at = time.monotonic()

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            try:
                resolved = (
                    await shopee_product_resolve_live(
                        browser,
                        live_link,
                    )
                )

                session_id = (
                    resolved["session_id"]
                )
                live_url = (
                    resolved["url"]
                )

                shopee_product_state[
                    "session_id"
                ] = session_id
                shopee_product_state[
                    "live_url"
                ] = live_url
                shopee_product_state[
                    "endpoint_session"
                ] = (
                    f"/api/v1/session/{session_id}"
                )

                while not _shopee_product_stop_requested:
                    if (
                        duration is not None
                        and
                        time.monotonic() - started_at
                        >= float(duration)
                    ):
                        break

                    try:
                        await shopee_product_read_once(
                            browser
                        )

                        await asyncio.sleep(
                            SHOPEE_PRODUCT_POLL_SUCCESS_SECONDS
                        )

                    except asyncio.CancelledError:
                        raise

                    except Exception as exc:
                        shopee_product_state[
                            "last_error"
                        ] = str(exc)

                        await asyncio.sleep(
                            SHOPEE_PRODUCT_POLL_ERROR_SECONDS
                        )

            finally:
                await browser.close()

    finally:
        shopee_product_state[
            "running"
        ] = False

    return shopee_product_status()


# ============================================================
# 12. API PUBLICA PARA O PROXIMO MODULO
# ============================================================

async def shopee_product_next_event(
    timeout=None,
):
    if timeout is None:
        return await shopee_product_output_queue.get()

    return await asyncio.wait_for(
        shopee_product_output_queue.get(),
        timeout=float(timeout),
    )


def shopee_product_current():
    return _shopee_product_safe_copy(
        shopee_product_state.get(
            "current_product"
        )
    )


def shopee_product_status():
    result = {
        key: value
        for key, value in shopee_product_state.items()
        if key not in {
            "current_product",
            "previous_product",
        }
    }

    result[
        "current_product"
    ] = _shopee_product_safe_copy(
        shopee_product_state.get(
            "current_product"
        )
    )

    result[
        "previous_product"
    ] = _shopee_product_safe_copy(
        shopee_product_state.get(
            "previous_product"
        )
    )

    result[
        "history_size"
    ] = len(
        shopee_product_event_history
    )

    result[
        "queue_size"
    ] = (
        shopee_product_output_queue.qsize()
    )

    return result


def shopee_product_recent_events(
    limit=20,
):
    try:
        limit = max(
            1,
            int(limit),
        )
    except Exception:
        limit = 20

    events = list(
        shopee_product_event_history
    )[-limit:]

    return _shopee_product_safe_copy(
        events
    )


# ============================================================
# 13. DIAGNOSTICO SEM CONSUMIR A FILA
#
# Pode ser chamado enquanto o Worker roda.
# Nao interfere no futuro Product Extractor.
# ============================================================

def mostrar_worker_shopee_produto():
    status = shopee_product_status()
    product = status.get(
        "current_product"
    )

    print("=" * 68)
    print(
        "AGCN LIVE - WORKER SHOPEE PRODUTO V1.0"
    )
    print("=" * 68)
    print(
        "Executando:",
        status.get("running"),
    )
    print(
        "Session ID:",
        status.get("session_id"),
    )
    print(
        "Leituras:",
        status.get("reads"),
    )
    print(
        "Leituras OK:",
        status.get("successful_reads"),
    )
    print(
        "Trocas detectadas:",
        status.get("changes"),
    )
    print(
        "Produto retirado da tela:",
        status.get("clears"),
    )
    print(
        "Ultimo HTTP:",
        status.get("last_http_status"),
    )
    print(
        "Erro:",
        status.get("last_error"),
    )
    print()

    if not product:
        print(
            "Produto atual: nenhum show_item detectado."
        )
    else:
        print("PRODUTO ATUAL")
        print(
            "item_id:",
            product.get("item_id"),
        )
        print(
            "shop_id:",
            product.get("shop_id"),
        )
        print(
            "nome:",
            product.get("name"),
        )
        print(
            "preco:",
            product.get("price"),
            product.get("currency"),
        )

    print()
    print(
        "Eventos no historico:",
        status.get("history_size"),
    )
    print(
        "Eventos aguardando Product Extractor:",
        status.get("queue_size"),
    )


# ============================================================
# 14. CELULA DE TESTE OPCIONAL
#
# Exemplo:
#
# tarefa = asyncio.create_task(
#     executar_worker_shopee_produto(
#         "https://br.shp.ee/SEU_LINK",
#         duration=60,
#     )
# )
#
# Depois, em outra celula:
#
# mostrar_worker_shopee_produto()
# shopee_product_recent_events(10)
#
# Para parar antes:
#
# shopee_product_stop()
#
# NAO colocamos await automatico aqui.
# ============================================================

print(
    "AGCN Worker Shopee Produto V1.0 carregado."
)
