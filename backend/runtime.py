"""AGCN LIVE / ALIVE runtime adapter.

Arquitetura ativa nesta versao:

Original Colab:
02 Shopee Worker
03 TikTok Worker
04 Live Engine
05 Coach Storage
06 Worker Coach Comentarios
07 Comment Dispatcher V1.1
08 Coach Produto
09 Coach Comercial
10 Coach Objecoes
11 Worker Audiencia
12 Context Fusion
13 Decision Coach
18 Interface V8 (somente ponte operacional do monitoramento)

Nova arquitetura web:
Product Context V1
Sales Coach V2

Os modulos antigos de produto/vendas 14-17 permanecem no repositorio como
historico, mas NAO sao executados por este runtime.
"""

from __future__ import annotations

import ast
import asyncio
import importlib
import re
import threading
import time
import unicodedata
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .product_context import ProductContext, ProductContextError
from .sales_coach import SalesCoach


ORIGINAL = Path(__file__).resolve().parent / "original"

ACTIVE_ORIGINAL_PREFIXES = {
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "18",
}

MODULES = [
    path
    for path in sorted(
        ORIGINAL.glob("[0-1][0-9]_*.py")
    )
    if path.name[:2]
    in ACTIVE_ORIGINAL_PREFIXES
]

SKIPPED_IMPORTS = {
    "requests",
    "playwright.async_api",
    "TikTokLive",
    "TikTokLive.events",
    "IPython.display",
    "google.colab",
}


class MissingCaptureDependency(
    RuntimeError
):
    pass


class _Unavailable:
    def __init__(
        self,
        name,
    ):
        self.name = name

    def __call__(
        self,
        *args,
        **kwargs,
    ):
        raise MissingCaptureDependency(
            "DependÃªncia do capturador ausente: "
            f"{self.name}."
        )

    def __getattr__(
        self,
        name,
    ):
        raise MissingCaptureDependency(
            "DependÃªncia do capturador ausente: "
            f"{self.name}."
        )


def _dependencies():
    namespace = {
        "JSON": lambda data: data,
        "clear_output":
            lambda *args, **kwargs: None,
    }

    missing = []

    imports = {
        "requests": [
            "requests",
        ],
        "playwright.async_api": [
            "async_playwright",
        ],
        "TikTokLive": [
            "TikTokLiveClient",
        ],
        "TikTokLive.events": [
            "ConnectEvent",
            "DisconnectEvent",
            "LiveEndEvent",
            "CommentEvent",
            "LikeEvent",
            "RoomUserSeqEvent",
            "FollowEvent",
            "ShareEvent",
            "GiftEvent",
        ],
    }

    for module_name, names in imports.items():
        try:
            module = importlib.import_module(
                module_name
            )
        except ImportError:
            missing.append(
                module_name
            )

            for name in names:
                namespace[name] = (
                    _Unavailable(
                        module_name
                    )
                )

            continue

        for name in names:
            namespace[name] = (
                module
                if name == module_name
                else getattr(
                    module,
                    name,
                )
            )

    return (
        namespace,
        missing,
    )


def _norm(value):
    text = str(
        value
        or ""
    ).strip().lower()[:1000]

    text = "".join(
        ch
        for ch in unicodedata.normalize(
            "NFKD",
            text,
        )
        if not unicodedata.combining(
            ch
        )
    )

    return re.sub(
        r"\s+",
        " ",
        text,
    ).strip()


def _normalize_sales_mode(value):
    raw = _norm(
        value
    ).replace(
        "/",
        "_",
    ).replace(
        " ",
        "_",
    )

    raw = re.sub(
        "_+",
        "_",
        raw,
    )

    aliases = {
        "leve": "leve",
        "light": "leve",
        "equilibrado": "leve",
        "balanced": "leve",
        "maximo": "maximo",
        "maximum": "maximo",
        "max": "maximo",
        "pressao": "maximo",
        "pressao_feira": "maximo",
        "feira": "maximo",
    }

    mode = aliases.get(
        raw,
        raw,
    )

    if mode not in {
        "leve",
        "maximo",
    }:
        raise ValueError(
            "Modo invÃ¡lido. "
            "Use leve ou maximo."
        )

    return mode


def _direct_shopee(value):
    parsed = urlparse(
        value
    )

    if (
        parsed.scheme != "https"
        or parsed.hostname
        != "live.shopee.com.br"
    ):
        return None

    session = (
        parse_qs(
            parsed.query
        ).get(
            "session"
        )
        or [None]
    )[0]

    if (
        not session
        or not re.fullmatch(
            r"[A-Za-z0-9_-]{3,100}",
            session,
        )
    ):
        return None

    return {
        "url": value,
        "sessionId": session,
    }


def validate_input(
    platform,
    value,
):
    value = str(
        value
        or ""
    ).strip()

    if platform == "shopee":
        parsed = urlparse(
            value
        )

        if (
            parsed.scheme == "https"
            and parsed.hostname
            == "br.shp.ee"
            and re.fullmatch(
                r"/[A-Za-z0-9_-]+/?",
                parsed.path,
            )
            and not parsed.query
        ):
            return value

        if _direct_shopee(
            value
        ):
            return value

        raise ValueError(
            "Informe um link https://br.shp.ee/... "
            "ou uma URL da LIVE com session em "
            "live.shopee.com.br."
        )

    if platform == "tiktok":
        if value.startswith(
            "https://"
        ):
            parsed = urlparse(
                value
            )

            if parsed.hostname not in {
                "tiktok.com",
                "www.tiktok.com",
                "m.tiktok.com",
            }:
                raise ValueError(
                    "Informe @username ou o perfil "
                    "https://www.tiktok.com/@username."
                )

            value = (
                parsed.path.split(
                    "/",
                    2,
                )[1]
                if parsed.path.startswith(
                    "/@"
                )
                else ""
            )

        value = value.lstrip(
            "@"
        )

        if not re.fullmatch(
            r"[A-Za-z0-9._]{2,24}",
            value,
        ):
            raise ValueError(
                "Informe um @username vÃ¡lido "
                "do TikTok."
            )

        return (
            "@"
            + value
        )

    raise ValueError(
        "Selecione Shopee ou TikTok."
    )


def _load_original_modules():
    ns, missing = _dependencies()

    ns.update({
        "__name__":
            "agcn_notebook",
        "__builtins__":
            __builtins__,
    })

    for path in MODULES:
        tree = ast.parse(
            path.read_text(
                encoding="utf-8"
            ),
            filename=str(
                path
            ),
        )

        nodes = []

        for node in tree.body:
            # Interface V8: elimina registro de callbacks,
            # HTML inline e display especificos do Colab.
            if (
                path.name.startswith(
                    "18_"
                )
                and node.lineno
                >= 1367
            ):
                continue

            if (
                isinstance(
                    node,
                    ast.ImportFrom,
                )
                and node.module
                in SKIPPED_IMPORTS
            ):
                continue

            if (
                isinstance(
                    node,
                    ast.Import,
                )
                and any(
                    alias.name
                    in SKIPPED_IMPORTS
                    for alias in node.names
                )
            ):
                continue

            if (
                isinstance(
                    node,
                    ast.Expr,
                )
                and isinstance(
                    node.value,
                    ast.Call,
                )
                and isinstance(
                    node.value.func,
                    ast.Name,
                )
                and node.value.func.id
                in {
                    "print",
                    "display",
                }
            ):
                continue

            nodes.append(
                node
            )

        tree.body = nodes

        ast.fix_missing_locations(
            tree
        )

        exec(
            compile(
                tree,
                str(path),
                "exec",
            ),
            ns,
        )

    # Mantemos a normalizacao segura usada na
    # adaptacao web atual.
    ns["_norm"] = _norm

    return (
        ns,
        missing,
    )


def load_runtime():
    ns, missing = (
        _load_original_modules()
    )

    runtime = NotebookRuntime(
        ns,
        missing,
    )

    # --------------------------------------------------------
    # DETECCAO DE URL SHOPEE DIRETA
    # --------------------------------------------------------
    original_detect = ns[
        "agcn_detectar_entrada"
    ]

    def detect(value):
        direct = _direct_shopee(
            value
        )

        if direct:
            return {
                "platform":
                    "shopee",
                "value":
                    value,
            }

        return original_detect(
            value
        )

    ns[
        "agcn_detectar_entrada"
    ] = detect

    # --------------------------------------------------------
    # RUNNER WEB V2
    #
    # Substitui somente o orquestrador de execucao da
    # Interface V8.
    #
    # O monitoramento/Live Coach continua executando os
    # wrappers originais 02-13.
    #
    # O fluxo antigo 14-17 nao existe neste namespace.
    # --------------------------------------------------------
    def runner(
        plataforma,
        valor,
        sales_config=None,
    ):
        ns["agcn_monitorando"] = True
        ns["agcn_status"] = (
            "iniciando"
        )
        ns["agcn_erro"] = None
        ns["agcn_sales_error"] = None

        painel_shopee_original = (
            ns.get("painel")
        )

        painel_tiktok_original = (
            ns.get(
                "painel_tiktok"
            )
        )

        clear_original = (
            ns.get(
                "clear_output"
            )
        )

        async def painel_shopee():
            while not ns.get(
                "encerrar",
                False,
            ):
                await asyncio.sleep(
                    0.5
                )

        async def painel_tiktok(
            client,
        ):
            while not (
                ns.get(
                    "estado_tiktok",
                    {},
                ).get(
                    "encerrada",
                    False,
                )
            ):
                await asyncio.sleep(
                    0.5
                )

        def clear_silencioso(
            *args,
            **kwargs,
        ):
            return None

        ns[
            "clear_output"
        ] = clear_silencioso

        ns[
            "painel"
        ] = painel_shopee

        ns[
            "painel_tiktok"
        ] = painel_tiktok

        loop = asyncio.new_event_loop()

        ns[
            "agcn_loop"
        ] = loop

        asyncio.set_event_loop(
            loop
        )

        sales_task = None

        async def sales_supervisor():
            # O Dispatcher cria a fila sales no reset
            # da LIVE. Esperamos essa fila existir antes
            # de iniciar o consumidor.
            for _ in range(
                600
            ):
                if (
                    ns.get(
                        "comment_dispatcher_sales_queue"
                    )
                    is not None
                ):
                    break

                if ns.get(
                    "agcn_stop_requested",
                    False,
                ):
                    return

                await asyncio.sleep(
                    0.05
                )
            else:
                runtime.sales_coach.last_error = (
                    "Canal sales do Comment Dispatcher "
                    "nÃ£o ficou pronto."
                )
                return

            await runtime.sales_coach.run(
                ns[
                    "comment_dispatcher_next_sales"
                ],
                live_running=None,
            )

        async def executar():
            nonlocal sales_task

            if plataforma == "shopee":
                ns[
                    "agcn_preparar_shopee"
                ](
                    valor
                )

            ns[
                "agcn_status"
            ] = "conectando"

            # O Sales Coach pode permanecer OFF.
            # Mesmo assim o consumidor fica pronto para
            # quando o usuario ativar o Product Context.
            runtime.sales_coach.reset_live_state()

            sales_task = (
                asyncio.create_task(
                    sales_supervisor()
                )
            )

            try:
                if plataforma == "shopee":
                    return await ns[
                        "executar_shopee_com_live_engine"
                    ]()

                return await ns[
                    "executar_tiktok_com_live_engine"
                ](
                    valor
                )

            finally:
                if (
                    sales_task is not None
                    and not sales_task.done()
                ):
                    sales_task.cancel()

                    try:
                        await sales_task
                    except BaseException:
                        pass

        task = None

        try:
            task = loop.create_task(
                executar()
            )

            ns[
                "agcn_task"
            ] = task

            ns[
                "agcn_status"
            ] = "ativo"

            loop.run_until_complete(
                task
            )

            if ns.get(
                "agcn_stop_requested",
                False,
            ):
                ns[
                    "agcn_status"
                ] = "encerrado"
            else:
                ns[
                    "agcn_status"
                ] = "finalizado"

        except asyncio.CancelledError:
            ns[
                "agcn_status"
            ] = "encerrado"

        except BaseException as exc:
            ns[
                "agcn_erro"
            ] = (
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            ns[
                "agcn_status"
            ] = "erro"

        finally:
            try:
                if (
                    sales_task is not None
                    and not sales_task.done()
                ):
                    sales_task.cancel()

                    loop.run_until_complete(
                        asyncio.gather(
                            sales_task,
                            return_exceptions=True,
                        )
                    )
            except Exception:
                pass

            try:
                pendentes = [
                    item
                    for item in asyncio.all_tasks(
                        loop
                    )
                    if not item.done()
                ]

                for item in pendentes:
                    item.cancel()

                if pendentes:
                    loop.run_until_complete(
                        asyncio.gather(
                            *pendentes,
                            return_exceptions=True,
                        )
                    )
            except Exception:
                pass

            try:
                loop.close()
            except Exception:
                pass

            if (
                painel_shopee_original
                is not None
            ):
                ns[
                    "painel"
                ] = painel_shopee_original

            if (
                painel_tiktok_original
                is not None
            ):
                ns[
                    "painel_tiktok"
                ] = painel_tiktok_original

            if (
                clear_original
                is not None
            ):
                ns[
                    "clear_output"
                ] = clear_original

            ns[
                "agcn_monitorando"
            ] = False

    ns[
        "agcn_runner"
    ] = runner

    # Campo legado usado pela Interface V8.
    # Agora representa o estado real do Product Context.
    ns[
        "agcn_sales_enabled"
    ] = bool(
        runtime.product_context.snapshot().get(
            "enabled"
        )
    )

    return runtime


class NotebookRuntime:
    def __init__(
        self,
        namespace,
        missing,
    ):
        self.ns = namespace
        self.missing = missing

        self.platform = None
        self.generation = 0

        self.product_context = (
            ProductContext()
        )

        self.sales_coach = (
            SalesCoach(
                self.product_context
            )
        )

    # ========================================================
    # LIVE
    # ========================================================

    def start(
        self,
        platform,
        value,
        price="",
        info="",
        facts=None,
        style=None,
    ):
        value = validate_input(
            platform,
            value,
        )

        if self.ns[
            "agcn_monitorando"
        ]:
            raise ValueError(
                "JÃ¡ existe um monitoramento "
                "ativo nesta sessÃ£o."
            )

        needed = (
            [
                "requests",
                "playwright.async_api",
            ]
            if platform == "shopee"
            else [
                "TikTokLive",
                "TikTokLive.events",
            ]
        )

        missing = [
            name
            for name in needed
            if name in self.missing
        ]

        if missing:
            raise MissingCaptureDependency(
                "DependÃªncias Python ainda "
                "nÃ£o instaladas neste servidor: "
                + ", ".join(
                    missing
                )
            )

        # Compatibilidade temporaria com o endpoint antigo:
        # se a versao atual do frontend mandar preco/info,
        # guardamos somente o que e factual, mas NAO ativamos
        # o Sales Coach e NAO inventamos nome de produto.
        if price or info:
            self.product_context.update(
                current_price=(
                    price
                    if price
                    else None
                ),
                additional_info=(
                    info
                    if info
                    else None
                ),
                replace=False,
            )

        self.sales_coach.reset_live_state()

        result = self.ns[
            "agcn_callback_start"
        ](
            value,
            seller_price="",
            seller_info="",
            sales_style="equilibrado",
        )

        if not result[
            "ok"
        ]:
            raise ValueError(
                result[
                    "message"
                ]
            )

        self.platform = platform
        self.generation += 1

        product = (
            self.product_context.snapshot()
        )

        self.ns[
            "agcn_sales_enabled"
        ] = bool(
            product.get(
                "enabled"
            )
        )

        # Corrige metadados legados devolvidos pela
        # Interface V8.
        result[
            "sales_enabled"
        ] = bool(
            product.get(
                "enabled"
            )
        )

        result[
            "sales_mode"
        ] = product.get(
            "mode",
            "leve",
        )

        return result

    def stop(self):
        if not self.ns[
            "agcn_monitorando"
        ]:
            return {
                "ok": True,
                "message":
                    "Monitoramento jÃ¡ encerrado.",
            }

        result = self.ns[
            "agcn_callback_stop"
        ]()

        # Uma solicitacao pode chegar antes que a
        # thread publique loop/task.
        for _ in range(
            30
        ):
            loop = self.ns.get(
                "agcn_loop"
            )

            task = self.ns.get(
                "agcn_task"
            )

            if not self.ns[
                "agcn_monitorando"
            ]:
                break

            if (
                loop
                and task
                and not loop.is_closed()
            ):
                try:
                    loop.call_soon_threadsafe(
                        task.cancel
                    )
                except RuntimeError:
                    pass

                break

            time.sleep(
                0.02
            )

        return result

    # ========================================================
    # LIVE COACH
    # ========================================================

    def alert_mode(
        self,
        value,
    ):
        return self.ns[
            "agcn_callback_set_alert_mode"
        ](
            value
        )

    # ========================================================
    # PRODUCT CONTEXT V1
    # ========================================================

    def product_context_state(
        self,
    ):
        return (
            self.product_context.snapshot()
        )

    def product_context_update(
        self,
        data,
        *,
        replace=True,
    ):
        if not isinstance(
            data,
            dict,
        ):
            raise ValueError(
                "Dados do produto invÃ¡lidos."
            )

        try:
            result = (
                self.product_context.configure(
                    data,
                    replace=replace,
                )
            )
        except ProductContextError as exc:
            raise ValueError(
                str(exc)
            ) from exc

        self._sync_sales_enabled()

        return {
            "ok": True,
            "product_context": result,
        }

    def product_context_activate(
        self,
        mode=None,
    ):
        try:
            result = (
                self.product_context.activate(
                    mode=mode
                )
            )
        except ProductContextError as exc:
            raise ValueError(
                str(exc)
            ) from exc

        self._sync_sales_enabled()

        return {
            "ok": True,
            "product_context": result,
        }

    def product_context_deactivate(
        self,
    ):
        result = (
            self.product_context.deactivate()
        )

        self._sync_sales_enabled()

        return {
            "ok": True,
            "product_context": result,
        }

    def product_context_clear(
        self,
    ):
        result = (
            self.product_context.clear_product(
                keep_mode=True
            )
        )

        self.sales_coach.reset_live_state()

        self._sync_sales_enabled()

        return {
            "ok": True,
            "product_context": result,
        }

    def sales_mode(
        self,
        value,
    ):
        mode = _normalize_sales_mode(
            value
        )

        try:
            result = (
                self.product_context.set_mode(
                    mode
                )
            )
        except ProductContextError as exc:
            raise ValueError(
                str(exc)
            ) from exc

        return {
            "ok": True,
            "mode":
                result[
                    "mode"
                ],
            "product_context":
                result,
        }

    def _sync_sales_enabled(
        self,
    ):
        enabled = bool(
            self.product_context.snapshot().get(
                "enabled"
            )
        )

        self.ns[
            "agcn_sales_enabled"
        ] = enabled

        return enabled

    # ========================================================
    # COMPATIBILIDADE TEMPORARIA COM SERVER/FRONTEND V8
    #
    # Estes metodos existem apenas para a branch continuar
    # coerente enquanto server.py e a Interface V9 ainda nao
    # foram substituidos.
    # ========================================================

    def sales_style(
        self,
        value,
    ):
        return self.sales_mode(
            value
        )

    def product_info(
        self,
        price="",
        info="",
        facts=None,
    ):
        data = {}

        if price:
            data[
                "current_price"
            ] = price

        if info:
            data[
                "additional_info"
            ] = info

        # O antigo campo JSON "facts" nao faz parte do
        # Product Context V1. Nao convertemos silenciosamente
        # estruturas arbitrarias em fatos de produto.
        if not data:
            return {
                "ok": True,
                "product_context":
                    self.product_context.snapshot(),
                "deprecated":
                    True,
            }

        result = (
            self.product_context_update(
                data,
                replace=False,
            )
        )

        result[
            "deprecated"
        ] = True

        return result

    def product_link(
        self,
        value,
    ):
        raise ValueError(
            "O Sales Coach V2 nÃ£o usa link do produto. "
            "Cadastre o produto no Product Context."
        )

    # ========================================================
    # STATUS
    # ========================================================

    def status(
        self,
    ):
        ns = self.ns

        state = ns[
            "agcn_callback_status"
        ]()

        platform = (
            state.get(
                "platform"
            )
            or self.platform
        )

        source = (
            ns.get(
                "estado",
                {},
            )
            if platform == "shopee"
            else ns.get(
                "estado_tiktok",
                {},
            )
        )

        if platform == "shopee":
            connected = bool(
                source.get(
                    "ultimaMetrica"
                )
                and source.get(
                    "sessionId"
                )
            )
        else:
            connected = bool(
                source.get(
                    "conectada"
                )
                or source.get(
                    "conectado"
                )
            )

        # Historico e snapshot: nao consomem filas.
        history = ns[
            "live_engine_recent_events"
        ](
            300
        )

        comments = []

        for event in history:
            if event.get(
                "type"
            ) != "comment":
                continue

            payload = (
                event.get(
                    "payload"
                )
                or {}
            )

            comments.append({
                "id": str(
                    event.get(
                        "event_id"
                    )
                    or event.get(
                        "id"
                    )
                    or (
                        f"{event.get('iso_time')}:"
                        f"{len(comments)}"
                    )
                ),
                "user":
                    ns[
                        "agcn_repair_text"
                    ](
                        payload.get(
                            "user"
                        )
                        or "UsuÃ¡rio"
                    ),
                "text":
                    ns[
                        "agcn_repair_text"
                    ](
                        payload.get(
                            "text"
                        )
                        or ""
                    ),
                "time":
                    payload.get(
                        "display_time"
                    )
                    or "",
            })

        error = (
            state.get(
                "error"
            )
            or source.get(
                "erro"
            )
        )

        if (
            not error
            and not state[
                "monitorando"
            ]
            and platform
            == "tiktok"
        ):
            error = source.get(
                "erro"
            )

        product_context = (
            self.product_context.snapshot()
        )

        product = (
            product_context.get(
                "product"
            )
            or {}
        )

        sales = (
            self.sales_coach.snapshot()
        )

        now = time.time()

        sales_messages = []

        for message in (
            sales.get(
                "messages"
            )
            or []
        ):
            expires_at = (
                message.get(
                    "expires_at"
                )
            )

            try:
                expires_at = float(
                    expires_at
                )
            except (
                TypeError,
                ValueError,
            ):
                expires_at = (
                    now
                    + float(
                        message.get(
                            "display_seconds"
                        )
                        or 10
                    )
                )

            if expires_at <= now:
                continue

            timestamp = (
                message.get(
                    "timestamp"
                )
                or now
            )

            try:
                hora = time.strftime(
                    "%H:%M:%S",
                    time.localtime(
                        float(
                            timestamp
                        )
                    ),
                )
            except Exception:
                hora = ""

            item = copy_dict = dict(
                message
            )

            item[
                "hora"
            ] = hora

            item[
                "expires_at"
            ] = expires_at

            sales_messages.append(
                copy_dict
            )

        enabled = bool(
            product_context.get(
                "enabled"
            )
        )

        self.ns[
            "agcn_sales_enabled"
        ] = enabled

        state.update({
            "connected":
                connected,

            "error":
                str(error)
                if error
                else None,

            "generation":
                self.generation,

            "server_time":
                now,

            "comments":
                comments[-80:],

            "product_context":
                product_context,

            # Campo simplificado para compatibilidade visual
            # durante a migracao para Interface V9.
            "product": {
                "state": (
                    "active"
                    if enabled
                    else "configured"
                    if product_context.get(
                        "ready"
                    )
                    else "empty"
                ),
                "name":
                    product.get(
                        "name"
                    ),
                "price": (
                    product.get(
                        "current_price"
                    )
                    if product.get(
                        "current_price"
                    )
                    is not None
                    else product.get(
                        "regular_price"
                    )
                ),
                "regular_price":
                    product.get(
                        "regular_price"
                    ),
                "current_price":
                    product.get(
                        "current_price"
                    ),
                "discount_percent":
                    product.get(
                        "discount_percent"
                    ),
                "description":
                    product.get(
                        "description"
                    ),
                "additional_info":
                    product.get(
                        "additional_info"
                    ),
                "error":
                    None,
                "url":
                    None,
                "conflicts":
                    [],
            },

            "sales_coach": {
                "version":
                    sales.get(
                        "version"
                    ),
                "enabled":
                    bool(
                        enabled
                        and state.get(
                            "monitorando"
                        )
                    ),
                "configured":
                    bool(
                        product_context.get(
                            "ready"
                        )
                    ),
                "active":
                    enabled,
                "available_for_platform":
                    platform
                    in {
                        None,
                        "shopee",
                        "tiktok",
                    },
                "mode":
                    product_context.get(
                        "mode",
                        "leve",
                    ),
                # Alias temporario ate a Interface V9
                # deixar de usar "style".
                "style":
                    product_context.get(
                        "mode",
                        "leve",
                    ),
                "running":
                    sales.get(
                        "running",
                        False,
                    ),
                "messages":
                    sales_messages,
                "recent_comment_count":
                    sales.get(
                        "recent_comment_count",
                        0,
                    ),
                "last_output_at":
                    sales.get(
                        "last_output_at"
                    ),
                "last_reactive_at":
                    sales.get(
                        "last_reactive_at"
                    ),
                "last_proactive_at":
                    sales.get(
                        "last_proactive_at"
                    ),
                "error":
                    sales.get(
                        "last_error"
                    ),
            },
        })

        return state
