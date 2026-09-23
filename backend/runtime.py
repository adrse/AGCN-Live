"""Thin web adapter around the original, sequential Colab modules.

The sources in original/ are byte-for-byte notebook cells. Each visitor receives
an independent namespace, just as each Colab run did. No internal asyncio.Queue
is read here: the Interface V8 callbacks and Live Engine history provide state.
"""

from __future__ import annotations

import ast
import asyncio
import importlib
import re
import threading
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .product_link import ProductPageWorker, validate_product_link

ORIGINAL = Path(__file__).resolve().parent / "original"
MODULES = sorted(ORIGINAL.glob("[0-1][0-9]_*.py"))
SKIPPED_IMPORTS = {"requests", "playwright.async_api", "TikTokLive", "TikTokLive.events", "IPython.display", "google.colab"}


class MissingCaptureDependency(RuntimeError):
    pass


class _Unavailable:
    def __init__(self, name):
        self.name = name

    def __call__(self, *args, **kwargs):
        raise MissingCaptureDependency(f"Dependência do capturador ausente: {self.name}.")

    def __getattr__(self, name):
        raise MissingCaptureDependency(f"Dependência do capturador ausente: {self.name}.")


class _BuilderScope(ast.NodeTransformer):
    """Two original notebook cells otherwise overwrite each other's stop/seq globals."""

    def visit_Name(self, node):
        if node.id in {"_stop_requested", "_output_seq"}:
            node.id = "_sales_builder" + node.id
        return node

    def visit_Global(self, node):
        node.names = ["_sales_builder" + name if name in {"_stop_requested", "_output_seq"} else name for name in node.names]
        return node


def _dependencies():
    namespace = {"JSON": lambda data: data, "clear_output": lambda *a, **k: None}
    missing = []
    imports = {
        "requests": ["requests"],
        "playwright.async_api": ["async_playwright"],
        "TikTokLive": ["TikTokLiveClient"],
        "TikTokLive.events": ["ConnectEvent", "DisconnectEvent", "LiveEndEvent", "CommentEvent", "LikeEvent", "RoomUserSeqEvent", "FollowEvent", "ShareEvent", "GiftEvent"],
    }
    for module_name, names in imports.items():
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            missing.append(module_name)
            for name in names:
                namespace[name] = _Unavailable(module_name)
            continue
        for name in names:
            namespace[name] = module if name == module_name else getattr(module, name)
    return namespace, missing


def _norm(value):
    text = str(value or "").strip().lower()[:1000]
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip()


def _normalize_style(value):
    style = _norm(value).replace("/", "_").replace(" ", "_")
    style = re.sub("_+", "_", style)
    aliases = {"balanced": "equilibrado", "pressure": "pressao_feira", "pressao": "pressao_feira", "feira": "pressao_feira"}
    style = aliases.get(style, style)
    if style not in {"equilibrado", "pressao_feira"}:
        raise ValueError("Estilo inválido. Use equilibrado ou pressao_feira.")
    return style


def _direct_shopee(value):
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.hostname != "live.shopee.com.br":
        return None
    session = (parse_qs(parsed.query).get("session") or [None])[0]
    if not session or not re.fullmatch(r"[A-Za-z0-9_-]{3,100}", session):
        return None
    return {"url": value, "sessionId": session}


def validate_input(platform, value):
    value = str(value or "").strip()
    if platform == "shopee":
        parsed = urlparse(value)
        if parsed.scheme == "https" and parsed.hostname == "br.shp.ee" and re.fullmatch(r"/[A-Za-z0-9_-]+/?", parsed.path) and not parsed.query:
            return value
        if _direct_shopee(value):
            return value
        raise ValueError("Informe um link https://br.shp.ee/... ou uma URL de LIVE com session em live.shopee.com.br.")
    if platform == "tiktok":
        if value.startswith("https://"):
            parsed = urlparse(value)
            if parsed.hostname not in {"tiktok.com", "www.tiktok.com", "m.tiktok.com"}:
                raise ValueError("Informe @username ou o perfil https://www.tiktok.com/@username.")
            value = parsed.path.split("/", 2)[1] if parsed.path.startswith("/@") else ""
        value = value.lstrip("@")
        if not re.fullmatch(r"[A-Za-z0-9._]{2,24}", value):
            raise ValueError("Informe um @username válido do TikTok.")
        return "@" + value
    raise ValueError("Selecione Shopee ou TikTok.")


def load_runtime():
    ns, missing = _dependencies()
    ns.update({"__name__": "agcn_notebook", "__builtins__": __builtins__})
    for path in MODULES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        nodes = []
        for node in tree.body:
            if path.name.startswith("18_") and node.lineno >= 1367:
                continue  # Colab callback registration, inline HTML and display
            if isinstance(node, ast.ImportFrom) and node.module in SKIPPED_IMPORTS:
                continue
            if isinstance(node, ast.Import) and any(alias.name in SKIPPED_IMPORTS for alias in node.names):
                continue
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id in {"print", "display"}:
                continue  # Notebook-only load banners
            nodes.append(node)
        tree.body = nodes
        if path.name.startswith("16_"):
            tree = _BuilderScope().visit(tree)
        ast.fix_missing_locations(tree)
        exec(compile(tree, str(path), "exec"), ns)

    # Original cells used malformed multi-character maketrans keys. Fix only
    # normalization, leaving candidate generation and decision logic untouched.
    ns["_norm"] = _norm
    ns["_sd_normalize_style"] = _normalize_style
    original_resolver = ns["resolver_link_mobile"]

    async def resolve_shopee(browser, url):
        return _direct_shopee(url) or await original_resolver(browser, url)

    ns["resolver_link_mobile"] = resolve_shopee
    original_detect = ns["agcn_detectar_entrada"]

    def detect(value):
        direct = _direct_shopee(value)
        return {"platform": "shopee", "value": value} if direct else original_detect(value)

    ns["agcn_detectar_entrada"] = detect
    runtime = NotebookRuntime(ns, missing)

    # Keep the original V8 processors and their exclusive queue consumers.
    # Only replace the Shopee LIVE product producer with an explicit product
    # page input; the validated Extractor → Builder → Decision chain remains.
    def start_sales_processors(_live_link=None):
        coroutines = [
            (ns["executar_product_extractor"](), "Product Extractor"),
            (ns["executar_product_sales_builder"](), "Product Sales Builder"),
            (ns["executar_sales_decision_coach"](), "Sales Decision Coach"),
            (ns["agcn_sales_output_bridge"](), "Sales Output Bridge"),
        ]
        ns["agcn_sales_tasks"] = [asyncio.create_task(ns["agcn_sales_guard"](coroutine, label)) for coroutine, label in coroutines]

    ns["agcn_iniciar_sales_tasks"] = start_sales_processors
    original_build_profile = ns["_pe_build_profile"]

    def build_profile(event):
        profile = original_build_profile(event)
        if event.get("platform") == "tiktok":
            # Original extractor schema uses structured_shopee internally.
            # Preserve that interface for the validated Sales Builder, while
            # accurately labeling every TikTok fact's source and provenance.
            def provenance(value):
                if isinstance(value, dict):
                    if value.get("source") in {"shopee", "shopee_title"}:
                        value["source"] = "tiktok_product_page"
                    for child in value.values():
                        provenance(child)
                elif isinstance(value, list):
                    for child in value:
                        provenance(child)
            provenance(profile)
            profile["provenance"]["automatic_source"] = "tiktok_product_page"
        elif event.get("source") == "shopee_product_page":
            def provenance(value):
                if isinstance(value, dict):
                    if value.get("source") in {"shopee", "shopee_title"}:
                        value["source"] = "shopee_product_page"
                    for child in value.values():
                        provenance(child)
                elif isinstance(value, list):
                    for child in value:
                        provenance(child)
            provenance(profile)
            profile["provenance"]["automatic_source"] = "shopee_product_page"
        return profile

    ns["_pe_build_profile"] = build_profile
    original_tiktok = ns["executar_tiktok_com_live_engine"]

    async def tiktok_with_product_cleanup(value):
        try:
            return await original_tiktok(value)
        finally:
            if ns.get("agcn_sales_tasks"):
                await ns["agcn_encerrar_sales_tasks"]()
            await runtime.close_product_browser()

    ns["executar_tiktok_com_live_engine"] = tiktok_with_product_cleanup
    original_close_sales = ns["agcn_encerrar_sales_tasks"]

    async def close_sales_and_browser():
        try:
            return await original_close_sales()
        finally:
            await runtime.close_product_browser()

    ns["agcn_encerrar_sales_tasks"] = close_sales_and_browser
    return runtime


class NotebookRuntime:
    def __init__(self, namespace, missing):
        self.ns = namespace
        self.missing = missing
        self.platform = None
        self.generation = 0
        self.seller_facts = {}
        self._seller_seeded = False
        self.product_browser = None
        self.product_job = None
        self.product_epoch = 0
        self.product_state = {"state": "waiting_link", "name": None, "error": None, "url": None}

    def start(self, platform, value, price="", info="", facts=None, style="equilibrado"):
        value = validate_input(platform, value)
        if self.ns["agcn_monitorando"]:
            raise ValueError("Já existe um monitoramento ativo nesta sessão.")
        needed = ["requests", "playwright.async_api"] if platform == "shopee" else ["TikTokLive", "TikTokLive.events"]
        missing = [name for name in needed if name in self.missing]
        if missing:
            raise MissingCaptureDependency("Dependências Python ainda não instaladas neste servidor: " + ", ".join(missing))
        self.seller_facts = facts or {}
        self._seller_seeded = False
        self.product_epoch += 1
        self.product_state = {"state": "waiting_link", "name": None, "error": None, "url": None}
        self.ns["_agcn_requested_sales_style"] = _normalize_style(style)
        result = self.ns["agcn_callback_start"](value, seller_price=price if platform == "shopee" else "", seller_info=info if platform == "shopee" else "", sales_style=style)
        if not result["ok"]:
            raise ValueError(result["message"])
        self.platform = platform
        self.generation += 1
        return result

    def _seed_facts(self):
        # V8 only accepts price and notes on start. Once its extractor reset
        # completes, pass structured seller facts to that extractor as well.
        if self._seller_seeded or not self.seller_facts or self.platform != "shopee":
            return
        ns = self.ns
        if not ns.get("agcn_sales_enabled") or not ns.get("product_extractor_state", {}).get("running"):
            return
        try:
            ns["_agcn_call_in_monitor_loop"](ns["product_extractor_set_seller_info"], facts=self.seller_facts, replace=False, emit_update=True)
            self._seller_seeded = True
        except Exception:
            pass  # Retried on the next status until startup completes.

    def stop(self):
        if not self.ns["agcn_monitorando"]:
            return {"ok": True, "message": "Monitoramento já encerrado."}
        self.product_epoch += 1
        loop = self.ns.get("agcn_loop")
        if loop and loop.is_running() and self.product_browser:
            try:
                asyncio.run_coroutine_threadsafe(self.close_product_browser(), loop).result(timeout=4)
            except Exception:
                pass
        result = self.ns["agcn_callback_stop"]()
        # A stop request can arrive before V8's background thread publishes
        # its loop/task. Relay cancellation once those handles are available.
        for _ in range(30):
            loop, task = self.ns.get("agcn_loop"), self.ns.get("agcn_task")
            if not self.ns["agcn_monitorando"]:
                break
            if loop and task and not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    pass
                break
            time.sleep(.02)
        return result

    def alert_mode(self, value):
        return self.ns["agcn_callback_set_alert_mode"](value)

    def sales_style(self, value):
        if not self.ns["agcn_monitorando"]:
            raise ValueError("Inicie uma LIVE para alterar o estilo de vendas.")
        self.ns["_agcn_requested_sales_style"] = _normalize_style(value)
        result = self.ns["_agcn_call_in_monitor_loop"](self.ns["sales_decision_set_style"], value, sync_builder=True)
        return {"ok": True, "style": result["style"]}

    def product_info(self, price="", info="", facts=None):
        if not self.ns["agcn_monitorando"]:
            raise ValueError("Atualize as informações durante uma LIVE ativa.")
        ns = self.ns
        result = ns["_agcn_call_in_monitor_loop"](
            ns["product_extractor_set_seller_info"], price=price or None,
            additional_info=info or None, facts=facts or {}, replace=True, emit_update=True,
        )
        return {"ok": True, "target": result.get("target") if isinstance(result, dict) else None}

    async def close_product_browser(self):
        if self.product_job and self.product_job is not asyncio.current_task():
            self.product_job.cancel()
        self.product_job = None
        if self.product_browser:
            browser, self.product_browser = self.product_browser, None
            await browser.close()

    async def _read_product(self, identity, epoch):
        ns = self.ns
        try:
            if self.platform == "tiktok" and not ns.get("agcn_sales_enabled"):
                ns["agcn_preparar_sales_pipeline"](sales_style=ns.get("_agcn_requested_sales_style", "equilibrado"))
                ns["agcn_iniciar_sales_tasks"]()
            if not self.product_browser:
                self.product_browser = ProductPageWorker()
            raw = await self.product_browser.read(identity)
            if epoch != self.product_epoch or not ns["agcn_monitorando"]:
                return
            old = ns["product_extractor_current_profile"]()
            if old:
                ns["product_extractor_process_event"]({"type": "product_cleared"})
            ns["product_extractor_process_event"]({
                "type": "product_started", "platform": identity["platform"],
                "source": identity["platform"] + "_product_page",
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "item_id": identity["item_id"], "shop_id": identity["shop_id"],
                "raw_product": raw,
            })
            self.product_state.update(state="identified", name=raw["name"], error=None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if epoch == self.product_epoch:
                self.product_state.update(state="error", name=None, error=str(exc))

    def product_link(self, value):
        if not self.ns["agcn_monitorando"]:
            raise ValueError("Inicie a LIVE antes de informar o link do produto.")
        identity = validate_product_link(value, self.platform)
        if "playwright.async_api" in self.missing:
            raise MissingCaptureDependency("Playwright não está instalado no servidor.")
        self.product_epoch += 1
        epoch = self.product_epoch
        self.product_state = {"state": "loading", "name": None, "error": None, "url": identity["url"]}

        def schedule():
            # V8 starts its monitoring event loop in a background thread.
            for _ in range(200):
                loop = self.ns.get("agcn_loop")
                if loop and loop.is_running():
                    def launch():
                        if self.product_job:
                            self.product_job.cancel()
                        self.product_job = asyncio.create_task(self._read_product(identity, epoch))
                    loop.call_soon_threadsafe(launch)
                    return
                if not self.ns["agcn_monitorando"] or epoch != self.product_epoch:
                    return
                time.sleep(.05)
            self.product_state.update(state="error", error="O monitoramento não iniciou a tempo.")

        threading.Thread(target=schedule, daemon=True).start()
        return {"ok": True, "platform": identity["platform"], "url": identity["url"]}

    def status(self):
        self._seed_facts()
        ns = self.ns
        state = ns["agcn_callback_status"]()
        platform = state.get("platform") or self.platform
        source = ns.get("estado", {}) if platform == "shopee" else ns.get("estado_tiktok", {})
        connected = bool(source.get("ultimaMetrica") and source.get("sessionId")) if platform == "shopee" else bool(source.get("conectada") or source.get("conectado"))
        # Event histories are snapshots, not additional consumers of any queue.
        history = ns["live_engine_recent_events"](300)
        comments = []
        for event in history:
            if event.get("type") != "comment":
                continue
            payload = event.get("payload") or {}
            comments.append({
                "id": str(event.get("event_id") or event.get("id") or f"{event.get('iso_time')}:{len(comments)}"),
                "user": ns["agcn_repair_text"](payload.get("user") or "Usuário"),
                "text": ns["agcn_repair_text"](payload.get("text") or ""),
                "time": payload.get("display_time") or "",
            })
        profile = ns["product_extractor_current_profile"]() if state["monitorando"] and ns.get("agcn_sales_enabled") else None
        name = self.product_state.get("name")
        if not name and isinstance(profile, dict):
            section = profile.get("product") or {}
            fact = section.get("name") or section.get("title") or {}
            name = fact.get("value") if isinstance(fact, dict) else fact
        price_fact = ((profile or {}).get("price") or {}).get("shopee_current") or {}
        price_value = price_fact.get("value") if isinstance(price_fact, dict) else None
        price = price_value.get("normalized") if isinstance(price_value, dict) else None
        error = state.get("error") or source.get("erro")
        if not error and not state["monitorando"] and platform == "tiktok":
            error = source.get("erro")
        state.update({
            "connected": connected,
            "error": str(error) if error else None,
            "generation": self.generation,
            "server_time": time.time(),
            "comments": comments[-80:],
            "product": {
                "state": self.product_state["state"] if state["monitorando"] else "waiting_link",
                "name": ns["agcn_repair_text"](name) if name else None,
                "price": price,
                "error": self.product_state.get("error"),
                "url": self.product_state.get("url"),
                "conflicts": (profile or {}).get("conflicts", []) if isinstance(profile, dict) else [],
            } if platform in {"shopee", "tiktok"} else None,
        })
        if state.get("sales_coach"):
            state["sales_coach"]["available_for_platform"] = platform in {"shopee", "tiktok"}
            state["sales_coach"]["enabled"] = bool(ns.get("agcn_sales_enabled") and state["monitorando"] and self.product_state.get("url"))
            state["sales_coach"]["error"] = self.product_state.get("error") or state["sales_coach"].get("error")
        return state
