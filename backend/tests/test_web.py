"""Integration checks for notebook adaptation and its real HTTP boundary.

Captured LIVE events in these tests are local fixtures, never production UI
data. External Shopee/TikTok connections require a deployed host and LIVE.
"""

import asyncio
import json
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from backend.runtime import MissingCaptureDependency, load_runtime, validate_input
from backend.product_link import ProductPageError, ProductPageWorker, _price_number, normalize_page_data, validate_product_link
from backend.server import Handler, _sessions


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.runtime = load_runtime()

    def test_input_validation_and_normalizers(self):
        self.assertEqual(validate_input("shopee", "https://br.shp.ee/Abc"), "https://br.shp.ee/Abc")
        self.assertEqual(validate_input("shopee", "https://live.shopee.com.br/share?session=123456"), "https://live.shopee.com.br/share?session=123456")
        self.assertEqual(validate_input("tiktok", "https://www.tiktok.com/@vendedora/live"), "@vendedora")
        with self.assertRaises(ValueError):
            validate_input("shopee", "https://malicious.example/abc")
        with self.assertRaises(ValueError):
            validate_input("tiktok", "https://malicious.example/@user")
        ns = self.runtime.ns
        self.assertEqual(ns["_norm"]("Ação promocional"), "acao promocional")
        self.assertEqual(ns["_sd_normalize_style"]("Pressão / Feira"), "pressao_feira")
        self.assertNotEqual("_sales_builder_stop_requested", "_stop_requested")
        ns["product_sales_builder_stop"]()
        self.assertTrue(ns["_sales_builder_stop_requested"])
        self.assertFalse(ns["_stop_requested"])

    def test_original_sales_modules_and_alert_modes(self):
        ns = self.runtime.ns
        self.assertTrue(ns["product_extractor_self_test"]()["ok"])
        self.assertTrue(ns["product_sales_builder_self_test"]()["ok"])
        self.assertTrue(ns["sales_decision_self_test"]()["ok"])
        self.assertEqual(self.runtime.alert_mode("high")["mode"], "high")
        self.assertEqual(self.runtime.alert_mode("essential")["mode"], "essential")
        self.assertEqual(self.runtime.alert_mode("all")["mode"], "all")
        self.assertEqual(ns["sales_decision_set_style"]("Pressão / Feira")["style"], "pressao_feira")
        self.assertEqual(ns["sales_decision_set_style"]("Equilibrado")["style"], "equilibrado")

    def test_tiktok_waits_for_product_link_before_sales_workers(self):
        rt = self.runtime
        rt.missing = []
        calls = []
        rt.ns["agcn_iniciar_sales_tasks"] = lambda link: calls.append(link)

        async def hold_tiktok(username):
            await asyncio.Event().wait()

        rt.ns["executar_tiktok_com_live_engine"] = hold_tiktok
        started = rt.start("tiktok", "@vendedora")
        self.assertTrue(started["ok"])
        self.assertEqual(started["platform"], "tiktok")
        self.assertFalse(rt.ns["agcn_sales_enabled"])
        self.assertEqual(calls, [])
        self.assertEqual(rt.sales_style("pressao_feira")["style"], "pressao_feira")
        self.assertEqual(rt.product_info(info="teste")["ok"], True)
        rt.stop()
        rt.ns["agcn_thread"].join(timeout=2)
        self.assertFalse(rt.ns["agcn_monitorando"])

    def test_shopee_starts_parallel_sales_flow_with_seller_provenance(self):
        rt = self.runtime
        rt.missing = []
        sales_started = []
        rt.ns["agcn_iniciar_sales_tasks"] = lambda link: sales_started.append(link)

        async def hold_shopee():
            await asyncio.Event().wait()

        rt.ns["executar_shopee_com_live_engine"] = hold_shopee
        link = "https://br.shp.ee/Produto123"
        started = rt.start("shopee", link, price="49,90", info="Material confirmado", style="pressao_feira")
        self.assertTrue(started["sales_enabled"])
        for _ in range(50):
            if sales_started:
                break
            time.sleep(.01)
        self.assertEqual(sales_started, [link])
        self.assertEqual(rt.ns["_pending_seller_info"]["price"], "49,90")
        self.assertEqual(rt.ns["_pending_seller_info"]["additional_info"], "Material confirmado")
        self.assertFalse(rt.status()["sales_coach"]["enabled"])
        self.assertEqual(rt.status()["product"]["state"], "waiting_link")
        rt.stop()
        rt.ns["agcn_thread"].join(timeout=2)
        self.assertFalse(rt.ns["agcn_monitorando"])

    def test_product_link_platform_validation_and_page_facts(self):
        shopee = validate_product_link("https://shopee.com.br/Tenis-tesla-i.968369213.58264206611?tracking=ignored", "shopee")
        self.assertEqual((shopee["shop_id"], shopee["item_id"]), ("968369213", "58264206611"))
        self.assertNotIn("tracking", shopee["url"])
        tiktok = validate_product_link("https://shop.tiktok.com/br/pdp/1737558033193076344?og_info=untrusted", "tiktok")
        self.assertEqual(tiktok["item_id"], "1737558033193076344")
        for bad in ("http://shopee.com.br/p-i.1.2", "https://127.0.0.1/br/pdp/2", "https://shop.tiktok.com@evil.example/br/pdp/1"):
            with self.assertRaises(ProductPageError):
                validate_product_link(bad)
        with self.assertRaises(ProductPageError):
            validate_product_link(tiktok["url"], "shopee")
        self.assertEqual(_price_number("R$\n49\n.00\nR$256,00"), 49.0)
        raw = normalize_page_data(tiktok, {"title": "Relógio Inteligente BML", "price_text": "R$\n49\n.00", "description": "Tela de 2,09 polegadas"})
        self.assertEqual(raw["price"], 49.0)
        self.assertEqual(raw["description"], "Tela de 2,09 polegadas")
        with self.assertRaises(ProductPageError):
            normalize_page_data(tiktok, {"blocked": True, "title": "qualquer produto"})

    def test_short_product_links_resolve_only_through_official_hosts(self):
        class Response:
            def __init__(self, location=""):
                self.status_code = 302 if location else 200
                self.headers = {"Location": location} if location else {}
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
        class Session:
            def __init__(self, redirect):
                self.redirect = redirect
                self.calls = []
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                return Response(self.redirect if len(self.calls) == 1 else "")

        tiktok = Session("https://shop.tiktok.com/br/pdp/1737558033193076344?og_info=unverified")
        with patch("backend.product_link.requests.Session", return_value=tiktok):
            identity = validate_product_link("https://vt.tiktok.com/ZS9AHbTHKcoJp-2HMVp/", "tiktok")
        self.assertEqual(identity["item_id"], "1737558033193076344")
        self.assertEqual(identity["url"], "https://shop.tiktok.com/br/pdp/1737558033193076344")
        self.assertEqual(len(tiktok.calls), 2)
        self.assertFalse(tiktok.calls[0][1]["allow_redirects"])
        shopee = Session("https://shopee.com.br/Produto-i.968369213.58264206611")
        with patch("backend.product_link.requests.Session", return_value=shopee):
            identity = validate_product_link("https://s.shopee.com.br/BU2jyrNid", "shopee")
        self.assertEqual(identity["item_id"], "58264206611")
        self.assertEqual(len(shopee.calls), 2)
        malicious = Session("https://127.0.0.1/private")
        with patch("backend.product_link.requests.Session", return_value=malicious):
            with self.assertRaises(ProductPageError):
                validate_product_link("https://vt.tiktok.com/ZS9AHbTHKcoJp-2HMVp/", "tiktok")
        self.assertEqual(len(malicious.calls), 1)
        with self.assertRaises(ProductPageError):
            validate_product_link("https://vt.tiktok.com/ZS9AHbTHKcoJp-2HMVp/", "shopee")

    def test_product_browser_route_guard_accepts_playwright_request(self):
        class Request:
            url = "https://shop.tiktok.com/br/pdp/123"
            frame = object()
            def is_navigation_request(self):
                return True
        class Route:
            request = Request()
            def __init__(self):
                self.continued = False
            async def continue_(self):
                self.continued = True
            async def abort(self):
                self.continued = False
        class Page:
            main_frame = Request.frame
            url = "https://shop.tiktok.com/br"
            async def route(self, pattern, handler):
                self.handler = handler
            async def goto(self, *_args, **_kwargs):
                pass
        class Context:
            async def new_page(self):
                return Page()
        class Browser:
            async def new_context(self, **_kwargs):
                return Context()
        class BrowserType:
            async def launch(self, **_kwargs):
                return Browser()
        class Playwright:
            chromium = BrowserType()
        class Manager:
            async def start(self):
                return Playwright()
        async def check():
            import inspect
            worker = ProductPageWorker()
            with patch("playwright.async_api.async_playwright", return_value=Manager()):
                await worker.open()
            route = Route()
            handler = worker.pages["tiktok"].handler
            self.assertEqual(len(inspect.signature(handler).parameters), 1)
            await handler(route)
            self.assertTrue(route.continued)
            route.request.url = "https://localhost/private"
            await handler(route)
            self.assertFalse(route.continued)
        asyncio.run(check())

    def test_tiktok_link_enters_original_extractor_builder_decision_pipeline(self):
        rt = self.runtime
        ns = rt.ns
        rt.platform = "tiktok"
        ns["agcn_monitorando"] = True
        ns["agcn_preparar_sales_pipeline"] = lambda **kw: ns.update(agcn_sales_enabled=True)
        ns["agcn_iniciar_sales_tasks"] = lambda *_: None

        class FakeBrowser:
            async def read(self, identity):
                return {"item_id": identity["item_id"], "shop_id": identity["shop_id"], "name": "Relógio BML", "price": 49.0, "description": "Tela de 2 polegadas"}

        rt.product_browser = FakeBrowser()
        identity = validate_product_link("https://shop.tiktok.com/br/pdp/1737558033193076344")
        asyncio.run(rt._read_product(identity, rt.product_epoch))
        profile = ns["product_extractor_current_profile"]()
        self.assertEqual(profile["provenance"]["automatic_source"], "tiktok_product_page")
        self.assertEqual(profile["product"]["name"]["source"], "tiktok_product_page")
        output = ns["product_extractor_recent_outputs"](1)[0]
        bundle = ns["product_sales_builder_process_output"](output)["bundle"]
        self.assertTrue(bundle["candidates"])
        self.assertIsNotNone(ns["sales_decision_process_builder_output"]({"type": "sales_candidates_started", "bundle": bundle}))

    def test_seller_information_only_uses_product_extractor(self):
        rt = self.runtime
        rt.platform = "shopee"
        rt.ns["agcn_monitorando"] = True
        received = []

        def capture(**kwargs):
            received.append(kwargs)
            return {"target": "pending"}

        rt.ns["product_extractor_set_seller_info"] = capture
        result = rt.product_info(price="49,90", info="Algodão", facts={"material": "algodão"})
        self.assertTrue(result["ok"])
        self.assertEqual(received, [{"price": "49,90", "additional_info": "Algodão", "facts": {"material": "algodão"}, "replace": True, "emit_update": True}])
        rt.ns["agcn_monitorando"] = False

    def test_status_reads_history_without_consuming_coach_queues(self):
        rt = self.runtime
        ns = rt.ns
        ns["live_engine_history"].clear()
        ns["live_engine_history"].append({"event_id": "test-comment", "type": "comment", "iso_time": "2026-09-23T12:00:00+00:00", "payload": {"user": "Compradora", "text": "Qual o tamanho?", "display_time": "09:00:00"}})
        ns["sales_decision_output_queue"].put_nowait({"type": "fixture"})
        before = ns["sales_decision_output_queue"].qsize()
        rt.status(); status = rt.status()
        self.assertEqual(before, ns["sales_decision_output_queue"].qsize())
        self.assertEqual(len(status["comments"]), 1)
        self.assertEqual(status["comments"][0]["id"], "test-comment")

        async def check_bridge():
            ns["sales_decision_output_queue"].get_nowait()
            task = asyncio.create_task(ns["agcn_sales_output_bridge"]())
            started = datetime.now(timezone.utc)
            ns["sales_decision_output_queue"].put_nowait({"type": "sales_coach_message", "seq": 7, "message": "Mostre o produto", "category": "demonstracao", "started_at": started.isoformat(), "expires_at": (started + timedelta(seconds=8)).isoformat(), "display_seconds": 8})
            await asyncio.sleep(.02)
            self.assertEqual(len(ns["sales_coach_tips"]), 1)
            self.assertEqual(ns["sales_decision_output_queue"].qsize(), 0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(check_bridge())


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)
        _sessions.clear()

    def request(self, path, body=None, sid=None):
        headers = {"Accept": "application/json"}
        if sid:
            headers["X-AGCN-Session"] = sid
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, headers=headers, data=json.dumps(body).encode() if body is not None else None)
        try:
            response = urllib.request.urlopen(req, timeout=5)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.headers, json.loads(response.read())

    def test_page_session_commands_and_errors(self):
        code, _, health = self.request("/api/health")
        self.assertEqual((code, health["name"]), (200, "AGCN LIVE"))
        code, headers, state = self.request("/api/state")
        self.assertEqual((code, state["status"]), (200, "parado"))
        sid = headers["X-AGCN-Session"]
        self.assertTrue(sid)
        _sessions[sid].runtime.missing = ["TikTokLive", "TikTokLive.events"]
        code, headers2, _ = self.request("/api/state")
        self.assertNotEqual(sid, headers2["X-AGCN-Session"])
        code, _, mode = self.request("/api/alert-mode", {"mode": "high"}, sid)
        self.assertEqual((code, mode["state"]["coach_mode"]["value"]), (200, "high"))
        code, _, response = self.request("/api/start", {"platform": "tiktok", "value": "@vendedora"}, sid)
        self.assertEqual(code, 503)
        self.assertIn("Dependências Python", response["message"])
        code, _, response = self.request("/api/start", {"platform": "shopee", "value": "https://example.com/"}, sid)
        self.assertEqual(code, 400)
        code, _, response = self.request("/api/product-info", {"facts": {"material": "algodão"}}, sid)
        self.assertEqual(code, 400)
        code, _, response = self.request("/api/stop", {}, sid)
        self.assertEqual((code, response["ok"]), (200, True))

    def test_sse_reads_snapshot_without_draining_a_queue(self):
        _, headers, _ = self.request("/api/state")
        sid = headers["X-AGCN-Session"]
        runtime = _sessions[sid].runtime
        runtime.ns["sales_decision_output_queue"].put_nowait({"type": "fixture"})
        before = runtime.ns["sales_decision_output_queue"].qsize()
        with urllib.request.urlopen(self.base + "/api/events?sid=" + sid, timeout=5) as response:
            self.assertEqual(response.headers.get_content_type(), "text/event-stream")
            self.assertEqual(response.readline().decode().strip(), "event: state")
            line = response.readline().decode()
            self.assertEqual(json.loads(line.removeprefix("data: "))["status"], "parado")
        self.assertEqual(runtime.ns["sales_decision_output_queue"].qsize(), before)


if __name__ == "__main__":
    unittest.main()
