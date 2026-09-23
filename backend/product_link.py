"""Browser-backed product input for the existing Product Extractor pipeline.

The browser is owned by the Python session, never by the visitor's frontend.
Only verified fields from the rendered product page become raw_product facts.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin, urlparse

import requests


class ProductPageError(ValueError):
    pass


PRODUCT_HOSTS = {
    "shopee": {"shopee.com.br", "www.shopee.com.br"},
    "tiktok": {"shop.tiktok.com"},
}
SHORT_HOSTS = {"shopee": "s.shopee.com.br", "tiktok": "vt.tiktok.com"}
REDIRECT_HOSTS = {
    "shopee": PRODUCT_HOSTS["shopee"] | {SHORT_HOSTS["shopee"]},
    "tiktok": PRODUCT_HOSTS["tiktok"] | {SHORT_HOSTS["tiktok"], "www.tiktok.com", "m.tiktok.com", "tiktok.com"},
}


def _checked_url(value, hosts):
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError as exc:
        raise ProductPageError("Link do produto inválido.") from exc
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or port is not None or parsed.hostname.lower() not in hosts:
        raise ProductPageError("O redirecionamento do produto saiu das páginas oficiais da plataforma.")
    return parsed


def resolve_short_link(value, platform):
    """Follow only same-platform HTTPS redirects, never request an arbitrary host."""
    hosts = REDIRECT_HOSTS[platform]
    current = value
    visited = set()
    with requests.Session() as session:
        for _ in range(6):
            parsed = _checked_url(current, hosts)
            if current in visited:
                raise ProductPageError("O link curto do produto entrou em um ciclo de redirecionamentos.")
            visited.add(current)
            # Avoid reading product bodies and keep redirect checks under our control.
            try:
                with session.get(current, allow_redirects=False, stream=True, timeout=(3, 7)) as response:
                    destination = response.headers.get("Location", "") if 300 <= response.status_code < 400 else ""
            except requests.RequestException as exc:
                raise ProductPageError("Não foi possível abrir o link curto do produto.") from exc
            if not destination:
                return current
            current = urljoin(parsed.geturl(), destination)
            if len(current) > 4096:
                raise ProductPageError("Redirecionamento do produto muito longo.")
            _checked_url(current, hosts)
    raise ProductPageError("O link curto do produto redirecionou muitas vezes.")


def validate_product_link(value, platform=None):
    value = str(value or "").strip()
    if len(value) > 2048:
        raise ProductPageError("O link do produto é muito longo.")
    try:
        url = urlparse(value)
        port = url.port
    except ValueError as exc:
        raise ProductPageError("Link do produto inválido.") from exc
    if url.scheme != "https" or url.username or url.password or port is not None:
        raise ProductPageError("Informe um link HTTPS de produto da Shopee ou TikTok Shop.")
    host = (url.hostname or "").lower()
    short_platform = next((name for name, short_host in SHORT_HOSTS.items() if host == short_host), None)
    if short_platform:
        if platform and platform != short_platform:
            raise ProductPageError("O link do produto deve pertencer à plataforma da LIVE selecionada.")
        if not re.fullmatch(r"/[A-Za-z0-9_-]{5,100}/?", url.path):
            raise ProductPageError("Link curto do produto inválido.")
        value = resolve_short_link(value, short_platform)
        url = _checked_url(value, PRODUCT_HOSTS[short_platform])
        host = (url.hostname or "").lower()
    detected = next((name for name, hosts in PRODUCT_HOSTS.items() if host in hosts), None)
    if not detected or (platform and detected != platform):
        raise ProductPageError("O link do produto deve pertencer à plataforma da LIVE selecionada.")
    if detected == "shopee":
        match = re.search(r"-i\.(\d+)\.(\d+)(?:/|$)", url.path)
        if not match:
            raise ProductPageError("Informe o endereço de um produto Shopee com shop_id e item_id.")
        shop_id, item_id = match.groups()
    else:
        match = re.fullmatch(r"/br/pdp/(?:[^/]+/)?(\d+)/?", url.path)
        if not match:
            raise ProductPageError("Informe um link de produto TikTok Shop /br/pdp/...")
        item_id, shop_id = match.group(1), "product_page"
    # Remove tracking parameters; this also prevents query text from being
    # treated as verified product information.
    clean_url = f"https://{host}{url.path.rstrip('/')}"
    return {"platform": detected, "url": clean_url, "item_id": item_id, "shop_id": shop_id}


def _price_number(text):
    if not text:
        return None
    match = re.search(r"R\$\s*(\d[\d\s.,]*)", text)
    if not match:
        return None
    raw = re.sub(r"\s+", "", match.group(1)).rstrip(".,")
    if not raw:
        return None
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
"""Browser-backed product input for the existing Product Extractor pipeline.

The browser is owned by the Python session, never by the visitor's frontend.
Only verified fields from the rendered product page become raw_product facts.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin, urlparse

import requests


class ProductPageError(ValueError):
    pass


PRODUCT_HOSTS = {
    "shopee": {"shopee.com.br", "www.shopee.com.br"},
    "tiktok": {"shop.tiktok.com"},
}
SHORT_HOSTS = {"shopee": "s.shopee.com.br", "tiktok": "vt.tiktok.com"}
REDIRECT_HOSTS = {
    "shopee": PRODUCT_HOSTS["shopee"] | {SHORT_HOSTS["shopee"]},
    "tiktok": PRODUCT_HOSTS["tiktok"] | {SHORT_HOSTS["tiktok"], "www.tiktok.com", "m.tiktok.com", "tiktok.com"},
}


def _checked_url(value, hosts):
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError as exc:
        raise ProductPageError("Link do produto inválido.") from exc
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or port is not None or parsed.hostname.lower() not in hosts:
        raise ProductPageError("O redirecionamento do produto saiu das páginas oficiais da plataforma.")
    return parsed


def resolve_short_link(value, platform):
    """Follow only same-platform HTTPS redirects, never request an arbitrary host."""
    hosts = REDIRECT_HOSTS[platform]
    current = value
    visited = set()
    with requests.Session() as session:
        for _ in range(6):
            parsed = _checked_url(current, hosts)
            if current in visited:
                raise ProductPageError("O link curto do produto entrou em um ciclo de redirecionamentos.")
            visited.add(current)
            # Avoid reading product bodies and keep redirect checks under our control.
            try:
                with session.get(current, allow_redirects=False, stream=True, timeout=(3, 7)) as response:
                    destination = response.headers.get("Location", "") if 300 <= response.status_code < 400 else ""
            except requests.RequestException as exc:
                raise ProductPageError("Não foi possível abrir o link curto do produto.") from exc
            if not destination:
                return current
            current = urljoin(parsed.geturl(), destination)
            if len(current) > 4096:
                raise ProductPageError("Redirecionamento do produto muito longo.")
            _checked_url(current, hosts)
    raise ProductPageError("O link curto do produto redirecionou muitas vezes.")


def validate_product_link(value, platform=None):
    value = str(value or "").strip()
    if len(value) > 2048:
        raise ProductPageError("O link do produto é muito longo.")
    try:
        url = urlparse(value)
        port = url.port
    except ValueError as exc:
        raise ProductPageError("Link do produto inválido.") from exc
    if url.scheme != "https" or url.username or url.password or port is not None:
        raise ProductPageError("Informe um link HTTPS de produto da Shopee ou TikTok Shop.")
    host = (url.hostname or "").lower()
    short_platform = next((name for name, short_host in SHORT_HOSTS.items() if host == short_host), None)
    if short_platform:
        if platform and platform != short_platform:
            raise ProductPageError("O link do produto deve pertencer à plataforma da LIVE selecionada.")
        if not re.fullmatch(r"/[A-Za-z0-9_-]{5,100}/?", url.path):
            raise ProductPageError("Link curto do produto inválido.")
        value = resolve_short_link(value, short_platform)
        url = _checked_url(value, PRODUCT_HOSTS[short_platform])
        host = (url.hostname or "").lower()
    detected = next((name for name, hosts in PRODUCT_HOSTS.items() if host in hosts), None)
    if not detected or (platform and detected != platform):
        raise ProductPageError("O link do produto deve pertencer à plataforma da LIVE selecionada.")
    if detected == "shopee":
        match = re.search(r"-i\.(\d+)\.(\d+)(?:/|$)", url.path)
        if not match:
            raise ProductPageError("Informe o endereço de um produto Shopee com shop_id e item_id.")
        shop_id, item_id = match.groups()
    else:
        match = re.fullmatch(r"/br/pdp/(?:[^/]+/)?(\d+)/?", url.path)
        if not match:
            raise ProductPageError("Informe um link de produto TikTok Shop /br/pdp/...")
        item_id, shop_id = match.group(1), "product_page"
    # Remove tracking parameters; this also prevents query text from being
    # treated as verified product information.
    clean_url = f"https://{host}{url.path.rstrip('/')}"
    return {"platform": detected, "url": clean_url, "item_id": item_id, "shop_id": shop_id}


def _price_number(text):
    if not text:
        return None
    match = re.search(r"R\$\s*(\d[\d\s.,]*)", text)
    if not match:
        return None
    raw = re.sub(r"\s+", "", match.group(1)).rstrip(".,")
    if not raw:
        return None
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif raw.count(".") > 1:
        raw = raw.replace(".", "")
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    return float(value) if value > 0 else None


def normalize_page_data(identity, data):
    """Reject incomplete or challenge pages; keep provenance on every fact."""
    if data.get("blocked"):
        raise ProductPageError("A plataforma bloqueou a leitura da página do produto. Tente novamente mais tarde.")
    name = re.sub(r"\s+", " ", str(data.get("title") or "")).strip()[:300]
    if not name or len(name) < 5:
        raise ProductPageError("Não foi possível verificar o nome do produto na página.")
    raw = {"item_id": identity["item_id"], "shop_id": identity["shop_id"], "name": name}
    price = _price_number(data.get("price_text"))
    if price is not None:
        raw.update(price=price, currency="BRL")
    description = re.sub(r"\s+", " ", str(data.get("description") or "")).strip()[:6000]
    if description:
        raw["description"] = description
    image = str(data.get("image") or "").strip()
    if image.startswith("https://"):
        raw["image"] = image[:1600]
    seller = re.sub(r"\s+", " ", str(data.get("seller") or "")).strip()[:150]
    if seller:
        raw["seller_name"] = seller
    return raw


class ProductPageWorker:
    """One Chromium instance with a ready tab for each platform per session."""

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.pages = {}

    async def open(self):
        if self.browser:
            return
        from playwright.async_api import async_playwright

        self.playwright = await async_playwright().start()
        try:
            self.browser = await self.playwright.chromium.launch(headless=True, args=["--no-sandbox"])
            for platform, home in (
                ("shopee", "https://shopee.com.br/"),
                ("tiktok", "https://shop.tiktok.com/br"),
            ):
                context = await self.browser.new_context(locale="pt-BR", timezone_id="America/Sao_Paulo")
                page = await context.new_page()
                allowed = PRODUCT_HOSTS[platform]

                def make_guard(allowed_hosts, own_page):
                    async def guard(route):
                        request = route.request
                        host = (urlparse(request.url).hostname or "").lower()
                        if request.is_navigation_request() and request.frame == own_page.main_frame and host not in allowed_hosts:
                            await route.abort()
                        else:
                            await route.continue_()
                    return guard

                await page.route("**/*", make_guard(allowed, page))
                self.pages[platform] = page
                try:
                    await page.goto(home, wait_until="domcontentloaded", timeout=12000)
                except Exception:
                    pass  # A product navigation will report the useful error.
        except Exception:
            await self.close()
            raise

    async def read(self, identity):
        await self.open()
        platform = identity["platform"]
        page = self.pages[platform]
        try:
            await page.goto(identity["url"], wait_until="domcontentloaded", timeout=25000)
            try:
                await page.locator("h1").first.wait_for(timeout=12000)
            except Exception:
                pass
        except Exception as exc:
            raise ProductPageError(f"Não foi possível abrir o produto na {platform.title()}: {type(exc).__name__}.") from exc
        final = urlparse(page.url)
        if final.hostname not in PRODUCT_HOSTS[platform] or "/verify/" in final.path or "/login" in final.path:
            raise ProductPageError(f"A {platform.title()} não disponibilizou a página do produto neste navegador.")
        data = await page.evaluate("""() => {
            const meta = name => document.querySelector(`meta[property="${name}"]`)?.content || '';
            const title = document.querySelector('h1')?.textContent?.trim() || meta('og:title');
            const heading = document.querySelector('h1');
            const priceBlock = heading?.parentElement?.parentElement;
            const priceText = priceBlock?.querySelector('div.flex.flex-row.items-baseline')?.innerText ||
                document.querySelector('meta[property="product:price:amount"]')?.content || '';
            const label = [...document.querySelectorAll('span,div')].find(node =>
                node.children.length < 3 && /^(Product description|Descrição do produto)$/i.test(node.textContent?.trim() || ''));
            const section = label?.closest('[class*="expandableSection"]');
            const description = section?.querySelector('[class*="sectionContent"]')?.innerText || '';
            const seller = priceBlock?.innerText?.match(/(?:Sold by|Vendido por)\\s+([^\\n]+)/i)?.[1] || '';
            return {title, price_text: priceText, description, seller,
                image: meta('og:image'),
                blocked: /verify\\/traffic\\/error|captcha/i.test(location.href) ||
                    /verifique que você é humano/i.test(document.body.innerText.slice(0,1500))};
        }""")
        return normalize_page_data(identity, data)

    async def close(self):
        browser, playwright = self.browser, self.playwright
        self.browser = self.playwright = None
        self.pages = {}
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
    elif raw.count(".") > 1:
        raw = raw.replace(".", "")
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    return float(value) if value > 0 else None


def normalize_page_data(identity, data):
    """Reject incomplete or challenge pages; keep provenance on every fact."""
    if data.get("blocked"):
        raise ProductPageError("A plataforma bloqueou a leitura da página do produto. Tente novamente mais tarde.")
    name = re.sub(r"\s+", " ", str(data.get("title") or "")).strip()[:300]
    if not name or len(name) < 5:
        raise ProductPageError("Não foi possível verificar o nome do produto na página.")
    raw = {"item_id": identity["item_id"], "shop_id": identity["shop_id"], "name": name}
    price = _price_number(data.get("price_text"))
    if price is not None:
        raw.update(price=price, currency="BRL")
    description = re.sub(r"\s+", " ", str(data.get("description") or "")).strip()[:6000]
    if description:
        raw["description"] = description
    image = str(data.get("image") or "").strip()
    if image.startswith("https://"):
        raw["image"] = image[:1600]
    seller = re.sub(r"\s+", " ", str(data.get("seller") or "")).strip()[:150]
    if seller:
        raw["seller_name"] = seller
    return raw


class ProductPageWorker:
    """One Chromium instance with a ready tab for each platform per session."""

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.pages = {}

    async def open(self):
        if self.browser:
            return
        from playwright.async_api import async_playwright

        self.playwright = await async_playwright().start()
        try:
            self.browser = await self.playwright.chromium.launch(headless=True, args=["--no-sandbox"])
            for platform, home in (
                ("shopee", "https://shopee.com.br/"),
                ("tiktok", "https://shop.tiktok.com/br"),
            ):
                context = await self.browser.new_context(locale="pt-BR", timezone_id="America/Sao_Paulo")
                page = await context.new_page()
                allowed = PRODUCT_HOSTS[platform]

                def make_guard(allowed_hosts, own_page):
                    async def guard(route):
                        request = route.request
                        host = (urlparse(request.url).hostname or "").lower()
                        if request.is_navigation_request() and request.frame == own_page.main_frame and host not in allowed_hosts:
                            await route.abort()
                        else:
                            await route.continue_()
                    return guard

                await page.route("**/*", make_guard(allowed, page))
                self.pages[platform] = page
                try:
                    await page.goto(home, wait_until="domcontentloaded", timeout=12000)
                except Exception:
                    pass  # A product navigation will report the useful error.
        except Exception:
            await self.close()
            raise

    async def read(self, identity):
        await self.open()
        platform = identity["platform"]
        page = self.pages[platform]
        try:
            await page.goto(identity["url"], wait_until="domcontentloaded", timeout=25000)
            try:
                await page.locator("h1").first.wait_for(timeout=12000)
            except Exception:
                pass
        except Exception as exc:
            raise ProductPageError(f"Não foi possível abrir o produto na {platform.title()}: {type(exc).__name__}.") from exc
        final = urlparse(page.url)
        if final.hostname not in PRODUCT_HOSTS[platform] or "/verify/" in final.path or "/login" in final.path:
            raise ProductPageError(f"A {platform.title()} não disponibilizou a página do produto neste navegador.")
        data = await page.evaluate("""() => {
            const meta = name => document.querySelector(`meta[property="${name}"]`)?.content || '';
            const title = document.querySelector('h1')?.textContent?.trim() || meta('og:title');
            const heading = document.querySelector('h1');
            const priceBlock = heading?.parentElement?.parentElement;
            const priceText = priceBlock?.querySelector('div.flex.flex-row.items-baseline')?.innerText ||
                document.querySelector('meta[property="product:price:amount"]')?.content || '';
            const label = [...document.querySelectorAll('span,div')].find(node =>
                node.children.length < 3 && /^(Product description|Descrição do produto)$/i.test(node.textContent?.trim() || ''));
            const section = label?.closest('[class*="expandableSection"]');
            const description = section?.querySelector('[class*="sectionContent"]')?.innerText || '';
            const seller = priceBlock?.innerText?.match(/(?:Sold by|Vendido por)\\s+([^\\n]+)/i)?.[1] || '';
            return {title, price_text: priceText, description, seller,
                image: meta('og:image'),
                blocked: /verify\\/traffic\\/error|captcha/i.test(location.href) ||
                    /verifique que você é humano/i.test(document.body.innerText.slice(0,1500))};
        }""")
        return normalize_page_data(identity, data)

    async def close(self):
        browser, playwright = self.browser, self.playwright
        self.browser = self.playwright = None
        self.pages = {}
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
