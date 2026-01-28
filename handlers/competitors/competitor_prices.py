# handlers/competitors/competitor_prices.py
from __future__ import annotations

import os
import re
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, Dict, List, Any

import httpx
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


# =======================
# Настройки (env-overrides)
# =======================

MSK_TZ = timezone(timedelta(hours=3))

SHEET_URL_DEFAULT = "https://docs.google.com/spreadsheets/d/1DhDdf5FbjIXShOhWN3g_xZdIjnLiMZUPkIAei-hG3Bc/edit#gid=0"
WORKSHEET_NAME_DEFAULT = "Прайс"

# Колонки (ссылка -> цена)
# AG->AH (например Store77)
# AI->AJ (например Cordstore / BigGeek и т.п.)
# AM->AN (Upstore24)
# AO->AP (Appmistore)
# AQ->AR (Alikson)
PAIR_DEFAULT: Tuple[Tuple[str, str], ...] = (
    ("AG", "AH"),
    ("AI", "AJ"),
    ("AK","AL"),
    ("AM", "AN"),
    ("AO", "AP"),
    ("AQ", "AR"),
)

START_ROW_DEFAULT = 2  # если 1-я строка заголовки

HTTP_TIMEOUT = float(os.getenv("COMP_HTTP_TIMEOUT", "20") or "20")
MAX_CONCURRENCY = int(os.getenv("COMP_CONCURRENCY", "20") or "20")  # для 1500 строк обычно можно 20-40

# project root = Under_price_final (handlers/competitors/competitor_prices.py -> parents[2])
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODULE_DIR = Path(__file__).resolve().parent

GS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Debug режим: печатать причины, почему цена не нашлась
DEBUG = (os.getenv("COMP_DEBUG") or "").strip().lower() in ("1", "true", "yes", "y")

# Если COMP_DEBUG_SAVE=1 — сохраняем HTML рядом (handlers/competitors/)
DEBUG_SAVE = (os.getenv("COMP_DEBUG_SAVE") or "").strip().lower() in ("1", "true", "yes", "y")

# Включить Playwright-фоллбек (Store77 антибот)
USE_PLAYWRIGHT = (os.getenv("COMP_USE_PLAYWRIGHT") or "1").strip().lower() in ("1", "true", "yes", "y")

# Ограничим число параллельных браузерных запросов (важно для ресурсов)
PLAYWRIGHT_CONCURRENCY = int(os.getenv("COMP_PW_CONCURRENCY", "2") or "2")
_PLAYWRIGHT_SEM = asyncio.Semaphore(PLAYWRIGHT_CONCURRENCY)

# Кэш цен по URL (persist)
CACHE_FILE = Path(os.getenv("COMP_CACHE_FILE", str(MODULE_DIR / "_competitor_price_cache.json")))
CACHE_TTL_SECS = int(os.getenv("COMP_CACHE_TTL_SECS", str(12 * 3600)) or str(12 * 3600))           # успех: 12 часов
CACHE_MISS_TTL_SECS = int(os.getenv("COMP_CACHE_MISS_TTL_SECS", str(15 * 60)) or str(15 * 60))      # не нашли: 15 минут


# =======================
# Utils: A1 / колонки
# =======================

def col_to_index(col: str) -> int:
    col = (col or "").strip().upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return n


def a1(col: str, row: int) -> str:
    return f"{col}{row}"


def _col_range(col: str, start_row: int, end_row: int) -> str:
    return f"{col}{start_row}:{col}{end_row}"


def parse_sheet_id(url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url or "")
    if not m:
        raise ValueError("Не удалось извлечь sheet_id из URL")
    return m.group(1)


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _safe_cell(col_values: list[list[str]], idx: int) -> str:
    """
    col_values: [['url'], [''], ...] (по строкам)
    idx: 0..N-1
    """
    if idx < 0 or idx >= len(col_values):
        return ""
    row = col_values[idx] or []
    return str(row[0]).strip() if row else ""


# =======================
# URL helpers (ВАЖНО: #fragment)
# =======================

def _normalize_url_key(url: str) -> str:
    """
    Ключ для кэша: сохраняем #fragment, потому что для BigGeek
    это выбор варианта (esim/nano-sim-i-esim/2-nano-sim).
    """
    return (url or "").strip()


def _request_url(url: str) -> str:
    """
    URL для HTTP-запроса: фрагмент (#...) не отправляется на сервер,
    поэтому отрезаем его, чтобы не путать редиректы/логи.
    """
    u = (url or "").strip()
    if "#" in u:
        u = u.split("#", 1)[0].strip()
    return u


def _get_fragment(url: str) -> str:
    """
    Возвращает строку после # (slug варианта).
    """
    u = (url or "").strip()
    if "#" not in u:
        return ""
    return u.split("#", 1)[1].strip().lower()


def _has_http(url: str) -> bool:
    return (url or "").strip().lower().startswith("http")


# =======================
# Google Sheets
# =======================

_gs_client = None


def _resolve_gs_key_file() -> Path:
    """
    Ищем ключ сервисного аккаунта в нескольких местах.

    Приоритет:
      1) ENV: GOOGLE_SERVICE_ACCOUNT_FILE или GS_KEY_FILE
      2) <project_root>/config/google_service_account.json
      3) <project_root>/data/config/gsheets_service.json
      4) <project_root>/data/config/google_service_account.json
    """
    env_path = (os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or os.getenv("GS_KEY_FILE") or "").strip()
    if env_path:
        p = Path(env_path).expanduser().resolve()
        if p.exists():
            return p

    candidates = [
        PROJECT_ROOT / "config" / "google_service_account.json",
        PROJECT_ROOT / "data" / "config" / "gsheets_service.json",
        PROJECT_ROOT / "data" / "config" / "google_service_account.json",
    ]

    for p in candidates:
        if p.exists():
            return p

    return candidates[0]


def get_gs_client():
    global _gs_client
    if _gs_client:
        return _gs_client

    key_file = _resolve_gs_key_file()
    if not key_file.exists():
        raise FileNotFoundError(f"Не найден ключ сервисного аккаунта: {key_file}")

    creds = Credentials.from_service_account_file(str(key_file), scopes=GS_SCOPES)
    _gs_client = gspread.authorize(creds)
    return _gs_client


# =======================
# Price cache (persist)
# =======================

class _PriceCache:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        self._data: Dict[str, Dict[str, Any]] = {}
        self._loaded = False

    async def ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            try:
                if self.path.exists():
                    raw = self.path.read_text(encoding="utf-8")
                    obj = json.loads(raw)
                    if isinstance(obj, dict):
                        self._data = obj
            except Exception:
                self._data = {}
            self._loaded = True

    async def get(self, url_key: str) -> Optional[int]:
        await self.ensure_loaded()
        u = _normalize_url_key(url_key)
        if not u:
            return None

        item = self._data.get(u)
        if not isinstance(item, dict):
            return None

        price = item.get("price")
        ts = item.get("ts")
        ok = item.get("ok")

        if not isinstance(ts, int):
            return None

        age = _now_ts() - ts
        ttl = CACHE_TTL_SECS if ok else CACHE_MISS_TTL_SECS
        if age > ttl:
            return None

        if isinstance(price, int):
            return price

        return None

    async def set(self, url_key: str, price: Optional[int], ok: bool) -> None:
        await self.ensure_loaded()
        u = _normalize_url_key(url_key)
        if not u:
            return
        self._data[u] = {
            "price": int(price) if isinstance(price, int) else None,
            "ts": _now_ts(),
            "ok": bool(ok),
        }

    async def save(self) -> None:
        await self.ensure_loaded()
        async with self._lock:
            try:
                tmp = self.path.with_suffix(self.path.suffix + ".tmp")
                tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp.replace(self.path)
            except Exception as e:
                if DEBUG:
                    print(f"⚠️ cache save failed: {e}")


_PRICE_CACHE = _PriceCache(CACHE_FILE)


# =======================
# HTTPX client cache
# =======================

class _HttpxClientCache:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._client: Optional[httpx.AsyncClient] = None

    async def get(self) -> httpx.AsyncClient:
        if self._client and not self._client.is_closed:
            return self._client

        async with self._lock:
            if self._client and not self._client.is_closed:
                return self._client

            self._client = httpx.AsyncClient(
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if DEBUG:
                print("✅ httpx client cache started")
            return self._client

    async def shutdown(self) -> None:
        async with self._lock:
            try:
                if self._client and not self._client.is_closed:
                    await self._client.aclose()
            except Exception:
                pass
            self._client = None
            if DEBUG:
                print("🧹 httpx client cache stopped")


_HTTPX_CACHE = _HttpxClientCache()


async def shutdown_httpx():
    """
    Опционально: вызвать при остановке приложения.
    """
    await _HTTPX_CACHE.shutdown()


# =======================
# Парсинг цен: универсальные утилиты
# =======================

_PRICE_DIGITS_RE = re.compile(r"[^\d]")
NUM_WITH_SPACES_RE = re.compile(r"(\d{1,3}(?:[ \u00A0]?\d{3})+|\d{4,})")


def _to_int_price(raw: str) -> Optional[int]:
    raw = (raw or "").replace("\xa0", " ").replace("\u00a0", " ")
    m = NUM_WITH_SPACES_RE.search(raw)
    if not m:
        return None
    digits = _PRICE_DIGITS_RE.sub("", m.group(1))
    return int(digits) if digits.isdigit() else None


def _extract_price_from_ldjson(html: str) -> Optional[int]:
    """
    Частый кейс: цена лежит в <script type="application/ld+json"> (schema.org Product/Offer).
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for s in scripts:
        raw = (s.string or s.get_text() or "").strip()
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        nodes = data if isinstance(data, list) else [data]

        i = 0
        while i < len(nodes):
            node = nodes[i]
            i += 1
            if not isinstance(node, dict):
                continue

            g = node.get("@graph")
            if isinstance(g, list):
                for x in g:
                    if isinstance(x, dict):
                        nodes.append(x)

            offers = node.get("offers")
            if isinstance(offers, dict):
                p = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                ip = _to_int_price(str(p)) if p is not None else None
                if ip:
                    return ip

            if isinstance(offers, list):
                for off in offers:
                    if not isinstance(off, dict):
                        continue
                    p = off.get("price") or off.get("lowPrice") or off.get("highPrice")
                    ip = _to_int_price(str(p)) if p is not None else None
                    if ip:
                        return ip

            p = node.get("price")
            ip = _to_int_price(str(p)) if p is not None else None
            if ip:
                return ip

    return None


# =======================
# Extractors per domain
# =======================

def _extract_price_store77(html: str) -> Optional[int]:
    """
    store77:
    - ld+json offers.price
    - p.price_title_product (главная)
    - meta product:price:amount
    - itemprop=price
    - json "price": 12345
    - fallback '... ₽'
    """
    if not html:
        return None

    p = _extract_price_from_ldjson(html)
    if p:
        return p

    soup = BeautifulSoup(html, "html.parser")

    price_node = soup.select_one("p.price_title_product, p#price_title_product_2.price_title_product")
    if price_node:
        p = _to_int_price(price_node.get_text(" ", strip=True))
        if p:
            return p

    meta = soup.find("meta", attrs={"property": "product:price:amount"})
    if meta and meta.get("content"):
        p = _to_int_price(meta["content"])
        if p:
            return p

    tag = soup.find(attrs={"itemprop": "price"})
    if tag:
        candidate = tag.get("content") or tag.get("value") or tag.get_text(" ", strip=True)
        p = _to_int_price(candidate or "")
        if p:
            return p

    m = re.search(r'"price"\s*:\s*"?(\d{3,})"?', html)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass

    m = re.search(r"(\d{1,3}(?:[ \u00A0]?\d{3})+)\s*₽", html)
    if m:
        p = _to_int_price(m.group(1))
        if p:
            return p

    return None


def _extract_price_cordstore(html: str) -> Optional[int]:
    """
    cordstore:
    <span class="price_value">106&nbsp;900</span>
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    node = soup.select_one("span.price_value")
    if node:
        p = _to_int_price(node.get_text(" ", strip=True))
        if p:
            return p

    p = _extract_price_from_ldjson(html)
    if p:
        return p

    return None


def _extract_price_biggeek(html: str, slug: str = "") -> Optional[int]:
    """
    biggeek:
    - нужный вариант определяется по slug из URL после # (например #esim)
    - самый надёжный источник — .prod-info-price__check-item[data-slug=...][data-price]
    - fallback: общий .prod-info-price[data-price]
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # 0) Если в ссылке есть #slug — берём именно этот вариант
    if slug:
        node = soup.select_one(f'.prod-info-price__check-item[data-slug="{slug}"][data-price]')
        if node:
            dp = (node.get("data-price") or "").strip()
            p = _to_int_price(dp)
            if p:
                return p

    # 1) Итоговая цена страницы (может быть дефолтной)
    top = soup.select_one(".prod-info-price[data-price]")
    if top:
        dp = (top.get("data-price") or "").strip()
        p = _to_int_price(dp)
        if p:
            return p

    # 2) checked variation -> data-variation-id
    checked = soup.select_one('input[name="product_variation_radio"][checked]')
    if checked:
        val = (checked.get("value") or "").strip()  # "34759#69264#"
        m = re.search(r"#(\d+)#", val)
        if m:
            var_id = m.group(1)
            node2 = soup.select_one(f'.prod-info-price__check-item[data-variation-id="{var_id}"][data-price]')
            if node2:
                dp = (node2.get("data-price") or "").strip()
                p = _to_int_price(dp)
                if p:
                    return p

    # 3) первая цена варианта
    node3 = soup.select_one(".prod-info-price__check-item[data-price]")
    if node3:
        dp = (node3.get("data-price") or "").strip()
        p = _to_int_price(dp)
        if p:
            return p

    # 4) видимая цена
    vis = soup.select_one("span.total-prod-price")
    if vis:
        p = _to_int_price(vis.get_text(" ", strip=True))
        if p:
            return p

    # 5) regex последний шанс
    m = re.search(r'class="prod-info-price"[^>]*\sdata-price="(\d{3,})"', html)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass

    return None


def _extract_price_upstore24(html: str) -> Optional[int]:
    """
    upstore24:
    <span class="product-price product-price--sale js-product-price">113 990&nbsp;₽</span>
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    node = soup.select_one(
        "span.product-price.js-product-price, "
        "span.product-price--sale.js-product-price, "
        "span.js-product-price"
    )
    if node:
        p = _to_int_price(node.get_text(" ", strip=True))
        if p:
            return p

    p = _extract_price_from_ldjson(html)
    if p:
        return p

    return None


def _extract_price_appmistore(html: str) -> Optional[int]:
    """
    appmistore:
    <span class="price__new-val font_24">129&nbsp;600 ₽<meta itemprop="price" content="129600">...</span>
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    node = soup.select_one("span.price__new-val")
    if node:
        # иногда meta внутри
        meta = node.select_one('meta[itemprop="price"]')
        if meta and meta.get("content"):
            p = _to_int_price(meta["content"])
            if p:
                return p

        p = _to_int_price(node.get_text(" ", strip=True))
        if p:
            return p

    # общий itemprop=price
    tag = soup.find(attrs={"itemprop": "price"})
    if tag:
        candidate = tag.get("content") or tag.get("value") or tag.get_text(" ", strip=True)
        p = _to_int_price(candidate or "")
        if p:
            return p

    p = _extract_price_from_ldjson(html)
    if p:
        return p

    return None


def _extract_price_alikson(html: str) -> Optional[int]:
    """
    alikson:
    <span class="product-card-price__cost--discount">86&nbsp;943 ₽ </span>
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    node = soup.select_one("span.product-card-price__cost--discount")
    if node:
        p = _to_int_price(node.get_text(" ", strip=True))
        if p:
            return p

    p = _extract_price_from_ldjson(html)
    if p:
        return p

    return None


def _is_antibot_stub_store77(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    return (
        'meta name="robots" content="noindex, noarchive"' in low
        or "created with ajaxload.info" in low
        or ("<title></title>" in low.replace(" ", ""))
    )


# =======================
# Playwright: кэш одного браузера
# =======================

class _PlaywrightBrowserCache:
    """
    Один chromium + один context на процесс.
    На каждый запрос создаём новую страницу.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._started = False
        self._p = None
        self._browser = None
        self._context = None

    async def ensure_started(self) -> bool:
        if self._started and self._browser and self._context:
            return True

        async with self._lock:
            if self._started and self._browser and self._context:
                return True

            try:
                from playwright.async_api import async_playwright
            except Exception as e:
                if DEBUG:
                    print(f"⚠️ Playwright not installed: {e}")
                return False

            try:
                self._p = await async_playwright().start()
                self._browser = await self._p.chromium.launch(headless=True)
                self._context = await self._browser.new_context(
                    locale="ru-RU",
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                    viewport={"width": 1365, "height": 768},
                )

                # не грузим тяжелое
                async def _route(route):
                    rtype = route.request.resource_type
                    if rtype in ("image", "media", "font"):
                        await route.abort()
                    else:
                        await route.continue_()

                await self._context.route("**/*", _route)

                self._started = True
                if DEBUG:
                    print("✅ Playwright browser cache started")
                return True
            except Exception as e:
                if DEBUG:
                    print(f"⚠️ Playwright start failed: {e}")
                await self.shutdown()
                return False

    async def fetch_html(self, url: str, wait_selector: Optional[str] = None) -> Optional[str]:
        ok = await self.ensure_started()
        if not ok or not self._context:
            return None

        async with _PLAYWRIGHT_SEM:
            page = await self._context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

                # пробуем дождаться “тишины” сети
                try:
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                except Exception:
                    pass

                if wait_selector:
                    try:
                        await page.wait_for_selector(wait_selector, timeout=25_000)
                    except Exception:
                        pass

                # берём HTML с ретраями (фикс “page is navigating”)
                html = None
                for _ in range(10):
                    try:
                        html = await page.content()
                        break
                    except Exception:
                        await page.wait_for_timeout(700)

                return html
            finally:
                try:
                    await page.close()
                except Exception:
                    pass

    async def shutdown(self) -> None:
        async with self._lock:
            try:
                if self._context:
                    await self._context.close()
            except Exception:
                pass
            try:
                if self._browser:
                    await self._browser.close()
            except Exception:
                pass
            try:
                if self._p:
                    await self._p.stop()
            except Exception:
                pass
            self._p = None
            self._browser = None
            self._context = None
            self._started = False
            if DEBUG:
                print("🧹 Playwright browser cache stopped")


_PW_CACHE = _PlaywrightBrowserCache()


async def shutdown_playwright():
    """
    Опционально: вызвать при остановке приложения (graceful shutdown).
    """
    await _PW_CACHE.shutdown()


# =======================
# Fetch price (with caches)
# =======================

def _request_headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }


async def fetch_price(url: str, client: httpx.AsyncClient) -> Optional[int]:
    """
    ВАЖНО:
    - кэшируем по URL С #fragment (key_url), чтобы не путать варианты BigGeek
    - HTTP-запрос делаем по URL БЕЗ #fragment (req_url)
    """
    raw_url = (url or "").strip()
    if not _has_http(raw_url):
        return None

    key_url = _normalize_url_key(raw_url)   # С # для кэша
    req_url = _request_url(raw_url)         # Без # для запроса
    slug = _get_fragment(raw_url)           # esim / nano-sim-i-esim / ...

    # 0) cache hit
    cached = await _PRICE_CACHE.get(key_url)
    if isinstance(cached, int):
        if DEBUG:
            print(f"⚡ cache hit: {key_url} -> {cached}")
        return cached

    try:
        r = await client.get(req_url, headers=_request_headers())
        if r.status_code != 200:
            if DEBUG:
                print(f"⚠️ fetch_price: HTTP {r.status_code} url={req_url} final={str(r.url)}")
            await _PRICE_CACHE.set(key_url, None, ok=False)
            return None

        html = r.text
        final_url = str(r.url)
        low = final_url.lower()

        price: Optional[int] = None

        # ---------- Store77: httpx -> playwright fallback ----------
        if "store77.net" in low:
            price = _extract_price_store77(html)
            if price:
                await _PRICE_CACHE.set(key_url, price, ok=True)
                return price

            is_stub = _is_antibot_stub_store77(html)
            if DEBUG:
                print(f"⚠️ store77: parsed=None stub={is_stub} url={final_url} len={len(html)}")

            if DEBUG_SAVE:
                try:
                    pth = MODULE_DIR / "_debug_store77_httpx.html"
                    pth.write_text(html, encoding="utf-8", errors="ignore")
                    if DEBUG:
                        print(f"🧾 saved: {pth}")
                except Exception:
                    pass

            if USE_PLAYWRIGHT and is_stub:
                if DEBUG:
                    print(f"🧠 store77: trying playwright (cached browser) for {final_url}")

                wait_sel = "p.price_title_product, p#price_title_product_2.price_title_product"
                html2 = await _PW_CACHE.fetch_html(final_url, wait_selector=wait_sel)
                if not html2:
                    await _PRICE_CACHE.set(key_url, None, ok=False)
                    return None

                if DEBUG_SAVE:
                    try:
                        pth2 = MODULE_DIR / "_debug_store77_playwright.html"
                        pth2.write_text(html2, encoding="utf-8", errors="ignore")
                        if DEBUG:
                            print(f"🧾 saved: {pth2}")
                    except Exception:
                        pass

                price2 = _extract_price_store77(html2)
                if DEBUG:
                    print(f"✅ store77: playwright parsed={price2} url={final_url}")

                await _PRICE_CACHE.set(key_url, price2, ok=bool(price2))
                return price2

            await _PRICE_CACHE.set(key_url, None, ok=False)
            return None

        # ---------- Cordstore ----------
        if "cordstore.ru" in low:
            price = _extract_price_cordstore(html)
            await _PRICE_CACHE.set(key_url, price, ok=bool(price))
            return price

        # ---------- BigGeek ----------
        if "biggeek.ru" in low:
            price = _extract_price_biggeek(html, slug=slug)
            if DEBUG and not price:
                snippet = re.sub(r"\s+", " ", html[:900])
                print(f"⚠️ biggeek price not found: slug={slug} req={req_url} final={final_url} len={len(html)} snippet={snippet}")
            await _PRICE_CACHE.set(key_url, price, ok=bool(price))
            return price

        # ---------- Upstore24 ----------
        if "upstore24.ru" in low:
            price = _extract_price_upstore24(html)
            await _PRICE_CACHE.set(key_url, price, ok=bool(price))
            return price

        # ---------- Appmistore ----------
        if "appmistore.ru" in low:
            price = _extract_price_appmistore(html)
            await _PRICE_CACHE.set(key_url, price, ok=bool(price))
            return price

        # ---------- Alikson ----------
        if "alikson.ru" in low:
            price = _extract_price_alikson(html)
            await _PRICE_CACHE.set(key_url, price, ok=bool(price))
            return price

        # неизвестный домен
        if DEBUG:
            print(f"⚠️ unsupported domain: {final_url}")
        await _PRICE_CACHE.set(key_url, None, ok=False)
        return None

    except Exception as e:
        if DEBUG:
            print(f"⚠️ fetch_price error: url={raw_url} err={e}")
        await _PRICE_CACHE.set(key_url, None, ok=False)
        return None


# =======================
# Обновление таблицы
# =======================

@dataclass(frozen=True)
class CellPair:
    link_col: str
    price_col: str


def _read_env_pairs() -> Tuple[CellPair, ...]:
    """
    Можно переопределить пары через env COMP_COL_PAIRS:
      COMP_COL_PAIRS="AG:AH,AI:AJ,AM:AN,AO:AP,AQ:AR"
    """
    raw = (os.getenv("COMP_COL_PAIRS") or "").strip()
    if not raw:
        return tuple(CellPair(a, b) for a, b in PAIR_DEFAULT)

    out: List[CellPair] = []
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        a, b = part.split(":", 1)
        a, b = a.strip().upper(), b.strip().upper()
        if a and b:
            out.append(CellPair(a, b))

    return tuple(out) if out else tuple(CellPair(a, b) for a, b in PAIR_DEFAULT)


async def update_competitor_prices_once(
    sheet_url: str = SHEET_URL_DEFAULT,
    worksheet_name: str = WORKSHEET_NAME_DEFAULT,
    start_row: int = START_ROW_DEFAULT,
) -> int:
    """
    Быстрый проход:
    - batch_get только колонок ссылок (+ колонок цен для сравнения)
    - парсим цены с кэшем URL
    - batch_update только если цена изменилась
    - сохраняем кэш на диск
    """
    gs = get_gs_client()
    sh = gs.open_by_key(parse_sheet_id(sheet_url))
    ws = sh.worksheet(worksheet_name)

    pairs = _read_env_pairs()
    if not pairs:
        return 0

    # какие колонки читать
    link_cols = list(dict.fromkeys([p.link_col for p in pairs]))
    price_cols = list(dict.fromkeys([p.price_col for p in pairs]))

    # диапазон строк
    max_rows = int(os.getenv("COMP_MAX_ROWS", "0") or "0")  # если 0 — читаем до ws.row_count
    sheet_last_row = ws.row_count
    end_row = sheet_last_row
    if max_rows > 0:
        end_row = min(end_row, start_row + max_rows - 1)

    # читаем пачкой: ссылки + текущие цены (чтобы не писать лишнее)
    link_ranges = [_col_range(c, start_row, end_row) for c in link_cols]
    price_ranges = [_col_range(c, start_row, end_row) for c in price_cols]
    batch_ranges = link_ranges + price_ranges

    batch_data = ws.batch_get(batch_ranges)

    link_data = batch_data[:len(link_ranges)]
    price_data = batch_data[len(link_ranges):]

    col_to_link_vals: Dict[str, list[list[str]]] = {c: (vals or []) for c, vals in zip(link_cols, link_data)}
    col_to_price_vals: Dict[str, list[list[str]]] = {c: (vals or []) for c, vals in zip(price_cols, price_data)}

    max_len = max(
        [*(len(v) for v in col_to_link_vals.values()), *(len(v) for v in col_to_price_vals.values())],
        default=0,
    )
    if max_len <= 0:
        return 0

    # последняя строка, где есть хоть одна ссылка
    last_idx = -1
    for i in range(max_len - 1, -1, -1):
        any_link = False
        for p in pairs:
            raw = _safe_cell(col_to_link_vals.get(p.link_col, []), i)
            if _has_http(raw):
                any_link = True
                break
        if any_link:
            last_idx = i
            break

    if last_idx < 0:
        return 0

    rows_count = last_idx + 1
    real_end_row = start_row + rows_count - 1

    if DEBUG:
        print(f"✅ competitors: rows {start_row}..{real_end_row} (count={rows_count}) pairs={[(p.link_col,p.price_col) for p in pairs]}")

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    client = await _HTTPX_CACHE.get()

    async def process_row(i: int) -> Dict[str, Optional[int]]:
        async with sem:
            res: Dict[str, Optional[int]] = {}
            for p in pairs:
                raw = _safe_cell(col_to_link_vals.get(p.link_col, []), i)
                if not _has_http(raw):
                    res[p.price_col] = None
                    continue
                # Передаём С #fragment — fetch_price сам решит:
                # - кэш по url с #
                # - запрос по url без #
                res[p.price_col] = await fetch_price(raw, client)
            return res

    tasks = [asyncio.create_task(process_row(i)) for i in range(rows_count)]
    results = await asyncio.gather(*tasks)

    updates = []
    updated_cells = 0

    # сравнение с текущими значениями цены (не апдейтим, если одинаково)
    for i, row_res in enumerate(results):
        row_num = start_row + i
        for price_col, new_price in row_res.items():
            if new_price is None:
                continue

            old_raw = _safe_cell(col_to_price_vals.get(price_col, []), i)
            old_price = _to_int_price(old_raw)

            if old_price == new_price:
                continue

            updates.append({"range": a1(price_col, row_num), "values": [[str(new_price)]]})
            updated_cells += 1

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    # сохраняем price cache на диск
    await _PRICE_CACHE.save()

    if DEBUG:
        print(f"✅ competitors: updated_cells={updated_cells}")

    return updated_cells


# =======================
# Ручной запуск (для кнопки)
# =======================

async def competitor_prices_run_once() -> int:
    """
    Ручной запуск (1 проход) с настройками из ENV.
    Возвращает: сколько ячеек обновили.
    """
    sheet_url = os.getenv("COMP_SHEET_URL", SHEET_URL_DEFAULT)
    worksheet = os.getenv("COMP_WORKSHEET", WORKSHEET_NAME_DEFAULT)
    start_row = int(os.getenv("COMP_START_ROW", str(START_ROW_DEFAULT)))

    return await update_competitor_prices_once(
        sheet_url=sheet_url,
        worksheet_name=worksheet,
        start_row=start_row,
    )


# =======================
# Планировщик: каждый день 12:00 МСК
# =======================

def _seconds_until_msk(hour: int, minute: int) -> int:
    now = datetime.now(MSK_TZ)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return max(1, int((target - now).total_seconds()))


async def competitor_prices_daily_job():
    """
    Фоновая задача: каждый день в 12:00 МСК обновляем цены.

    ENV:
      COMP_SHEET_URL="..."
      COMP_WORKSHEET="Прайс"
      COMP_START_ROW="2"
      COMP_COL_PAIRS="AG:AH,AI:AJ,AM:AN,AO:AP,AQ:AR"
      GOOGLE_SERVICE_ACCOUNT_FILE="/path/to/google_service_account.json"

      COMP_DEBUG="1"
      COMP_DEBUG_SAVE="1"
      COMP_USE_PLAYWRIGHT="1"
      COMP_PW_CONCURRENCY="2"

      COMP_CONCURRENCY="20"
      COMP_HTTP_TIMEOUT="20"
      COMP_MAX_ROWS="2000"            # опционально

      COMP_CACHE_TTL_SECS="43200"     # успех (12ч)
      COMP_CACHE_MISS_TTL_SECS="900"  # не нашли (15м)
      COMP_CACHE_FILE=".../cache.json"
    """
    while True:
        try:
            await asyncio.sleep(_seconds_until_msk(12, 0))
            await competitor_prices_run_once()
        except Exception:
            await asyncio.sleep(60)
