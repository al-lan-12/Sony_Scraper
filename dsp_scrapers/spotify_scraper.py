# dsp_scrapers/spotify_scraper.py
# ----------------------------------------------------
# Spotify Premium scraper (Playwright)
# Integrated for Streamlit app:
#   - no Colab-only code
#   - provides run_spotify_scraper(test_mode, test_countries) -> xlsx path
#   - keeps your "known good" Colab structure (market discovery + concurrency)
#   - adds robustness so FR doesn't become Price=1 (from "1 mois")
# ----------------------------------------------------

from __future__ import annotations

import asyncio
import functools
import re
from datetime import date
from difflib import get_close_matches
from typing import Optional, List, Tuple

import pandas as pd
import pycountry
from babel.numbers import get_territory_currencies
from playwright.async_api import async_playwright

# tqdm is optional (nice for logs). If not available, we fall back quietly.
try:
    from tqdm.auto import tqdm  # type: ignore
except Exception:  # pragma: no cover
    tqdm = None  # type: ignore

# googletrans is optional (pages can be parsed without it with our robust fallback).
try:
    from googletrans import Translator  # type: ignore
except Exception:  # pragma: no cover
    Translator = None  # type: ignore


# ---------- Config ----------
STANDARD_PLAN_NAMES = [
    "Platinum",
    "Lite",
    "Individual",
    "Student",
    "Family",
    "Duo",
    "Audiobooks",
    "Basic",
    "Mini",
    "Standard",
]

MAX_CONCURRENCY = 3
HEADLESS = True

# Default test values (used only if app calls test_mode=True and provides no countries)
TEST_MODE = False
TEST_MARKETS = ["bg"]  # 2-letter markets like ['fr','de','bg']


# ---------- Utilities ----------
def log(msg: str) -> None:
    print(msg, flush=True)


translator = Translator() if Translator else None


@functools.lru_cache(maxsize=1024)
def translate_text_cached(text: str) -> str:
    """
    Best-effort translation to English for heuristics.
    If translation fails/unavailable, return original lowercased.
    """
    t = (text or "").strip()
    if not t:
        return ""
    if translator is None:
        return t.lower()
    try:
        return translator.translate(t, dest="en").text.lower()
    except Exception:
        return t.lower()


def _clean_spaces(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()


def normalize_plan_name(name: str) -> str:
    raw = (name or "").strip().lower()

    # Manual overrides
    if re.search(r"\b(personal|personnel|staff)\b", raw):
        return "Individual"

    # 1) Direct substring match
    for std in STANDARD_PLAN_NAMES:
        if std.lower() in raw:
            return std

    # 2) Try translated version
    translated = translate_text_cached(raw)
    for std in STANDARD_PLAN_NAMES:
        if std.lower() in translated:
            return std

    # 3) Token-based exact matching
    tokens = re.findall(r"[a-z]+", raw)
    for token in tokens:
        for std in STANDARD_PLAN_NAMES:
            if token == std.lower():
                return std

    # 4) Fuzzy matching fallback
    match = get_close_matches(
        translated,
        [n.lower() for n in STANDARD_PLAN_NAMES],
        n=1,
        cutoff=0.6,
    )
    if match:
        return match[0].capitalize()

    return "Other"


def is_generic_trial(text: str) -> bool:
    text = (text or "").strip()
    if not text:
        return False
    translated = translate_text_cached(text)
    PROMO = [
        "go premium",
        "control of your music",
        "cancel anytime",
        "no commitment",
        "listen on your phone",
        "pay different ways",
        "no ads",
        "full control",
        "annulez à tout moment",
        "enjoy music",
    ]
    return sum(p in translated for p in PROMO) > 1


# ---- Strong tokens (explicit, unambiguous) ----
STRONG_TOKENS: List[Tuple[str, str]] = [
    # Explicit US dollar markers
    (r"(?i)US\$", "USD"),
    (r"(?i)\$US", "USD"),
    (r"(?i)U\$S", "USD"),
    # Other $-based symbols with prefixes
    (r"(?i)\bA\$", "AUD"),
    (r"(?i)\bNZ\$", "NZD"),
    (r"(?i)\bHK\$", "HKD"),
    (r"(?i)\bNT\$", "TWD"),
    (r"(?i)\bS\$", "SGD"),
    (r"(?i)\bRD\$", "DOP"),
    (r"(?i)\bN\$", "NAD"),
    # Latin America / others
    (r"R\$", "BRL"),
    (r"S/\.", "PEN"),
    (r"S/", "PEN"),
    (r"Bs\.?", "BOB"),
    (r"Gs\.?", "PYG"),
    (r"₲", "PYG"),
    (r"Q(?=[\s\d])", "GTQ"),
    # Single-char symbols
    (r"€", "EUR"),
    (r"£", "GBP"),
    (r"¥", "JPY"),
    (r"₹", "INR"),
    (r"₩", "KRW"),
    (r"₫", "VND"),
    (r"₺", "TRY"),
    (r"₪", "ILS"),
    (r"₴", "UAH"),
    (r"₼", "AZN"),
    (r"₾", "GEL"),
    (r"₭", "LAK"),
    (r"฿", "THB"),
    (r"₦", "NGN"),
    (r"₵", "GHS"),
    (r"KSh", "KES"),
    (r"TSh", "TZS"),
    (r"USh", "UGX"),
    (r"Rp", "IDR"),
    (r"zł", "PLN"),
    (r"Kč", "CZK"),
    (r"Ft", "HUF"),
    (r"lei", "RON"),
    (r"лв", "BGN"),
    (r"ден", "MKD"),
    (r"RM", "MYR"),
    (r"₱", "PHP"),
]

# ---- Ambiguous tokens → resolve by territory default ----
AMBIG_TOKENS = {
    r"\$": {
        "USD",
        "MXN",
        "ARS",
        "CLP",
        "COP",
        "CAD",
        "AUD",
        "NZD",
        "SGD",
        "HKD",
        "TWD",
        "UYU",
        "BBD",
        "BSD",
        "DOP",
        "CRC",
        "PAB",
        "HNL",
        "JMD",
    },
    r"\bkr\.?\b": {"SEK", "NOK", "DKK", "ISK"},
    r"\bRs\.?\b": {"INR", "PKR", "LKR", "NPR"},
    r"₨": {"INR", "PKR", "LKR", "NPR"},
    r"(?i)\bC\$\b": {"CAD", "NIO"},
    r"\bR(?=[\s\d])": {"ZAR"},
}

# ---- Last-resort country → currency map (covers Spotify markets) ----
HARDCODE_FALLBACKS = {
    # Americas + dollarised
    "US": "USD",
    "CA": "CAD",
    "MX": "MXN",
    "BR": "BRL",
    "AR": "ARS",
    "CL": "CLP",
    "CO": "COP",
    "PE": "PEN",
    "UY": "UYU",
    "PY": "PYG",
    "BO": "BOB",
    "NI": "NIO",
    "GT": "GTQ",
    "CR": "CRC",
    "PA": "PAB",
    "HN": "HNL",
    "DO": "DOP",
    "JM": "JMD",
    "BB": "BBD",
    "BS": "BSD",
    "BZ": "BZD",
    "EC": "USD",
    "SV": "USD",
    # Europe
    "GB": "GBP",
    "IE": "EUR",
    "FR": "EUR",
    "DE": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "PT": "EUR",
    "NL": "EUR",
    "BE": "EUR",
    "LU": "EUR",
    "AT": "EUR",
    "FI": "EUR",
    "EE": "EUR",
    "LV": "EUR",
    "LT": "EUR",
    "SK": "EUR",
    "SI": "EUR",
    "GR": "EUR",
    "CY": "EUR",
    "MT": "EUR",
    "BG": "BGN",
    "RO": "RON",
    "PL": "PLN",
    "CZ": "CZK",
    "HU": "HUF",
    "HR": "EUR",
    "DK": "DKK",
    "SE": "SEK",
    "NO": "NOK",
    "IS": "ISK",
    "CH": "CHF",
    "RS": "RSD",
    "BA": "BAM",
    "MK": "MKD",
    "AL": "ALL",
    "UA": "UAH",
    "GE": "GEL",
    "AZ": "AZN",
    "AM": "AMD",
    "KZ": "KZT",
    "MD": "MDL",
    "BY": "BYN",
    "TR": "TRY",
    # MENA
    "AE": "AED",
    "SA": "SAR",
    "QA": "QAR",
    "KW": "KWD",
    "BH": "BHD",
    "OM": "OMR",
    "IL": "ILS",
    "EG": "EGP",
    "MA": "MAD",
    "TN": "TND",
    "DZ": "DZD",
    "IQ": "IQD",
    # Africa
    "ZA": "ZAR",
    "NG": "NGN",
    "GH": "GHS",
    "KE": "KES",
    "TZ": "TZS",
    "UG": "UGX",
    "CM": "XAF",
    "CI": "XOF",
    "SN": "XOF",
    "RW": "RWF",
    "BI": "BIF",
    "CD": "CDF",
    # APAC & Pacific
    "JP": "JPY",
    "KR": "KRW",
    "CN": "CNY",
    "TW": "TWD",
    "HK": "HKD",
    "SG": "SGD",
    "MY": "MYR",
    "TH": "THB",
    "VN": "VND",
    "PH": "PHP",
    "ID": "IDR",
    "IN": "INR",
    "PK": "PKR",
    "LK": "LKR",
    "NP": "NPR",
    "BD": "BDT",
    "AU": "AUD",
    "NZ": "NZD",
    "KI": "AUD",
    "NR": "AUD",
    "TV": "AUD",
    "MH": "USD",
}

KNOWN_ISO = set(HARDCODE_FALLBACKS.values()) | {
    "EUR",
    "USD",
    "GBP",
    "AUD",
    "CAD",
    "NZD",
    "SGD",
    "HKD",
    "TWD",
    "MXN",
    "ARS",
    "CLP",
    "COP",
    "PEN",
    "BOB",
    "NIO",
    "GTQ",
    "PYG",
    "UYU",
    "ZAR",
    "NAD",
    "CHF",
    "NOK",
    "SEK",
    "DKK",
    "PLN",
    "CZK",
    "HUF",
    "RON",
    "BGN",
    "RSD",
    "BAM",
    "MKD",
    "ALL",
    "GEL",
    "AMD",
    "AZN",
    "UAH",
    "KZT",
    "MDL",
    "BYN",
    "TRY",
    "ILS",
    "AED",
    "SAR",
    "QAR",
    "KWD",
    "BHD",
    "OMR",
    "PKR",
    "LKR",
    "NPR",
    "INR",
    "BDT",
    "VND",
    "THB",
    "MYR",
    "IDR",
    "PHP",
    "LAK",
    "CNY",
    "JPY",
    "KRW",
    "EGP",
    "MAD",
    "TND",
    "DZD",
    "XAF",
    "XOF",
    "CDF",
    "RWF",
    "BIF",
}


def default_currency_for_alpha2(alpha2: str) -> str:
    """Babel first, then hardcoded fallback, then empty string."""
    iso2 = (alpha2 or "").upper()
    try:
        currs = get_territory_currencies(iso2, date=date.today(), non_tender=False)
        if currs:
            return currs[0]
    except Exception:
        pass
    return HARDCODE_FALLBACKS.get(iso2, "")


def detect_currency_in_text(text: str, alpha2: str) -> Tuple[str, str]:
    """
    Find the currency used in a line of text.
    Returns (currency_iso, source_tag).
    """
    s = _clean_spaces(text)
    if not s:
        return "", "territory_default"

    # 1) Strong symbol / token
    for pat, iso in STRONG_TOKENS:
        if re.search(pat, s):
            return iso, "symbol"

    # 2) 3-letter ISO codes, near a number
    S = s.upper()
    for m in re.finditer(r"\b([A-Z]{3})\b", S):
        code = m.group(1)
        if code not in KNOWN_ISO:
            continue
        a, b = m.span()
        window = S[max(0, a - 6) : min(len(S), b + 6)]
        if re.search(r"\d", window):
            return code, "code"

    # 3) Ambiguous tokens → country default
    for pat in AMBIG_TOKENS.keys():
        if re.search(pat, s):
            d = default_currency_for_alpha2(alpha2)
            return (d or ""), "ambiguous->default"

    # 4) Territory default fallback
    return default_currency_for_alpha2(alpha2), "territory_default"


def _normalize_number(p: str) -> str:
    p = (p or "").replace(" ", "")
    dm = re.search(r"([.,])(\d{1,2})$", p)
    if dm:
        frac = dm.group(2)
        base = p[: -len(dm.group(0))].replace(".", "").replace(",", "")
        try:
            return str(float(base + "." + frac))
        except Exception:
            return ""
    try:
        return str(float(p.replace(".", "").replace(",", "")))
    except Exception:
        return ""


# ------------ PRICE PARSING (robust for FR) ------------
def extract_amount_number(text: str) -> str:
    """
    Return the monetary number in a line, preferring the number closest to a currency sign/code.

    Critical robustness:
    - Handles number BEFORE currency: "10,99 €" and "0 € pour 1 mois"
    - Prevents "1 mois" from becoming the extracted price
    """
    if not isinstance(text, str) or not text.strip():
        return ""
    t = _clean_spaces(text)
    tr = translate_text_cached(t)

    # (A) Prefer number immediately BEFORE a strong currency token (EU-style: "10,99 €")
    for pat, _iso in STRONG_TOKENS:
        for m in re.finditer(pat, t):
            before = t[: m.start()].rstrip()
            nb = re.search(r"(\d+(?:[.,]\d+)?)\s*$", before)
            if nb:
                return _normalize_number(nb.group(1))

    # (B) Prefer number immediately AFTER a strong currency token (US-style: "$10.99")
    for pat, _iso in STRONG_TOKENS:
        m = re.search(pat, t)
        if m:
            after = t[m.end() :]
            n = re.search(r"\d+(?:[.,]\d+)?", after)
            if n:
                return _normalize_number(n.group(0))

    # (C) ISO codes around numbers
    S = t.upper()
    m = re.search(r"\b([A-Z]{3})\b\s*(\d+(?:[.,]\d+)?)", S)
    if m and m.group(1) in KNOWN_ISO:
        return _normalize_number(m.group(2))
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*\b([A-Z]{3})\b", S)
    if m and m.group(2) in KNOWN_ISO:
        return _normalize_number(m.group(1))

    # (D) Original fallback heuristics (kept from your script)
    m = re.search(
        r"(?:US\$|[€£¥₩₫₺₪₴₼₾₭฿₦₵₱]|NT\$|HK\$|S/\.|S/|R\$|RD\$|N\$|KSh|TSh|USh)\s*\d+(?:[.,]\d+)?",
        tr,
        re.I,
    )
    if not m:
        m = re.search(
            r"\b(?:USD|EUR|GBP|AUD|CAD|NZD|SGD|HKD|TWD|MXN|ARS|CLP|COP|PEN|BOB|NIO|GTQ|PYG|UYU|BRL|ZAR|NAD|CHF|NOK|SEK|DKK|PLN|CZK|HUF|RON|BGN|RSD|BAM|MKD|TRY|ILS|AED|SAR|QAR|KWD|BHD|OMR|INR|PKR|LKR|NPR|MYR|IDR|PHP|VND|THB|KRW|JPY|CNY)\s*\d+(?:[.,]\d+)?",
            tr,
            re.I,
        )
    if m:
        token = m.group(0)
        n = re.search(r"(?<!\d)(\d+(?:[.,]\d+)?)", token)
        if n:
            return _normalize_number(n.group(1))

    m = re.search(
        r"(?:after|then|per\s+month|monthly|month)\D{0,12}(\d+(?:[.,]\d+)?)",
        tr,
        re.I,
    )
    if m and not re.search(r"hour|hours|hr|hrs|minute|min", m.group(0), re.I):
        return _normalize_number(m.group(1))

    if is_generic_trial(t):
        m2 = re.search(
            r"(?:for\s+1\s+month|trial|free\s+for\s+\d+\s+month(?:s)?)\D{0,12}(\d+(?:[.,]\d+)?)",
            tr,
            re.I,
        )
        if m2 and not re.search(r"hour|hours|hr|hrs|minute|min", m2.group(0), re.I):
            return _normalize_number(m2.group(1))

    candidates = []
    for m in re.finditer(r"\d+(?:[.,]\d+)?", tr):
        num = m.group(0)
        end = m.span()[1]
        tail = tr[end : end + 8]
        if re.search(r"^\s*[/-]?\s*(?:hour|hours|hr|hrs|minute|min)\b", tail, re.I):
            continue
        candidates.append(num)
    if candidates:
        return _normalize_number(candidates[-1])
    return ""


# ---------- Price-line chooser ----------
MONTHY_RE = re.compile(r"(?:/ ?month|\bper month\b|\ba month\b|\bmonthly\b)", re.I)
AFTER_RE = re.compile(r"\b(after|thereafter|then|month after)\b", re.I)
FOR_N_MONTHS_RE = re.compile(r"\bfor\s+\d+\s+month", re.I)


def looks_monthly_en(s_en: str) -> bool:
    return bool(MONTHY_RE.search(s_en))


def choose_price_line(p_texts: List[str], alpha2: str) -> str:
    """Pick the most reliable price line from the <p> lines in a card."""
    lines = [(_clean_spaces(x), translate_text_cached(_clean_spaces(x))) for x in (p_texts or []) if x and x.strip()]
    if not lines:
        return ""

    # 1) Monthly + 'after/thereafter' (real monthly after promo)
    for raw, en in lines[:4]:
        if looks_monthly_en(en) and AFTER_RE.search(en):
            return raw

    # 2) Monthly but NOT 'for N months' (avoid promo)
    for raw, en in lines[:4]:
        if looks_monthly_en(en) and not FOR_N_MONTHS_RE.search(en):
            return raw

    # 3) Robust fallback: among lines with currency+digits, pick the highest extracted amount.
    # This prevents selecting "0 € pour 1 mois" over "Puis 10,99 €/mois".
    scored: List[Tuple[float, str]] = []
    for raw, _en in lines[:4]:
        cur, _src = detect_currency_in_text(raw, alpha2)
        if not cur or not re.search(r"\d", raw):
            continue
        amt_s = extract_amount_number(raw)
        try:
            amt = float(amt_s) if amt_s else None
        except Exception:
            amt = None
        if amt is not None:
            scored.append((amt, raw))

    if scored:
        positives = [x for x in scored if x[0] > 0]
        pool = positives if positives else scored
        pool.sort(key=lambda x: x[0], reverse=True)
        return pool[0][1]

    # 4) Final fallback: first <p>
    return lines[0][0]


def pick_after_line(p_texts: List[str]) -> str:
    for pt in p_texts[:4]:
        en = translate_text_cached(_clean_spaces(pt))
        if looks_monthly_en(en) and AFTER_RE.search(en):
            return pt
    return ""


def get_country_info(locale_code: str) -> Tuple[str, str, str]:
    base = (locale_code or "").split("-")[0]
    try:
        c = pycountry.countries.lookup(base)
        return c.name, c.alpha_2, c.alpha_3
    except Exception:
        return "Unknown", base.upper(), base.upper()


def detect_currency_from_hints(texts: List[str], alpha2: str) -> Tuple[str, str]:
    for t in texts:
        if not t:
            continue
        cur, src = detect_currency_in_text(t, alpha2)
        if cur:
            return cur, src
    return default_currency_for_alpha2(alpha2), "territory_default"


# ---------- Playwright helpers ----------
async def new_context(playwright):
    browser = await playwright.chromium.launch(
        headless=HEADLESS,
        args=[
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-extensions",
        ],
    )
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="UTC",
        ignore_https_errors=True,
    )

    async def route_block(route):
        if route.request.resource_type in {"image", "media", "font"}:
            await route.abort()
        else:
            await route.continue_()

    await ctx.route("**/*", route_block)
    await ctx.add_cookies([{"name": "sp_lang", "value": "en", "domain": ".spotify.com", "path": "/"}])
    return browser, ctx


async def safe_goto(page, url: str, timeout: int = 60000) -> bool:
    for i in range(3):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return True
        except Exception:
            if i == 2:
                return False
            await asyncio.sleep(1.0 + i * 0.6)
    return False


# ---------- Market discovery ----------
async def fetch_markets(playwright) -> List[str]:
    """
    Return a list of locale codes from Spotify's directory
    (e.g. ['fr', 'de', 'br-en']).
    Prefers '-en' where available.
    """
    browser, ctx = await new_context(playwright)
    page = await ctx.new_page()
    ok = await safe_goto(page, "https://www.spotify.com/select-your-country-region/", timeout=70000)

    result: List[str] = []
    if ok:
        links = await page.eval_on_selector_all(
            "a[href^='/']:not([href*='help']):not([href='#'])",
            "els => els.map(a => a.getAttribute('href'))",
        )
        base_choice = {}
        for href in links or []:
            if not href:
                continue
            code = href.strip("/").split("/")[0]
            if not re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", code):
                continue
            base = code.split("-")[0]
            if base not in base_choice or code.endswith("-en"):
                base_choice[base] = code
        result = list(base_choice.values())

    try:
        await browser.close()
    except Exception:
        pass
    return result


# ---------- Scrape one market ----------
async def scrape_country(locale: str, playwright, semaphore: asyncio.Semaphore) -> List[dict]:
    async with semaphore:
        browser, ctx = await new_context(playwright)
        page = await ctx.new_page()
        url = f"https://www.spotify.com/{locale}/premium/"

        cname, a2, a3 = get_country_info(locale)

        plans: List[dict] = []
        ok = await safe_goto(page, url, timeout=70000)
        if ok:
            await page.wait_for_timeout(1200)
            cards = await page.query_selector_all("section:has(h3), div:has(h3), article:has(h3)")

            seen = set()
            for card in cards:
                try:
                    h3 = await card.query_selector("h3")
                    title = await (h3.inner_text() if h3 else "Unknown")
                    if not title.strip():
                        continue

                    std = normalize_plan_name(title)
                    if std == "Other":
                        continue

                    title_key = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
                    key = (std, title_key)
                    if key in seen:
                        continue
                    seen.add(key)

                    p_tags = await card.query_selector_all("p")
                    p_texts: List[str] = []
                    for p in p_tags:
                        try:
                            t = await p.inner_text()
                            if t:
                                p_texts.append(t)
                        except Exception:
                            pass

                    # ---------- Smart price picking ----------
                    price_line = choose_price_line(p_texts, a2)
                    amount = extract_amount_number(price_line)

                    currency, _src = detect_currency_in_text(price_line, a2)
                    if not currency:
                        currency, _src = detect_currency_from_hints([price_line, " ".join(p_texts), title], a2)

                    trial = p_texts[0] if p_texts else ""
                    after = pick_after_line(p_texts)

                    if amount:
                        plans.append(
                            {
                                "Country Code": locale,
                                "Country Name (resolved)": cname,
                                "Country Standard Name": cname,
                                "Alpha-2": a2,
                                "Alpha-3": a3,
                                "Plan Name": title,
                                "Standard Plan Name": std,
                                "Trial Info": trial,
                                "Currency": currency,
                                "Price": amount,
                                "Billing Frequency": "month",
                                "Price After Trial": after,
                                "URL": url,
                            }
                        )
                except Exception:
                    pass

        try:
            await browser.close()
        except Exception:
            pass
        return plans


# ---------- Master runner ----------
async def run() -> Optional[str]:
    async with async_playwright() as pw:
        log("🔎 Discovering markets from directory…")
        markets = await fetch_markets(pw)
        if not markets:
            log("❌ Couldn’t resolve markets (Spotify blocked/empty). Re-run shortly.")
            return None

        if TEST_MODE:
            desired = set(TEST_MARKETS)
            desired_bases = {c.split("-")[0] for c in desired}
            picked: List[str] = []
            for loc in markets:
                base = loc.split("-")[0]
                if base in desired_bases and (loc.endswith("-en") or base not in [p.split("-")[0] for p in picked]):
                    picked.append(loc)
            for code in TEST_MARKETS:
                if code not in picked and code.split("-")[0] not in [p.split("-")[0] for p in picked]:
                    picked.append(code)
            markets = picked
            log(f"🧪 Test mode: scraping {len(markets)} markets: {markets}")
        else:
            log(f"✅ Found {len(markets)} markets (English preferred where available).")

        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        tasks = [scrape_country(loc, pw, sem) for loc in markets]

        all_plans: List[dict] = []

        if tqdm is not None:
            pbar = tqdm(total=len(tasks), desc="Scraping /premium pages", unit="market")
            for fut in asyncio.as_completed(tasks):
                res = await fut
                if res:
                    all_plans.extend(res)
                pbar.update(1)
            pbar.close()
        else:
            for fut in asyncio.as_completed(tasks):
                res = await fut
                if res:
                    all_plans.extend(res)

        if not all_plans:
            log("❌ No plan cards scraped.")
            return None

        df = pd.DataFrame(all_plans)
        df["Numerical Price"] = pd.to_numeric(df["Price"], errors="coerce")
        df.sort_values(["Alpha-2", "Standard Plan Name", "Plan Name"], inplace=True, kind="stable")

        desired_columns = [
            "Country Standard Name",
            "Alpha-2",
            "Alpha-3",
            "Country Code",
            "Country Name (resolved)",
            "Standard Plan Name",
            "Plan Name",
            "Trial Info",
            "Currency",
            "Price",
            "Billing Frequency",
            "Price After Trial",
            "URL",
        ]
        df = df[desired_columns]

        df.rename(
            columns={
                "Alpha-2": "Country Alpha-2",
                "Alpha-3": "Country Alpha-3",
                "Country Name (resolved)": "Country Name",
            },
            inplace=True,
        )

        base = f"spotify_cleaned_playwright{'_TEST' if TEST_MODE else ''}"
        xlsx_out = f"{base}.xlsx"

        # Write XLSX
        with pd.ExcelWriter(xlsx_out, engine="openpyxl") as w:
            df.to_excel(w, index=False)

        log(
            f"\n🎉 Done! Saved {xlsx_out} | Rows: {len(df)} "
            f"Countries: {df['Country Alpha-2'].nunique()}"
        )
        return xlsx_out


# -------------------------------------------------------------------
# Streamlit / external entry point
# -------------------------------------------------------------------
async def _run_spotify_async(test_mode: bool, test_countries: Optional[List[str]]) -> Optional[str]:
    global TEST_MODE, TEST_MARKETS
    TEST_MODE = bool(test_mode)

    if TEST_MODE and test_countries:
        TEST_MARKETS = [c.strip().lower() for c in test_countries if c and len(c.strip()) == 2]
        log(f"[SPOTIFY] UI-driven TEST_MARKETS: {TEST_MARKETS}")

    return await run()


def run_spotify_scraper(test_mode: bool = False, test_countries: Optional[List[str]] = None) -> Optional[str]:
    """
    Public function for the Streamlit app.
    Returns the Excel filename (relative to current working dir) or None.
    """
    try:
        return asyncio.run(_run_spotify_async(test_mode=test_mode, test_countries=test_countries))
    except RuntimeError as e:
        # If called from an existing event loop (rare in Streamlit since you call in a thread),
        # fall back to using that loop.
        if "asyncio.run()" not in str(e):
            raise
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_run_spotify_async(test_mode=test_mode, test_countries=test_countries))


# CLI testing
if __name__ == "__main__":
    path = run_spotify_scraper(test_mode=True, test_countries=["fr", "de", "bg", "hk", "in", "id"])
    print("Output written to:", path)
