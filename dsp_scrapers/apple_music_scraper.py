# apple_music_plans_robust.py
# ------------------------------------------------------------
# Apple Music scraper (robust)
# - Picks RECURRING monthly price (not intro/trial):
#     1) prefer tokens whose nearby translated context contains "then/after/thereafter"
#     2) else prefer tokens whose nearby translated context looks monthly (/month, per month, monthly, etc.)
#     3) else choose MAX numeric token (trial is almost always smaller)
# - Fixes Turkey missing by supporting "TL" (TRY) token
# - Fixes Hungary "Ft" currency raw token and supports other letter tokens (Kč, zł, lei, лв, etc.)
# - Robust redirect detection + currency parsing (no 'TRY' false positives)
# - Banner fallback also picks recurring token (not first match)
# ------------------------------------------------------------

import re, time, threading, sqlite3, asyncio
from datetime import datetime, UTC, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from pathlib import Path  # put near the top of the file if not already imported

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag
import pandas as pd
import pycountry
from deep_translator import GoogleTranslator
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from babel.numbers import get_territory_currencies
from functools import lru_cache

# Always write outputs to a fixed base directory (repo root: one level above dsp_scrapers)
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR  # or BASE_DIR / "outputs" if you prefer a subfolder

# =========================== Config ===========================

MAX_WORKERS = 6

SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504]
        )
    ),
)
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122 Safari/537.36"
        ),
        "Accept-Language": "en;q=0.9",
    }
)

# Translation (for language-agnostic heuristics)
translator = GoogleTranslator(source="auto", target="en")

TIER_ORDER = ["Student", "Individual", "Family"]

EXTRA_REGIONS = {"HK", "MO", "XK", "PR"}
MANUAL_REGION_METADATA = {
    "XK": {"name": "Kosovo"},
    "MO": {"name": "Macao"},
    "HK": {"name": "Hong Kong"},
    "PR": {"name": "Puerto Rico"},
}

APPLE_BASE_BY_CC = {"CN": "https://www.apple.com.cn"}

REGION_LOCALE_PATHS = {
    "HK": ["hk/en", "hk/zh", "hk/zh-tw", "hk-zh", "hk-zh-tw", "hk"],
    "MO": ["mo/en", "mo/zh", "mo/zh-tw", "mo-zh", "mo-zh-tw", "mo"],
    "CN": [""],
}
REGION_LOCALE_PATHS.update(
    {
        "US": [""],
        "GB": ["uk", "gb"],
    }
)

MISSING_DB = OUTPUT_DIR / "apple_music_missing.sqlite"
MISSING_CSV = OUTPUT_DIR / "apple_music_missing.csv"
MISSING_BUFFER = []

# Symbol currencies (for quick hinting)
CURRENCY_CHARS = r"[$€£¥₩₫₱₹₪₭₮₦₲₴₡₵₺₼₸៛₨₥₾฿₽]"

# --- IMPORTANT: Add letter/locale tokens used on Apple pages ---
LOCAL_CURRENCY_TOKENS = [
    r"\bTL\b",   # Turkey (often "59,99 TL")
    r"\bFt\b",   # Hungary (often "1990 Ft/hó")
    r"zł",       # Poland
    r"Kč",       # Czechia
    r"\blei\b",  # Romania
    r"лв",       # Bulgaria
    r"\bгрн\b",  # Ukraine (UAH, occasionally in Cyrillic)
    r"\b₽\b",    # Ruble sign (sometimes appears as symbol; kept for safety)
]

# Currency tokenization: used by price regex to capture "currency + number" or "number + currency"
# Keep specific tokens before generic ones (e.g., Rp before R).
CURRENCY_TOKEN = (
    r"(US\$|CA\$|AU\$|HK\$|NT\$|MOP\$|NZ\$|"
    + "|".join(LOCAL_CURRENCY_TOKENS) +
    r"|RM|S/\.|R\$|CHF|Rp|kr|"
    r"\$|€|£|¥|₩|₫|₱|₹|₪|₭|₮|₦|₲|₴|₡|₵|₺|₼|₸|៛|₨|₥|₾|฿|₽|"
    r"TSh|KSh|USh|ZAR|ZWL|R|"
    r"SAR|QAR|AED|KWD|BHD|OMR)"
)

NUMBER_TOKEN = r"(\d+(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?)"

BANNER_PRICE_REGEX = re.compile(
    rf"(?:{CURRENCY_TOKEN}\s*{NUMBER_TOKEN}|{NUMBER_TOKEN}\s*{CURRENCY_TOKEN})"
)

STRICT_PRICE_NUMBER = re.compile(
    r"(\d{1,3}(?:[.,\s]\d{3})+|\d+[.,]\d{1,2})"
)

BANNER_SEMAPHORE = threading.Semaphore(3)

COUNTRY_CORRECTIONS = {
    "台灣": "Taiwan",
    "대한민국": "South Korea",
    "ไทย": "Thailand",
    "澳門": "Macao",
}
MANUAL_COUNTRY_FIXES = {
    "Space": "Macao",
    "Italia": "Italy",
    "Suisse": "Switzerland",
    "Finnish": "Finland",
    "The Netherlands": "Netherlands",
    "Moldova, Republic of": "Moldova",
    "Greek": "Greece",
}

TEST_MODE = True
TEST_COUNTRIES = ["US", "KW", "ID", "IN", "TR", "HU", "FR"]

# ================= Utilities =================

def _clean(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

def _cast_num(x):
    try:
        if isinstance(x, int) or x is None:
            return x
        if isinstance(x, float) and abs(x - int(x)) < 1e-9:
            return int(x)
    except Exception:
        pass
    return x

@lru_cache(maxsize=4096)
def translate_text_cached(text: str) -> str:
    try:
        return (translator.translate(text or "") or "").lower()
    except Exception:
        return (text or "").lower()

# ================= Currency logic =================

HARDCODE_FALLBACKS = {
    # Americas + dollarised
    "US": "USD", "CA": "CAD", "MX": "MXN", "BR": "BRL", "AR": "ARS",
    "CL": "CLP", "CO": "COP", "PE": "PEN", "UY": "UYU", "PY": "PYG",
    "BO": "BOB", "NI": "NIO", "GT": "GTQ", "CR": "CRC", "PA": "PAB",
    "HN": "HNL", "DO": "DOP", "JM": "JMD", "BB": "BBD", "BS": "BSD",
    "BZ": "BZD", "EC": "USD", "SV": "USD", "PR": "USD",

    # Europe & Eurasia
    "GB": "GBP", "IE": "EUR", "FR": "EUR", "DE": "EUR", "ES": "EUR", "IT": "EUR",
    "PT": "EUR", "NL": "EUR", "BE": "EUR", "LU": "EUR", "AT": "EUR", "FI": "EUR",
    "EE": "EUR", "LV": "EUR", "LT": "EUR", "SK": "EUR", "SI": "EUR", "GR": "EUR",
    "CY": "EUR", "MT": "EUR",
    "BG": "BGN", "RO": "RON", "PL": "PLN", "CZ": "CZK", "HU": "HUF", "HR": "EUR",
    "DK": "DKK", "SE": "SEK", "NO": "NOK", "IS": "ISK", "CH": "CHF",
    "RS": "RSD", "BA": "BAM", "MK": "MKD", "AL": "ALL",
    "UA": "UAH", "GE": "GEL", "AZ": "AZN", "AM": "AMD", "KZ": "KZT", "MD": "MDL",
    "BY": "BYN", "TR": "TRY", "RU": "RUB",

    # MENA
    "AE": "AED", "SA": "SAR", "QA": "QAR", "KW": "KWD", "BH": "BHD", "OM": "OMR",
    "IL": "ILS", "EG": "EGP", "MA": "MAD", "TN": "TND", "DZ": "DZD", "IQ": "IQD",

    # Africa
    "ZA": "ZAR", "NG": "NGN", "GH": "GHS", "KE": "KES", "TZ": "TZS", "UG": "UGX",
    "CM": "XAF", "CI": "XOF", "SN": "XOF", "RW": "RWF", "BI": "BIF", "CD": "CDF",
    "BJ": "XOF", "TD": "XAF", "CG": "XAF", "GA": "XAF", "NE": "XOF",

    # APAC & Pacific
    "JP": "JPY", "KR": "KRW", "CN": "CNY", "TW": "TWD", "HK": "HKD", "MO": "MOP",
    "SG": "SGD", "MY": "MYR", "TH": "THB", "VN": "VND", "PH": "PHP", "ID": "IDR",
    "IN": "INR", "PK": "PKR", "LK": "LKR", "NP": "NPR", "BD": "BDT",
    "KH": "USD", "MN": "MNT", "TJ": "TJS",
    "AU": "AUD", "NZ": "NZD",
    "KI": "AUD", "NR": "AUD", "TV": "AUD", "MH": "USD",
}
KNOWN_ISO = set(HARDCODE_FALLBACKS.values())

# Strong tokens (explicit/unambiguous). Add TL/Ft etc.
STRONG_TOKENS = [
    # Apple locale abbreviations
    (r"(?i)\bTL\b", "TRY"),
    (r"(?i)\bFt\b", "HUF"),
    (r"zł", "PLN"),
    (r"Kč", "CZK"),
    (r"(?i)\blei\b", "RON"),
    (r"лв", "BGN"),

    # Explicit USD markers
    (r"(?i)US\$", "USD"), (r"(?i)\$US", "USD"), (r"(?i)U\$S", "USD"),

    # Other $-prefixes
    (r"(?i)\bA\$", "AUD"), (r"(?i)\bNZ\$", "NZD"), (r"(?i)\bHK\$", "HKD"),
    (r"(?i)\bNT\$", "TWD"), (r"(?i)\bS\$", "SGD"), (r"(?i)\bRD\$", "DOP"),
    (r"(?i)\bN\$", "NAD"),

    # Common tokens
    (r"R\$", "BRL"), (r"S/\.", "PEN"), (r"S/", "PEN"),
    (r"Bs\.?", "BOB"), (r"Gs\.?", "PYG"), (r"₲", "PYG"),
    (r"Q(?=[\s\d])", "GTQ"),
    (r"KSh", "KES"), (r"TSh", "TZS"), (r"USh", "UGX"),
    (r"Rp", "IDR"),
    (r"€", "EUR"), (r"£", "GBP"), (r"₹", "INR"),
    (r"(?<![A-Z])R\s?(?=\d)", "ZAR"),
]

SINGLE_SYMBOL_TO_ISO = {
    "₩": "KRW", "₫": "VND", "₺": "TRY", "₪": "ILS", "₴": "UAH",
    "₼": "AZN", "₾": "GEL", "₭": "LAK", "฿": "THB", "₦": "NGN",
    "₵": "GHS", "₱": "PHP", "₸": "KZT", "₽": "RUB",
}

AMBIG_TOKENS = {
    r"\$": {"USD","MXN","ARS","CLP","COP","CAD","AUD","NZD","SGD","HKD","TWD",
            "UYU","BBD","BSD","DOP","CRC","PAB","HNL","JMD"},
    r"\bkr\.?\b": {"SEK","NOK","DKK","ISK"},
    r"\bRs\.?\b": {"INR","PKR","LKR","NPR"},
    r"₨": {"INR","PKR","LKR","NPR"},
    r"(?i)\bC\$\b": {"CAD","NIO"},
}

DOLLAR_CURRENCIES = {"USD","CAD","AUD","NZD","SGD","HKD","TWD","MXN","ARS",
                     "CLP","COP","UYU","BBD","BSD","DOP","CRC","PAB","HNL","JMD"}

def default_currency_for_alpha2(alpha2: str) -> str:
    iso2 = (alpha2 or "").upper()
    if iso2 in HARDCODE_FALLBACKS:
        return HARDCODE_FALLBACKS[iso2]
    try:
        currs = get_territory_currencies(iso2, date=date.today(), non_tender=False)
        if currs:
            return currs[0]
    except Exception:
        pass
    return ""

def detect_currency_in_text(text: str, alpha2: str):
    s = _clean(text)
    if not s:
        return "", "territory_default"

    for pat, iso in STRONG_TOKENS:
        if re.search(pat, s):
            return iso, "symbol"

    if "¥" in s and not re.search(r"[A-Z]{3}", s, re.I):
        cc = (alpha2 or "").upper()
        return ("CNY" if cc == "CN" else "JPY" if cc == "JP" else default_currency_for_alpha2(cc), "symbol")

    for sym, iso in SINGLE_SYMBOL_TO_ISO.items():
        if sym in s:
            return iso, "symbol"

    S = s.upper()
    for m in re.finditer(r"\b([A-Z]{3})\b", S):
        code = m.group(1)
        if code == "TRY":
            continue
        if code in KNOWN_ISO:
            a, b = m.span()
            window = S[max(0, a - 6):min(len(S), b + 6)]
            if re.search(r"\d", window):
                return code, "code"

    for pat in AMBIG_TOKENS.keys():
        if re.search(pat, s):
            return default_currency_for_alpha2(alpha2), "ambiguous->default"

    return default_currency_for_alpha2(alpha2), "territory_default"

def detect_currency_from_display(display_text: str, alpha2: str):
    s = _clean(display_text or "")
    if not s:
        return "", "empty", ""

    for pat, iso in STRONG_TOKENS:
        m = re.search(pat, s)
        if m:
            return iso, "symbol", m.group(0)

    if "¥" in s and not re.search(r"[A-Z]{3}", s, re.I):
        cc = (alpha2 or "").upper()
        return (
            "CNY" if cc == "CN" else "JPY" if cc == "JP" else default_currency_for_alpha2(cc),
            "symbol",
            "¥",
        )

    for sym, iso in SINGLE_SYMBOL_TO_ISO.items():
        if sym in s:
            return iso, "symbol", sym

    S = s.upper()
    for m in re.finditer(r"\b([A-Z]{3})\b", S):
        code = m.group(1)
        if code == "TRY":
            continue
        if code in KNOWN_ISO:
            return code, "code", code

    if re.search(r"(^|[^A-Z])\$(?=\s*\d)", s):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "$"
    if re.search(r"\bkr\b", s, re.I):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "kr"
    if re.search(r"\bRs\b", s):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "Rs"
    if "₨" in s:
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "₨"

    return default_currency_for_alpha2(alpha2), "territory_default", ""

def resolve_dollar_ambiguity(iso_guess: str, raw_token: str, amount, alpha2: str, context_text: str):
    if raw_token != "$":
        return iso_guess, None
    default_iso = default_currency_for_alpha2(alpha2)
    if default_iso in DOLLAR_CURRENCIES:
        return iso_guess, None
    if re.search(r"(?i)\bUS\$|\$US|\bUSD\b", context_text):
        return "USD", "context-usd"
    if alpha2 in {"KW", "QA", "BH", "OM"}:
        return "USD", "gcc-usd"
    try:
        v = float(amount)
        if v <= 50:
            return "USD", "small-$-usd"
    except Exception:
        pass
    return iso_guess, None

def _normalize_number(p: str) -> str:
    p = (p or "").replace(" ", "")
    dm = re.search(r"([.,])(\d{1,2})$", p)
    if dm:
        frac = dm.group(2)
        base = p[:-len(dm.group(0))].replace(".", "").replace(",", "")
        try:
            return str(float(base + "." + frac))
        except Exception:
            return ""
    try:
        return str(float(p.replace(".", "").replace(",", "")))
    except Exception:
        return ""

def extract_amount_number(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    t = _clean(text)
    S = t.upper()
    num_pat = r"\d[\d\s.,]*"

    for pat, _ in STRONG_TOKENS:
        m = re.search(pat, t)
        if m:
            n = re.search(num_pat, t[m.end():])
            if n:
                return _normalize_number(n.group(0))

    m = re.search(r"\b([A-Z]{3})\b\s*(" + num_pat + ")", S)
    if m and m.group(1) != "TRY":
        return _normalize_number(m.group(2))

    m = re.search("(" + num_pat + r")\s*\b([A-Z]{3})\b", S)
    if m and m.group(2) != "TRY":
        return _normalize_number(m.group(1))

    m = re.search(
        r"(?:US\$|[€£¥₩₫₺₪₴₼₾₭฿₦₵₱₸₽]|NT\$|HK\$|S/\.|S/|R\$|RD\$|N\$|KSh|TSh|USh|Rp|TL|Ft)\s*" + num_pat,
        t,
        re.I,
    )
    if m:
        n = re.search(num_pat, m.group(0))
        if n:
            return _normalize_number(n.group(0))

    cand = [m.group(0) for m in re.finditer(num_pat, t)]
    if cand:
        return _normalize_number(cand[-1])
    return ""

# ================= Plan name normalization =================

@lru_cache(maxsize=None)
def standardize_plan(plan_text, idx):
    raw = (plan_text or "").strip().lower()
    if not raw:
        return TIER_ORDER[idx] if idx < len(TIER_ORDER) else "Individual"

    if "student" in raw:
        return "Student"
    if "family" in raw:
        return "Family"
    if "individual" in raw or "personal" in raw:
        return "Individual"

    en = translate_text_cached(raw)
    if "student" in en:
        return "Student"
    if "family" in en:
        return "Family"
    if "individual" in en or "personal" in en:
        return "Individual"

    return TIER_ORDER[idx] if idx < len(TIER_ORDER) else "Individual"

# ================= Missing logging =================

def init_missing_db():
    con = sqlite3.connect(str(MISSING_DB))
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS missing (
        ts TEXT, country TEXT, country_code TEXT, url TEXT, reason TEXT)"""
    )
    con.commit()
    con.close()

def log_missing(country, code, url, reason):
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    con = sqlite3.connect(str(MISSING_DB))
    cur = con.cursor()
    cur.execute(
        "INSERT INTO missing (ts,country,country_code,url,reason) VALUES (?,?,?,?,?)",
        (ts, country, code, url, reason),
    )
    con.commit()
    con.close()
    MISSING_BUFFER.append(
        {"ts": ts, "country": country, "country_code": code, "url": url, "reason": reason}
    )

# ================= Recurring price selection (trial-proof) =================
# We avoid "penalize trial" because Apple often writes: "$0.49 for 3 months..., then $5.49/month"
# Both tokens share the same sentence. Instead we:
#   - Prefer tokens whose nearby translated context contains "then/after/thereafter"
#   - Else tokens whose nearby translated context looks monthly
#   - Else MAX numeric token (recurring almost always larger)

EN_THEN_RE = re.compile(r"(?i)\b(then|after|thereafter|following|from then)\b")
EN_MONTH_RE = re.compile(r"(?i)(/\s*month\b|\bper\s+month\b|\bmonthly\b|\beach\s+month\b|\ba\s+month\b)")

def pick_recurring_price_token(text: str):
    if not text:
        return None, None, []

    t = _clean(text)
    cands = []

    for m in BANNER_PRICE_REGEX.finditer(t):
        token = m.group(0)
        num = extract_amount_number(token)
        if not num:
            continue
        try:
            val = float(num)
        except Exception:
            continue

        a, b = m.span()
        ctx_raw = t[max(0, a - 60): min(len(t), b + 60)]
        ctx_en = translate_text_cached(ctx_raw)

        has_then = bool(EN_THEN_RE.search(ctx_en))
        has_month = bool(EN_MONTH_RE.search(ctx_en))

        cands.append((token, val, has_then, has_month, ctx_en, a))

    if not cands:
        return None, None, []

    pool = [c for c in cands if c[2]] or [c for c in cands if c[3]] or cands

    # pick max value; tie-breaker later in text
    pool.sort(key=lambda x: (x[1], x[5]), reverse=True)
    best = pool[0]

    debug = [(c[0], c[1], c[2], c[3], c[4]) for c in cands]
    return best[0], _cast_num(best[1]), debug

# ================= DOM parsing =================

def candidate_price_nodes(card: Tag):
    nodes = list(
        card.select(
            "p.plan-type.cost, p.tile-headline, "
            "[class*=cost], [class*=price], [class*=headline], [class*=subhead]"
        )
    )
    for el in card.find_all(True):
        txt = (el.get("aria-label") or el.get_text(" ", strip=True) or "")
        if txt and re.search(
            rf"{CURRENCY_CHARS}|Rp|\bTL\b|\bFt\b|Kč|zł|\blei\b|лв|грн|"
            r"SAR|QAR|AED|KWD|BHD|OMR|RM|HK\$|NT\$|US\$",
            txt,
            re.I,
        ):
            nodes.append(el)
    seen, out = set(), []
    for n in nodes:
        if id(n) not in seen:
            seen.add(id(n))
            out.append(n)
    return out

def find_plan_cards(soup: BeautifulSoup):
    section = (
        soup.find("section", attrs={"data-analytics-name": re.compile("plans", re.I)})
        or soup.find("section", class_=re.compile("plans|pricing|subscriptions", re.I))
        or soup
    )

    cards = section.select(
        "div.plan-list-item, "
        "li.gallery-item, "
        "li[role='listitem'], "
        "div[class*='plan'], "
        "div[class*='pricing'], "
        "div[class*='tile'], "
        "article[class*='plan'], "
        "[data-analytics-name*='student'], "
        "[data-analytics-name*='individual'], "
        "[data-analytics-name*='family'], "
        "#student, #individual, #family"
    )

    if not cards or len(cards) < 2:
        cards = soup.select("#student, #individual, #family") or cards

    return section, cards

def classify_plan_card(card: Tag, idx: int):
    cid = (card.get("id") or "").lower()
    if cid in {"student", "individual", "family"}:
        return cid.capitalize()

    dan = (card.get("data-analytics-name") or "").lower()
    if "student" in dan:
        return "Student"
    if "individual" in dan or "personal" in dan:
        return "Individual"
    if "family" in dan:
        return "Family"

    head = card.select_one("h1,h2,h3,h4,p,span")
    if head:
        return standardize_plan(head.get_text(" ", strip=True), idx)

    return TIER_ORDER[idx] if idx < len(TIER_ORDER) else "Individual"

def extract_plan_entries_from_dom_apple(soup: BeautifulSoup, alpha2: str):
    _, cards = find_plan_cards(soup)
    if not cards:
        return {}

    entries = {}
    for idx, card in enumerate(cards):
        std = classify_plan_card(card, idx)

        headline_bits = []
        for el in candidate_price_nodes(card):
            raw = (el.get("aria-label") or el.get_text(" ", strip=True) or "")
            if raw:
                headline_bits.append(raw)

        headline_text = " ".join(headline_bits)
        full_text = " ".join(card.stripped_strings)

        chosen_tok, chosen_val, _dbg = pick_recurring_price_token(headline_text)
        if not chosen_tok:
            chosen_tok, chosen_val, _dbg = pick_recurring_price_token(full_text)
        if not chosen_tok:
            continue

        iso, src, raw_cur = detect_currency_from_display(chosen_tok, alpha2)

        if src in {"ambiguous_symbol->default", "territory_default"}:
            iso2, src2 = detect_currency_in_text(full_text, alpha2)
            if src2 in {"symbol", "code"} and iso2:
                iso, src, raw_cur = iso2, f"context-{src2}", raw_cur
            iso_res, why = resolve_dollar_ambiguity(iso, raw_cur, chosen_val, alpha2, full_text)
            if why:
                iso, src = iso_res, f"heuristic-{why}"

        if std not in entries:
            entries[std] = {
                "Currency": iso,
                "Currency Source": src,
                "Currency Raw": raw_cur,
                "Price Display": _clean(chosen_tok),
                "Price Value": _cast_num(float(chosen_val)),
            }

    return entries

def extract_plan_entries_from_dom_generic(soup: BeautifulSoup, alpha2: str):
    entries = {}
    plan_lists = soup.find_all(attrs={"class": re.compile(r"(plan|tier|pricing)", re.I)}) or [soup]
    for container in plan_lists:
        cards = container.find_all(True, class_=re.compile(r"(plan|tier|card|tile)", re.I)) or [container]
        for idx, card in enumerate(cards):
            lab = (
                card.find("p", class_=re.compile("plan-type|name", re.I))
                or card.find(re.compile("h[1-4]"))
                or card
            )
            plan_name = _clean(lab.get_text(" ", strip=True)) if lab else f"Plan {idx+1}"
            std = standardize_plan(plan_name, idx)

            headline_bits = []
            for el in candidate_price_nodes(card):
                raw = (el.get("aria-label") or el.get_text(" ", strip=True) or "")
                if raw:
                    headline_bits.append(raw)

            headline_text = " ".join(headline_bits)
            full_text = " ".join(card.stripped_strings)

            chosen_tok, chosen_val, _dbg = pick_recurring_price_token(headline_text)
            if not chosen_tok:
                chosen_tok, chosen_val, _dbg = pick_recurring_price_token(full_text)
            if not chosen_tok:
                continue

            iso, src, raw_cur = detect_currency_from_display(chosen_tok, alpha2)
            if src in {"ambiguous_symbol->default", "territory_default"}:
                iso2, src2 = detect_currency_in_text(full_text, alpha2)
                if src2 in {"symbol", "code"} and iso2:
                    iso, src, raw_cur = iso2, f"context-{src2}", raw_cur
                iso_res, why = resolve_dollar_ambiguity(iso, raw_cur, chosen_val, alpha2, full_text)
                if why:
                    iso, src = iso_res, f"heuristic-{why}"

            entries.setdefault(
                std,
                {
                    "Currency": iso,
                    "Currency Source": src,
                    "Currency Raw": raw_cur,
                    "Price Display": _clean(chosen_tok),
                    "Price Value": _cast_num(float(chosen_val)),
                },
            )
    return entries

def extract_plan_entries_from_dom(soup: BeautifulSoup, alpha2: str):
    entries = extract_plan_entries_from_dom_apple(soup, alpha2)
    if entries:
        return entries
    return extract_plan_entries_from_dom_generic(soup, alpha2)

# ================= Banner fallback =================

APPLE_HOST_RE = r"apple\.com(?:\.cn)?"
CC_URL_RE = re.compile(rf"{APPLE_HOST_RE}/([a-z]{{2}})(?:/|-[a-z]{{2}}(?:-[a-z]{{2}})?/)", re.I)
MUSIC_CC_URL_RE = re.compile(r"music\.apple\.com/([a-z]{2})/", re.I)

def _extract_cc(url):
    if not url:
        return ""
    m = CC_URL_RE.search(url) or MUSIC_CC_URL_RE.search(url)
    return (m.group(1) or "").upper() if m else ""

async def _get_music_banner_text_async(country_code: str):
    cc = country_code.lower()
    candidates = [
        f"https://music.apple.com/{cc}/new",
        f"https://music.apple.com/{cc}/browse",
        f"https://music.apple.com/{cc}/listen-now",
    ]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        last = ""
        for url in candidates:
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_load_state("networkidle", timeout=10000)
                last = page.url or (resp.url if resp else url)

                sels = [
                    "cwc-music-upsell-banner-web [data-test='subheader-text']",
                    "[data-test='subheader-text']",
                    ".cwc-upsell-banner__subhead",
                ]
                for sel in sels:
                    try:
                        el = page.locator(sel).first
                        await el.wait_for(state="visible", timeout=3000)
                        t = await el.inner_text()
                        if t and t.strip():
                            await browser.close()
                            return t, last
                    except PWTimeoutError:
                        continue

                t = await page.evaluate("document.body && document.body.innerText || ''")
                if t and (BANNER_PRICE_REGEX.search(t) or STRICT_PRICE_NUMBER.search(t)):
                    await browser.close()
                    return t, last
            except Exception:
                continue
        await browser.close()
        return "", last

def banner_individual_row(alpha2: str, country_name: str, meta=None):
    with BANNER_SEMAPHORE:
        try:
            text, final_url = asyncio.run(_get_music_banner_text_async(alpha2))
        except RuntimeError:
            holder = {}
            def runner():
                holder["pair"] = asyncio.run(_get_music_banner_text_async(alpha2))
            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join()
            text, final_url = holder.get("pair", ("", ""))

    store_cc = _extract_cc(final_url)
    if not store_cc or store_cc != alpha2.upper():
        log_missing(
            country_name,
            alpha2,
            final_url or f"https://music.apple.com/{alpha2.lower()}/new",
            f"music.apple.com storefront mismatch (requested={alpha2}, final={store_cc or 'NONE'})",
        )
        return []

    chosen_tok, chosen_val, _dbg = pick_recurring_price_token(text)
    if not chosen_tok or chosen_val is None:
        return []

    disp = _clean(chosen_tok)
    val = _cast_num(float(chosen_val))

    iso, src, raw = detect_currency_from_display(disp, alpha2)
    raw = raw or ""

    if src in {"ambiguous_symbol->default", "territory_default"}:
        iso2, src2 = detect_currency_in_text(text, alpha2)
        if iso2 and src2 in {"symbol", "code"}:
            iso, src = iso2, f"context-{src2}"
        iso_res, why = resolve_dollar_ambiguity(iso, raw, val, alpha2, text)
        if why:
            iso, src = iso_res, f"heuristic-{why}"

    row = {
        "Country": country_name,
        "Country Code": alpha2,
        "Currency": iso,
        "Currency Source": src,
        "Currency Raw": raw,
        "Plan": "Individual",
        "Price Display": disp,
        "Price Value": val,
        "Source": "music.apple.com banner (fallback)",
    }
    return [row]

# ================= Redirect detection =================

def looks_like_us_hub_url(url: str) -> bool:
    if not url:
        return False
    u = urlparse(url)
    return (u.netloc.endswith("apple.com") and u.path.rstrip("/") == "/apple-music")

def looks_like_us_hub_html(soup: BeautifulSoup) -> bool:
    can = soup.find("link", rel=re.compile("canonical", re.I))
    if can and can.get("href") and looks_like_us_hub_url(can.get("href")):
        return True
    og = soup.find("meta", property="og:url")
    if og and og.get("content") and looks_like_us_hub_url(og.get("content")):
        return True
    return False

def looks_like_us_content(soup: BeautifulSoup) -> bool:
    text = soup.get_text(" ", strip=True)
    t = text.lower()
    price_hit = re.search(r"\$ ?10\.99|\$ ?5\.99|\$ ?16\.99", text)
    copy_hit = ("try 1 month free" in t) or ("no commitment" in t and "cancel anytime" in t)
    return bool(price_hit and copy_hit)

def _storefront_equivalent(requested_cc: str, detected_cc: str) -> bool:
    if not detected_cc:
        return False
    r = (requested_cc or "").upper()
    d = (detected_cc or "").upper()
    if r == d:
        return True
    if r == "GB" and d == "UK":
        return True
    return False

# ================= Country helpers =================

@lru_cache(maxsize=None)
def normalize_country_name(name):
    name = COUNTRY_CORRECTIONS.get(name, name)
    try:
        return pycountry.countries.lookup(name).name
    except Exception:
        try:
            t = translator.translate(name)
            t = COUNTRY_CORRECTIONS.get(t, t)
            return pycountry.countries.lookup(t).name
        except Exception:
            return name

@lru_cache(maxsize=None)
def get_country_code(name):
    name = MANUAL_COUNTRY_FIXES.get(name, name)
    try:
        return pycountry.countries.lookup(name).alpha_2
    except Exception:
        try:
            m = pycountry.countries.search_fuzzy(name)
            return m[0].alpha_2 if m else ""
        except Exception:
            return ""

@lru_cache(maxsize=None)
def get_country_name_from_code(code):
    try:
        obj = pycountry.countries.get(alpha_2=code)
        if obj:
            return obj.name
    except Exception:
        pass
    return MANUAL_REGION_METADATA.get(code, {}).get("name", code)

# ================= Main scrape =================

def scrape_country(alpha2: str):
    cc = (alpha2 or "").upper()
    if not cc or len(cc) != 2:
        return []

    base = APPLE_BASE_BY_CC.get(cc, "https://www.apple.com")
    paths = REGION_LOCALE_PATHS.get(cc, [cc.lower()])

    last_url = None
    had_apple_page = False

    for path in paths:
        url = f"{base}/apple-music/" if path == "" else f"{base}/{path}/apple-music/"
        last_url = url
        try:
            resp = SESSION.get(url, timeout=15, allow_redirects=True)

            if resp.status_code == 200 and "apple.com" in urlparse(resp.url).netloc:
                had_apple_page = True

            if cc != "US" and looks_like_us_hub_url(resp.url):
                cn = normalize_country_name(get_country_name_from_code(cc))
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": "US hub",
                        "Redirect Reason": "Final URL is US hub",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )

            final_cc = _extract_cc(resp.url)
            if final_cc and not _storefront_equivalent(cc, final_cc) and not resp.url.startswith(APPLE_BASE_BY_CC.get(cc, "")):
                cn = normalize_country_name(get_country_name_from_code(cc))
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": final_cc,
                        "Redirect Reason": f"HTTP redirect to {final_cc}",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )

            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            if cc != "US" and looks_like_us_hub_html(soup):
                cn = normalize_country_name(get_country_name_from_code(cc))
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": "US hub",
                        "Redirect Reason": "Canonical/OG URL indicates US hub",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )

            country_name = normalize_country_name(get_country_name_from_code(cc))
            country_name = MANUAL_COUNTRY_FIXES.get(country_name, country_name)
            code = (get_country_code(country_name) or cc).upper()

            entries = extract_plan_entries_from_dom(soup, code)

            if cc != "US" and not entries and looks_like_us_content(soup):
                return banner_individual_row(
                    code,
                    country_name,
                    meta={
                        "Redirected": True,
                        "Redirected To": "US hub",
                        "Redirect Reason": "Page content matches US hub",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )

            rows = []
            for std in TIER_ORDER:
                if std in entries:
                    info = entries[std]
                    rows.append(
                        {
                            "Country": country_name,
                            "Country Code": code,
                            "Currency": info["Currency"],
                            "Currency Source": info["Currency Source"],
                            "Currency Raw": info["Currency Raw"],
                            "Plan": std,
                            "Price Display": info["Price Display"],
                            "Price Value": info["Price Value"],
                            "Source": "apple.com.cn page" if base.endswith(".cn") else "apple.com page",
                            "Redirected": False,
                            "Redirected To": "",
                            "Redirect Reason": "",
                            "Apple URL": resp.url,
                            "Has Apple Music Page": True,
                        }
                    )
            if rows:
                return rows

            return banner_individual_row(
                code,
                country_name,
                meta={
                    "Redirected": False,
                    "Redirected To": "",
                    "Redirect Reason": "",
                    "Apple URL": resp.url,
                    "Has Apple Music Page": True,
                },
            )

        except Exception:
            continue

    cn = normalize_country_name(get_country_name_from_code(cc))
    return banner_individual_row(
        cc,
        cn,
        meta={
            "Redirected": False,
            "Redirected To": "",
            "Redirect Reason": "No country-specific Apple Music page; banner-only",
            "Apple URL": last_url or "",
            "Has Apple Music Page": had_apple_page,
        },
    )

# ================= Runner =================

def run_scraper(country_codes_override=None):
    init_missing_db()

    iso_codes = {c.alpha_2 for c in pycountry.countries}
    all_codes = sorted(iso_codes.union(EXTRA_REGIONS))

    if country_codes_override:
        requested = {(cc or "").strip().upper() for cc in country_codes_override if (cc or "").strip()}
        requested = {cc for cc in requested if len(cc) == 2}
        all_codes = sorted(requested)
        print(f"🎯 Subset mode: scraping {len(all_codes)} countries: {all_codes}")
    elif TEST_MODE:
        all_codes = sorted({c.strip().upper() for c in TEST_COUNTRIES if c and len(c.strip()) == 2})
        print(f"🧪 TEST MODE: scraping {len(all_codes)} countries: {all_codes}")
    else:
        print(f"🌍 FULL MODE: scraping {len(all_codes)} countries")

    if not all_codes:
        print("⚠️ No country codes to scrape.")
        return

    all_rows = []
    failed_codes = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(scrape_country, cc): cc for cc in all_codes}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Scraping countries", unit="cc"):
            cc = futures[fut]
            try:
                res = fut.result()
                if res:
                    all_rows.extend(res)
            except Exception as e:
                failed_codes.append(cc)
                cn = normalize_country_name(get_country_name_from_code(cc))
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Future exception: {type(e).__name__}: {e}",
                )

    if failed_codes:
        print(f"🔁 Retrying {len(failed_codes)} failed countries sequentially…")
        for cc in failed_codes:
            try:
                res = scrape_country(cc)
                if res:
                    all_rows.extend(res)
                    MISSING_BUFFER[:] = [m for m in MISSING_BUFFER if m.get("country_code") != cc]
            except Exception as e:
                cn = normalize_country_name(get_country_name_from_code(cc))
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Retry exception: {type(e).__name__}: {e}",
                )

    if not all_rows:
        print("⚠️ No rows scraped at all.")
        return

    df = pd.DataFrame(all_rows)
    df["Plan"] = pd.Categorical(df["Plan"], TIER_ORDER, ordered=True)
    df.sort_values(["Country", "Plan"], inplace=True, ignore_index=True)

    out_name = "apple_music_plans_TEST.xlsx" if TEST_MODE or country_codes_override else "apple_music_plans_all.xlsx"
    full_path = OUTPUT_DIR / out_name
    df.to_excel(full_path, index=False)
    print(f"✅ Exported to {full_path} (rows={len(df)})")

    if MISSING_BUFFER:
        pd.DataFrame(MISSING_BUFFER).to_csv(MISSING_CSV, index=False)
        print(f"⚠️ Logged {len(MISSING_BUFFER)} issues to {MISSING_CSV} / {MISSING_DB}")

    return str(full_path)


def run_apple_music_scraper(test_mode: bool = True, test_countries=None) -> str | None:
    """
    Entry point used by the Streamlit app.

    - In test_mode, honours `test_countries` by passing them into run_scraper.
    - In full mode, ignores `test_countries` and scrapes all countries.
    - Returns the absolute path to the Excel file, or None if nothing was written.
    """
    global TEST_MODE, TEST_COUNTRIES
    TEST_MODE = bool(test_mode)

    country_override = None
    if TEST_MODE and test_countries:
        TEST_COUNTRIES = [
            c.strip().upper()
            for c in test_countries
            if c and len(c.strip()) == 2
        ]
        country_override = TEST_COUNTRIES
        print(f"[APPLE MUSIC] UI-driven test countries: {TEST_COUNTRIES}")

    start = time.time()
    excel_path = run_scraper(country_codes_override=country_override)
    print(f"[APPLE MUSIC] Finished in {round(time.time() - start, 2)}s")

    return excel_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Apple Music scraper CLI")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full scrape (all countries) instead of test mode.",
    )
    parser.add_argument(
        "--countries",
        nargs="*",
        help="Optional list of ISO alpha-2 country codes for test mode (e.g. BR FR IN).",
    )
    args = parser.parse_args()

    test_mode = not args.full
    test_countries = args.countries if test_mode else None

    path = run_apple_music_scraper(
        test_mode=test_mode,
        test_countries=test_countries,
    )
    print(f"Output written to: {path}")
