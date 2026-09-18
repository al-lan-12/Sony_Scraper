# dsp_scrapers/icloud_plus_scraper.py
"""
iCloud+ scraper used by the Streamlit app.

Stages:
1. Storage pricing, per country, from the Apple support article (TABLE layout).
2. US-billed countries article via Playwright (optional / non-fatal).
3. iCloud+ "with Apple TV & Apple Arcade" locale pages on icloud.com, which add:
     - whether Apple TV + Apple Arcade are bundled into iCloud+ in that market
     - the discounted Apple Music individual/family price for iCloud+ subscribers
   (These two fields exist ONLY on icloud.com, not in the support article.)

Public API expected by the app:
    run_icloud_plus_scraper(test_mode: bool = True, test_countries=None) -> str

--- FIX NOTES (2026-09-18) ---
1. Apple changed the pricing article from <p> paragraphs to HTML <table>s.
   get_regular_pricing() now parses the tables - this is why it started
   returning 0 rows ("no regular pricing rows parsed").
2. clean_country_name() had a regex bug: r"[,\\s]+$" (double backslash)
   matched a literal 's', truncating names like Barbados -> Barbado.
3. Duplicate guard so a Eurozone country also listed explicitly (Croatia)
   is not emitted twice.
4. Playwright import is lazy + non-fatal; asyncio runs in an isolated thread
   with a Proactor loop so it survives Streamlit on Windows.
5. NEW: bundle enrichment stage (TV/Arcade + discounted Apple Music).
"""

import asyncio
import difflib
import math
import re
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import pycountry
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

URL_PRICING = "https://support.apple.com/en-gb/108047"
URL_USD_BILLED = "https://support.apple.com/en-us/111740"

ICLOUD_BASE = "https://www.icloud.com"
# Any locale works as the seed - its footer links to every other locale.
ICLOUD_BUNDLE_SEEDS = [
    "https://www.icloud.com/en-qa/info/icloud-plus-tv-arcade",
    "https://www.icloud.com/info/icloud-plus-tv-arcade",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}

OUT_BASENAME = "icloud_plus_pricing"

EUROZONE_COUNTRIES = [
    ("Austria", "AT"), ("Belgium", "BE"), ("Croatia", "HR"), ("Cyprus", "CY"),
    ("Estonia", "EE"), ("Finland", "FI"), ("France", "FR"), ("Germany", "DE"),
    ("Greece", "GR"), ("Ireland", "IE"), ("Italy", "IT"), ("Latvia", "LV"),
    ("Lithuania", "LT"), ("Luxembourg", "LU"), ("Malta", "MT"), ("Netherlands", "NL"),
    ("Portugal", "PT"), ("Slovakia", "SK"), ("Slovenia", "SI"), ("Spain", "ES"),
]

EXPECTED_PLANS = ("50 GB", "200 GB", "2 TB", "6 TB", "12 TB")

US_PRICING: Dict[str, Dict[str, Any]] = {
    "50 GB": {"Price": 0.99, "Price_Display": "$0.99"},
    "200 GB": {"Price": 2.99, "Price_Display": "$2.99"},
    "2 TB": {"Price": 9.99, "Price_Display": "$9.99"},
    "6 TB": {"Price": 29.99, "Price_Display": "$29.99"},
    "12 TB": {"Price": 59.99, "Price_Display": "$59.99"},
}

TEST_ISO2_SAMPLE = ["GB", "US", "BR", "JP", "ZA"]

BUNDLE_COLUMNS = [
    "TV_Arcade_Included",
    "AppleMusic_Individual",
    "AppleMusic_Individual_Display",
    "AppleMusic_Family",
    "AppleMusic_Family_Display",
    "Bundle_Source_URL",
]


# ---------------------------------------------------------------------------
# Normalisation / helpers
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    """Normalise whitespace + odd unicode characters."""
    if not s:
        return ""
    s = (
        s.replace("\xa0", " ")
        .replace(" ", " ")
        .replace(" ", " ")
        .replace("​", "")
        .replace("‌", "")
        .replace("‍", "")
        .replace("﻿", "")
        .replace("­", "")
        .replace("¬", " ")
    )
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("’", "'").replace("‘", "'")   # curly -> straight apostrophe
    return re.sub(r"\s+", " ", s).strip()


def strip_accents(s: str) -> str:
    """Remove diacritics from string."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s or "") if unicodedata.category(c) != "Mn"
    )


def clean_country_name(name: str) -> str:
    """Clean a country name: drop footnote markers, numbers, and normalise variants."""
    if not name:
        return ""

    name = norm(name)

    # Strip superscript footnote digits that Apple appends (e.g. Bahamas)
    name = re.sub(r"[²³¹⁰-⁹]+", "", name)

    # Remove trailing numeric footnotes and punctuation
    name = re.sub(r"[\s]*[0-9]+[,\s]*$", "", name)
    name = re.sub(r"[,\s]+$", "", name)   # FIXED: was r"[,\\s]+$" (stripped a literal 's')
    name = re.sub(r"\s+", " ", name).strip()

    name_lower = name.lower().strip()
    name_map = {
        "antigua": "Antigua and Barbuda",
        "barbuda": "Antigua and Barbuda",
        "bahamas": "Bahamas",
        "bahrain": "Bahrain",
        "barbados": "Barbados",
        "bosnia": "Bosnia and Herzegovina",
        "brunei": "Brunei Darussalam",
        "cabo verde": "Cabo Verde",
        "cape verde": "Cabo Verde",
        "czech republic": "Czechia",
        "democratic republic of the congo": "Democratic Republic of the Congo",
        "democratic republic of congo": "Democratic Republic of the Congo",
        "dr congo": "Democratic Republic of the Congo",
        "republic of the congo": "Congo",
        "republic of congo": "Congo",
        "ivory coast": "Côte d'Ivoire",
        "cote divoire": "Côte d'Ivoire",
        "cote d'ivoire": "Côte d'Ivoire",
        "côte d'ivoire": "Côte d'Ivoire",
        "eswatini": "Eswatini",
        "swaziland": "Eswatini",
        "north macedonia": "North Macedonia",
        "macedonia": "North Macedonia",
        "micronesia": "Micronesia",
        "kyrgyz republic": "Kyrgyzstan",
        "saint kitts": "Saint Kitts and Nevis",
        "st. kitts": "Saint Kitts and Nevis",
        "st kitts": "Saint Kitts and Nevis",
        "nevis": "Saint Kitts and Nevis",
        "saint vincent": "Saint Vincent and the Grenadines",
        "st. vincent": "Saint Vincent and the Grenadines",
        "st vincent": "Saint Vincent and the Grenadines",
        "east timor": "Timor-Leste",
        "timor": "Timor-Leste",
        "vatican city": "Holy See",
        "vietnam": "Viet Nam",
        "russia": "Russian Federation",
        "taiwan": "Taiwan, Province of China",
        "venezuela": "Venezuela, Bolivarian Republic of",
        "bolivia": "Bolivia, Plurinational State of",
        "iran": "Iran, Islamic Republic of",
    }

    # Longest key first so "democratic republic of the congo" wins over "republic of..."
    for key in sorted(name_map, key=len, reverse=True):
        if name_lower.startswith(key):
            return name_map[key]

    return name


def get_country_iso_code(name: str) -> str:
    """Get ISO 3166-1 alpha-2 country code for a country name."""
    if not name:
        return ""

    try:
        return pycountry.countries.lookup(name).alpha_2
    except LookupError:
        pass

    special_cases = {
        "vietnam": "VN", "viet nam": "VN", "laos": "LA", "russia": "RU",
        "brunei": "BN", "brunei darussalam": "BN",
        "macau": "MO", "macao": "MO", "taiwan": "TW", "palestine": "PS",
        "kosovo": "XK", "holy see": "VA", "vatican city": "VA", "bahamas": "BS",
        "congo": "CG", "republic of congo": "CG", "republic of the congo": "CG",
        "democratic republic of the congo": "CD", "dr congo": "CD",
        "côte d'ivoire": "CI", "cote d'ivoire": "CI", "ivory coast": "CI",
        "cabo verde": "CV", "cape verde": "CV",
        "timor-leste": "TL", "east timor": "TL",
        "eswatini": "SZ", "swaziland": "SZ", "micronesia": "FM",
        "china mainland": "CN", "republic of korea": "KR",
        "türkiye": "TR", "turkiye": "TR",
        "kyrgyz republic": "KG", "kyrgyzstan": "KG",
        "british virgin islands": "VG", "virgin islands, british": "VG",
        "turks and caicos": "TC", "turks and caicos islands": "TC",
        "são tomé and príncipe": "ST", "sao tome and principe": "ST",
        "saint kitts and nevis": "KN", "saint lucia": "LC",
        "saint vincent and the grenadines": "VC",
        "north macedonia": "MK", "moldova": "MD", "tanzania": "TZ",
        "bolivia": "BO", "venezuela": "VE", "iran": "IR",
    }
    key = name.lower().strip()
    if key in special_cases:
        return special_cases[key]
    if strip_accents(key) in special_cases:
        return special_cases[strip_accents(key)]

    # Fuzzy matching as a last resort
    try:
        all_countries = [(c.name, c.alpha_2) for c in pycountry.countries]
        for c in pycountry.countries:
            if hasattr(c, "common_name"):
                all_countries.append((c.common_name, c.alpha_2))
            if hasattr(c, "official_name"):
                all_countries.append((c.official_name, c.alpha_2))

        best_ratio, best_code = 0.0, None
        for country_name, code in all_countries:
            ratio = difflib.SequenceMatcher(None, name.lower(), country_name.lower()).ratio()
            if ratio > best_ratio and ratio > 0.85:
                best_ratio, best_code = ratio, code
        return best_code or ""
    except Exception:
        return ""


def parse_numeric_price(s: str) -> float:
    """Turn a messy price string into a float, or NaN on failure."""
    if not s:
        return math.nan
    s = norm(s)
    s = re.sub(r"^[^\d]+", "", s)

    m = re.search(r"\d[\d.,]*", s)
    if not m:
        return math.nan
    token = m.group(0).rstrip(".,")

    if "," in token and "." in token:
        last_c, last_d = token.rfind(","), token.rfind(".")
        dec = "," if last_c > last_d else "."
        thou = "." if dec == "," else ","
        token = token.replace(thou, "").replace(dec, ".")
    elif "," in token:
        parts = token.split(",")
        token = token.replace(",", ".") if len(parts[-1]) == 2 else token.replace(",", "")
    elif "." in token:
        parts = token.split(".")
        if len(parts) > 2 and all(len(p) == 3 for p in parts[1:]) and len(parts[-1]) in (3, 0):
            token = "".join(parts)

    try:
        return float(token)
    except Exception:
        return math.nan


def _match_plan_header(text: str) -> Optional[str]:
    """Map a table header cell like '200 GB' / '2 TB' to a canonical plan name."""
    m = re.search(r"(\d+)\s*(GB|TB)", text or "", re.IGNORECASE)
    if not m:
        return None
    return f"{int(m.group(1))} {m.group(2).upper()}"


def _fetch_text(url: str, timeout: int = 25) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text


# ---------------------------------------------------------------------------
# Stage 1: storage pricing (support article)
# ---------------------------------------------------------------------------

def get_regular_pricing() -> List[Dict[str, Any]]:
    """
    Parse iCloud+ storage pricing from the support article.

    The article is a set of HTML <table>s:
        | Country (Currency) | 50 GB | 200 GB | 2 TB | 6 TB | 12 TB |
    (It used to be <p class="gb-paragraph"> lines - parsing those is why the
    scraper started returning 0 rows.)
    """
    try:
        html = _fetch_text(URL_PRICING, timeout=30)
    except Exception as e:
        print(f"Error fetching pricing data: {e}")
        return []

    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, Any]] = []

    for table in soup.find_all("table"):
        for sup in table.find_all("sup"):
            sup.decompose()

        trs = table.find_all("tr")
        if not trs:
            continue

        header_cells = [norm(c.get_text(" ", strip=True)) for c in trs[0].find_all(["th", "td"])]
        if not header_cells or "country" not in header_cells[0].lower():
            continue

        plan_by_col: Dict[int, str] = {}
        for idx, h in enumerate(header_cells[1:], start=1):
            plan = _match_plan_header(h)
            if plan:
                plan_by_col[idx] = plan
        if not plan_by_col:
            continue

        for tr in trs[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            first = norm(cells[0].get_text(" ", strip=True))
            if not first:
                continue

            m = re.match(r"^(.*?)\s*\(([^)]+)\)\s*$", first)
            country_raw, currency = (m.group(1), m.group(2).strip()) if m else (first, "")

            country = clean_country_name(country_raw)
            if not country or country.lower() == "country":
                continue

            for idx, plan in plan_by_col.items():
                if idx >= len(cells):
                    continue
                price_text = norm(cells[idx].get_text(" ", strip=True))
                if not price_text or price_text == "-":
                    continue
                rows.append(
                    {
                        "Country": country,
                        "Currency": currency,
                        "Plan": plan,
                        "Price": parse_numeric_price(price_text),
                        "Price_Display": price_text,
                    }
                )

    return rows


# ---------------------------------------------------------------------------
# Stage 2: US-billed countries (optional)
# ---------------------------------------------------------------------------

async def get_us_billed_countries() -> Set[str]:
    """
    US-billed countries via Playwright.

    OPTIONAL: the main pricing table already lists every USD-billed country,
    so failures here are non-fatal.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Playwright not installed - skipping US-billed-only countries "
              "(already covered by the main pricing table).")
        return set()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            try:
                await page.goto(URL_USD_BILLED, wait_until="networkidle", timeout=30000)
                await page.wait_for_selector("p", timeout=10000)
                html_content = await page.content()
            finally:
                await browser.close()
    except Exception as e:
        print(f"Error fetching US-billed countries (non-fatal): {e}")
        return set()

    soup = BeautifulSoup(html_content, "html.parser")
    countries: Set[str] = set()

    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if not text or len(text) > 200:
            continue
        if any(w in text.lower() for w in
               ["support", "currency", "billed", "price", "north america", "europe", "asia"]):
            continue
        for part in re.split(r"[,]|\sand\s|\sor\s", text):
            country = clean_country_name(part)
            if country and len(country.split()) <= 5:
                countries.add(country)

    return countries


# ---------------------------------------------------------------------------
# Stage 3: iCloud+ TV / Arcade bundle + discounted Apple Music (icloud.com)
# ---------------------------------------------------------------------------

_BUNDLE_PATH_RE = re.compile(r"^/(?:([a-z]{2})-([a-z]{2})/)?info/icloud-plus-tv-arcade/?$")

# The colon matters: it distinguishes the list intro ("available in: Albania, …")
# from the FAQ heading above it ("…available in my country?").
_AVAILABILITY_RE = re.compile(
    r"available in:\s*[0-9]*\s*(.{50,6000}?)(?:Can my family|\?|$)", re.S | re.I
)

# "...reduced price of QAR 19.99/month (Individual Plan) or QAR 33.99/month (Family Plan)..."
_MUSIC_RE = re.compile(
    r"reduced price of\s*(.+?)\s*/\s*month\s*\(\s*Individual Plan\s*\)"
    r"\s*or\s*(.+?)\s*/\s*month\s*\(\s*Family Plan\s*\)",
    re.I | re.S,
)


def _page_text(html: str) -> str:
    """Whole-page visible text with footnote superscripts removed."""
    soup = BeautifulSoup(html, "lxml")
    for sup in soup.find_all("sup"):
        sup.decompose()
    return norm(soup.get_text(" "))


def parse_tv_arcade_countries(html: str) -> Set[str]:
    """Country names where iCloud+ includes Apple TV + Apple Arcade."""
    text = _page_text(html)
    m = _AVAILABILITY_RE.search(text)
    if not m:
        return set()

    out: Set[str] = set()
    for part in m.group(1).split(","):
        part = re.sub(r"^\s*and\s+", "", part.strip(), flags=re.I)
        name = clean_country_name(part)
        if name and 2 <= len(name) <= 60 and not re.search(r"\d", name):
            out.add(name)
    return out


def discover_bundle_locales(html: str) -> Dict[str, str]:
    """Map ISO alpha-2 -> best (English-preferred) locale URL from the page footer."""
    soup = BeautifulSoup(html, "lxml")
    by_iso: Dict[str, str] = {}
    is_en: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].split("#")[0]
        if href.startswith(ICLOUD_BASE):
            href = href[len(ICLOUD_BASE):]
        if not href.startswith("/"):
            continue

        m = _BUNDLE_PATH_RE.match(href)
        if not m:
            continue

        lang, region = m.group(1), m.group(2)
        if region is None:
            iso, lang = "US", "en"          # bare /info/... is the US page
        else:
            iso = region.upper()

        if iso in is_en and lang != "en":
            continue                        # don't downgrade an English page
        by_iso[iso] = f"{ICLOUD_BASE}{href}"
        if lang == "en":
            is_en.add(iso)

    return by_iso


def parse_apple_music_offer(html: str) -> Optional[Dict[str, Any]]:
    """Extract the iCloud+-subscriber Apple Music individual/family price."""
    text = _page_text(html)
    m = _MUSIC_RE.search(text)
    if not m:
        return None

    ind_display, fam_display = norm(m.group(1)), norm(m.group(2))
    # Guard against the regex running away across sentences
    if len(ind_display) > 30 or len(fam_display) > 30:
        return None

    return {
        "AppleMusic_Individual": parse_numeric_price(ind_display),
        "AppleMusic_Individual_Display": ind_display,
        "AppleMusic_Family": parse_numeric_price(fam_display),
        "AppleMusic_Family_Display": fam_display,
    }


def fetch_bundle_metadata(
    wanted_iso: Optional[Set[str]] = None,
    max_workers: int = 8,
) -> Tuple[Set[str], Dict[str, Dict[str, Any]]]:
    """
    Returns (set of ISO codes with TV+Arcade bundled, {ISO: apple-music offer}).

    Only fetches the locale pages for `wanted_iso` when supplied, so test runs
    don't hit ~100 URLs.
    """
    seed_html = ""
    for seed in ICLOUD_BUNDLE_SEEDS:
        try:
            seed_html = _fetch_text(seed)
            break
        except Exception as e:
            print(f"  bundle seed {seed} failed: {e}")
    if not seed_html:
        return set(), {}

    tv_names = parse_tv_arcade_countries(seed_html)
    tv_iso = {get_country_iso_code(n) for n in tv_names}
    unmapped = sorted(n for n in tv_names if not get_country_iso_code(n))
    tv_iso.discard("")
    print(f"  TV/Arcade markets: {len(tv_names)} names -> {len(tv_iso)} ISO codes"
          + (f" (unmapped: {unmapped})" if unmapped else ""))

    locales = discover_bundle_locales(seed_html)
    if wanted_iso:
        locales = {k: v for k, v in locales.items() if k in wanted_iso}
    print(f"  fetching Apple Music offer from {len(locales)} locale page(s)…")

    music: Dict[str, Dict[str, Any]] = {}

    def _one(iso: str, url: str):
        offer = parse_apple_music_offer(_fetch_text(url))
        if offer:
            offer["Bundle_Source_URL"] = url
        return iso, offer

    if locales:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_one, iso, url) for iso, url in locales.items()]
            for fut in as_completed(futures):
                try:
                    iso, offer = fut.result()
                except Exception:
                    continue
                if offer:
                    music[iso] = offer

    missed = sorted(set(locales) - set(music))
    print(f"  Apple Music offer parsed for {len(music)}/{len(locales)} markets"
          + (f"; no English footnote for: {missed}" if missed else ""))

    return tv_iso, music


def _attach_bundle_columns(df: pd.DataFrame, wanted_iso: Optional[Set[str]]) -> pd.DataFrame:
    """Add TV/Arcade + Apple Music columns. Non-fatal on failure."""
    for col in BUNDLE_COLUMNS:
        df[col] = None

    try:
        tv_iso, music = fetch_bundle_metadata(wanted_iso)
    except Exception as e:
        print(f"Bundle enrichment failed (non-fatal): {e}")
        return df

    if not tv_iso and not music:
        return df

    iso_series = df["Country_Code_2"].astype(str).str.upper()
    df["TV_Arcade_Included"] = iso_series.map(lambda c: "Yes" if c in tv_iso else "No")

    for field in ("AppleMusic_Individual", "AppleMusic_Individual_Display",
                  "AppleMusic_Family", "AppleMusic_Family_Display", "Bundle_Source_URL"):
        df[field] = iso_series.map(lambda c: music.get(c, {}).get(field))

    return df


# ---------------------------------------------------------------------------
# Combine
# ---------------------------------------------------------------------------

async def _build_combined_dataframe(
    wanted_iso: Optional[Set[str]] = None,
    include_bundle: bool = True,
) -> pd.DataFrame:
    """Fetch USD-billed list + storage pricing, merge, then enrich with bundle data."""
    print("Fetching US-billed countries…")
    us_billed = await get_us_billed_countries()
    print(f"Found {len(us_billed)} US-billed countries.")

    print("Fetching regular pricing data…")
    regular_pricing = get_regular_pricing()
    if not regular_pricing:
        raise RuntimeError("iCloud+ combined scraper: no regular pricing rows parsed.")

    print(f"Found pricing data for {len({row['Country'] for row in regular_pricing})} countries.")

    final_rows: List[Dict[str, Any]] = []
    seen_countries: Set[str] = set()
    euro_pricing: Dict[str, Dict[str, Any]] = {}

    for row in regular_pricing:
        if clean_country_name(row["Country"]).lower().strip() == "euro":
            euro_pricing[row["Plan"]] = {
                "Currency": row["Currency"],
                "Price": row["Price"],
                "Price_Display": row["Price_Display"],
            }

    if euro_pricing:
        for euro_country, euro_code in EUROZONE_COUNTRIES:
            for plan in EXPECTED_PLANS:
                if plan in euro_pricing:
                    pricing = euro_pricing[plan]
                    final_rows.append({
                        "Country": euro_country,
                        "ISO_Code": euro_code,
                        "Country_Code_2": euro_code,
                        "Currency": pricing["Currency"],
                        "Plan": plan,
                        "Price": pricing["Price"],
                        "Price_Display": pricing["Price_Display"],
                        "Price_Source": "original-euro",
                        "Original_Currency": None,
                        "Original_Price": None,
                        "Original_Display": None,
                    })
            seen_countries.add(euro_country)

    for row in regular_pricing:
        country = clean_country_name(row["Country"])
        if country.lower().strip() == "euro":
            continue
        # Skip countries already added via the Euro expansion (e.g. Croatia).
        if country in seen_countries:
            continue

        iso_code = get_country_iso_code(country)
        final_rows.append({
            "Country": country,
            "ISO_Code": iso_code,
            "Country_Code_2": iso_code,
            "Currency": row["Currency"],
            "Plan": row["Plan"],
            "Price": row["Price"],
            "Price_Display": row["Price_Display"],
            "Price_Source": "original",
            "Original_Currency": None,
            "Original_Price": None,
            "Original_Display": None,
        })

    for r in final_rows:
        seen_countries.add(r["Country"])

    for country in us_billed:
        country = clean_country_name(country)
        if country and country not in seen_countries:
            iso_code = get_country_iso_code(country)
            for plan in EXPECTED_PLANS:
                final_rows.append({
                    "Country": country,
                    "ISO_Code": iso_code,
                    "Country_Code_2": iso_code,
                    "Currency": "USD",
                    "Plan": plan,
                    "Price": US_PRICING[plan]["Price"],
                    "Price_Display": US_PRICING[plan]["Price_Display"],
                    "Price_Source": "us-billed-only",
                    "Original_Currency": None,
                    "Original_Price": None,
                    "Original_Display": None,
                })
            seen_countries.add(country)

    df = pd.DataFrame(final_rows)

    if include_bundle:
        print("Fetching iCloud+ TV/Arcade + Apple Music bundle data…")
        df = _attach_bundle_columns(df, wanted_iso)
    else:
        for col in BUNDLE_COLUMNS:
            df[col] = None

    df["Plan"] = pd.Categorical(df["Plan"], categories=EXPECTED_PLANS, ordered=True)
    df.sort_values(["Country", "Plan"], inplace=True, ignore_index=True)
    return df


def _run_async(coro):
    """
    Run a coroutine safely, even from inside Streamlit on Windows.

    Streamlit executes the script in a worker thread, and Playwright needs a
    ProactorEventLoop to spawn its browser subprocess on Windows.  Running in a
    dedicated thread with a fresh loop avoids both 'asyncio.run() cannot be
    called from a running event loop' and the Windows subprocess
    NotImplementedError.
    """
    box: Dict[str, Any] = {}

    def _worker():
        if hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            box["result"] = loop.run_until_complete(coro)
        except BaseException as e:  # noqa: BLE001 - re-raised on the caller thread
            box["error"] = e
        finally:
            loop.close()

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join()

    if "error" in box:
        raise box["error"]
    return box["result"]


# ---------------------------------------------------------------------------
# Public API for the app
# ---------------------------------------------------------------------------

def run_icloud_plus_scraper(
    test_mode: bool = True,
    test_countries=None,
    include_bundle: bool = True,
) -> str:
    """
    Run the combined iCloud+ scraper and return the Excel file path.

    test_mode:
        True  -> only a subset of countries (and only those locale pages are fetched)
        False -> all countries (~100 extra icloud.com fetches for bundle data)
    test_countries:
        Optional list of ISO alpha-2 codes (e.g. ["GB", "US"]), test mode only.
    include_bundle:
        Set False to skip the icloud.com stage entirely (storage pricing only).
    """
    wanted_iso: Optional[Set[str]] = None
    if test_mode:
        wanted_iso = set(TEST_ISO2_SAMPLE)
        if test_countries:
            wanted_iso |= {str(c).strip().upper() for c in test_countries if c}

    df = _run_async(_build_combined_dataframe(wanted_iso=wanted_iso, include_bundle=include_bundle))

    if test_mode:
        iso_series = df["Country_Code_2"].astype(str).str.upper()
        if test_countries:
            wanted = {str(c).strip().upper() for c in test_countries if c}
            filtered = df[iso_series.isin(wanted)].copy() if wanted else df.iloc[0:0].copy()
            df = filtered if not filtered.empty else df[iso_series.isin(TEST_ISO2_SAMPLE)].copy()
        else:
            df = df[iso_series.isin(TEST_ISO2_SAMPLE)].copy()

    if df.empty:
        raise RuntimeError("iCloud+ scraper ended up with 0 rows after filtering.")

    suffix = "_TEST" if test_mode else "_all"
    out_path = Path(f"{OUT_BASENAME}{suffix}.xlsx").resolve()

    original_countries = df[df["Price_Source"].isin(["original", "original-euro"])]["Country"].nunique()
    us_billed_countries = df[df["Price_Source"].isin(["us-billed", "us-billed-only"])]["Country"].nunique()
    tv_arcade_countries = df[df["TV_Arcade_Included"] == "Yes"]["Country"].nunique()
    music_countries = df[df["AppleMusic_Individual"].notna()]["Country"].nunique()

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="iCloud+ Prices", index=False)

        meta_data = {
            "Metric": [
                "Total Rows",
                "Total Countries",
                "Countries with Original Pricing",
                "Countries with US Pricing",
                "Countries with TV+Arcade Bundled",
                "Countries with Apple Music Offer",
            ],
            "Value": [
                len(df),
                df["Country"].nunique(),
                original_countries,
                us_billed_countries,
                tv_arcade_countries,
                music_countries,
            ],
        }
        pd.DataFrame(meta_data).to_excel(writer, sheet_name="_Meta", index=False)

    print(f"[iCloud+] Saved {out_path}")
    return str(out_path)


if __name__ == "__main__":
    # Quick manual test: python -m dsp_scrapers.icloud_plus_scraper
    print(run_icloud_plus_scraper(test_mode=True))
