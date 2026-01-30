"""Apple One pricing scraper with full + test modes.

This scraper discovers all Apple country locales, fetches Apple One pages once
per slug with retry/backoff, and parses plan prices with currency detection. It
returns an Excel file path suitable for the Streamlit app's data grid.
"""

from __future__ import annotations

import random
import re
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import country_converter as coco

    _COCO = coco.CountryConverter()
except Exception:
    _COCO = None

try:
    import pycountry
except Exception:
    pycountry = None

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}

KEEP_EMPTY_ROWS = True
REGIONAL_SLUGS = {"la"}
POLITE_DELAY = (0.35, 0.7)
TEST_COUNTRY_CODES = ["GB", "US", "BR", "JP", "ZA"]
OUT_BASENAME = "apple_one_pricing"


def make_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def _strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn")


def extract_slug(href: str) -> str | None:
    """Return the first locale segment from an Apple URL."""
    if not href or not href.startswith("/"):
        return None
    href = href.split("?", 1)[0].split("#", 1)[0]
    match = re.match(r"^/([a-z]{2}(?:-[a-z]{2})?)(?:/|$)", href, flags=re.I)
    return match.group(1).lower() if match else None


def is_region_name(name: str) -> bool:
    name_norm = _strip_accents(name).lower()
    return any(term in name_norm for term in ("america latina", "caribe", "latin america"))


def iso_from_display_name(name: str) -> str | None:
    if not name:
        return None

    if _COCO is not None:
        code = _COCO.convert(names=[name], to="ISO2", not_found=None)
        if isinstance(code, list):
            code = code[0]
        if code and code != "not found":
            return "GB" if code == "UK" else code

    if pycountry is not None:
        try:
            result = pycountry.countries.search_fuzzy(_strip_accents(name))
            if result:
                return result[0].alpha_2
        except Exception:
            pass

    return None


def iso_from_slug_or_name(slug: str | None, country_name: str) -> str | None:
    if not slug:
        return "US"
    cc = slug.split("-")[0].upper()
    if cc == "UK":
        return "GB"
    if cc.lower() in REGIONAL_SLUGS:
        return iso_from_display_name(country_name)
    return cc


def get_country_entries(session: requests.Session) -> List[Dict[str, str]]:
    url = "https://www.apple.com/choose-country-region/"
    soup = BeautifulSoup(session.get(url, timeout=20).text, "html.parser")

    entries: Dict[str, Dict[str, str]] = {}

    for anchor in soup.select('a[href^="/"]'):
        name = anchor.get_text(strip=True)
        if not name:
            continue
        if is_region_name(name):
            continue

        href = anchor.get("href", "")
        slug = extract_slug(href)
        if not slug and href not in ("/", "/apple-one/"):
            continue

        iso = iso_from_slug_or_name(slug, name)
        if not iso:
            continue

        entries.setdefault(iso, {"name": name, "slug": slug or "", "iso": iso})

    entries.setdefault("US", {"name": "United States", "slug": "", "iso": "US"})
    return sorted(entries.values(), key=lambda d: d["name"].lower())


def normalize_amount(amount: str) -> str:
    amt = amount.replace("\xa0", " ")
    amt = re.sub(r"\s+", "", amt)

    if re.fullmatch(r"\d+\.\d{3}", amt):
        return amt.replace(".", "")
    if re.fullmatch(r"\d+,\d{3}", amt):
        return amt.replace(",", "")

    return amt.replace(",", ".")


PRICE_RE = re.compile(
    r"(?P<cur_before>[^\d\s]+)?\s*" r"(?P<amount>\d[\d\s\.,]*)" r"(?:\s*(?P<cur_after>[^\d\s]+))?"
)


def parse_currency_amount(text: str) -> Tuple[str, str] | None:
    match = PRICE_RE.search(text)
    if not match:
        return None
    amount = normalize_amount(match.group("amount"))
    currency = (match.group("cur_before") or match.group("cur_after") or "").strip()
    return amount, currency


def scrape_apple_one_prices(session: requests.Session, entries: List[Dict[str, str]]) -> pd.DataFrame:
    plan_classes = {
        "plan-individual": "Individual",
        "plan-family": "Family",
        "plan-premier": "Premier",
    }

    slug_cache: Dict[str, Dict[str, str]] = {}
    rows: List[Dict[str, str]] = []

    for entry in entries:
        name, slug, iso = entry["name"], entry["slug"], entry["iso"]
        url = f"https://www.apple.com/{slug}/apple-one/" if slug else "https://www.apple.com/apple-one/"

        if slug not in slug_cache:
            cached = {"Currency": "", "Individual": "", "Family": "", "Premier": ""}
            try:
                response = session.get(url, timeout=15)
                if response.status_code != 404:
                    soup = BeautifulSoup(response.text, "html.parser")
                    for css_class, label in plan_classes.items():
                        tag = soup.select_one(f"p.typography-plan-subhead.{css_class}") or soup.select_one(
                            f".{css_class}"
                        )
                        if not tag:
                            continue
                        raw = tag.get_text(" ", strip=True)
                        parsed = parse_currency_amount(raw)
                        if parsed:
                            amount, currency = parsed
                            cached[label] = amount
                            if not cached["Currency"] and currency:
                                cached["Currency"] = currency
            except requests.RequestException as ex:
                print(f"[warn] {iso} {url}: {ex}")
            slug_cache[slug] = cached
            time.sleep(random.uniform(*POLITE_DELAY))

        cached = slug_cache[slug]
        row = {
            "Country": name,
            "Country Code": iso,
            "Currency": cached["Currency"],
            "Individual": cached["Individual"],
            "Family": cached["Family"],
            "Premier": cached["Premier"],
        }

        if any([row["Individual"], row["Family"], row["Premier"]]) or KEEP_EMPTY_ROWS:
            rows.append(row)

    return pd.DataFrame(rows)


def run_apple_one_scraper(test_mode: bool = True, test_countries=None) -> str:
    session = make_session()
    entries = get_country_entries(session)

    if test_mode:
        countries = [c.upper() for c in test_countries] if test_countries else TEST_COUNTRY_CODES
        wanted = set(countries)
        entries = [e for e in entries if e.get("iso", "").upper() in wanted]

    df = scrape_apple_one_prices(session, entries)
    out_name = f"{OUT_BASENAME}_TEST.xlsx" if test_mode else f"{OUT_BASENAME}_all.xlsx"
    df.to_excel(out_name, index=False)
    return str(Path(out_name).resolve())


if __name__ == "__main__":
    path = run_apple_one_scraper(test_mode=True)
    print(f"Wrote {path}")
