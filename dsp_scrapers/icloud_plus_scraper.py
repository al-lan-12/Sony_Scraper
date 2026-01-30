# dsp_scrapers/icloud_plus_scraper.py
"""
iCloud+ scraper used by the Streamlit app.

This version integrates the "combined" logic:
- Scrapes the regular iCloud+ pricing support article
- Scrapes the US-billed countries article with Playwright
- Expands Eurozone countries from the single "Euro" row
- Adds rows for countries that are only billed in USD (US_PRICING)
- Returns a single Excel file path, like the other scrapers.

Public API expected by the app:
    run_icloud_plus_scraper(test_mode: bool = True, test_countries=None) -> str
"""

import asyncio
import difflib
import math
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Set

import pandas as pd
import pycountry
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

URL_PRICING = "https://support.apple.com/en-gb/108047"
URL_USD_BILLED = "https://support.apple.com/en-us/111740"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}

# Base filename – app code just shows whatever we return
OUT_BASENAME = "icloud_plus_pricing"

# Eurozone countries with their ISO codes (for expanding the single "Euro" row)
EUROZONE_COUNTRIES = [
    ("Austria", "AT"), ("Belgium", "BE"), ("Croatia", "HR"), ("Cyprus", "CY"),
    ("Estonia", "EE"), ("Finland", "FI"), ("France", "FR"), ("Germany", "DE"),
    ("Greece", "GR"), ("Ireland", "IE"), ("Italy", "IT"), ("Latvia", "LV"),
    ("Lithuania", "LT"), ("Luxembourg", "LU"), ("Malta", "MT"), ("Netherlands", "NL"),
    ("Portugal", "PT"), ("Slovakia", "SK"), ("Slovenia", "SI"), ("Spain", "ES"),
]

EXPECTED_PLANS = ("50 GB", "200 GB", "2 TB", "6 TB", "12 TB")

# Official US pricing – used for countries that are billed in USD but
# do not appear in the regular pricing table at all.
US_PRICING: Dict[str, Dict[str, Any]] = {
    "50 GB": {"Price": 0.99, "Price_Display": "$0.99"},
    "200 GB": {"Price": 2.99, "Price_Display": "$2.99"},
    "2 TB": {"Price": 9.99, "Price_Display": "$9.99"},
    "6 TB": {"Price": 29.99, "Price_Display": "$29.99"},
    "12 TB": {"Price": 59.99, "Price_Display": "$59.99"},
}

# Default small sample for test mode when no explicit ISO list is given
TEST_ISO2_SAMPLE = ["GB", "US", "BR", "JP", "ZA"]


# ---------------------------------------------------------------------------
# Normalisation / helpers
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    """Normalise whitespace + odd unicode characters."""
    if not s:
        return ""
    s = (
        s.replace("\xa0", " ")
        .replace("\u202f", " ")
        .replace("\u2009", " ")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("\ufeff", "")
        .replace("\u00ad", "")
        .replace("\u00ac", " ")
    )
    s = s.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", s).strip()


def strip_accents(s: str) -> str:
    """Remove diacritics from string."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s or "") if unicodedata.category(c) != "Mn"
    )


def clean_country_name(name: str) -> str:
    """
    Clean country name by removing numbers, extra spaces, and standardising format.
    Also handles special cases and country name variations.
    """
    if not name:
        return ""

    # First do basic cleaning
    name = name.strip()

    # Remove any trailing numbers and punctuation
    name = re.sub(r"[\s]*[0-9]+[,\s]*$", "", name)
    name = re.sub(r"[,\\s]+$", "", name)

    # Normalize whitespace
    name = re.sub(r"\s+", " ", name)

    # Handle special cases and variations
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
        "democratic republic of congo": "Democratic Republic of the Congo",
        "dr congo": "Democratic Republic of the Congo",
        "republic of congo": "Congo",
        "ivory coast": "Côte d'Ivoire",
        "cote divoire": "Côte d'Ivoire",
        "eswatini": "Eswatini",
        "swaziland": "Eswatini",
        "macedonia": "North Macedonia",
        "micronesia": "Micronesia",
        "saint kitts": "Saint Kitts and Nevis",
        "nevis": "Saint Kitts and Nevis",
        "saint vincent": "Saint Vincent and the Grenadines",
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

    for key, value in name_map.items():
        if name_lower.startswith(key):
            return value

    return name


def get_country_iso_code(name: str) -> str:
    """Get ISO 3166-1 alpha-2 country code for a country name."""
    if not name:
        return ""

    # Direct lookup
    try:
        country = pycountry.countries.lookup(name)
        return country.alpha_2
    except LookupError:
        pass

    # Special-case mapping
    special_cases = {
        "vietnam": "VN",
        "viet nam": "VN",
        "laos": "LA",
        "russia": "RU",
        "brunei": "BN",
        "brunei darussalam": "BN",
        "macau": "MO",
        "taiwan": "TW",
        "palestine": "PS",
        "kosovo": "XK",
        "holy see": "VA",
        "vatican city": "VA",
        "bahamas": "BS",
        "congo": "CG",
        "democratic republic of the congo": "CD",
        "dr congo": "CD",
        "côte d'ivoire": "CI",
        "ivory coast": "CI",
        "cabo verde": "CV",
        "cape verde": "CV",
        "timor-leste": "TL",
        "east timor": "TL",
        "eswatini": "SZ",
        "swaziland": "SZ",
        "micronesia": "FM",
    }
    key = name.lower().strip()
    if key in special_cases:
        return special_cases[key]

    # Fuzzy matching as a last resort
    try:
        all_countries = [(c.name, c.alpha_2) for c in pycountry.countries]
        for c in pycountry.countries:
            if hasattr(c, "common_name"):
                all_countries.append((c.common_name, c.alpha_2))
            if hasattr(c, "official_name"):
                all_countries.append((c.official_name, c.alpha_2))

        best_ratio = 0.0
        best_code = None
        for country_name, code in all_countries:
            ratio = difflib.SequenceMatcher(None, name.lower(), country_name.lower()).ratio()
            if ratio > best_ratio and ratio > 0.85:
                best_ratio = ratio
                best_code = code

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

    token = m.group(0)

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


# ---------------------------------------------------------------------------
# Scraping helpers
# ---------------------------------------------------------------------------

async def get_us_billed_countries() -> Set[str]:
    """Get the list of US-billed countries using Playwright."""
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        try:
            await page.goto(URL_USD_BILLED, wait_until="networkidle", timeout=30000)
            await page.wait_for_selector("p", timeout=10000)
            html_content = await page.content()
        except Exception as e:
            print(f"Error fetching US-billed countries: {e}")
            return set()
        finally:
            await browser.close()

    soup = BeautifulSoup(html_content, "html.parser")
    countries: Set[str] = set()

    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if not text or len(text) > 200:
            continue

        # Skip obvious non-country paragraphs
        if any(
            word in text.lower()
            for word in ["support", "currency", "billed", "price", "north america", "europe", "asia"]
        ):
            continue

        parts = re.split(r"[,]|\sand\s|\sor\s", text)
        for part in parts:
            country = clean_country_name(part)
            if country and len(country.split()) <= 5:
                countries.add(country)

    return countries


def get_regular_pricing() -> List[Dict[str, Any]]:
    """Get regular iCloud+ pricing for all countries from the support article."""
    try:
        html = requests.get(URL_PRICING, headers=HEADERS, timeout=30).text
    except Exception as e:
        print(f"Error fetching pricing data: {e}")
        return []

    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, Any]] = []
    current_country = None
    current_currency = None
    current_plans: List[Dict[str, Any]] = []

    for p in soup.select("p.gb-paragraph") or soup.select("article p, main p"):
        text = norm(p.get_text(" ", strip=True))
        if not text:
            continue

        # Country / currency line
        m_country = re.match(r"^(.*?)\s*(?:\d+)?\s*\(([^)]+)\)", text)
        if m_country:
            if current_plans and current_country:
                for plan in current_plans:
                    rows.append(
                        {
                            "Country": clean_country_name(current_country),
                            "Currency": current_currency,
                            "Plan": plan["Plan"],
                            "Price": plan["Price"],
                            "Price_Display": plan["Price_Display"],
                        }
                    )

            current_country = m_country.group(1)
            current_currency = m_country.group(2).strip()
            current_plans = []
            continue

        # Plan / price line
        m_plan = re.match(r"(\d+)\s*(GB|TB)[^\d]+(.+)", text, re.IGNORECASE)
        if m_plan and current_country:
            current_plans.append(
                {
                    "Plan": f"{int(m_plan.group(1))} {m_plan.group(2).upper()}",
                    "Price": parse_numeric_price(m_plan.group(3)),
                    "Price_Display": m_plan.group(3).strip(),
                }
            )

    # Flush last country's plans
    if current_plans and current_country:
        for plan in current_plans:
            rows.append(
                {
                    "Country": clean_country_name(current_country),
                    "Currency": current_currency,
                    "Plan": plan["Plan"],
                    "Price": plan["Price"],
                    "Price_Display": plan["Price_Display"],
                }
            )

    return rows


async def _build_combined_dataframe() -> pd.DataFrame:
    """Core async workflow: fetch USD-billed list + regular pricing, then merge."""
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

    # First pass: collect the generic Euro-row pricing
    for row in regular_pricing:
        country = clean_country_name(row["Country"])
        if country.lower().strip() == "euro":
            euro_pricing[row["Plan"]] = {
                "Currency": row["Currency"],
                "Price": row["Price"],
                "Price_Display": row["Price_Display"],
            }

    # Expand Euro-row into all Eurozone countries
    if euro_pricing:
        for euro_country, euro_code in EUROZONE_COUNTRIES:
            for plan in EXPECTED_PLANS:
                if plan in euro_pricing:
                    pricing = euro_pricing[plan]
                    final_rows.append(
                        {
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
                        }
                    )
            seen_countries.add(euro_country)

    # Now process non-Euro countries from the regular article (keep their original pricing)
    for row in regular_pricing:
        country = clean_country_name(row["Country"])
        if country.lower().strip() == "euro":
            continue

        iso_code = get_country_iso_code(country)
        final_rows.append(
            {
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
            }
        )
        seen_countries.add(country)

    # Add any USD-billed countries that weren't in the regular pricing at all
    for country in us_billed:
        country = clean_country_name(country)
        if country not in seen_countries:
            iso_code = get_country_iso_code(country)
            for plan in EXPECTED_PLANS:
                final_rows.append(
                    {
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
                    }
                )

    df = pd.DataFrame(final_rows)
    df["Plan"] = pd.Categorical(df["Plan"], categories=EXPECTED_PLANS, ordered=True)
    df.sort_values(["Country", "Plan"], inplace=True, ignore_index=True)

    return df


# ---------------------------------------------------------------------------
# Public API for the app
# ---------------------------------------------------------------------------

def run_icloud_plus_scraper(test_mode: bool = True, test_countries=None) -> str:
    """
    Run the combined iCloud+ scraper and return the Excel file path.

    Parameters
    ----------
    test_mode:
        True  -> return only a subset of countries
        False -> return all countries.
    test_countries:
        Optional list of ISO alpha-2 codes (e.g. ["GB", "US"]) used
        only in test mode.  If None in test mode, we fall back to a
        small built-in demo subset.
    """
    df = asyncio.run(_build_combined_dataframe())

    if test_mode:
        if test_countries:
            wanted = {str(code or "").strip().upper() for code in test_countries if code}
            if wanted:
                df = df[df["Country_Code_2"].str.upper().isin(wanted)].copy()
            # Fallback if nothing matched
            if df.empty:
                df = df[df["Country_Code_2"].str.upper().isin(TEST_ISO2_SAMPLE)].copy()
        else:
            df = df[df["Country_Code_2"].str.upper().isin(TEST_ISO2_SAMPLE)].copy()

    if df.empty:
        raise RuntimeError("iCloud+ scraper ended up with 0 rows after filtering.")

    suffix = "_TEST" if test_mode else "_all"
    out_path = Path(f"{OUT_BASENAME}{suffix}.xlsx").resolve()

    # Simple metrics sheet, similar to the standalone combined script
    original_countries = df[df["Price_Source"] == "original"]["Country"].nunique()
    us_billed_countries = df[df["Price_Source"].isin(["us-billed", "us-billed-only"])]["Country"].nunique()
    changed_rows = df[df["Original_Price"].notna()].shape[0]

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="iCloud+ Prices", index=False)

        meta_data = {
            "Metric": [
                "Total Rows",
                "Total Countries",
                "Countries with Original Pricing",
                "Countries with US Pricing",
                "Countries with Price Changes",
            ],
            "Value": [
                len(df),
                df["Country"].nunique(),
                original_countries,
                us_billed_countries,
                changed_rows,
            ],
        }
        pd.DataFrame(meta_data).to_excel(writer, sheet_name="_Meta", index=False)

    print(f"[✅ iCloud+] Saved {out_path}")
    return str(out_path)


if __name__ == "__main__":
    # Quick manual test: python -m dsp_scrapers.icloud_plus_scraper
    print(run_icloud_plus_scraper(test_mode=True))
