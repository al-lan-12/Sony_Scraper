# dsp_scrapers/netflix_scraper.py

import asyncio
import re
from pathlib import Path
from typing import List, Dict, Any

from difflib import get_close_matches

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pycountry


# --- Helpers taken from your notebook ---------------------------------------
def _iso2_to_netflix_labels(all_countries, iso_codes):
    """Map ISO alpha-2 codes to Netflix country labels.

    The Netflix help centre exposes `window.netflix.data.article.allCountries`,
    which is a list of objects with a `label` field (the text shown in the
    country picker).  This helper converts ISO-2 codes coming from the UI
    into those labels using pycountry plus a small fuzzy-match fallback.
    """
    labels = []
    lower_map = {c.lower(): c for c in all_countries}

    for code in iso_codes or []:
        code = (code or "").upper()
        if not code:
            continue

        country = pycountry.countries.get(alpha_2=code)
        if not country:
            continue

        # Try common_name, official_name, then name
        candidates = []
        for attr in ("common_name", "official_name", "name"):
            val = getattr(country, attr, None)
            if val:
                candidates.append(val)

        chosen = None
        for cand in candidates:
            key = cand.lower()
            if key in lower_map:
                chosen = lower_map[key]
                break

        if not chosen and candidates:
            # Fuzzy match as a last resort
            match = get_close_matches(candidates[0], all_countries, n=1, cutoff=0.7)
            if match:
                chosen = match[0]

        if chosen and chosen not in labels:
            labels.append(chosen)

    return labels

def extract_price_details(price_text: str):
    """
    Split 'X CUR / month (note...)' into (currency, amount, note, raw_text).
    """
    if not price_text or "month" not in price_text.lower():
        return "Unknown", "", price_text, price_text

    text = price_text.strip()
    month_split = re.split(r"/\s*month", text, flags=re.IGNORECASE)
    price_part = month_split[0].strip()
    note_part = month_split[1].strip() if len(month_split) > 1 else ""

    number_match = re.search(r"([\d,.]+)", price_part)
    currency_match = re.search(r"([^\d\s,.]+)", price_part)

    amount = number_match.group(1).replace(",", "") if number_match else ""
    currency = currency_match.group(1) if currency_match else "Unknown"

    return currency, amount, note_part, text


async def process_country(country_label: str, page) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    try:
        await page.goto("https://help.netflix.com/en/node/24926", timeout=60000)
        await page.wait_for_timeout(2000)

        # Cookie banner
        try:
            await page.click("#onetrust-accept-btn-handler", timeout=3000)
        except Exception:
            pass

        # Open country selector
        await page.click("div.css-hlgwow", timeout=5000)
        input_box = await page.wait_for_selector('//input[@type="text"]')
        await input_box.fill("")
        await input_box.type(country_label)
        await input_box.press("Enter")
        await page.wait_for_timeout(3000)

        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        pricing_header = soup.find("h3", string=lambda s: s and "Pricing" in s)
        if pricing_header:
            ul = pricing_header.find_next("ul")
            if ul:
                for li in ul.find_all("li"):
                    if ":" in li.text:
                        plan, price_text = li.text.strip().split(":", 1)
                        currency, amount, note, raw = extract_price_details(price_text.strip())
                        results.append(
                            {
                                "Country": country_label,
                                "Plan": plan.strip(),
                                "Price_Display": raw,
                                "Currency": currency,
                                "Amount": amount,
                                "Note": note,
                            }
                        )

        if not results:
            results.append(
                {
                    "Country": country_label,
                    "Plan": "N/A",
                    "Price_Display": "N/A",
                    "Currency": "",
                    "Amount": "",
                    "Note": "",
                }
            )

        print(f"✅ {country_label}")
        return results

    except Exception as e:
        print(f"❌ Error: {country_label} — {e}")
        return [
            {
                "Country": country_label,
                "Plan": "ERROR",
                "Price_Display": str(e),
                "Currency": "",
                "Amount": "",
                "Note": "",
            }
        ]


# --- Main async runner -------------------------------------------------------

async def _run_netflix_async(test_mode: bool = True, test_countries=None) -> str:
    """Scrape Netflix pricing for all countries (or a subset in test mode).

    Returns
    -------
    str
        Absolute path to the created Excel file.
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()

        # First page just to get the list of countries
        page = await context.new_page()
        await page.goto("https://help.netflix.com/en/node/24926", timeout=60000)
        await page.wait_for_timeout(3000)

        try:
            await page.click("#onetrust-accept-btn-handler", timeout=3000)
        except Exception:
            pass

        countries_data = await page.evaluate("window.netflix.data.article.allCountries")
        all_countries = [entry["label"] for entry in countries_data]
        await page.close()

        if test_mode:
            suffix = "_TEST"
            if test_countries:
                # Try to honour the specific countries chosen in the UI
                countries = _iso2_to_netflix_labels(all_countries, test_countries)
                if not countries:
                    # Fallback: small fixed sample if mapping failed
                    countries = all_countries[:8]
            else:
                # Old behaviour: just take the first few countries as a demo
                countries = all_countries[:8]
        else:
            countries = all_countries
            suffix = ""

        print(f"🌍 Netflix: scraping {len(countries)} countries (test_mode={test_mode})")

        results: List[Dict[str, Any]] = []
        batch_size = 6

        for i in range(0, len(countries), batch_size):
            batch = countries[i : i + batch_size]
            tasks = []
            pages = []

            for country in batch:
                tab = await context.new_page()
                pages.append(tab)
                tasks.append(process_country(country, tab))

            batch_results = await asyncio.gather(*tasks)

            # Close all pages in this batch to avoid hitting limits
            for tab in pages:
                await tab.close()

            for r in batch_results:
                results.extend(r)

        await context.close()
        await browser.close()

        if not results:
            raise RuntimeError("Netflix scraper produced no rows – markup may have changed.")

        df = pd.DataFrame(results)
        out_name = f"netflix_pricing_by_country{suffix}.xlsx"
        out_path = Path(out_name).resolve()
        df.to_excel(out_path, index=False, engine="openpyxl")

        print(f"✅ Netflix: saved {out_path}")
        return str(out_path)


def run_netflix_scraper(test_mode: bool = True, test_countries=None) -> str:
    """Public wrapper used by the Streamlit app.

    Parameters
    ----------
    test_mode:
        If True, run in test mode (usually fewer countries).
    test_countries:
        Optional list of ISO alpha-2 codes (e.g. ["GB", "US"]) used
        only in test mode.
    """
    return asyncio.run(
        _run_netflix_async(test_mode=test_mode, test_countries=test_countries)
    )

