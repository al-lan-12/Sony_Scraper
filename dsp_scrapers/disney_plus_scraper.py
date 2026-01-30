#!/usr/bin/env python3
"""
Disney+ Prices — Multilingual scraper with robust navigation & per-price expansion
+ Deterministic English plan naming (multilingual regex)
+ Robust Excel saving (absolute path, engine fallbacks, CSV fallback)
+ Fuzzy country matching (picker & ISO-2 mapping) -> CANONICAL EN country in output
+ Country-FIRST currency disambiguation (no locale bleed; $/kr/₨/R resolved by country)
+ Saint/Sainte normalization and aliases (e.g., ST. LUCIA -> LC)
+ Türkiye/TR/TRY handled
+ Safer page-currency sniffing (only trusts repeated explicit ISO-3 on page)
+ **Fix:** United States no longer maps to United Kingdom (ISO-aware selection; no dropping 'states/kingdom')

Output columns:
    country_name, country_iso2, locale_used, url,
    plan_en, plan_en_canonical,
    price_text_full, price_text_fragment,
    price_value, currency_iso3, billing_period
"""

from __future__ import annotations
import os
import re
import json
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
from collections import Counter

import pycountry
from unidecode import unidecode
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, Locator, TimeoutError as PWTimeout
import pandas as pd

# ========= FILE LOCATIONS / SETTINGS =========
BASE_DIR = Path(__file__).resolve().parent
EXCEL_PATH = (BASE_DIR / "disney_prices_enriched.xlsx")
JSON_PATH  = (BASE_DIR / "disney_prices_enriched.json")  # only if SAVE_JSON=True
OUTDIR     = (BASE_DIR / "disney_debug")
OUTDIR.mkdir(exist_ok=True)

# Toggle enriched JSON output. Default OFF so you only get Excel.
SAVE_JSON = False

# ========== USER SETTINGS ==========
MODE       = "full"      # "test" or "full"
HEADLESS   = True        # True = faster/more reliable; set False to watch the browser
BROWSER    = "chromium"  # chromium / firefox / webkit
START_URL  = "https://help.disneyplus.com/en-GB/article/disneyplus-price"

# Filled by run_disney_scraper() when the app is in test mode; used to
# carry through the user-selected ISO alpha-2 country codes.
SELECTED_ISO2: List[str] = []
# ========


# Try RapidFuzz for better fuzzy matching
try:
    from rapidfuzz import process, fuzz
    _RF = True
except Exception:
    _RF = False

# ---------------------- SAVE EXCEL ----------------------
def save_excel_robust(df: pd.DataFrame, out_path: Path):
    """Save to Excel; try default engine, then specific engines, then CSV fallback."""
    out_path = out_path.resolve()

    def _try_save_excel(path: Path, engine: Optional[str] = None):
        if engine:
            df.to_excel(path, index=False, engine=engine)
        else:
            df.to_excel(path, index=False)

    try:
        _try_save_excel(out_path, None)
        print(f"Saved Excel -> {out_path}")
        return
    except PermissionError:
        ts_path = out_path.with_name(f"{out_path.stem}_{int(time.time())}{out_path.suffix}")
        try:
            _try_save_excel(ts_path, None)
            print(f"[WARN] Original Excel was locked. Saved copy -> {ts_path}")
            return
        except Exception as e:
            last_err = e
    except Exception as e:
        last_err = e

    tried = []
    for eng in ("openpyxl", "xlsxwriter"):
        try:
            _try_save_excel(out_path, eng)
            print(f"Saved Excel -> {out_path} (engine={eng})")
            return
        except Exception as e:
            tried.append((eng, f"{e.__class__.__name__}: {e}"))

    csv_path = out_path.with_suffix(".csv")
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        msg = "; ".join([f"{eng} failed ({err})" for eng, err in tried]) or repr(last_err)
        print(f"[WARN] Excel save failed ({msg}). Saved CSV fallback -> {csv_path}")
    except Exception as e2:
        alt_path = out_path.with_name(out_path.stem + f"_{int(time.time())}.csv")
        df.to_csv(alt_path, index=False, encoding="utf-8-sig")
        print(f"[WARN] Excel & CSV encountered issues ({e2}). Wrote -> {alt_path}")

# ---------------------- FUZZY HELPERS ----------------------
# IMPORTANT: do NOT drop 'state(s)' or 'kingdom' – keeps 'United States' vs 'United Kingdom' distinct.
_WORDS_TO_DROP = {
    "the","of","and","republic","federal","democratic",
    "people","commonwealth","union","arab","emirates","islamic","plurinational"
    # 'kingdom','state','states' intentionally omitted
}
def _norm_for_match(s: str) -> str:
    s = unidecode((s or "")).lower()
    # Normalize Saint/Sainte & abbreviations (st., st, ste., ste)
    s = re.sub(r"\bste[.\s]+", "sainte ", s)
    s = re.sub(r"\bst[.\s]+",  "saint ",  s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = " ".join(w for w in s.split() if w not in _WORDS_TO_DROP)
    return s.strip()

def closest_text(target: str, options: List[str], cutoff: int = 88) -> Optional[str]:
    """Return the option whose normalized form is closest to target (0..100 cutoff)."""
    if not options: return None
    t = _norm_for_match(target)
    pool = [(_norm_for_match(o), o) for o in options]

    if _RF:
        cand = process.extractOne(t, [p[0] for p in pool], scorer=fuzz.WRatio)
        if cand:
            text_norm, score, _ = cand
            if score >= cutoff:
                for n, orig in pool:
                    if n == text_norm:
                        return orig
        return None

    best = max(((SequenceMatcher(None, t, n).ratio(), orig) for n, orig in pool), key=lambda x: x[0])
    return best[1] if best[0] * 100 >= cutoff else None

# ---------------------- UTILS ----------------------
def _clean(x: str) -> str:
    return re.sub(r"\s+", " ", x or "").strip()
def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unidecode(name or "").lower())

def canonical_country_from_iso2(iso2: str) -> str:
    """Return canonical English country name (prefer common_name > name)."""
    try:
        c = pycountry.countries.get(alpha_2=iso2.upper())
        if not c: return iso2
        return getattr(c, "common_name", c.name)
    except Exception:
        return iso2

# ---------------------- PRICE PATTERNS ----------------------
try:
    ISO_CODES = {c.alpha_3 for c in pycountry.currencies}
except Exception:
    ISO_CODES = set()

# Currency symbols we consider as "strong" tokens.
# NOTE: we intentionally EXCLUDE plain 'R' (too ambiguous) and rely on region (ZA) instead.
CURRENCY_SYMBOLS = {
    "$","€","£","¥","₩","₹","₽","₺","₱","₦","₫","₴","₭","฿","₲","₵","₡","₸","₪","₨","₮","؋","៛","﷼","¢","ƒ",
    "A$","C$","HK$","NT$","NZ$","R$","S$","US$","AU$","CA$","MX$","JP¥","￥"
}
LOCAL_SUFFIXES = {
    "ft","tl","zł","zl","kč","kc","lei","дин","дин.","ден","ден.","руб","лв","lв","лв.","грн",
    "so'm","som","сом","ريال","جنيه","dh","mad","sar","qar","omr","kwd","aed",
    "¥","円","元","kr","kr.","bs.","bs","r$","s$","hk$","a$","c$","us$","nz$","ca$","au$","nt$","rm","s/","s/."
}.union({s for s in CURRENCY_SYMBOLS})

SEP_SPACES = "\u00A0\u202F"

PRICE_NUMBER_RE = re.compile(
    r"(?<!\w)"
    r"(?:\d{1,3}(?:[ ,\.\u00A0\u202F'’]\d{3})+|\d+)"
    r"(?:[.,]\d{1,2})?"
    r"(?!\w)"
)

def _strip_space_seps(s: str) -> str:
    return (s.replace(" ", "").replace("\u00A0","").replace("\u202F","")
             .replace("’","").replace("'",""))

def parse_number_locale_agnostic(num_str: str) -> Optional[float]:
    s = num_str.strip().replace("\u202F", " ").replace("\u00A0", " ").replace("’", "'")
    has_dot, has_com = "." in s, "," in s
    if has_dot and has_com:
        last_dot, last_com = s.rfind("."), s.rfind(",")
        last_idx = max(last_dot, last_com)
        after = s[last_idx+1:]
        if re.fullmatch(r"\d{1,2}", after):
            main = _strip_space_seps(s[:last_idx].replace(",", "").replace(".", ""))
            try: return float(f"{int(main)}.{after}")
            except: return None
        main = _strip_space_seps(s.replace(".", "").replace(",", ""))
        try: return float(main)
        except: return None
    if has_com and not has_dot:
        parts = s.rsplit(",", 1)
        if len(parts)==2 and re.fullmatch(r"\d{1,2}", parts[1]):
            main = _strip_space_seps(parts[0]).replace(",", "")
            try: return float(f"{int(main)}.{parts[1]}")
            except: return None
        main = _strip_space_seps(s).replace(",", "")
        try: return float(main)
        except: return None
    if has_dot and not has_com:
        parts = s.rsplit(".", 1)
        if len(parts)==2 and re.fullmatch(r"\d{1,2}", parts[1]):
            main = _strip_space_seps(parts[0]).replace(".", "")
            try: return float(f"{int(main)}.{parts[1]}")
            except: return None
        main = _strip_space_seps(s).replace(".", "")
        try: return float(main)
        except: return None
    try:
        return float(_strip_space_seps(s))
    except:
        return None

def _is_currency_symbol(ch: str) -> bool:
    return unicodedata.category(ch) == "Sc" or ch in CURRENCY_SYMBOLS

def _scan_currency_tokens(text: str) -> List[Tuple[str, int, int]]:
    tokens: List[Tuple[str,int,int]] = []
    # symbols and prefixed currency markers
    for i in range(len(text)):
        ch = text[i]
        if _is_currency_symbol(ch):
            tokens.append((ch, i, i+1))
    # 3-letter ISO codes (strong)
    for m in re.finditer(r"\b([A-Z]{3})\b", text):
        code = m.group(1).upper()
        if code in ISO_CODES:
            tokens.append((code, m.start(), m.end()))
    # prefixed like US$, AU$, HK$, R$, NT$, etc.
    for m in re.finditer(r"\b([A-Z]{1,3}\$)\b", text):
        tokens.append((m.group(1).upper(), m.start(), m.end()))
    # select local textual suffixes that clearly indicate currency
    for m in re.finditer(r"\b[0-9a-zA-Z\u00A1-\uFFFF]{1,4}\b", text):
        tok = m.group(0).strip()
        low = tok.lower().strip(".")
        if low in LOCAL_SUFFIXES:
            tokens.append((tok, m.start(), m.end()))
    return tokens

def _nearest_currency_around(text: str, start: int, end: int) -> Optional[Tuple[str,int]]:
    left_win, right_win = max(0, start-24), min(len(text), end+24)
    around = text[left_win:right_win]
    candidates = _scan_currency_tokens(around)
    if not candidates: return None
    numL, numR = start - left_win, end - left_win
    best = None
    for tok, s, e in candidates:
        dist = 0 if not (e < numL or s > numR) else min(abs(e - numL), abs(s - numR))
        if e <= numL: dist -= 2
        elif s >= numR: dist -= 1
        if (best is None) or (dist < best[0]): best = (dist, tok)
    return (best[1], max(best[0], 0)) if best else None

# ======= Billing period detection (expanded multilingual) =======
MONTH_TOKENS_RE = re.compile(
    r"(?:" +
    r"\bmensual(?:mente)?\b|\bmensile\b|\bmensuel\b|\bmonatlich\b|\bmonthly\b|per\s*month|/month|" +
    r"\bal\s*mes\b|\bpor\s*mes\b|\bpor\s*m[eê]s\b|\bper\s*mese\b|" +
    r"\bm[eė]si[eę]cznie\b|\bkuukaudessa\b|\bayl[iı]k\b|" +
    r"月額|毎月|/月|per\s*mois|par\s*mois" +
    r")", re.I
)
YEAR_TOKENS_RE  = re.compile(
    r"(?:" +
    r"\banual(?:es)?\b|\bannual(?:ly)?\b|\bannuel\b|\bj[aä]hrlich\b|per\s*year|/year|" +
    r"\bal\s*a[nñ]o\b|\bpor\s*a[nñ]o\b|\bpor\s*ano\b|\bper\s*anno\b|" +
    r"\brocznie\b|\bv[eė]sia[ik]s\b|\byearly\b|" +
    r"年額|年間|/年" +
    r")", re.I
)

def _nearest_label_by_proximity(text: str, s: int, e: int, win: int = 120) -> Optional[str]:
    winL, winR = max(0, s-win), min(len(text), e+win)
    window = text[winL:winR].lower()
    center = (s + e) / 2.0
    month_pos = [m.start()+winL for m in MONTH_TOKENS_RE.finditer(window)]
    year_pos  = [m.start()+winL for m in YEAR_TOKENS_RE.finditer(window)]
    if not month_pos and not year_pos: return None
    if month_pos and not year_pos: return "Monthly"
    if year_pos and not month_pos: return "Annual"
    d_month = min(abs(p - center) for p in month_pos) if month_pos else 1e9
    d_year  = min(abs(p - center) for p in year_pos)  if year_pos  else 1e9
    if d_month < d_year: return "Monthly"
    if d_year  < d_month: return "Annual"
    return None

def detect_period_from_context(text: str, start: int, end: int) -> str:
    prox = _nearest_label_by_proximity(text, start, end, win=120)
    return prox or "Unknown"

# Region→currency map (primary disambiguation; never fallback to locale)
REGION_TO_CURRENCY = {
    "US":"USD","GB":"GBP","IE":"EUR","DE":"EUR","FR":"EUR","ES":"EUR","IT":"EUR","NL":"EUR","PT":"EUR","BE":"EUR",
    "AT":"EUR","FI":"EUR","EE":"EUR","LV":"EUR","LT":"EUR","SK":"EUR","SI":"EUR","GR":"EUR","CY":"EUR","MT":"EUR",
    "PL":"PLN","CZ":"CZK","HU":"HUF","RO":"RON","BG":"BGN","HR":"EUR",
    "SE":"SEK","DK":"DKK","NO":"NOK","IS":"ISK",
    "CH":"CHF",
    "JP":"JPY","KR":"KRW","TW":"TWD","HK":"HKD","SG":"SGD","MY":"MYR","ID":"IDR","PH":"PHP","TH":"THB","VN":"VND",
    "AU":"AUD","NZ":"NZD","CA":"CAD",
    "BR":"BRL","MX":"MXN","AR":"ARS","CL":"CLP","CO":"COP","PE":"PEN","UY":"UYU","VE":"VES","BO":"BOB",
    "TR":"TRY","AE":"AED","SA":"SAR","QA":"QAR","OM":"OMR","KW":"KWD","BH":"BHD","EG":"EGP","MA":"MAD",
    "IL":"ILS","IN":"INR","PK":"PKR","BD":"BDT","LK":"LKR","NP":"NPR","MU":"MUR","MV":"MVR","SC":"SCR",
    "ZA":"ZAR","NG":"NGN","KE":"KES","GH":"GHS","TZ":"TZS","UG":"UGX","CI":"XOF","SN":"XOF","CM":"XAF",
    # Caribbean additions (esp. for Saint Lucia)
    "TT":"TTD","BB":"BBD","BS":"BSD","JM":"JMD",
    "LC":"XCD","VC":"XCD","GD":"XCD","DM":"XCD","AG":"XCD","AI":"XCD","KN":"XCD","MS":"XCD"
}

# --------- Page currency inference (safe) ----------
def infer_page_currency(html_text: str,
                        country_iso2: Optional[str]) -> Optional[str]:
    """
    Prefer country currency. Only override if there is a repeated explicit ISO-3 signal.
    """
    default = REGION_TO_CURRENCY.get((country_iso2 or "").upper())

    if not html_text:
        return default

    iso_hits = [m.group(1).upper() for m in re.finditer(r"\b([A-Z]{3})\b", html_text) if m.group(1).upper() in ISO_CODES]
    if iso_hits:
        counts = Counter(iso_hits)
        if len(counts) == 1 and next(iter(counts.values())) >= 2:
            return next(iter(counts.keys()))
    return default

def extract_all_prices(text: str,
                       page_currency_hint: Optional[str] = None) -> List[Tuple[Optional[float], Optional[str], int, int]]:
    out = []
    for m in PRICE_NUMBER_RE.finditer(text):
        raw_num = m.group(0)
        val = parse_number_locale_agnostic(raw_num)
        if val is None: continue
        near_cur = _nearest_currency_around(text, m.start(), m.end())
        has_tight_currency = False
        tok_clean = None
        if near_cur:
            tok, dist = near_cur
            if (tok.upper() in ISO_CODES) or _is_currency_symbol(tok[:1]) or re.fullmatch(r"[A-Z]{1,3}\$", tok):
                tok_clean = tok
            has_tight_currency = (near_cur[1] <= 3)
        is_year_like = False
        try:
            iv = int(str(val)); is_year_like = (float(iv) == val and 1900 <= iv <= 2100)
        except: pass
        if is_year_like and not has_tight_currency:
            continue
        cur = tok_clean.strip() if tok_clean else (page_currency_hint or None)
        out.append((val, cur, m.start(), m.end()))
    return out

# ---------------------- SMART ENGLISH NAMING ----------------------
TIER_PATTERNS = {
    "Premium": re.compile(r"\b(premium|premiun|pr[eé]mium|prämiu?m|премиум|プレミアム|高级|高級)\b"),
    "Standard": re.compile(r"\b(standard|estandar|est[aá]ndar|standardowy|στάνταρ|スタンダード)\b"),
    "Extra Member": re.compile(r"\b(extra\s+member|miembro\s+extra|membre\s+extra|membro\s+extra)\b"),
}
ADS_WITH_RE  = re.compile(r"(with\s+ads|con\s+anuncios|mit\s+werbung|avec\s+(pub|publicit[eé]s?)|con\s+pubblicit[aà]|com\s+an[uú]ncios|z\s+reklam)", re.I)
ADS_WITHOUT_RE = re.compile(r"(without\s+ads|no\s+ads|sin\s+anuncios|ohne\s+werbung|sans\s+(pub|publicit[eé]s?)|senza\s+pubblicit[aà]|sem\s+an[uú]ncios|bez\s+reklam)", re.I)

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", unidecode((s or "").strip()).lower())

def detect_tier_english(plan_raw: str) -> str:
    n = _norm(plan_raw)
    for tier, pat in TIER_PATTERNS.items():
        if pat.search(n):
            return tier
    return "Standard"

def detect_ads_flag(*texts: str) -> Optional[str]:
    blob = " ".join([t for t in texts if t]).strip()
    if not blob:
        return None
    if ADS_WITHOUT_RE.search(blob):
        return "Without Ads"
    if ADS_WITH_RE.search(blob):
        return "With Ads"
    return None

def canonical_plan_english(plan_raw: str, price_text_full: str, price_text_fragment: str) -> str:
    tier = detect_tier_english(plan_raw)
    ads = detect_ads_flag(plan_raw, price_text_full, price_text_fragment) or "Unspecified"
    if ads == "With Ads":
        return f"Disney+ {tier} With Ads"
    if ads == "Without Ads":
        return f"Disney+ {tier} Without Ads"
    return f"Disney+ {tier}"

def _is_english_locale(loc: Optional[str]) -> bool:
    return str(loc or "").lower().startswith("en-")

# ---------------------- EXPANSION ----------------------
def expand_prices_into_rows(base_row: Dict,
                            page_currency_hint: Optional[str]) -> List[Dict]:
    text = base_row["price_text_full"]
    matches = extract_all_prices(text, page_currency_hint=page_currency_hint)

    if not matches:
        r = dict(base_row)
        r.update({
            "price_text_fragment": text,
            "price_value": None,
            "currency": page_currency_hint,
            "billing_period": "Unknown",
            "detected_language": "unknown",
        })
        return [r]

    frag_lower = text.lower()
    frag_has_month = bool(MONTH_TOKENS_RE.search(frag_lower))
    frag_has_year  = bool(YEAR_TOKENS_RE.search(frag_lower))
    frag_label = "Monthly" if (frag_has_month and not frag_has_year) else \
                 "Annual"  if (frag_has_year  and not frag_has_month) else None

    expanded: List[Dict] = []
    for (val, cur, s, e) in matches:
        period = detect_period_from_context(text, s, e)
        if period == "Unknown" and frag_label:
            period = frag_label
        frag = _clean(text[max(0, s-40):min(len(text), e+40)])
        r = dict(base_row)
        r.update({
            "price_text_fragment": frag,
            "price_value": val,
            "currency": cur or page_currency_hint,
            "billing_period": period,
            "detected_language": "unknown",
        })
        expanded.append(r)

    # Heuristic for monthly/annual pair
    amb = [r for r in expanded if r["billing_period"] == "Unknown"]
    if len(expanded) == 2 and len(amb) >= 1:
        a, b = expanded[0]["price_value"], expanded[1]["price_value"]
        try: ratio = max(a, b) / min(a, b) if (a and b and min(a,b) > 0) else None
        except: ratio = None
        if ratio and 9.0 <= ratio <= 13.5:
            big_idx = 0 if a >= b else 1; sml_idx = 1 - big_idx
            if expanded[big_idx]["billing_period"] == "Unknown": expanded[big_idx]["billing_period"] = "Annual"
            if expanded[sml_idx]["billing_period"] == "Unknown": expanded[sml_idx]["billing_period"] = "Monthly"

    return expanded

def parse_article_html(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict] = []
    # 1) Prefer tables
    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) < 2: continue
            plan = _clean(tds[0].get_text(" ", strip=True))
            price_cell = _clean(tds[-1].get_text(" ", strip=True))
            if PRICE_NUMBER_RE.search(price_cell):
                rows.append({"plan": plan, "price_text_full": price_cell})
    # 2) Fallback to list items
    if not rows:
        for li in soup.find_all("li"):
            blob = _clean(li.get_text(" ", strip=True))
            if PRICE_NUMBER_RE.search(blob):
                rows.append({"plan": "Unknown", "price_text_full": blob})
    # 3) Last fallback to block elements
    if not rows:
        for el in soup.find_all(["p","div","section","article"]):
            blob = _clean(el.get_text(" ", strip=True))
            if PRICE_NUMBER_RE.search(blob):
                rows.append({"plan": "Unknown", "price_text_full": blob})
    out, seen = [], set()
    for r in rows:
        key = (r["plan"], r["price_text_full"])
        if key not in seen:
            seen.add(key)
            r["plan_normalized"] = r["plan"].replace("Disney+ ", "").strip()
            out.append(r)
    return out

# ---------------------- Page interaction helpers ----------------------
def close_cookies(page: Page):
    for sel in [
        'button:has-text("Accept All")',
        '#onetrust-accept-btn-handler',
        'button[aria-label="Accept All"]',
        'text="Accept All"'
    ]:
        try:
            b = page.locator(sel)
            if b.count() and b.first.is_visible():
                print("  [info] accepting cookies…")
                b.first.click()
                page.wait_for_timeout(500)
                break
        except Exception:
            pass

def goto_relaxed(page: Page, url: str, timeout_ms: int = 20000) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        try:
            page.goto(url, timeout=timeout_ms)
        except Exception:
            return False
    try:
        page.wait_for_selector("article, main, table, h1", timeout=7000)
    except Exception:
        pass
    return True

def scroll_to_footer(page: Page):
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(500)
    page.keyboard.press("End")
    page.wait_for_timeout(400)

def discover_pickers(page: Page):
    scroll_to_footer(page)
    candidates = page.locator("button[aria-haspopup='listbox'], [role='combobox']")
    if candidates.count() == 0:
        candidates = page.locator("button, [role='button']")
    country_btn = language_btn = None
    country_list: List[str] = []

    print(f"  [info] probing {candidates.count()} candidate pickers…")
    for i in range(min(60, candidates.count())):
        trig = candidates.nth(i)
        try:
            trig.scroll_into_view_if_needed()
            trig.click()
            lb = page.get_by_role("listbox")
            for _ in range(30):
                if lb.count() and lb.locator("[role='option'], li").count() > 0:
                    break
                page.wait_for_timeout(150)
            opts = lb.locator("[role='option'], li, .slds-listbox__option, [data-value]")
            texts = []
            for el in opts.all():
                try:
                    t = re.sub(r"\s+", " ", el.inner_text().strip())
                    if t and "Selected" not in t:
                        texts.append(t)
                except Exception:
                    pass
            page.keyboard.press("Escape")
            if not texts: continue
            if (not country_btn) and (len(texts) > 40 or any("Afghanistan" in t for t in texts)):
                country_btn = trig; country_list = list(dict.fromkeys(texts)); continue
            if (not language_btn) and any("English" in t for t in texts):
                language_btn = trig; continue
            if country_btn and language_btn: break
        except Exception:
            try: page.keyboard.press("Escape")
            except: pass
            continue

    if not country_btn or not language_btn:
        page.screenshot(path=str(OUTDIR / "could_not_find_pickers.png"))
        raise RuntimeError("Could not find country/language pickers")

    return country_btn, language_btn, country_list

def select_from_list_by_normalized_text(page: Page, trigger: Locator, target_label: str) -> Tuple[bool, Optional[str]]:
    """
    Open a combobox/listbox and select the option that resolves to the SAME ISO-2 as target_label.
    If no option resolves to the same ISO-2, **do not** pick a 'closest' country; return (False, None).
    Returns (success, clicked_label_text).
    """
    page.wait_for_timeout(150)
    trigger.scroll_into_view_if_needed()
    trigger.click()
    lb = page.get_by_role("listbox")
    page.wait_for_timeout(250)

    opts = lb.locator("[role='option'], li, .slds-listbox__option, [data-value]")
    els = opts.all()
    if not els:
        page.keyboard.press("Escape")
        return False, None

    items: List[Tuple[str, Locator]] = []
    for el in els:
        try:
            raw = el.inner_text().strip()
            if raw:
                items.append((raw, el))
        except Exception:
            pass

    # 1) Resolve ISO-2 for the target (handles aliases like USA, U.S.A., etc.)
    target_iso2 = country_to_iso2_fuzzy(target_label)

    # 2) Prefer options whose own ISO-2 equals the target ISO-2
    iso_matches: List[Tuple[str, Locator]] = []
    for raw, loc in items:
        try:
            if country_to_iso2_fuzzy(raw) == target_iso2:
                iso_matches.append((raw, loc))
        except Exception:
            pass

    if iso_matches:
        # exact string (case/space/punct-insensitive) first, then just take the first ISO match
        norm_target = re.sub(r"\s+", " ", target_label.strip().lower())
        for raw, loc in iso_matches:
            if re.sub(r"\s+", " ", raw.strip().lower()) == norm_target:
                try:
                    loc.scroll_into_view_if_needed()
                    loc.click(force=True, timeout=1500)
                    page.wait_for_timeout(150)
                    page.keyboard.press("Escape")
                    return True, raw
                except Exception:
                    pass
        # fallback to first ISO match
        raw, loc = iso_matches[0]
        try:
            loc.scroll_into_view_if_needed()
            loc.click(force=True, timeout=1500)
            page.wait_for_timeout(150)
            page.keyboard.press("Escape")
            return True, raw
        except Exception:
            pass

    # 3) If there is no ISO-equal option, don't risk a wrong country.
    #    Only if there is a VERY high-confidence fuzzy match do we accept it.
    want_label = closest_text(target_label, [raw for raw, _ in items], cutoff=94)
    if not want_label:
        page.keyboard.press("Escape")
        return False, None

    el = next(loc for raw, loc in items if raw == want_label)
    try: el.scroll_into_view_if_needed()
    except Exception: pass

    for _ in range(4):
        try:
            el.click(force=True, timeout=1500)
            page.wait_for_timeout(150)
            page.keyboard.press("Escape")
            return True, want_label
        except Exception:
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(150)

    page.keyboard.press("Escape")
    return False, None

# ---------------------- NORMALIZATION HELPERS (ISO CODES) ----------------------
CURRENCY_MAP = {
    "$":"USD","US$":"USD","$US":"USD","MX$":"MXN","$MXN":"MXN","AR$":"ARS","CLP$":"CLP",
    "€":"EUR",
    "£":"GBP",
    "¥":"JPY","JP¥":"JPY","¥JP":"JPY","CNY":"CNY","￥":"JPY",
    "₩":"KRW",
    "₹":"INR",
    "₽":"RUB","руб":"RUB","руб.":"RUB",
    "₺":"TRY","TL":"TRY",
    "₱":"PHP",
    "₦":"NGN",
    "₫":"VND",
    "₴":"UAH","грн":"UAH",
    "฿":"THB",
    "₲":"PYG",
    "₵":"GHS",
    "₡":"CRC",
    "₸":"KZT",
    "₪":"ILS",
    "₨":"PKR",   # generic rupee sign -> refined with region below
    "₮":"MNT",
    "A$":"AUD","AU$":"AUD",
    "C$":"CAD","CA$":"CAD",
    "HK$":"HKD",
    "NT$":"TWD",
    "NZ$":"NZD",
    "R$":"BRL",
    "S$":"SGD",
    "RM":"MYR",
    "S/":"PEN","S/.":"PEN",
    "KČ":"CZK","Kč":"CZK","KC":"CZK","Kc":"CZK",
    "ZŁ":"PLN","Zł":"PLN","zł":"PLN","zl":"PLN",
    "LEI":"RON","Lei":"RON","lei":"RON",
    "FT":"HUF","Ft":"HUF","ft":"HUF",
    "ЛВ":"BGN","лв":"BGN","лв.":"BGN",
    "ДЕН":"MKD","ден":"MKD","ден.":"MKD",
    # 'R' intentionally omitted (ambiguous) – resolved by ZA region only
}

SPECIAL_COUNTRY_ALIASES = {
    "UK": "GB",
    "UAE": "AE",
    "Côte d’Ivoire": "CI",
    "Cote d'Ivoire": "CI",
    "Ivory Coast": "CI",
    "Bolivia": "BO",
    "Congo (DRC)": "CD",
    "Congo, The Democratic Republic of the": "CD",
    "Congo": "CG",
    "Moldova": "MD",
    "Palestine": "PS",
    "Vatican City": "VA",
    "Russia": "RU",
    "Syria": "SY",
    "Laos": "LA",
    "Macau": "MO",
    "Hong Kong": "HK",
    "North Macedonia": "MK",
    "South Korea": "KR",
    "South Sudan": "SS",
    "Taiwan": "TW",
    "Tanzania": "TZ",
    "Venezuela": "VE",
    "Vietnam": "VN",
    # USA variants
    "USA": "US",
    "U.S.": "US",
    "U.S.A.": "US",
    "United States": "US",
    "United States of America": "US",
    # UK explicit (avoid confusion)
    "United Kingdom": "GB",
    # Czechia
    "Czechia": "CZ",
    # Türkiye variants
    "Türkiye": "TR",
    "Turkiye": "TR",
    "Turkey": "TR",
    # Saint Lucia variants (picker commonly shows "ST. LUCIA")
    "Saint Lucia": "LC",
    "St Lucia": "LC",
    "St. Lucia": "LC",
    "ST. LUCIA": "LC",
}

# Build fuzzy choices from pycountry + aliases
_COUNTRY_CHOICES: List[Tuple[str, str]] = []
for c in pycountry.countries:
    names = {c.name}
    for attr in ("official_name", "common_name"):
        if hasattr(c, attr):
            names.add(getattr(c, attr))
    for nm in names:
        _COUNTRY_CHOICES.append((c.alpha_2, nm))
for alias, iso2 in [(k, v) for k, v in SPECIAL_COUNTRY_ALIASES.items()]:
    _COUNTRY_CHOICES.append((iso2, alias))

def country_to_iso2_fuzzy(name: str, cutoff: int = 88) -> str:
    """Return ISO-2 for a (possibly messy) country label; keeps original on failure."""
    if not name:
        return name
    if isinstance(name, str) and len(name) == 2 and name.isalpha():
        return name.upper()
    # 1) direct lookup fast path
    try:
        return pycountry.countries.lookup(name).alpha_2
    except Exception:
        pass
    # 2) alias map
    if name in SPECIAL_COUNTRY_ALIASES:
        return SPECIAL_COUNTRY_ALIASES[name]
    # 3) fuzzy against choices
    match = closest_text(name, [n for _, n in _COUNTRY_CHOICES], cutoff=cutoff)
    if match:
        for iso2, nm in _COUNTRY_CHOICES:
            if nm == match:
                return iso2
    # 4) last attempt: strip leading "the "
    cleaned = re.sub(r"^(the\s+)", "", name.strip(), flags=re.I)
    try:
        return pycountry.countries.lookup(cleaned).alpha_2
    except Exception:
        return name  # keep original string if unresolved

def _region_from_locale(locale_used: Optional[str]) -> Optional[str]:
    if not locale_used or "-" not in locale_used:
        return None
    return locale_used.split("-")[1].upper()

def normalize_currency_iso3(cur_val: Optional[str],
                            country_iso2: Optional[str],
                            price_text_full: Optional[str] = None,
                            price_text_fragment: Optional[str] = None) -> Optional[str]:
    """
    Normalize to ISO-3. Priority:
      1) explicit ISO-3 near the price
      2) known symbols/variants
      3) ambiguous symbols -> resolve by country_iso2 (STRICT)
      4) last resort -> country map
    """
    def _find_iso3_in_text(txt: str) -> Optional[str]:
        for m in re.finditer(r"\b([A-Z]{3})\b", txt or ""):
            code = m.group(1).upper()
            if code in ISO_CODES:
                return code
        return None

    iso_from_text = _find_iso3_in_text(price_text_fragment or "") or _find_iso3_in_text(price_text_full or "")
    if iso_from_text:
        return iso_from_text

    region = (country_iso2 or "").upper() if country_iso2 else None

    if cur_val is None or str(cur_val).strip() == "":
        return REGION_TO_CURRENCY.get(region)

    cur_raw = str(cur_val).strip()
    up = cur_raw.upper().replace(" ", "")

    if up in CURRENCY_MAP:
        code = CURRENCY_MAP[up]
        if up == "₨" and region in {"IN","PK","LK","NP","MU","MV","SC"}:
            return REGION_TO_CURRENCY.get(region, code)
        if up == "$":
            return REGION_TO_CURRENCY.get(region, "USD")
        return code

    if up in {"KR", "KR.", "KRONA", "KRONER"} or cur_raw.lower() in {"kr","kr.","krona","kroner"}:
        return REGION_TO_CURRENCY.get(region, None)

    if up == "R":
        return "ZAR" if region == "ZA" else REGION_TO_CURRENCY.get(region, None)

    if len(up) == 3 and up.isalpha():
        try:
            return pycountry.currencies.lookup(up).alpha_3
        except Exception:
            return up

    try:
        return pycountry.currencies.lookup(up).alpha_3
    except Exception:
        return REGION_TO_CURRENCY.get(region, None)

# ---------------------- MAIN ----------------------
def main():
    raw_rows: List[Dict] = []

    with sync_playwright() as p:
        browser = getattr(p, BROWSER).launch(headless=HEADLESS)
        ctx = browser.new_context(
            locale="en-GB",
            viewport={"width": 1400, "height": 900},
            ignore_https_errors=True
        )
        ctx.set_default_timeout(15000)
        ctx.set_default_navigation_timeout(20000)

        def maybe_block(route):
            rt = route.request.resource_type
            if rt in ("image","media","font"): return route.abort()
            return route.continue_()
        ctx.route("**/*", maybe_block)

        page = ctx.new_page()

        print("Opening price article…")
        if not goto_relaxed(page, START_URL, timeout_ms=20000):
            print("[fatal] cannot open start URL"); return
        close_cookies(page)

        try:
            country_btn, language_btn, countries = discover_pickers(page)
        except Exception as e:
            print(f"[fatal] {e}"); return

        if MODE == "test":
            if SELECTED_ISO2:
                print(f"[TEST] Using ISO-2 selection: {SELECTED_ISO2}")
                picked: List[str] = []
                for iso2 in SELECTED_ISO2:
                    name = canonical_country_from_iso2(iso2)
                    label = closest_text(name, countries, cutoff=80)
                    if label and label not in picked:
                        picked.append(label)

                if picked:
                    countries = picked
                    print(f"[TEST] Mapped to picker countries: {countries}")
                else:
                    # Fallback to the previous hard-coded demo sample
                    countries = [
                        "United States",
                        "United Kingdom",
                        "Japan",
                        "Türkiye",
                        "Brazil",
                        "Saint Lucia",
                    ]
            else:
                # No explicit selection from the UI – use the original test sample
                countries = [
                    "United States",
                    "United Kingdom",
                    "Japan",
                    "Türkiye",
                    "Brazil",
                    "Saint Lucia",
                ]


        for i, requested_country in enumerate(countries, 1):
            print(f"\n[{i}/{len(countries)}] {requested_country}")
            try:
                ok, clicked_label = select_from_list_by_normalized_text(page, country_btn, requested_country)
                if not ok:
                    print(f"  [skip] '{requested_country}' not available in picker (no safe match).")
                    continue

                page.wait_for_timeout(500)
                try:
                    select_from_list_by_normalized_text(page, language_btn, "English")
                except Exception:
                    pass
                page.wait_for_timeout(400)

                try:
                    lang_attr = page.evaluate("document.documentElement.lang") or "en-GB"
                except Exception:
                    lang_attr = "en-GB"

                price_url = f"https://help.disneyplus.com/{lang_attr}/article/disneyplus-price"
                if not goto_relaxed(page, price_url, timeout_ms=20000):
                    alt_url = START_URL.replace("en-GB", lang_attr)
                    if not goto_relaxed(page, alt_url, timeout_ms=20000):
                        print("  [timeout] navigation failed, skipping"); continue

                html = page.evaluate("document.body.innerHTML")
                rows = parse_article_html(html)
                if not rows:
                    print("  [retry] reload once…")
                    page.reload(wait_until="domcontentloaded")
                    page.wait_for_timeout(1200)
                    html = page.evaluate("document.body.innerHTML")
                    rows = parse_article_html(html)

                soup = BeautifulSoup(html, "html.parser")
                page_text = soup.get_text(" ", strip=True)

                # COUNTRY: resolve to ISO-2 from the clicked label, then canonical EN name
                iso2 = country_to_iso2_fuzzy(clicked_label or requested_country)
                if len(iso2) != 2 or not iso2.isalpha():
                    iso2 = country_to_iso2_fuzzy(requested_country)
                iso2 = (iso2.upper() if isinstance(iso2, str) else iso2)
                country_canonical = canonical_country_from_iso2(iso2) if isinstance(iso2, str) and len(iso2)==2 else (clicked_label or requested_country)

                # CURRENCY: safe page hint (country-first; ISO-3 override only)
                page_currency_hint = infer_page_currency(page_text, iso2)

                for r in rows:
                    r["country_name"] = country_canonical
                    r["country_name_original"] = clicked_label
                    r["locale_used"]  = lang_attr
                    r["url"]          = page.url
                    r["page_text"]    = page_text
                    r["page_currency_hint"] = page_currency_hint
                    r["country_iso2"] = iso2

                page.screenshot(path=str(OUTDIR / f"article_{i:03d}_{normalize_name(country_canonical)}.png"), full_page=True)
                print(f"  [ok] rows in page: {len(rows)}")
                raw_rows.extend(rows)

            except PWTimeout:
                print("  [timeout] skipping due to timeout"); continue
            except Exception as e:
                print(f"  [error] {e}")
                try: page.screenshot(path=str(OUTDIR / f"error_{i:03d}.png"))
                except Exception: pass
                continue

        ctx.close(); browser.close()

    if not raw_rows:
        print("No data scraped."); return

    # --------- ENRICH + EXPAND TO ONE ROW PER PRICE ----------
    expanded: List[Dict] = []
    for base in raw_rows:
        hint = base.get("page_currency_hint") or infer_page_currency(
            base.get("page_text","") or "", base.get("country_iso2")
        )
        expanded.extend(expand_prices_into_rows(base, page_currency_hint=hint))

    df = pd.DataFrame(expanded)

    df["plan_en_canonical"] = df.apply(
        lambda r: canonical_plan_english(
            r.get("plan") or "",
            r.get("price_text_full") or "",
            r.get("price_text_fragment") or ""
        ),
        axis=1
    )

    df["plan_en"] = df.apply(
        lambda r: (r.get("plan") or "").strip() if _is_english_locale(r.get("locale_used"))
                  else r.get("plan_en_canonical"),
        axis=1
    )

    df["country_iso2"] = df["country_iso2"].apply(lambda x: (x.upper() if isinstance(x, str) else x))

    df["currency_iso3"] = df.apply(
        lambda r: normalize_currency_iso3(
            r.get("currency"),
            r.get("country_iso2"),
            price_text_full=r.get("price_text_full"),
            price_text_fragment=r.get("price_text_fragment"),
        ),
        axis=1
    )

    df = df[[
        "country_name", "country_iso2", "locale_used", "url",
        "plan_en", "plan_en_canonical",
        "price_text_full", "price_text_fragment",
        "price_value", "currency_iso3", "billing_period"
    ]]

    def _expected_cur(iso2: str) -> Optional[str]:
        return REGION_TO_CURRENCY.get(iso2) if isinstance(iso2, str) and len(iso2)==2 else None
    df["_expected_by_country"] = df["country_iso2"].apply(_expected_cur)
    mism = df[(df["_expected_by_country"].notna()) & (df["currency_iso3"].notna()) & (df["_expected_by_country"] != df["currency_iso3"])]
    if not mism.empty:
        print("\n[Audit] Currency mismatches (expected_by_country vs detected):")
        print(mism[["country_name","country_iso2","currency_iso3","_expected_by_country","price_text_fragment"]].head(20).to_string(index=False))
    df = df.drop(columns=["_expected_by_country"])

    save_excel_robust(df, EXCEL_PATH)

    if SAVE_JSON:
        JSON_PATH.write_text(json.dumps(df.to_dict(orient="records"), indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved JSON (enriched) -> {JSON_PATH.resolve()}")

    print(f"\nExcel written to: {EXCEL_PATH.resolve()}")
    print("\nSample rows:")
    print(df.head(12))

def run_disney_scraper(mode: str = "full", test_countries=None) -> str:
    """Wrapper used by the web app.

    Parameters
    ----------
    mode:
        "test" or "full" – controls how many countries are scraped.
    test_countries:
        Optional list of ISO alpha-2 codes (e.g. ["GB", "US"]) used
        when *mode == "test"*.  In full mode this is ignored.
    """
    global MODE, SELECTED_ISO2
    MODE = mode  # "test" or "full"
    SELECTED_ISO2 = [c.upper() for c in (test_countries or [])]

    main()  # this will write to EXCEL_PATH

    return str(EXCEL_PATH.resolve())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
