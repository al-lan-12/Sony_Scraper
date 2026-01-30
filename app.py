import os
import time
import threading
import base64
from pathlib import Path
import subprocess
import sys
from io import BytesIO
from typing import Optional

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# Pillow is used to crop transparent padding around logos (recommended)
try:
    from PIL import Image
except Exception:
    Image = None

# --- Ensure Playwright browsers are installed (Spotify / Netflix / Disney+ / Apple Music) ---
def ensure_playwright_chromium():
    """
    On Streamlit Cloud we can't run `playwright install` manually.
    This checks for a cached Chromium browser and installs it if missing.
    """
    cache_dir = Path.home() / ".cache" / "ms-playwright"
    has_chromium = False

    try:
        if cache_dir.exists():
            for child in cache_dir.iterdir():
                if child.is_dir() and "chromium" in child.name:
                    has_chromium = True
                    break
    except Exception:
        pass

    if not has_chromium:
        try:
            print("[bootstrap] Installing Playwright Chromium browser…", flush=True)
            subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=True,
            )
            print("[bootstrap] Playwright Chromium install finished.", flush=True)
        except Exception as e:
            print(f"[bootstrap] WARNING: Playwright browser install failed: {e}", flush=True)


# Run once when the app script starts
ensure_playwright_chromium()

from dsp_scrapers import run_scraper  # noqa: E402
import pycountry  # noqa: E402

# -------------------------------------------------------------------
# Canonical full-result filenames (must match what your scrapers save)
# -------------------------------------------------------------------
FULL_RESULT_FILES = {
    "Apple Music": "apple_music_plans_all.xlsx",
    "Apple One": "apple_one_pricing_all.xlsx",
    "iCloud+": "icloud_plus_pricing_all.xlsx",
    "Spotify": "spotify_cleaned_playwright.xlsx",
    "Netflix": "netflix_pricing_by_country.xlsx",
    "Disney+": "disney_prices_enriched.xlsx",
}

# Build a list like "Afghanistan (AF)", "Albania (AL)", ...
COUNTRY_OPTIONS = sorted(
    [f"{c.name} ({c.alpha_2})" for c in pycountry.countries],
    key=str.lower,
)


def _extract_alpha2(selection):
    """Turn ['France (FR)', 'Japan (JP)'] into ['FR', 'JP']."""  # noqa: D401
    codes = []
    for item in selection:
        if "(" in item and ")" in item:
            codes.append(item.split("(")[-1].strip(") ").upper())
    return codes


SONY_RED = "#e31c23"

# ===================== PAGE CONFIG =====================

st.set_page_config(
    page_title="DSP Price Scraper",
    page_icon="sony_logo.png",
    layout="wide",
)

# ===================== GLOBAL STYLES =====================

st.markdown(
    f"""
    <style>
    body {{
        background-color: #000000;
        color: #f5f5f5;
    }}

    /* hide sidebar completely */
    [data-testid="stSidebar"] {{
        display: none;
    }}

    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2.5rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
        max-width: 1700px;
        margin-left: auto;
        margin-right: auto;
        background-color: #000000;
    }}

    h1, h2, h3, h4, h5, h6, label, p {{
        color: #f5f5f5 !important;
    }}

    .header-wrapper {{
        text-align: center;
        max-width: 900px;
        margin-left: auto;
        margin-right: auto;
        margin-bottom: 1.8rem;
    }}
    .header-title {{
        font-size: 2.5rem;
        font-weight: 800;
        letter-spacing: 0.09em;
        margin-top: 0.6rem;
        margin-bottom: 0.35rem;
        color: #ffffff;
        text-transform: uppercase;
    }}
    .header-subtitle {{
        font-size: 0.98rem;
        color: #f2f2f2;
        margin: 0 auto 0.5rem auto;
    }}
    .header-pill {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 0.16rem 0.9rem;
        border-radius: 999px;
        font-size: 0.76rem;
        background: {SONY_RED};
        color: #ffffff;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-top: 0.25rem;
    }}

    .how-card {{
        background-color: #050505;
        border-radius: 0.8rem;
        padding: 0.9rem 1.3rem;
        border: 1px solid #262626;
        color: #f5f5f5;
        margin-bottom: 1.2rem;
    }}
    .how-card ul {{
        margin-top: 0.35rem;
        margin-bottom: 0;
        padding-left: 1.1rem;
    }}
    .how-card li {{
        font-size: 0.9rem;
    }}

    .section-heading {{
        font-size: 1.2rem;
        font-weight: 600;
        margin-top: 0.9rem;
        margin-bottom: 0.4rem;
        color: #ffffff;
    }}

    /* center DSP tabs and enlarge labels */
    .stTabs [role="tablist"] {{
        justify-content: center;
    }}
    .stTabs [role="tab"] p {{
        font-size: 1.02rem;
        font-weight: 600;
    }}

    /* AgGrid styling */
    .ag-theme-streamlit .ag-root-wrapper {{
        border-radius: 0.7rem;
        border: 1px solid #444444;
    }}
    .ag-theme-streamlit .ag-header {{
        background: #111111;
        color: #fafafa;
        font-weight: 600;
    }}
    .ag-theme-streamlit .ag-row-even {{
        background-color: #050505;
    }}
    .ag-theme-streamlit .ag-row-odd {{
        background-color: #020202;
    }}

    /* Primary buttons (run buttons) */
    div.stButton > button {{
        border-radius: 999px !important;
        background: {SONY_RED} !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding-left: 1.3rem !important;
        padding-right: 1.3rem !important;
    }}

    /* Download button in Sony red */
    .stDownloadButton > button {{
        border-radius: 999px !important;
        background: {SONY_RED} !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding-left: 1.3rem !important;
        padding-right: 1.3rem !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ===================== LOGO HELPERS =====================

# 👇 One knob to adjust all DSP logo sizes
LOGO_WIDTH_PX = 95        # try 80–100
LOGO_CANVAS_PX = 250      # internal quality/consistency

def _logo_bytes_cropped(path: str) -> Optional[bytes]:
    """
    Crop transparent padding and center into a square canvas.
    Returns PNG bytes.
    """
    if not path:
        return None
    p = Path(path)
    if not p.is_file():
        return None

    # If Pillow isn't available, return raw bytes (no cropping)
    if Image is None:
        try:
            return p.read_bytes()
        except Exception:
            return None

    try:
        img = Image.open(str(p)).convert("RGBA")

        # Crop transparent padding
        alpha = img.split()[-1]
        bbox = alpha.getbbox()
        if bbox:
            img = img.crop(bbox)

        # Fit into square canvas (keeps all logos consistent)
        img.thumbnail((LOGO_CANVAS_PX, LOGO_CANVAS_PX), Image.LANCZOS)
        canvas = Image.new("RGBA", (LOGO_CANVAS_PX, LOGO_CANVAS_PX), (0, 0, 0, 0))
        x = (LOGO_CANVAS_PX - img.width) // 2
        y = (LOGO_CANVAS_PX - img.height) // 2
        canvas.paste(img, (x, y), img)

        buf = BytesIO()
        canvas.save(buf, format="PNG")
        return buf.getvalue()
    except Exception:
        return None


def show_logo(path: str):
    data = _logo_bytes_cropped(path)
    if not data:
        return
    st.image(data, width=LOGO_WIDTH_PX)


def centered_sony_logo():
    logo_path = "sony_logo.png"
    data = _logo_bytes_cropped(logo_path)
    if not data:
        return
    b64 = base64.b64encode(data).decode("utf-8")
    st.markdown(
        f"""
        <p style="text-align:center; margin-bottom:0.3rem;">
            <img src="data:image/png;base64,{b64}" width="120">
        </p>
        """,
        unsafe_allow_html=True,
    )

# ===================== HELPERS =====================

def run_with_progress(dsp_name: str, test_mode: bool, test_countries=None):
    status_placeholder = st.empty()
    progress = st.progress(0, text=f"Starting {dsp_name} scraper…")

    result = {"path": None, "error": None}

    def worker():
        try:
            result["path"] = run_scraper(
                dsp_name=dsp_name,
                test_mode=test_mode,
                test_countries=test_countries,
            )
        except Exception as e:
            result["error"] = str(e)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    start = time.time()
    expected = 90 if test_mode else 600

    while thread.is_alive():
        elapsed = time.time() - start
        pct = min(0.95, elapsed / expected)
        pct_int = int(pct * 100)
        remaining = max(0, int(expected - elapsed))
        progress.progress(
            pct_int,
            text=f"{dsp_name}: {pct_int}% • Est. remaining ~{remaining:02d}s",
        )
        time.sleep(0.6)

    thread.join()

    if result["error"]:
        progress.empty()
        status_placeholder.error(
            f"Error while running {dsp_name}: {result['error']}"
        )
        return None

    if not result["path"]:
        progress.empty()
        status_placeholder.error(
            f"{dsp_name}: scraper did not produce any output file."
        )
        return None

    progress.progress(100, text=f"{dsp_name}: 100% • Completed")
    status_placeholder.success("Scrape finished successfully.")
    return result["path"]

def render_table(excel_path: str, dsp_name: str):
    if not excel_path or not os.path.exists(excel_path):
        st.error("File not found – scraper may not have produced an output.")
        return

    st.markdown(f"###  Data explorer – {dsp_name}")

    df = pd.read_excel(excel_path)

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filter=True,
        sortable=True,
        resizable=True,
        floatingFilter=True,
    )
    gb.configure_pagination(
        enabled=True,
        paginationAutoPageSize=False,
        paginationPageSize=50,
    )
    gb.configure_side_bar()

    grid_options = gb.build()

    AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.NO_UPDATE,
        theme="streamlit",
        height=520,
        fit_columns_on_grid_load=True,
    )

    with open(excel_path, "rb") as f:
        data = f.read()

    st.download_button(
        " Download full Excel file",
        data=data,
        file_name=os.path.basename(excel_path),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def dsp_panel(dsp_name: str, logo_filename: Optional[str], description: str):
    # --- session state for per-DSP results: separate for full & test ---
    if "dsp_results" not in st.session_state:
        st.session_state["dsp_results"] = {"full": {}, "test": {}}

    full_results = st.session_state["dsp_results"]["full"]
    test_results = st.session_state["dsp_results"]["test"]

    # --- header row: logo + text ---
    # Narrow logo column so there isn't a big empty left area.
    col_logo, col_text = st.columns([0.85, 7.15], vertical_alignment="center")

    with col_logo:
        if logo_filename:
            show_logo(logo_filename)

    with col_text:
        st.markdown(
            f"#### {dsp_name}\n"
            f"<p class='small-text'>{description}</p>",
            unsafe_allow_html=True,
        )

    # --- mode selector ---
    st.markdown("##### Mode")
    mode = st.radio(
        "Mode",
        options=["Full (all countries)", "Test (quick run)"],
        horizontal=True,
        label_visibility="collapsed",
        key=f"mode_{dsp_name}",
    )
    test_mode = mode.startswith("Test")

    results_dict = test_results if test_mode else full_results

    # Auto-load canonical full file (if exists)
    if not test_mode and dsp_name not in full_results:
        default_file = FULL_RESULT_FILES.get(dsp_name)
        if default_file:
            p = Path(default_file)
            if p.is_file():
                full_results[dsp_name] = str(p.resolve())

    selected_codes = []
    if test_mode:
        st.markdown("##### Countries for test runs (optional)")
        selected_labels = st.multiselect(
            "Start typing a country name or code",
            options=COUNTRY_OPTIONS,
            default=st.session_state.get(f"test_countries_{dsp_name}", []),
            label_visibility="collapsed",
            key=f"countries_{dsp_name}",
        )
        st.session_state[f"test_countries_{dsp_name}"] = selected_labels
        selected_codes = _extract_alpha2(selected_labels)

    st.write("")

    # --- run button ---
    if st.button(f" Run {dsp_name} scraper", key=f"run_{dsp_name}"):
        excel_path = run_with_progress(
            dsp_name=dsp_name,
            test_mode=test_mode,
            test_countries=selected_codes,
        )
        if excel_path:
            results_dict[dsp_name] = excel_path
            if not test_mode:
                full_results[dsp_name] = excel_path

    # --- render last result ---
    excel_path = results_dict.get(dsp_name)
    if excel_path:
        st.markdown("---")
        render_table(excel_path, dsp_name)
    else:
        if not test_mode:
            st.info("No full run cached yet for this DSP – run a full scrape to populate it.")
        else:
            st.info("Run a test scrape for this DSP to see results here.")


# ===================== HEADER =====================

centered_sony_logo()

st.markdown(
    """
    <div class="header-wrapper">
        <div class="header-title">DSP PRICE SCRAPER</div>
        <p class="header-subtitle">
            Central hub for Apple Music, Apple One, iCloud+, Spotify, Netflix; Disney+ pricing.
            Run scrapes on demand, explore the results in a table,
            and export straight to Excel.
        </p>
        <div class="header-pill">DSP ANALYTICS TOOL</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="how-card">
        <b>How it works</b>
        <ul>
            <li>Select <b>Apple</b>, <b>Spotify</b>, <b>Netflix</b> or <b>Disney+</b> in the tabs below.</li>
            <li>Within Apple you can choose between <b>Apple Music</b>, <b>Apple One</b> and <b>iCloud+</b>.</li>
            <li>Use <b>Full</b> mode for a complete global run, or <b>Test</b> for a quick sample.</li>
            <li>Click <b>Run scraper</b> to launch the underlying code for that DSP.</li>
            <li>Track progress with a live percentage, elapsed time and estimated remaining time.</li>
            <li>Explore and download the results from the interactive table.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="section-heading">Choose your DSP</div>', unsafe_allow_html=True)

# ===================== MAIN DSP TABS =====================

main_tabs = st.tabs(["Apple", "Spotify", "Netflix", "Disney+"])

# Apple tab: Apple Music + Apple One + iCloud+
with main_tabs[0]:
    apple_tabs = st.tabs(["Apple Music", "Apple One", "iCloud+"])

    with apple_tabs[0]:
        dsp_panel(
            dsp_name="Apple Music",
            logo_filename="apple_music_logo.png",
            description="Scrape global Apple Music subscription prices, currencies and country codes.",
        )

    with apple_tabs[1]:
        dsp_panel(
            dsp_name="Apple One",
            logo_filename="apple_one_logo.png",  # <- change if your filename differs
            description="Scrape Apple One bundle pricing with currency, plan and country codes.",
        )

    with apple_tabs[2]:
        dsp_panel(
            dsp_name="iCloud+",
            logo_filename="icloud_logo.png",
            description="Scrape iCloud+ storage plan prices by country, including plan size and currency.",
        )

# Spotify tab
with main_tabs[1]:
    dsp_panel(
        dsp_name="Spotify",
        logo_filename="spotify_logo.png",
        description="Scrape Spotify Premium pricing with currency, plan and country codes.",
    )

# Netflix tab
with main_tabs[2]:
    dsp_panel(
        dsp_name="Netflix",
        logo_filename="netflix_logo.png",
        description="Scrape Netflix pricing with currency, plan and country codes.",
    )

# Disney+ tab
with main_tabs[3]:
    dsp_panel(
        dsp_name="Disney+",
        logo_filename="disney_plus_logo.png",
        description="Scrape Disney+ pricing with currency, plan and country codes.",
    )






