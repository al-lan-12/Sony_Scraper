# dsp_scrapers/__init__.py

"""Dispatcher and shared options for all DSP scrapers.

The Streamlit app calls :func:`run_scraper` with a friendly DSP name
("Apple Music", "iCloud+", etc.).  This module maps that to the
correct scraper function and passes through the test-mode arguments.
"""

from .apple_music_scraper import run_apple_music_scraper
from .apple_one_scraper import run_apple_one_scraper
from .disney_plus_scraper import run_disney_scraper
from .spotify_scraper import run_spotify_scraper
from .icloud_plus_scraper import run_icloud_plus_scraper
from .netflix_scraper import run_netflix_scraper


# The keys here must match the labels used in the Streamlit UI
DSP_OPTIONS = {
    "Apple Music": "apple_music",
    "Apple One": "apple_one",
    "iCloud+": "icloud_plus",
    "Spotify": "spotify",
    "Netflix": "netflix",
    "Disney+": "disney",
}


def run_scraper(dsp_name: str, test_mode: bool, test_countries=None) -> str:
    """Run the appropriate scraper and return the path to its Excel file.

    Parameters
    ----------
    dsp_name:
        Friendly name as shown in the UI (e.g. "Apple Music").
    test_mode:
        If True, scraper should run in "test" mode (fewer countries).
    test_countries:
        Optional list of ISO alpha-2 country codes (e.g. ["GB", "US"])
        that should be used *only* in test mode.  When None, each scraper
        falls back to its own built-in test selection.
    """
    kind = DSP_OPTIONS.get(dsp_name)
    if not kind:
        raise ValueError(f"Unknown DSP name: {dsp_name!r}")

    # Apple Music: fully honours test_countries
    if kind == "apple_music":
        return run_apple_music_scraper(
            test_mode=test_mode,
            test_countries=test_countries,
        )

    # Apple One: honours test_countries in test mode
    if kind == "apple_one":
        return run_apple_one_scraper(
            test_mode=test_mode,
            test_countries=test_countries,
        )

    # iCloud+: now honours test_countries in test mode
    if kind == "icloud_plus":
        return run_icloud_plus_scraper(
            test_mode=test_mode,
            test_countries=test_countries,
        )

    # Spotify: already honours test_countries (mapped to TEST_MARKETS)
    if kind == "spotify":
        return run_spotify_scraper(
            test_mode=test_mode,
            test_countries=test_countries,
        )

    # Netflix: now honours test_countries in test mode
    if kind == "netflix":
        return run_netflix_scraper(
            test_mode=test_mode,
            test_countries=test_countries,
        )

    # Disney+: now honours test_countries in test mode
    if kind == "disney":
        mode = "test" if test_mode else "full"
        return run_disney_scraper(
            mode=mode,
            test_countries=test_countries,
        )

    # Should never happen, but keeps mypy/linters happy
    raise ValueError(f"Unsupported DSP kind: {kind!r}")
