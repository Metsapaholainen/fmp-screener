#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  📈 FMP STOCK SCREENER — Professional Fundamentals Edition     ║
║  Lynch + Buffett strategies powered by Financial Modeling Prep  ║
║  No hardcoded data — everything from FMP API + AI analysis     ║
╚══════════════════════════════════════════════════════════════════╝

Separate from stockscreenerultra.py — this is the FMP-native screener.
Covers: NASDAQ + NYSE + AMEX (full US market ~3,500+ stocks)

Tabs:
  1. Overview          — AI market pulse + top picks + tab summaries
  2. IV Discount       — DCF intrinsic value + Piotroski ≥7, top 50
  2b. IV by Sector     — Same, 30 per sector
  3. Stalwarts         — PEG 1-2, rev 8-20%, >$2B (Lynch category)
  4. Fast Growers      — PEG <1.5, rev >20% (Lynch category)
  5. Slow Growers      — Dividend >2%, stable (Lynch category)
  6. Cyclicals         — Low P/E in cyclical sectors (Lynch category)
  7. Turnarounds       — Down >40%, recovering (Lynch category)
  8. Asset Plays       — P/B <1, hidden value (Lynch category)
  9. Sector Valuations — 11 SPDR ETFs with PEG/FCF/growth
  10. Insider Buying   — FMP insider transaction data
  11. Picks Tracking   — Performance measurement

Requirements:
  - FMP API key (env var: FMP_API_KEY)
  - Anthropic API key (env var: ANTHROPIC_API_KEY) for AI overview
  - pip: openpyxl, requests

Usage:
  python stockscreener_fmp.py
"""

import os, sys, json, time, datetime, csv, math, pickle
from collections import defaultdict

# ── Windows UTF-8 fix: force stdout/stderr to UTF-8 so emojis don't crash on redirect ──
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

def _load_dotenv():
    """Load .env file from the script's directory into os.environ (no extra deps)."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            # Overwrite if not set OR if set to empty string (e.g. blank PyCharm run config entry)
            if k and v and not os.environ.get(k):
                os.environ[k] = v

_load_dotenv()

# ─────────────────────────────────────────────
# DEPENDENCIES
# ─────────────────────────────────────────────
try:
    import requests
except ImportError:
    sys.exit("pip install requests")
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
except ImportError:
    sys.exit("pip install openpyxl")

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
FMP_KEY = os.environ.get("FMP_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
NTFY_TOPIC    = os.environ.get("NTFY_TOPIC",    "")  # optional: phone push notifications
GITHUB_REPO   = os.environ.get("GITHUB_REPO",   "")  # "username/repo" for GitHub Pages hosting
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN",  "")  # GitHub Personal Access Token
FMP_BASE = "https://financialmodelingprep.com/stable"

CACHE_FILE = "fmp_screener_cache.json"
CACHE_DAYS = 1
PICKS_LOG = "fmp_picks_log.csv"
AI_PICKS_LOG = "fmp_ai_picks_log.csv"
PORTFOLIO_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fmp_portfolio.json")
OUTPUT_DIR = "."

# Portfolio manager constants
PORTFOLIO_INITIAL_CASH = 100_000.0   # paper money starting balance
PORTFOLIO_TARGET_POSITION = 10_000.0 # ~$10K per position (equal weight)
PORTFOLIO_MAX_POSITIONS = 10         # hard cap

TOP_N = 50  # main tab results
SECTOR_N = 30  # per-sector results

# ── Fast Grower: small-cap scoring & filter constants ────────────────────
FG_MICRO_CAP_BONUS        = 12    # $100M–$500M: most underfollowed, highest 10x potential
FG_SMALL_CAP_BONUS        =  8    # $500M–$2B:   still neglected by institutional coverage
FG_SMALL_MID_BONUS        =  3    # $2B–$5B:     marginal recognition; slight nudge
FG_MICRO_CAP_MAX          = 500e6
FG_SMALL_CAP_MAX          =   2e9
FG_SMALL_MID_MAX          =   5e9
FG_RELAXED_FCF_REV_GROWTH = 0.25  # minimum rev growth to qualify for FCF exception

def _cap_size_label(mktcap: float) -> str:
    """Return a human-readable market cap tier label for display."""
    if mktcap >= 200e9: return "Mega"
    if mktcap >=  10e9: return "Large"
    if mktcap >=   2e9: return "Mid"
    if mktcap >= 500e6: return "Small"
    if mktcap >= 100e6: return "Micro"
    return "Nano"


# ─────────────────────────────────────────────
# RUN TIMER  (phase-by-phase elapsed + ETA)
# ─────────────────────────────────────────────
_run_start: float = 0.0
_phase_start: float = 0.0
_phase_name: str = ""

# Typical phase durations (seconds) for ETA — updated each run via exponential smoothing
# Order matches the actual run sequence in main()
_PHASE_WEIGHTS = {
    "macro":        3,
    "universe":     25,
    "enrichment":   40,
    "52w_ranges":   90,
    "tabs":         35,
    "ai_analysis":  210,
    "portfolio":    30,
    "save":         5,
}


def _fmt_elapsed(sec: float) -> str:
    """Format seconds as 'm:ss' or 'Xs'."""
    if sec >= 60:
        return f"{int(sec // 60)}m{int(sec % 60):02d}s"
    return f"{int(sec):.0f}s"


def phase_start(name: str, label: str = "") -> None:
    """Mark start of a new phase. Prints elapsed time since run start + ETA."""
    global _phase_start, _phase_name
    _phase_start = time.time()
    _phase_name  = name

    elapsed = _phase_start - _run_start if _run_start else 0
    # Compute ETA: sum of remaining phase weights
    phases = list(_PHASE_WEIGHTS.keys())
    try:
        idx = phases.index(name)
    except ValueError:
        idx = len(phases)
    remaining_est = sum(v for k, v in _PHASE_WEIGHTS.items()
                        if phases.index(k) > idx)
    eta_str = f" | ETA ~{_fmt_elapsed(remaining_est)} left" if remaining_est > 0 else ""
    print(f"\n  ⏱  [{_fmt_elapsed(elapsed)} elapsed{eta_str}]  {label or name.upper()}")


def phase_done(extra: str = "") -> None:
    """Print phase completion with duration."""
    dur = time.time() - _phase_start if _phase_start else 0
    suffix = f"  — {extra}" if extra else ""
    print(f"     ✔ done in {_fmt_elapsed(dur)}{suffix}")


def notify_phone(title: str, message: str, tags: str = "white_check_mark") -> None:
    """Send push notification via ntfy.sh (free, no account needed).
    Set NTFY_TOPIC env var to enable. Install ntfy app on phone and subscribe to the topic.
    Uses JSON body so emojis and Unicode in titles/messages work correctly.
    """
    if not NTFY_TOPIC:
        return
    try:
        requests.post(
            "https://ntfy.sh/",
            json={
                "topic":    NTFY_TOPIC,
                "title":    title,
                "message":  message,
                "tags":     tags.split(",") if tags else [],
                "priority": 3,
            },
            timeout=8,
        )
    except Exception:
        pass  # never crash the run over a notification


def push_html_to_github(html_content: str) -> str | None:
    """Upload HTML dashboard to GitHub via Contents API. Returns public Pages URL or None.
    Requires GITHUB_REPO='username/repo' and GITHUB_TOKEN in .env.
    No git clone or subprocess needed — pure HTTP.
    """
    if not GITHUB_REPO or not GITHUB_TOKEN:
        return None
    try:
        import base64
        encoded = base64.b64encode(html_content.encode("utf-8")).decode()
        hdrs = {"Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json"}
        # Get current file SHA (required for updates; absent on first push)
        r = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/contents/index.html",
                         headers=hdrs, timeout=10)
        sha = r.json().get("sha") if r.status_code == 200 else None
        today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        payload = {"message": f"Screener run {today}", "content": encoded, "branch": "main"}
        if sha:
            payload["sha"] = sha
        r2 = requests.put(f"https://api.github.com/repos/{GITHUB_REPO}/contents/index.html",
                          json=payload, headers=hdrs, timeout=20)
        if r2.status_code in (200, 201):
            owner, repo = GITHUB_REPO.split("/", 1)
            return f"https://{owner}.github.io/{repo}/"
        print(f"  ⚠️ GitHub push failed: {r2.status_code} {r2.text[:120]}")
    except Exception as e:
        print(f"  ⚠️ GitHub push error: {e}")
    return None

# Excel styling
THIN_BORDER = Border(
    left=Side(style="thin", color="D0D0D0"),
    right=Side(style="thin", color="D0D0D0"),
    top=Side(style="thin", color="D0D0D0"),
    bottom=Side(style="thin", color="D0D0D0"),
)
ALT_FILL = PatternFill("solid", fgColor="F5F5F5")
PLAIN_FILL = PatternFill("solid", fgColor="FFFFFF")
GREEN_FILL = PatternFill("solid", fgColor="E8F5E9")
AMBER_FILL = PatternFill("solid", fgColor="FFF8E1")
RED_FILL = PatternFill("solid", fgColor="FFEBEE")

# Sector ETFs for sector analysis
SECTOR_ETFS = {
    "XLK": "Technology", "XLF": "Financial Services", "XLV": "Healthcare",
    "XLY": "Consumer Cyclical", "XLP": "Consumer Defensive", "XLE": "Energy",
    "XLI": "Industrials", "XLB": "Basic Materials", "XLRE": "Real Estate",
    "XLU": "Utilities", "XLC": "Communication Services",
}

CYCLICAL_SECTORS = {
    "Energy", "Basic Materials", "Industrials", "Consumer Cyclical",
    "Real Estate", "Financial Services",
}

# ─────────────────────────────────────────────
# FMP API CLIENT
# ─────────────────────────────────────────────

_fmp_call_count = 0
FMP_V3 = "https://financialmodelingprep.com/api/v3"


def fetch_live_price(ticker: str) -> float | None:
    """Fetch real-time price via FMP stable quote endpoint."""
    global _fmp_call_count
    if not FMP_KEY:
        return None
    try:
        r = requests.get(f"{FMP_BASE}/quote",
                         params={"symbol": ticker, "apikey": FMP_KEY}, timeout=10)
        _fmp_call_count += 1
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data and data[0].get("price"):
                return float(data[0]["price"])
            elif isinstance(data, dict) and data.get("price"):
                return float(data["price"])
    except Exception:
        pass
    return None


def fetch_spy_history(from_date: str) -> dict:
    """Fetch SPY historical EOD closes in a single batch call.
    Returns {date_str: close_price} dict. Empty dict on failure (graceful degradation).
    """
    global _fmp_call_count
    if not FMP_KEY:
        return {}
    try:
        r = requests.get(f"{FMP_BASE}/historical-price-eod/light",
                         params={"symbol": "SPY", "from": from_date,
                                 "to": datetime.date.today().isoformat(), "apikey": FMP_KEY},
                         timeout=20)
        _fmp_call_count += 1
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return {rec["date"]: float(rec.get("price") or rec.get("close") or 0)
                        for rec in data if rec.get("date") and (rec.get("price") or rec.get("close"))}
    except Exception:
        pass
    return {}


def fetch_52w_ranges(tickers: list) -> dict:
    """Fetch 52-week high/low for enrichment candidates using individual /stable/quote calls.
    Uses the same parallel threading as _parallel_fetch. Cached with other fundamentals.
    Returns {ticker: {"yearHigh": float, "yearLow": float}}
    Note: FMP /stable/quote batch (comma-separated) returns [] on Starter plan — must use
    individual calls. ~4000 calls run in parallel threads, adds ~60s to fresh runs only.
    """
    global _fmp_call_count
    cache_key = "52w_ranges"
    cached = _cache.get(cache_key, {})

    # Only use cache if it covers at least 80% of requested tickers (prevents debug run
    # (20 stocks) from permanently polluting the production cache for 4000 stocks)
    missing = [t for t in tickers if t not in cached]
    if len(missing) <= len(tickers) * 0.20:
        print(f"  📦 Using cached 52w ranges ({len(cached)} stocks, {len(missing)} missing)")
        return cached

    if cached:
        print(f"  📊 Fetching 52w ranges for {len(missing)} stocks (cached {len(cached)}, parallel)...")
    else:
        print(f"  📊 Fetching 52w ranges for {len(tickers)} stocks (parallel)...")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    fetch_list = missing if cached else tickers
    results = dict(cached)  # start from what we have
    _lock = threading.Lock()
    _throttle = threading.Semaphore(4)

    def _fetch_one(t):
        with _throttle:
            try:
                r = requests.get(f"{FMP_BASE}/quote",
                                 params={"symbol": t, "apikey": FMP_KEY}, timeout=10)
                time.sleep(0.2)
                if r.status_code == 200:
                    data = r.json()
                    item = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})
                    yh = item.get("yearHigh"); yl = item.get("yearLow")
                    if yh and yl and float(yh) > 0:
                        return t, {"yearHigh": float(yh), "yearLow": float(yl)}
            except Exception:
                pass
            return t, None

    done_count = 0
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(_fetch_one, t): t for t in fetch_list}
        for future in as_completed(futures):
            t, data = future.result()
            if data:
                with _lock:
                    results[t] = data
            done_count += 1
            if done_count % 500 == 0:
                print(f"    [{done_count}/{len(fetch_list)}] 52w ranges fetched...")

    _cache[cache_key] = results
    print(f"  ✅ 52w ranges: {len(results)} stocks ({len(results)-len(cached)} new)")
    return results


def fetch_macro_indicators() -> dict:
    """Fetch live macro indicators from FRED via the ivo-welch.info CSV gateway.

    Completely free — no API key needed. Same FRED data used by Fed economists.
    Cached for 1 day via the standard _cache system.
    Returns a dict with values + pre-computed signals (or empty dict on full failure).
    """
    cache_key = "macro_indicators"
    cached = _cache.get(cache_key)
    if cached and isinstance(cached, dict) and cached.get("as_of"):
        # Check age: reuse if < 1 day old
        try:
            ts = _cache.get("_timestamp", "")
            if ts:
                age = (datetime.datetime.now() - datetime.datetime.fromisoformat(ts)).total_seconds() / 86400
                if age < CACHE_DAYS:
                    print(f"  📦 Using cached macro indicators (as of {cached.get('as_of','?')})")
                    return cached
        except Exception:
            pass

    print("  🌍 Fetching macro indicators from FRED...")

    FRED_URL = "https://www.ivo-welch.info/cgi-bin/fredwrap?symbol={}"
    SERIES = {
        "dgs10":    "DGS10",      # 10Y Treasury yield (%) — daily
        "dgs2":     "GS2",        # 2Y Treasury yield (%) — monthly (GS2 works; DGS2 not in gateway)
        "t10y2y":   "T10Y2Y",     # Yield curve spread 10Y-2Y (%) — daily
        "vix":      "VIXCLS",     # VIX fear index — daily
        "fedfunds": "FEDFUNDS",   # Fed Funds effective rate (monthly, %)
        "cpi":      "CPIAUCSL",   # CPI all urban consumers (for YoY calc)
        "unrate":   "UNRATENSA",  # Unemployment rate NSA (%) — UNRATE not in gateway
    }

    raw = {}
    for key, series_id in SERIES.items():
        try:
            r = requests.get(FRED_URL.format(series_id), timeout=12)
            if r.status_code != 200:
                print(f"    ⚠️ FRED {series_id}: HTTP {r.status_code}")
                continue
            lines = [ln.strip() for ln in r.text.strip().splitlines()
                     if ln.strip() and not ln.startswith("#")]
            # Filter valid data rows (format: yyyymmdd,value or date,value)
            data_rows = []
            for ln in lines:
                parts = ln.split(",")
                if len(parts) >= 2:
                    try:
                        val = float(parts[1])
                        data_rows.append((parts[0].strip(), val))
                    except ValueError:
                        continue
            if not data_rows:
                print(f"    ⚠️ FRED {series_id}: no data rows")
                continue
            raw[key] = data_rows  # list of (date_str, value)
        except Exception as e:
            print(f"    ⚠️ FRED {series_id} error: {e}")

    if not raw:
        print("  ⚠️ All FRED fetches failed — macro indicators unavailable")
        return {}

    def _latest(k):
        rows = raw.get(k, [])
        # Skip NaN / missing values (FRED uses "." for missing)
        for date_str, val in reversed(rows):
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                return date_str, val
        return None, None

    # Extract latest values
    _dgs10_date, dgs10    = _latest("dgs10")
    _dgs2_date,  dgs2     = _latest("dgs2")
    _t10y2y_date, t10y2y = _latest("t10y2y")
    _vix_date,   vix      = _latest("vix")
    _ff_date,    fedfunds = _latest("fedfunds")
    _ur_date,    unrate   = _latest("unrate")

    # CPI YoY: compare latest to same month 12 months prior
    cpi_yoy = None
    cpi_as_of = None
    cpi_rows = raw.get("cpi", [])
    if len(cpi_rows) >= 13:
        # Latest value
        latest_date, latest_cpi = _latest("cpi")
        # Find value ~12 months prior (same month last year)
        if latest_cpi and latest_date:
            cpi_as_of = latest_date
            try:
                latest_yr = int(latest_date[:4])
                latest_mo = int(latest_date[4:6]) if len(latest_date) >= 6 else None
                prior_val = None
                for date_str, val in cpi_rows:
                    try:
                        yr = int(date_str[:4])
                        mo = int(date_str[4:6]) if len(date_str) >= 6 else None
                        if yr == latest_yr - 1 and mo == latest_mo:
                            prior_val = val
                            break
                    except (ValueError, IndexError):
                        continue
                if prior_val and prior_val > 0:
                    cpi_yoy = round((latest_cpi / prior_val - 1) * 100, 2)
            except (ValueError, IndexError):
                pass

    # Yield curve: prefer FRED T10Y2Y, fall back to computed
    if t10y2y is None and dgs10 is not None and dgs2 is not None:
        t10y2y = round(dgs10 - dgs2, 2)

    # Determine as_of date (most recent across all fetched series)
    as_of_candidates = [d for d in [_dgs10_date, _vix_date, _ff_date, _ur_date] if d]
    as_of = max(as_of_candidates) if as_of_candidates else "unknown"
    # Format nicely: YYYYMMDD → YYYY-MM-DD
    try:
        if len(as_of) == 8:
            as_of = f"{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]}"
    except Exception:
        pass

    # ── Pre-compute signals for AI and color-coding ──────────────────────────
    def _curve_signal(v):
        if v is None: return "UNKNOWN"
        if v < -0.10: return "INVERTED"
        if v < 0.50:  return "FLAT"
        if v < 1.50:  return "NORMAL"
        return "STEEP"

    def _vix_signal(v):
        if v is None: return "UNKNOWN"
        if v > 40: return "PANIC"
        if v > 25: return "FEAR"
        if v > 18: return "CAUTION"
        return "CALM"

    def _rate_signal(v):
        if v is None: return "UNKNOWN"
        if v > 5.0: return "HIGH"
        if v > 4.0: return "ELEVATED"
        if v > 2.0: return "NORMAL"
        return "LOW"

    def _inflation_signal(v):
        if v is None: return "UNKNOWN"
        if v > 5.0: return "HOT"
        if v > 3.0: return "ABOVE_TARGET"
        if v > 2.0: return "NEAR_TARGET"
        return "LOW"

    def _labor_signal(v):
        if v is None: return "UNKNOWN"
        if v < 4.0: return "TIGHT"
        if v < 5.0: return "HEALTHY"
        if v < 6.5: return "SOFTENING"
        return "WEAK"

    macro = {
        "dgs10":            round(dgs10, 2)    if dgs10    is not None else None,
        "dgs2":             round(dgs2, 2)     if dgs2     is not None else None,
        "yield_curve":      round(t10y2y, 2)   if t10y2y   is not None else None,
        "vix":              round(vix, 1)      if vix      is not None else None,
        "fedfunds":         round(fedfunds, 2) if fedfunds is not None else None,
        "cpi_yoy":          cpi_yoy,
        "unrate":           round(unrate, 1)   if unrate   is not None else None,
        "curve_signal":     _curve_signal(t10y2y),
        "vix_signal":       _vix_signal(vix),
        "rate_signal":      _rate_signal(dgs10),
        "inflation_signal": _inflation_signal(cpi_yoy),
        "labor_signal":     _labor_signal(unrate),
        "as_of":            as_of,
    }

    _cache[cache_key] = macro
    fetched = [k for k in ["dgs10","dgs2","yield_curve","vix","fedfunds","cpi_yoy","unrate"]
               if macro.get(k) is not None]
    print(f"  ✅ Macro indicators fetched: {', '.join(fetched)} (as of {as_of})")
    return macro


def fmp_get(endpoint: str, params: dict = None) -> dict | list | None:
    """Make an FMP API call. Returns parsed JSON or None on failure."""
    global _fmp_call_count
    if not FMP_KEY:
        return None
    url = f"{FMP_BASE}/{endpoint}"
    p = dict(params or {})
    p["apikey"] = FMP_KEY
    try:
        r = requests.get(url, params=p, timeout=15)
        _fmp_call_count += 1
        if r.status_code == 200:
            data = r.json()
            # FMP returns {"Error Message": "..."} on some errors
            if isinstance(data, dict) and "Error Message" in data:
                print(f"  ⚠️ FMP error on {endpoint}: {data['Error Message'][:80]}")
                return None
            return data
        elif r.status_code == 403:
            print(f"  ⚠️ FMP 403 (endpoint may require subscription): {endpoint}")
            return None
        elif r.status_code == 429:
            print(f"  ⚠️ FMP rate limited — waiting 5s...")
            time.sleep(5)
            r = requests.get(url, params=p, timeout=15)
            _fmp_call_count += 1
            if r.status_code == 200:
                return r.json()
        else:
            print(f"  ⚠️ FMP {r.status_code} on {endpoint}")
    except Exception as e:
        print(f"  ⚠️ FMP request error: {e}")
    return None


def fmp_get_batch(tickers: list, endpoint_template: str, batch_size: int = 5,
                  delay: float = 0.15) -> dict:
    """Fetch data for multiple tickers, returns {ticker: data_dict}.
    endpoint_template should have {} for ticker, e.g. 'v3/rating/{}'
    """
    results = {}
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        for t in batch:
            ep = endpoint_template.format(t)
            data = fmp_get(ep)
            if data and isinstance(data, list) and data:
                results[t] = data[0]
            elif data and isinstance(data, dict):
                results[t] = data
            time.sleep(delay)
        # Progress
        done = min(i + batch_size, len(tickers))
        if done % 50 == 0 or done == len(tickers):
            print(f"    [{done}/{len(tickers)}] fetched...")
    return results


# ─────────────────────────────────────────────
# CACHE SYSTEM
# ─────────────────────────────────────────────

_cache = {}


def load_cache() -> dict:
    """Load FMP screener cache from disk."""
    global _cache
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
            ts = data.get("_timestamp", "")
            if ts:
                age = (datetime.datetime.now() - datetime.datetime.fromisoformat(ts)).total_seconds() / 86400
                if age < CACHE_DAYS:
                    _cache = data
                    n = len(data.get("universe", {}))
                    print(f"  📦 FMP cache loaded ({n} stocks, age: {age:.1f}d)")
                    return _cache
                else:
                    print(f"  🔄 FMP cache expired ({age:.0f}d > {CACHE_DAYS}d TTL)")
    except Exception as e:
        print(f"  ⚠️ Cache load error: {e}")
    _cache = {}
    return _cache


def save_cache():
    """Save FMP screener cache to disk."""
    _cache["_timestamp"] = datetime.datetime.now().isoformat()
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(_cache, f, default=str)
        print(f"  💾 FMP cache saved ({len(_cache.get('universe', {}))} stocks)")
    except Exception as e:
        print(f"  ⚠️ Cache save error: {e}")


# ─────────────────────────────────────────────
# PHASE 1: UNIVERSE DISCOVERY
# ─────────────────────────────────────────────

def fetch_us_universe() -> dict:
    """Fetch all US stocks from NASDAQ + NYSE + AMEX via FMP.
    Strategy 1: stock-screener endpoint (may be premium)
    Strategy 2: stock/list endpoint (free tier — gets all symbols)
    Returns {ticker: {symbol, name, sector, industry, mktCap, price, ...}}
    """
    # Always fetch fresh universe so prices are current (only 3 API calls)
    print("\n  🌐 Fetching US stock universe from FMP (fresh prices)...")
    universe = {}

    # Strategy 1: stock-screener (lowercase exchange names per FMP docs)
    for exchange in ["nasdaq", "nyse", "amex"]:
        data = fmp_get("company-screener", {
            "exchange": exchange,
            "marketCapMoreThan": 50_000_000,
            "isActivelyTrading": "true",
            "limit": 5000,
        })
        if data and isinstance(data, list) and len(data) > 10:
            for stock in data:
                t = stock.get("symbol", "")
                if not t or len(t) > 6: continue
                universe[t] = {
                    "symbol": t,
                    "name": (stock.get("companyName") or "")[:30],
                    "sector": stock.get("sector") or "Unknown",
                    "industry": stock.get("industry") or "",
                    "mktCap": stock.get("marketCap") or 0,
                    "price": stock.get("price") or 0,
                    "beta": stock.get("beta"),
                    "volume": stock.get("volume"),
                    "exchange": exchange.upper(),
                    # 52-week range — included in FMP screener response
                    "yearHigh": stock.get("yearHigh") or stock.get("52WeekHigh"),
                    "yearLow":  stock.get("yearLow")  or stock.get("52WeekLow"),
                }
            print(f"    ✅ {exchange.upper()}: {len(data)} stocks")
        else:
            print(f"    ⚠️ {exchange.upper()} screener: {len(data) if data else 'no'} results")
        time.sleep(0.3)

    # Strategy 2 fallback: if screener failed, use symbol list + profiles
    if len(universe) < 100:
        print("  🔄 Screener returned too few results — using symbol list fallback...")
        # Get all tradeable symbols
        all_symbols = fmp_get("stock-list")
        if all_symbols and isinstance(all_symbols, list):
            us_symbols = [s for s in all_symbols
                          if s.get("exchangeShortName") in ("NASDAQ", "NYSE", "AMEX")
                          and s.get("type") == "stock"
                          and s.get("symbol")
                          and len(s.get("symbol", "")) <= 5
                          and "." not in s.get("symbol", "")
                          and "-" not in s.get("symbol", "")]
            print(f"    📋 Found {len(us_symbols)} US stock symbols")

            # Batch fetch profiles (30 at a time)
            for i in range(0, len(us_symbols), 30):
                batch = us_symbols[i:i + 30]
                batch_str = ",".join(s["symbol"] for s in batch)
                profiles = fmp_get(f"profile", {"symbol": batch_str})
                if profiles and isinstance(profiles, list):
                    for p in profiles:
                        t = p.get("symbol", "")
                        mktCap = p.get("mktCap") or 0
                        if not t or mktCap < 50_000_000: continue
                        if not p.get("isActivelyTrading", True): continue
                        universe[t] = {
                            "symbol": t,
                            "name": (p.get("companyName") or "")[:30],
                            "sector": p.get("sector") or "Unknown",
                            "industry": p.get("industry") or "",
                            "mktCap": mktCap,
                            "price": p.get("price") or 0,
                            "beta": p.get("beta"),
                            "volume": p.get("volAvg"),
                            "exchange": p.get("exchangeShortName") or "",
                        }
                done = min(i + 30, len(us_symbols))
                if done % 300 == 0:
                    print(f"    [{done}/{len(us_symbols)}] profiles loaded...")
                time.sleep(0.15)

    _cache["universe"] = universe
    print(f"  ✅ US Universe: {len(universe)} stocks loaded")
    return universe


# ─────────────────────────────────────────────
# PHASE 2: BULK FUNDAMENTAL DATA
# ─────────────────────────────────────────────

def fetch_bulk_ratios(universe: dict) -> dict:
    """Fetch key financial ratios for all stocks.
    Uses FMP key-metrics-ttm endpoint (1 call per stock, batched).
    Returns {ticker: {pe, pb, roe, roa, fcfYield, ...}}
    """
    cache_key = "ratios"
    if _cache.get(cache_key):
        print(f"  📦 Using cached ratios ({len(_cache[cache_key])} stocks)")
        return _cache[cache_key]

    print("\n  📊 Fetching financial ratios from FMP...")
    tickers = list(universe.keys())

    # Use bulk profile endpoint — gets basic ratios for many at once
    # FMP /v3/profile/AAPL,MSFT,... supports up to ~50 tickers per call
    ratios = {}
    batch_size = 30
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_str = ",".join(batch)
        data = fmp_get(f"profile", {"symbol": batch_str})
        if data and isinstance(data, list):
            for item in data:
                t = item.get("symbol", "")
                if t:
                    ratios[t] = {
                        "pe": item.get("pe"),
                        "price": item.get("price"),
                        "mktCap": item.get("mktCap"),
                        "beta": item.get("beta"),
                        "volAvg": item.get("volAvg"),
                        "lastDiv": item.get("lastDiv"),
                        "sector": item.get("sector"),
                        "industry": item.get("industry"),
                        "name": (item.get("companyName") or "")[:30],
                        "exchange": item.get("exchangeShortName"),
                        "isActivelyTrading": item.get("isActivelyTrading"),
                    }
        done = min(i + batch_size, len(tickers))
        if done % 300 == 0:
            print(f"    [{done}/{len(tickers)}] profiles fetched...")
        time.sleep(0.15)

    _cache[cache_key] = ratios
    print(f"  ✅ Profiles loaded: {len(ratios)} stocks")
    return ratios


def _parallel_fetch(tickers: list, endpoint: str, label: str,
                    cache_key: str, params_extra: dict = None) -> dict:
    """Generic parallel fetcher for per-ticker FMP endpoints.
    Uses ThreadPoolExecutor with rate-limit-aware throttling.
    """
    if _cache.get(cache_key):
        print(f"  📦 Using cached {label} ({len(_cache[cache_key])} stocks)")
        return _cache[cache_key]

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    print(f"\n  📊 Fetching {label}...")
    results = {}
    total = len(tickers)
    _lock = threading.Lock()
    _throttle = threading.Semaphore(4)  # max 4 concurrent — stay under 300/min

    def _fetch_one(t):
        with _throttle:
            p = {"symbol": t}
            if params_extra:
                p.update(params_extra)
            data = fmp_get(endpoint, p)
            time.sleep(0.2)  # ~200 req/min with 4 workers
            if data and isinstance(data, list) and data:
                return t, data[0]
            elif data and isinstance(data, dict) and "Error" not in str(data)[:50]:
                return t, data
            return t, None

    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(_fetch_one, t): t for t in tickers}
        done_count = 0
        for future in as_completed(futures):
            t, data = future.result()
            if data:
                with _lock:
                    results[t] = data
            done_count += 1
            if done_count % 100 == 0:
                print(f"    [{done_count}/{total}] {label} fetched...")

    _cache[cache_key] = results
    print(f"  ✅ {label} loaded: {len(results)} stocks")
    return results


def fetch_key_metrics(tickers: list) -> dict:
    return _parallel_fetch(tickers, "key-metrics-ttm", "key metrics", "key_metrics")


def fetch_dcf_bulk(tickers: list) -> dict:
    return _parallel_fetch(tickers, "discounted-cash-flow", "DCF values", "dcf")


def fetch_growth_estimates(tickers: list) -> dict:
    """Fetch analyst estimates — store full list for CAGR calculation."""
    cache_key = "estimates"
    if _cache.get(cache_key):
        print(f"  📦 Using cached growth estimates ({len(_cache[cache_key])} stocks)")
        return _cache[cache_key]

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    print(f"\n  📊 Fetching growth estimates...")
    results = {}
    _lock = threading.Lock()
    _throttle = threading.Semaphore(4)

    def _f(t):
        with _throttle:
            data = fmp_get("analyst-estimates", {"symbol": t, "period": "annual", "limit": 5})
            time.sleep(0.2)
            if data and isinstance(data, list) and len(data) >= 2:
                return t, data  # full list, not data[0]
            return t, None

    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(_f, t): t for t in tickers}
        done = 0
        for fut in as_completed(futs):
            t, data = fut.result()
            if data:
                with _lock: results[t] = data
            done += 1
            if done % 100 == 0: print(f"    [{done}/{len(tickers)}] estimates fetched...")

    _cache[cache_key] = results
    print(f"  ✅ estimates loaded: {len(results)} stocks")
    return results


def fetch_financial_scores(tickers: list) -> dict:
    return _parallel_fetch(tickers, "financial-scores", "Piotroski scores", "scores")


def fetch_ratings(tickers: list) -> dict:
    """Ratings endpoint not available on stable API — skip."""
    print("\n  ⏭️ Skipping ratings (not available on stable API)")
    return {}


def fetch_financial_growth(tickers: list) -> dict:
    """Fetch 5 years of annual growth metrics — keeps full list for trend/acceleration detection.
    Most recent year = data[0], previous year = data[1], etc.
    """
    cache_key = "growth"
    if _cache.get(cache_key):
        print(f"  📦 Using cached growth metrics ({len(_cache[cache_key])} stocks)")
        return _cache[cache_key]

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    print(f"\n  📊 Fetching growth metrics...")
    results = {}
    _lock = threading.Lock()
    _throttle = threading.Semaphore(4)

    def _f(t):
        with _throttle:
            data = fmp_get("financial-growth", {"symbol": t, "period": "annual", "limit": "5"})
            time.sleep(0.2)
            if data and isinstance(data, list) and data:
                return t, data  # return full list for trend detection
            return t, None

    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = {pool.submit(_f, t): t for t in tickers}
        done = 0
        for fut in as_completed(futs):
            t, data = fut.result()
            if data:
                with _lock: results[t] = data
            done += 1
            if done % 100 == 0: print(f"    [{done}/{len(tickers)}] growth metrics fetched...")

    _cache[cache_key] = results
    print(f"  ✅ growth metrics loaded: {len(results)} stocks")
    return results


def fetch_ratios_ttm(tickers: list) -> dict:
    """Fetch financial ratios TTM (ROE, ROA, P/B, D/E, Div Yield, Gross Margin).
    This is the dedicated ratios endpoint — more complete than key-metrics for profitability.
    """
    return _parallel_fetch(tickers, "ratios-ttm", "financial ratios TTM", "ratios_ttm")


def fetch_balance_sheet(tickers: list) -> dict:
    """Fetch latest annual balance sheet for net cash calculation.
    Net cash = cash + short-term investments - total debt
    Used to find cash-rich companies (asset plays, turnarounds).
    """
    return _parallel_fetch(tickers, "balance-sheet-statement", "balance sheets", "balance_sheet",
                           {"period": "annual", "limit": "1"})


def fetch_earnings_surprises(tickers: list) -> dict:
    """Fetch last 8 quarters of earnings surprises for beat-rate calculation.
    Beat rate = % of quarters where actual EPS beat estimated EPS.
    High beat rate = management consistently under-promises and over-delivers.
    Tries two FMP endpoint names; fails fast if neither is available on the plan.
    """
    cache_key = "earnings_surprises"
    if _cache.get(cache_key):
        print(f"  📦 Using cached earnings surprises ({len(_cache[cache_key])} stocks)")
        return _cache[cache_key]

    # Probe with the first ticker to confirm the endpoint is available before
    # attempting all 4,000 stocks (avoids 4,000 noisy 404 warnings).
    _probe_ticker = tickers[0] if tickers else None
    _endpoint = None
    for _ep in ("earnings-surprises", "historical-earnings"):
        _probe = fmp_get(_ep, {"symbol": _probe_ticker, "limit": 2}) if _probe_ticker else None
        if _probe and isinstance(_probe, list):
            _endpoint = _ep
            break

    if not _endpoint:
        print("  ⏭️ Skipping earnings surprises (endpoint not available on this plan)")
        _cache[cache_key] = {}
        return {}

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    print(f"\n  📊 Fetching earnings surprises ({_endpoint})...")
    results = {}
    _lock = threading.Lock()
    _throttle = threading.Semaphore(4)

    def _f(t):
        with _throttle:
            data = fmp_get(_endpoint, {"symbol": t, "limit": 8})
            time.sleep(0.2)
            if data and isinstance(data, list) and len(data) >= 2:
                return t, data
            return t, None

    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = {pool.submit(_f, t): t for t in tickers}
        done = 0
        for fut in as_completed(futs):
            t, data = fut.result()
            if data:
                with _lock: results[t] = data
            done += 1
            if done % 100 == 0: print(f"    [{done}/{len(tickers)}] earnings surprises fetched...")

    _cache[cache_key] = results
    print(f"  ✅ earnings surprises loaded: {len(results)} stocks")
    return results


def _parse_openinsider_table(html: str) -> list:
    """Parse the tinytable from an OpenInsider HTML page into trade dicts."""
    from html.parser import HTMLParser
    import re

    class _Parser(HTMLParser):
        def __init__(self):
            super().__init__()
            self._in_t = False; self._depth = 0
            self._in_row = False; self._in_cell = False
            self.rows = []; self._row = []; self._cell = ""

        def handle_starttag(self, tag, attrs):
            amap = dict(attrs)
            if tag == "table":
                if "tinytable" in amap.get("class", "") or "tinytable" in amap.get("id", ""):
                    self._in_t = True; self._depth = 1
                elif self._in_t:
                    self._depth += 1
            if not self._in_t: return
            if tag == "tr":
                self._in_row = True; self._row = []
            elif tag in ("td", "th") and self._in_row:
                self._in_cell = True; self._cell = ""
            elif tag == "br" and self._in_cell:
                self._cell += " "

        def handle_endtag(self, tag):
            if tag == "table" and self._in_t:
                self._depth -= 1
                if self._depth <= 0: self._in_t = False
            if not self._in_t: return
            if tag == "tr" and self._in_row:
                self._in_row = False
                if self._row: self.rows.append(self._row)
                self._row = []
            elif tag in ("td", "th") and self._in_cell:
                self._in_cell = False
                self._row.append(" ".join(self._cell.split()))
                self._cell = ""

        def handle_data(self, data):
            if self._in_cell: self._cell += data

    parser = _Parser()
    parser.feed(html)
    if not parser.rows:
        return []

    # Detect header row to find column positions dynamically
    header = [c.lower() for c in parser.rows[0]]
    def _col(names):
        for i, h in enumerate(header):
            if any(n in h for n in names): return i
        return None

    # Known OpenInsider column order (0=X, 1=FilingDate, 2=TradeDate,
    # 3=Ticker, 4=Company, 5=Name, 6=Title, 7=TradeType, 8=Price,
    # 9=Qty, 10=Owned, 11=ΔOwn, 12=Value)
    has_header = len(header) >= 5 and any(h in ("ticker", "x") for h in header[:5])
    if has_header:
        idx = {
            "date":    _col(["trade date"]) or _col(["date"]) or 2,
            "ticker":  _col(["ticker"]) or 3,
            "company": _col(["company", "issuer"]) or 4,
            "name":    _col(["insider", "owner", "reporting"]) or 5,
            "title":   _col(["title", "relationship"]) or 6,
            "type":    _col(["trade type", "transaction"]) or 7,
            "price":   _col(["price"]) or 8,
            "qty":     _col(["qty", "shares"]) or 9,
            "value":   _col(["value"]) or 12,
        }
        data_rows = parser.rows[1:]
    else:
        idx = {"date": 2, "ticker": 3, "company": 4, "name": 5,
               "title": 6, "type": 7, "price": 8, "qty": 9, "value": 12}
        data_rows = parser.rows

    def _num(s):
        s = re.sub(r'[+$,\s]', '', s).replace("(", "-").replace(")", "")
        if not s or s in ("-", ""): return 0.0
        try:
            if s.upper().endswith("M"): return float(s[:-1]) * 1_000_000
            if s.upper().endswith("K"): return float(s[:-1]) * 1_000
            if s.upper().endswith("B"): return float(s[:-1]) * 1_000_000_000
            return float(s)
        except: return 0.0

    max_idx = max(idx.values())
    trades = []
    for cells in data_rows:
        if len(cells) <= max_idx: continue
        ticker = re.sub(r'[^A-Z]', '', cells[idx["ticker"]].upper())
        if not ticker or len(ticker) > 6: continue
        trade_type = cells[idx["type"]]
        if not trade_type.upper().startswith("P"): continue  # purchases only
        price = _num(cells[idx["price"]])
        qty   = abs(_num(cells[idx["qty"]]))
        value = _num(cells[idx["value"]])
        if value == 0 and price > 0 and qty > 0:
            value = price * qty
        trades.append({
            "symbol":               ticker,
            "transactionType":      "P-Purchase",
            "transactionDate":      cells[idx["date"]],
            "securitiesTransacted": qty,
            "price":                price,
            "reportingName":        cells[idx["name"]],
            "typeOfOwner":          cells[idx["title"]],
            "_value":               value,
            "_source":              "openinsider",
            "_company":             cells[idx["company"]],
        })
    return trades


def fetch_openinsider_data(days: int = 30, min_value_k: int = 50) -> list:
    """Scrape recent insider purchases from openinsider.com (free, no API key).
    Returns trades in FMP-compatible format so downstream code works unchanged.
    Caches for 1 day.
    """
    cache_key = "openinsider"
    cached = _cache.get(cache_key, [])
    if cached:
        print(f"  📦 Using cached OpenInsider data ({len(cached)} trades)")
        return cached

    print("  🌐 Fetching insider purchases from openinsider.com...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/123.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://openinsider.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })

    screener_params = (
        f"s=&o=&pl=&ph=&ll=&lh=&fd={days}&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&"
        f"xp=1&vl={min_value_k}&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&"
        f"grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&"
        f"sortcol=0&cnt=500&Action=1"
    )
    urls = [
        f"https://openinsider.com/screener?{screener_params}",
        "https://openinsider.com/clustered-buys",
    ]
    all_trades = []

    def _fetch_html(url, session, timeout=25):
        """Try requests first, fall back to stdlib urllib (different TLS stack)."""
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text
        except Exception:
            pass
        # urllib fallback — different TLS fingerprint, sometimes bypasses blocks
        try:
            import urllib.request, ssl
            req = urllib.request.Request(url)
            for k, v in session.headers.items():
                req.add_header(k, v)
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        return None

    for url in urls:
        html = _fetch_html(url, session)
        if html:
            trades = _parse_openinsider_table(html)
            if trades:
                all_trades.extend(trades)
                print(f"    ✅ {len(trades)} trades from openinsider.com")
                time.sleep(0.5)
        else:
            print(f"  ⚠️ OpenInsider unreachable — check Windows Firewall / antivirus")

    # Deduplicate by (ticker, date, qty)
    seen, deduped = set(), []
    for tr in all_trades:
        key = (tr["symbol"], tr["transactionDate"], tr["securitiesTransacted"])
        if key not in seen:
            seen.add(key); deduped.append(tr)

    _cache[cache_key] = deduped
    print(f"  ✅ OpenInsider: {len(deduped)} insider purchases loaded")
    return deduped


def fetch_finviz_insider_data() -> list:
    """Scrape recent insider purchases from finviz.com/insidertrading.ashx.
    Used as fallback when openinsider.com is unreachable.
    """
    import re
    cache_key = "finviz_insider"
    cached = _cache.get(cache_key, [])
    if cached:
        return cached

    url = "https://finviz.com/insidertrading.ashx?or=-10&tc=1&o=-transactionDate"
    ua  = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36")
    headers = {"User-Agent": ua, "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://finviz.com/"}
    try:
        import urllib.request, ssl
        req = urllib.request.Request(url)
        for k, v in headers.items():
            req.add_header(k, v)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  ⚠️ Finviz insider fetch error: {str(e)[:80]}")
        return []

    # Finviz insider table: Ticker | Owner | Relationship | Date | Transaction | Cost | #Shares | Value | Total
    rows = []
    def _num(s):
        s = re.sub(r'[+$,\s]', '', s)
        try: return float(s)
        except: return 0.0

    # Find all <tr> blocks inside the insider table
    tr_blocks = re.findall(r'<tr[^>]*class="[^"]*insider[^"]*"[^>]*>(.*?)</tr>|'
                           r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
    for match in tr_blocks:
        tr = match[0] or match[1]
        tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL | re.IGNORECASE)
        if len(tds) < 8: continue
        strip = lambda s: re.sub(r'<[^>]+>', '', s).strip()
        cells = [strip(td) for td in tds]
        # columns: 0=Ticker, 1=Owner, 2=Relationship, 3=Date, 4=Transaction, 5=Cost, 6=#Shares, 7=Value
        txn = cells[4] if len(cells) > 4 else ""
        if "buy" not in txn.lower() and "purchase" not in txn.lower(): continue
        ticker = re.sub(r'[^A-Z]', '', cells[0].upper())
        if not ticker or len(ticker) > 6: continue
        price  = _num(cells[5]) if len(cells) > 5 else 0
        qty    = _num(cells[6]) if len(cells) > 6 else 0
        value  = _num(cells[7]) if len(cells) > 7 else price * qty
        rows.append({
            "symbol":               ticker,
            "transactionType":      "P-Purchase",
            "transactionDate":      cells[3] if len(cells) > 3 else "",
            "securitiesTransacted": qty,
            "price":                price,
            "reportingName":        cells[1] if len(cells) > 1 else "",
            "typeOfOwner":          cells[2] if len(cells) > 2 else "",
            "_value":               value,
            "_source":              "finviz",
        })

    _cache[cache_key] = rows
    if rows:
        print(f"    ✅ Finviz: {len(rows)} insider purchases")
    else:
        print(f"  ⚠️ Finviz: no purchases found (may need browser session)")
    return rows


def fetch_insider_trading(tickers: list = None) -> list:
    """Fetch recent insider purchases for the given ticker list.
    Global endpoint often fails on basic FMP tiers — per-ticker is more reliable.
    """
    cache_key = "insider"
    if _cache.get(cache_key):
        print(f"  📦 Using cached insider data ({len(_cache[cache_key])} transactions)")
        return _cache[cache_key]

    print("\n  🏦 Fetching insider trading data...")
    all_trades = []

    # Try global feed first; many FMP plans return 404 for this endpoint
    _insider_available = False
    for ep in ["insider-trading", "insider-trading-rss-feed"]:
        data = fmp_get(ep, {"limit": 500, "transactionType": "P-Purchase"})
        if data and isinstance(data, list) and len(data) > 5:
            all_trades = [tr for tr in data if _is_purchase(tr.get("transactionType", ""))]
            if all_trades:
                _insider_available = True
                print(f"    ✅ Global feed: {len(all_trades)} purchases")
                break
        time.sleep(0.3)

    # Per-ticker fallback — only attempt if global feed wasn't a 404
    if not _insider_available and tickers:
        # Probe one ticker to check if endpoint is available at all
        _probe = fmp_get("insider-trading", {"symbol": tickers[0], "limit": 1})
        if _probe is not None:  # None = 404; [] = available but no data
            print(f"    🔄 Fetching per-ticker insider data for top {min(100, len(tickers))} stocks...")
            for t in tickers[:100]:
                data = fmp_get("insider-trading", {"symbol": t, "limit": 10})
                if data and isinstance(data, list):
                    all_trades.extend(tr for tr in data if _is_purchase(tr.get("transactionType", "")))
                time.sleep(0.1)
            # Deduplicate
            seen = set()
            deduped = []
            for tr in all_trades:
                key = (tr.get("symbol"), tr.get("filingDate"), tr.get("securitiesTransacted"))
                if key not in seen:
                    seen.add(key)
                    deduped.append(tr)
            all_trades = deduped
        else:
            print("    ⚠️ insider-trading endpoint not available on this FMP plan")

    if not all_trades:
        print("    🔄 Trying openinsider.com...")
        all_trades = fetch_openinsider_data()
    if not all_trades:
        print("    🔄 Trying finviz.com insider data...")
        all_trades = fetch_finviz_insider_data()

    _cache[cache_key] = all_trades
    print(f"  ✅ Insider trades loaded: {len(all_trades)} purchases")
    return all_trades


# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _is_purchase(txn_type: str) -> bool:
    """Return True if the insider transaction is a buy/purchase (not sale/award)."""
    t = txn_type.upper()
    return (t.startswith("P") or "PURCHASE" in t or "BUY" in t) and "SALE" not in t


def _is_common_stock(s: dict) -> bool:
    """Filter out ETFs, mutual funds, preferred stocks from Lynch/quality tabs.
    These pollute the results with non-actionable picks.
    """
    ticker = s.get("ticker", "")
    name = (s.get("name") or "").lower()
    # Preferred stocks: tickers like MER-PK, BAC-PL, C-PN
    if "-" in ticker:
        return False
    # Mutual fund tickers typically 5 chars ending in X (RNNEX, IFAFX, AMECX, AMPFX)
    if len(ticker) == 5 and ticker.endswith("X"):
        return False
    # Fund/ETF name keywords
    fund_kw = (
        "fund", " etf", "income fund", "bond fund", "money market",
        "index fund", "total return fund", "growth fund", "value fund",
        "equity fund", "balanced fund", "allocation fund",
        # Closed-end fund / CEF / BDC name patterns not ending in "fund":
        "municipal income", "municipal bond", "muni bond", "amt-free",
        "nuveen ", "pimco ", "eaton vance", "kayne anderson", "eagle point",
        "western asset", "blackrock income", "calamos", "gabelli",
        # Closed-end / specialty finance vehicles with no real equity earnings:
        "credit company", "credit corp", "credit opportunity",
    )
    if any(kw in name for kw in fund_kw):
        return False
    return True


def _first(*args):
    """Return first non-None value. Handles 0.0 and negative numbers correctly
    (unlike `or` which treats 0.0 and negatives as falsy and skips them).
    """
    for a in args:
        if a is not None:
            return a
    return None


# ─────────────────────────────────────────────
# DATA ASSEMBLY — Merge all FMP data into unified stock records
# ─────────────────────────────────────────────

def assemble_stock_data(universe, profiles, key_metrics, ratios_ttm, dcf_data,
                        estimates, scores, ratings, growth_data, insider_data,
                        balance_sheet=None, earnings_surp=None) -> dict:
    """Merge all FMP data sources into a single dict per stock."""
    print("\n  🔧 Assembling unified stock data...")


    # Build insider lookup: {ticker: {count, total_value, latest_date}}
    insider_lookup = defaultdict(lambda: {"count": 0, "totalValue": 0, "latestDate": ""})
    for trade in insider_data:
        t = trade.get("symbol", "")
        val = abs(trade.get("securitiesTransacted", 0) * (trade.get("price", 0) or 0))
        insider_lookup[t]["count"] += 1
        insider_lookup[t]["totalValue"] += val
        d = trade.get("transactionDate", "")
        if d > insider_lookup[t]["latestDate"]:
            insider_lookup[t]["latestDate"] = d

    stocks = {}
    for t, u in universe.items():
        prof = profiles.get(t, {})
        km = key_metrics.get(t, {})
        rtm = ratios_ttm.get(t, {})
        dcf = dcf_data.get(t, {})
        est = estimates.get(t, {})
        sc = scores.get(t, {})
        rat = ratings.get(t, {})
        # growth_data is now a list (most recent year first); gr = most recent, gr_prev = prior year
        _gr_list = growth_data.get(t, [])
        if isinstance(_gr_list, list) and _gr_list:
            gr      = _gr_list[0]                                     # current year
            gr_prev = _gr_list[1] if len(_gr_list) > 1 else {}        # prior year
            gr_yr2  = _gr_list[2] if len(_gr_list) > 2 else {}        # year before that
            gr_yr3  = _gr_list[3] if len(_gr_list) > 3 else {}        # 3 years ago
            gr_yr4  = _gr_list[4] if len(_gr_list) > 4 else {}        # 4 years ago
        elif isinstance(_gr_list, dict):
            gr = _gr_list; gr_prev = {}; gr_yr2 = {}; gr_yr3 = {}; gr_yr4 = {}
        else:
            gr = {}; gr_prev = {}; gr_yr2 = {}; gr_yr3 = {}; gr_yr4 = {}
        bs = (balance_sheet or {}).get(t, {})
        es = (earnings_surp or {}).get(t, [])
        ins = insider_lookup.get(t, {})

        price = prof.get("price") or u.get("price") or 0
        mktCap = prof.get("mktCap") or u.get("mktCap") or u.get("marketCap") or 0
        # PE: try multiple sources (ratios_ttm uses priceToEarningsRatioTTM)
        pe_val = (prof.get("pe") or u.get("pe") or
                  rtm.get("priceToEarningsRatioTTM") or
                  km.get("peRatioTTM") or km.get("priceEarningsRatioTTM") or
                  prof.get("peRatio") or u.get("peRatio"))
        # Fallback: compute from earningsYield
        if not pe_val and km.get("earningsYieldTTM") and km["earningsYieldTTM"] > 0.001:
            pe_val = round(1 / km["earningsYieldTTM"], 1)
        # Sanity: zero, negative, or extreme PE is not useful for screening
        # Note: must use `is not None` — `if pe_val` would silently pass pe_val=0
        if pe_val is not None and (pe_val < 0.5 or pe_val > 500):
            pe_val = None
        elif pe_val is not None:
            pe_val = round(pe_val, 1)
        if not price or price <= 0:
            continue

        # PEG calculation — primary: FMP analyst EPS CAGR from estimates endpoint
        peg = None
        growth_5y = None
        rev_growth_5y = None
        if isinstance(est, list) and len(est) >= 2:
            eps_vals = [d.get("epsAvg") or d.get("estimatedEpsAvg") for d in est
                        if (d.get("epsAvg") or d.get("estimatedEpsAvg")) and (
                                    d.get("epsAvg") or d.get("estimatedEpsAvg")) > 0]
            rev_vals = [d.get("revenueAvg") or d.get("estimatedRevenueAvg") for d in est
                        if (d.get("revenueAvg") or d.get("estimatedRevenueAvg")) and (
                                    d.get("revenueAvg") or d.get("estimatedRevenueAvg")) > 0]
            if len(eps_vals) >= 2:
                n = len(eps_vals) - 1
                try:
                    growth_5y = round((eps_vals[0] / eps_vals[-1]) ** (1 / n) - 1, 4)
                except:
                    pass
            if len(rev_vals) >= 2:
                n = len(rev_vals) - 1
                try:
                    rev_growth_5y = round((rev_vals[0] / rev_vals[-1]) ** (1 / n) - 1, 4)
                except:
                    pass
        elif isinstance(est, dict):
            growth_5y = est.get("epsGrowth5y")

        if pe_val and pe_val > 5 and growth_5y and 0.03 < growth_5y < 0.60:
            peg = round(pe_val / (growth_5y * 100), 2)
            if peg < 0.2 or peg > 50:
                peg = None

        # Fallback PEG: use trailing growth when analyst estimates unavailable
        # Lynch himself used the most recent available growth rate — this is more authentic
        if peg is None and pe_val and pe_val > 3:
            eg_tr = _first(gr.get("epsgrowth"), gr.get("epsGrowth"))
            rg_tr = _first(gr.get("revenueGrowth"), gr.get("revGrowth"))
            trailing_g = eg_tr if (eg_tr and 0.03 < eg_tr < 0.60) else (
                         rg_tr if (rg_tr and 0.03 < rg_tr < 0.60) else None)
            if trailing_g:
                _peg_fb = round(pe_val / (trailing_g * 100), 2)
                if 0.2 < _peg_fb < 50:
                    peg = _peg_fb

        # ── Forward P/E and Forward PEG ────────────────────────────────────────
        # Use est[0] (next fiscal year) for a fully consistent forward valuation.
        # Avoids the trailing-P/E ÷ forward-growth mismatch that creates fake cheap signals.
        fwd_pe = None
        fwd_peg = None
        if isinstance(est, list) and est and price > 0:
            _next_eps = est[0].get("epsAvg") or est[0].get("estimatedEpsAvg")
            if _next_eps and _next_eps > 0:
                _fpe = round(price / _next_eps, 1)
                if 4 < _fpe < 150:
                    fwd_pe = _fpe
                    if growth_5y and 0.03 < growth_5y < 0.60:
                        _fpeg = round(_fpe / (growth_5y * 100), 2)
                        if 0.1 < _fpeg < 50:
                            fwd_peg = _fpeg

        # IV from DCF — cap at 8× price to filter FMP model outliers (e.g. some insurers/HMOs)
        iv = dcf.get("dcf") or dcf.get("dcfValue")
        if iv is not None and iv <= 0:
            iv = None  # negative IV = DCF artifact (e.g. negative equity base) — not meaningful
        if iv and price > 0 and iv > price * 8:
            iv = None  # discard — model artifact, not investable signal
        mos = None
        if iv and iv > 0 and price > 0:
            mos = round((iv - price) / iv, 4)

        # ── Net cash per share (from balance sheet) ────────────────────────
        # Net cash = cash + short-term investments - total debt
        # Positive net cash = company has more cash than debt (asset play signal)
        # SKIP Financial Services: their "shortTermInvestments" is insurance float /
        # loan book / deposit-backed assets — not free cash. Net cash metric is
        # meaningless for banks, insurers, and reinsurers.
        _sector_raw = (u.get("sector") or prof.get("sector") or "").lower()
        _is_financial = "financial" in _sector_raw or "insurance" in _sector_raw
        net_cash_per_share = None
        net_cash_ratio = None  # netCashPerShare / price — how much of price is pure cash
        if bs and not _is_financial:
            _cash = _first(bs.get("cashAndCashEquivalents"), bs.get("cash")) or 0
            _st_inv = bs.get("shortTermInvestments") or 0
            _total_debt = _first(bs.get("totalDebt"), bs.get("longTermDebt")) or 0
            # Derive shares from mktCap/price first (most reliable — avoids balance sheet unit ambiguity)
            # commonStock = par value dollars, NOT shares — never use it for share count
            if mktCap and price > 0:
                _shares = mktCap / price
            else:
                _shares = _first(bs.get("sharesOutstanding"), bs.get("weightedAverageShsOut"))
            _net_cash = _cash + _st_inv - _total_debt
            if _shares and _shares > 0:
                _ncps = _net_cash / _shares
                # Sanity: if net cash per share is unreasonably large (>10× price), discard
                if price > 0 and abs(_ncps) <= price * 10:
                    net_cash_per_share = round(_ncps, 2)
                    if price > 0:
                        net_cash_ratio = round(_ncps / price, 4)

        # ── Earnings beat rate (from earnings surprises) ───────────────────
        # beat_rate = fraction of last N quarters where actual EPS > estimated EPS
        beat_rate = None
        eps_beat_streak = 0  # consecutive quarters beating estimates (most recent first)
        if isinstance(es, list) and len(es) >= 2:
            beats = []
            for rec in es:
                actual = rec.get("actualEarningResult") or rec.get("actual")
                est_eps = rec.get("estimatedEarning") or rec.get("estimated")
                if actual is not None and est_eps is not None:
                    beats.append(1 if actual >= est_eps else 0)
            if beats:
                beat_rate = round(sum(beats) / len(beats), 3)
                for b in beats:  # most recent first in FMP response
                    if b == 1:
                        eps_beat_streak += 1
                    else:
                        break

        # ── 52-week positioning ────────────────────────────────────────────
        _yr_high = u.get("yearHigh")
        _yr_low  = u.get("yearLow")
        price_vs_52h = round(price / _yr_high, 4) if (_yr_high and _yr_high > 0) else None
        price_vs_52l = round(price / _yr_low,  4) if (_yr_low  and _yr_low  > 0) else None

        # ── Per-share data corruption check ────────────────────────────────
        # FMP occasionally stores total company figures as per-share values for certain tickers
        # (e.g. MCHB: bookValuePerShare=$129,595 at price=$14 — bank total assets stored as per-share)
        # When detected, nullify all ratios derived from book/equity so they don't corrupt rankings.
        _bv_ps = _first(rtm.get("bookValuePerShareTTM"))
        _per_share_corrupt = (_bv_ps is not None and price > 0 and _bv_ps > price * 100)

        # ── Per-metric bounds checks ────────────────────────────────────────
        # P/B > 200 is not screening-useful (negative book or data error); < 0 is invalid
        _pb_raw = _first(rtm.get("priceToBookRatioTTM"))
        pb_val = (_pb_raw if (_pb_raw is not None and 0 < _pb_raw <= 200) else None)

        # ROE/ROA/ROIC: values > ±500% are almost always FMP errors (tiny denominator)
        # Round to 4 decimal places (e.g. 0.1523 → 0.1523 = 15.23%) to avoid spurious precision
        _roe_raw = _first(km.get("returnOnEquityTTM"))
        roe_val = (round(_roe_raw, 4) if (_roe_raw is not None and abs(_roe_raw) <= 5.0) else None)
        # Sanity 1: negative TOTAL equity (e.g. PEGA — heavy buybacks) makes ROE denominator invalid.
        # IMPORTANT: use total bookValuePerShare (NOT tangible).
        # Companies like ADBE have negative *tangible* equity from goodwill/intangibles but positive
        # total equity, and FMP's returnOnEquityTTM correctly uses total equity — so tangible BVPS
        # is irrelevant here and would incorrectly nullify perfectly valid ROE figures.
        _bvps_total = _first(km.get("bookValuePerShareTTM"), rtm.get("bookValuePerShareTTM"))
        if roe_val is not None and _bvps_total is not None and _bvps_total <= 0:
            roe_val = None  # negative total equity — ROE denominator is meaningless
        # Sanity 2: ROE >> ROIC by a large margin = leverage-inflated ROE, not genuine returns.
        # e.g. PEGA ROE=60%, ROIC=12% → the 60% is entirely driven by financial leverage, not operations.
        # A quality screen should use ROIC; flag ROE as unreliable when divergence is extreme.
        _roic_for_roe_check = _first(km.get("returnOnInvestedCapitalTTM"))
        if (roe_val is not None and _roic_for_roe_check is not None
                and _roic_for_roe_check > 0
                and roe_val > _roic_for_roe_check * 3.0):
            roe_val = None  # leverage-inflated — ROIC is the reliable signal here

        _roa_raw = _first(km.get("returnOnAssetsTTM"))
        roa_val = (round(_roa_raw, 4) if (_roa_raw is not None and abs(_roa_raw) <= 2.0) else None)

        _roic_raw = _first(km.get("returnOnInvestedCapitalTTM"))
        roic_val = (round(_roic_raw, 4) if (_roic_raw is not None and abs(_roic_raw) <= 5.0) else None)
        # Sanity cap: ROIC > 80% is a data artifact (near-zero invested capital base / leverage)
        # Even the best asset-light compounders (Visa, Adobe, Mastercard) run 30-50% ROIC
        # Above 0.80 is almost always a denominator artifact, not a real business signal
        if roic_val is not None and roic_val > 0.80:
            roic_val = None

        # Tangible book: if > 50× price it is certainly a data error
        _tb_raw = _first(rtm.get("tangibleBookValuePerShareTTM"))
        tb_val = (_tb_raw if (_tb_raw is None or price <= 0 or abs(_tb_raw) <= price * 50) else None)

        # D/E: values above 100 are either negative-equity artifacts or data errors
        _de_raw = _first(rtm.get("debtToEquityRatioTTM"))
        de_val = (_de_raw if (_de_raw is not None and -100 < _de_raw <= 100) else None)

        # Apply per-share corruption mask: zero out book-derived metrics
        if _per_share_corrupt:
            pb_val = roe_val = roa_val = roic_val = tb_val = None

        stocks[t] = {
            "ticker": t,
            "name": u.get("name", ""),
            "sector": u.get("sector", "Unknown"),
            "industry": u.get("industry", ""),
            "exchange": u.get("exchange", ""),
            "price": price,
            "mktCap": mktCap,
            "mktCapB": round(mktCap / 1e9, 2) if mktCap else 0,
            "pe": pe_val,
            "peg": peg,
            "fwdPE": fwd_pe,
            "fwdPEG": fwd_peg,
            # P/B, D/E, Div Yield, Gross Margin → ratios-ttm (confirmed field names)
            "pb": pb_val,
            "ps": _first(rtm.get("priceToSalesRatioTTM"), km.get("evToSalesTTM")),
            # ROE/ROA/ROIC → key-metrics-ttm (bounds-checked above)
            "roe": roe_val,
            "roa": roa_val,
            "roic": roic_val,
            # FCF yield: prefer 1/P_FCF (current-price-based) over pre-computed yield
            # (pre-computed uses stale market cap from last report date — distorts fast-movers
            #  and payment processors where gross TPV flows inflate raw FCF figures)
            "fcfYield": (lambda _pfcf, _fy: (
                1.0 / _pfcf if (_pfcf and _pfcf > 0)
                else (_fy if (_fy is not None and 0 < _fy <= 0.25) else None)
            ))(
                _first(km.get("priceToFreeCashFlowsRatioTTM"), rtm.get("priceToFreeCashFlowsRatioTTM")),
                _first(km.get("freeCashFlowYieldTTM"))
            ),
            # Div yield: ratios-ttm confirmed; fallback to profile lastDiv/price
            # Cap at 30% — above that is almost always a data error or imminent dividend cut
            "divYield": (lambda _dy: (min(_dy, 0.30) if _dy else None))(
                _first(
                    rtm.get("dividendYieldTTM"),
                    (prof.get("lastDiv") / price) if (prof.get("lastDiv") and price > 0) else None,
                )
            ),
            "de": de_val,
            "grossMargin": _first(rtm.get("grossProfitMarginTTM")),
            "operatingMargin": _first(rtm.get("operatingProfitMarginTTM")),
            "currentRatio": _first(rtm.get("currentRatioTTM"), km.get("currentRatioTTM")),
            "bookValue": (None if _per_share_corrupt else _bv_ps),
            "tangibleBook": tb_val,
            "iv": iv,
            "mos": mos,
            "piotroski": sc.get("piotroskiScore") or sc.get("piotroski"),
            "altmanZ": sc.get("altmanZScore") or sc.get("altmanZ"),
            "ratingScore": rat.get("ratingScore") or rat.get("score"),
            "recommendation": rat.get("ratingRecommendation") or rat.get("recommendation"),
            "epsGrowth5y": growth_5y,
            "revGrowth5y": rev_growth_5y,
            "revGrowth": _first(gr.get("revenueGrowth"), gr.get("revGrowth")),
            "epsGrowth": _first(gr.get("epsgrowth"), gr.get("epsGrowth")),
            "fcfGrowth": _first(gr.get("freeCashFlowGrowth"), gr.get("fcfGrowth")),
            "netIncomeGrowth": gr.get("netIncomeGrowth"),
            "fiveYRevGrowth": gr.get("fiveYRevenueGrowthPerShare"),
            # ── YoY growth trend — prior year for acceleration/deceleration detection ──
            "revGrowthPrev": _first(gr_prev.get("revenueGrowth"), gr_prev.get("revGrowth")),
            "epsGrowthPrev": _first(gr_prev.get("epsgrowth"), gr_prev.get("epsGrowth")),
            "revGrowthYr2":  _first(gr_yr2.get("revenueGrowth"),  gr_yr2.get("revGrowth")),
            "revGrowthYr3":  _first(gr_yr3.get("revenueGrowth"),  gr_yr3.get("revGrowth")),
            "revGrowthYr4":  _first(gr_yr4.get("revenueGrowth"),  gr_yr4.get("revGrowth")),
            # Revenue consistency: fraction of available years (up to 5) with positive growth
            # 1.0 = all years positive, 0.0 = all years negative
            "revConsistency": (lambda: (
                lambda _h: round(sum(1 for v in _h if v > 0) / len(_h), 2) if _h else None)(
                [v for v in [
                    _first(gr.get("revenueGrowth"), gr.get("revGrowth")),
                    _first(gr_prev.get("revenueGrowth"), gr_prev.get("revGrowth")),
                    _first(gr_yr2.get("revenueGrowth"), gr_yr2.get("revGrowth")),
                    _first(gr_yr3.get("revenueGrowth"), gr_yr3.get("revGrowth")),
                    _first(gr_yr4.get("revenueGrowth"), gr_yr4.get("revGrowth")),
                ] if v is not None]))(),
            # Share dilution: YoY change in weighted avg shares (negative = buybacks = good)
            "sharesGrowth": (lambda _sg: round(_sg, 4) if _sg is not None and abs(_sg) < 0.5 else None)(
                _first(gr.get("weightedAverageSharesGrowth"), gr.get("weightedAverageSharesDilutedGrowth"))
            ),
            # FCF Margin = FCF Yield × P/S ≈ FCF / Revenue (how much of each revenue dollar becomes FCF)
            # Formula: FCF/Revenue = (FCF/MktCap) × (MktCap/Revenue) = fcfYield × P/S
            "fcfMargin": (lambda _fy, _ps: round(_fy * _ps, 3)
                          if (_fy and _fy > 0 and _ps and _ps > 0) else None)(
                (1.0 / _first(km.get("priceToFreeCashFlowsRatioTTM"), rtm.get("priceToFreeCashFlowsRatioTTM")))
                if _first(km.get("priceToFreeCashFlowsRatioTTM"), rtm.get("priceToFreeCashFlowsRatioTTM")) else
                _first(km.get("freeCashFlowYieldTTM")),
                _first(rtm.get("priceToSalesRatioTTM"), km.get("evToSalesTTM"))
            ),
            # FCF Growth Consistency: fraction of available years (up to 5) with positive FCF growth
            "fcfGrowthConsistency": (lambda: (
                lambda _h: round(sum(1 for v in _h if v > 0) / len(_h), 2) if _h else None)(
                [v for v in [
                    _first(gr.get("freeCashFlowGrowth"), gr.get("fcfGrowth")),
                    _first(gr_prev.get("freeCashFlowGrowth"), gr_prev.get("fcfGrowth")),
                    _first(gr_yr2.get("freeCashFlowGrowth"), gr_yr2.get("fcfGrowth")),
                    _first(gr_yr3.get("freeCashFlowGrowth"), gr_yr3.get("fcfGrowth")),
                    _first(gr_yr4.get("freeCashFlowGrowth"), gr_yr4.get("fcfGrowth")),
                ] if v is not None]))(),
            # FCF Conversion = FCF Yield × P/E ≈ FCF / Net Income
            # 1.0 = FCF equals earnings (high quality); <0.5 = accounting tricks suspected
            "fcfConversion": (lambda _fy, _pe: round(_fy * _pe, 2)
                              if (_fy and _fy > 0 and _pe and 0 < _pe < 200) else None)(
                (1.0 / _first(km.get("priceToFreeCashFlowsRatioTTM"), rtm.get("priceToFreeCashFlowsRatioTTM")))
                if _first(km.get("priceToFreeCashFlowsRatioTTM"), rtm.get("priceToFreeCashFlowsRatioTTM")) else
                _first(km.get("freeCashFlowYieldTTM")),
                pe_val
            ),
            # EPS Growth Optimism: forward EPS CAGR vs 2yr historical EPS growth average
            # If analysts project EPS growing much faster than it has historically, that's a red flag
            "epsGrowthOptimism": (lambda: (
                lambda _fwd, _hist: round(_fwd / _hist - 1, 2)
                if (_fwd and _hist and _hist > 0.01) else None)(
                growth_5y,  # forward 5yr EPS CAGR from analyst estimates
                (lambda _h: sum(_h) / len(_h) if _h else None)(
                    [v for v in [
                        _first(gr_prev.get("epsgrowth"), gr_prev.get("epsGrowth")),
                        _first(gr_yr2.get("epsgrowth"), gr_yr2.get("epsGrowth")),
                    ] if v is not None and v > 0]
                )
            ))(),
            # Growth Optimism = forward analyst estimate CAGR vs 3yr historical average
            # >0 means analysts are more optimistic than history; flag >0.50 as unreliable
            "growthOptimism": (lambda: (
                lambda _fwd, _hist: round(_fwd / _hist - 1, 2)
                if (_fwd and _hist and _hist > 0.01) else None)(
                rev_growth_5y,
                (lambda _h: sum(_h) / len(_h) if _h else None)(
                    [v for v in [
                        _first(gr_prev.get("revenueGrowth"), gr_prev.get("revGrowth")),
                        _first(gr_yr2.get("revenueGrowth"), gr_yr2.get("revGrowth")),
                        _first(gr_yr3.get("revenueGrowth"), gr_yr3.get("revGrowth")),
                    ] if v is not None and v > 0]
                )
            ))(),
            "insiderBuys": ins.get("count", 0),
            "insiderValue": ins.get("totalValue", 0),
            "insiderDate": ins.get("latestDate", ""),
            "beta": prof.get("beta") or u.get("beta"),
            # ── New enrichment fields ──────────────────────────────────────
            # EV/EBITDA — better than P/E for cyclicals + capital-intensive sectors
            "evEbitda": _first(km.get("evToEbitdaTTM")),
            # P/FCF — cash-flow based valuation (more reliable than P/E for many sectors)
            # Prefer direct ratio; fall back to 1/fcfYield when ratio is unavailable
            "pFcf": (lambda _r, _fy: (
                _r if (_r and 0 < _r < 500)
                else (round(1.0 / _fy, 1) if (_fy and _fy > 0) else None)
            ))(
                _first(rtm.get("priceToFreeCashFlowsRatioTTM"),
                       km.get("priceToFreeCashFlowsRatioTTM")),
                _first(km.get("freeCashFlowYieldTTM"),
                       rtm.get("freeCashFlowYieldTTM")),
            ),
            # Graham Net-Net per share — Benjamin Graham's cheapest possible valuation
            # (current assets - ALL liabilities) / shares; positive = buying below net working capital
            "grahamNetNet": _first(km.get("grahamNetNetTTM")),
            # Net Debt/EBITDA — leverage quality metric (negative = net cash position)
            "netDebtEbitda": _first(km.get("netDebtToEBITDATTM")),
            # Net cash per share (computed from balance sheet above)
            "netCashPerShare": net_cash_per_share,
            "netCashRatio": net_cash_ratio,  # netCashPerShare / price
            # Earnings beat metrics
            "beatRate": beat_rate,           # 0.0–1.0, e.g. 0.875 = beat 7 of 8 quarters
            "epsBeatStreak": eps_beat_streak,  # consecutive quarterly beats (most recent)
            # 52-week positioning
            "priceVs52H": price_vs_52h,  # price / 52wk high; <0.7 = beaten down
            "priceVs52L": price_vs_52l,  # price / 52wk low; >1.5 = well off lows
        }

    print(f"  ✅ Assembled {len(stocks)} stocks with full data")
    return stocks


# ─────────────────────────────────────────────
# EXCEL HELPERS
# ─────────────────────────────────────────────

def add_title(ws, title, subtitle="", row=1):
    """Add a styled title row."""
    ws.cell(row=row, column=1, value=title).font = Font(bold=True, name="Arial", size=14, color="FFFFFF")
    ws.cell(row=row, column=1).fill = PatternFill("solid", fgColor="1A237E")
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=20)
    ws.row_dimensions[row].height = 30
    if subtitle:
        row += 1
        ws.cell(row=row, column=1, value=subtitle).font = Font(italic=True, name="Arial", size=9, color="555555")
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=20)
    return row + 1


def write_table(ws, rows, headers, start_row, header_color="1A237E", widths=None, freeze=True):
    """Write a formatted table with headers and data rows."""
    # Headers
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=start_row, column=ci, value=h)
        c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor=header_color)
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = THIN_BORDER
    ws.row_dimensions[start_row].height = 26
    hdr_row = start_row
    start_row += 1

    # Data rows
    for ri, row in enumerate(rows):
        for ci, h in enumerate(headers, 1):
            v = row.get(h)
            cell = ws.cell(row=start_row, column=ci, value=v)
            cell.border = THIN_BORDER
            cell.font = Font(name="Arial", size=9)
            cell.fill = ALT_FILL if ri % 2 == 0 else PLAIN_FILL
            cell.alignment = Alignment(horizontal="center", vertical="center")

            # Formatting
            if h == "Price" and isinstance(v, (int, float)):
                cell.number_format = "$#,##0.00"
            elif h == "IV" and isinstance(v, (int, float)):
                cell.number_format = "$#,##0.00"
            elif h == "ROIC" and isinstance(v, (int, float)):
                cell.number_format = "0.0%"
                # Colour-code against ROIC benchmarks: <5% weak, 5-10% ok, 10-15% good, 15-20% very good, 20%+ excellent
                if v >= 0.20:
                    cell.font = Font(bold=True, name="Arial", size=9, color="1B5E20")   # excellent — bold green
                elif v >= 0.15:
                    cell.font = Font(name="Arial", size=9, color="1B5E20")               # very good — green
                elif v >= 0.10:
                    cell.font = Font(name="Arial", size=9, color="33691E")               # good — muted green
                elif v >= 0.05:
                    cell.font = Font(name="Arial", size=9, color="827717")               # ok — amber
                else:
                    cell.font = Font(name="Arial", size=9, color="B71C1C")               # weak — red
            elif h in ("MoS", "ROE", "ROA", "FCF Yield", "Div Yield", "Gross Margin",
                       "Rev Growth", "EPS Growth", "Rev Growth 5Y", "EPS Growth 5Y",
                       "Avg MoS", "Avg FCF Yield", "Avg ROE") and isinstance(v, (int, float)):
                cell.number_format = "0.0%"
                if v > 0.01:
                    cell.font = Font(name="Arial", size=9, color="1B5E20")
                elif v < -0.01:
                    cell.font = Font(name="Arial", size=9, color="B71C1C")
            elif h in ("PEG", "Fwd PEG") and isinstance(v, (int, float)):
                cell.number_format = "0.00"
                if 0 < v < 1.0:
                    cell.fill = PatternFill("solid", fgColor="1B5E20")
                    cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                elif 0 < v < 1.5:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
                elif 0 < v < 2.0:
                    cell.fill = PatternFill("solid", fgColor="FFF9C4")
            elif h in ("Shares Δ",) and isinstance(v, (int, float)):
                cell.number_format = "+0.0%;-0.0%;0%"
                if v < -0.02:   # buybacks — green
                    cell.font = Font(name="Arial", size=9, color="1B5E20")
                elif v > 0.04:  # dilution — red
                    cell.font = Font(name="Arial", size=9, color="B71C1C")
            elif h in ("Rev Consist.",) and isinstance(v, (int, float)):
                cell.number_format = "0%"
                if v >= 0.80:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
                elif v < 0.40:
                    cell.fill = PatternFill("solid", fgColor="FFCDD2")
            elif h in ("FCF Margin",) and isinstance(v, (int, float)):
                cell.number_format = "0%"
                if v >= 0.20:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")  # high margin — great
                elif v >= 0.10:
                    cell.fill = PatternFill("solid", fgColor="F1F8E9")
                elif v < 0.05:
                    cell.fill = PatternFill("solid", fgColor="FFF9C4")  # thin margin — caution
            elif h in ("Oper Margin",) and isinstance(v, (int, float)):
                cell.number_format = "0%"
                if v >= 0.25:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")  # high op margin — scalable biz
                elif v >= 0.10:
                    cell.fill = PatternFill("solid", fgColor="F1F8E9")
                elif v < 0.05:
                    cell.fill = PatternFill("solid", fgColor="FFCDD2")  # thin/negative — caution
            elif h in ("FCF Consist.",) and isinstance(v, (int, float)):
                cell.number_format = "0%"
                if v >= 0.80:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
                elif v < 0.40:
                    cell.fill = PatternFill("solid", fgColor="FFCDD2")
            elif h in ("FCF Conv.",) and isinstance(v, (int, float)):
                cell.number_format = "0.00"
                if v >= 0.80:   # FCF ≈ earnings — high quality
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
                elif v < 0.50:  # FCF << earnings — accounting concern
                    cell.fill = PatternFill("solid", fgColor="FFCDD2")
            elif h in ("Grwth Gap", "EPS Gap") and isinstance(v, (int, float)):
                cell.number_format = "+0%;-0%;0%"
                if v > 0.50:    # analysts far too optimistic — flag red
                    cell.font = Font(name="Arial", size=9, color="B71C1C")
                    cell.fill = PatternFill("solid", fgColor="FFCDD2")
                elif v < 0:     # analysts conservative vs history — positive signal
                    cell.font = Font(name="Arial", size=9, color="1B5E20")
            elif h in ("52w Pos", "52w vs High") and isinstance(v, (int, float)):
                cell.number_format = "0%"
                if v <= 0.70:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")   # pulled back — good entry
                elif v >= 0.95:
                    cell.fill = PatternFill("solid", fgColor="FFF9C4")   # extended — near highs
            elif h == "52w vs Low" and isinstance(v, (int, float)):
                cell.number_format = "0%"
                if v <= 1.30:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")   # near lows — early recovery
                elif v >= 1.75:
                    cell.fill = PatternFill("solid", fgColor="FFF9C4")   # well off lows — less upside
            elif h == "Net D/E" and isinstance(v, (int, float)):
                cell.number_format = "0.0"
                if v <= 2.0:
                    cell.font = Font(name="Arial", size=9, color="1B5E20")  # low leverage — green
                elif v >= 5.0:
                    cell.font = Font(name="Arial", size=9, color="B71C1C")  # high leverage — red
            elif h in ("P/E", "Fwd P/E", "P/B", "P/S", "D/E") and isinstance(v, (int, float)):
                cell.number_format = "0.0"
            elif h == "Piotroski" and isinstance(v, (int, float)):
                cell.font = Font(bold=True, name="Arial", size=10)
                if v >= 8:
                    cell.fill = PatternFill("solid", fgColor="1B5E20"); cell.font = Font(bold=True, name="Arial",
                                                                                         size=10, color="FFFFFF")
                elif v >= 6:
                    cell.fill = GREEN_FILL
                elif v <= 3:
                    cell.fill = RED_FILL
            elif h == "MktCap ($B)" and isinstance(v, (int, float)):
                cell.number_format = "$#,##0.0"
            elif h == "Cap Size" and isinstance(v, str):
                if v in ("Micro", "Small"):
                    cell.fill = PatternFill("solid", fgColor="E8F5E9")
                    cell.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                elif v == "Mid":
                    cell.fill = PatternFill("solid", fgColor="FFF9C4")
                elif v in ("Large", "Mega"):
                    cell.fill = PatternFill("solid", fgColor="FFCDD2")
            elif h == "Rating" and isinstance(v, str):
                if "Strong Buy" in str(v):
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
                elif "Buy" in str(v):
                    cell.fill = GREEN_FILL
                elif "Sell" in str(v):
                    cell.fill = RED_FILL
            elif h == "🏦 Insider" and v and str(v) != "0" and str(v) != "":
                cell.fill = GREEN_FILL;
                cell.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
            elif h == "Rank" and isinstance(v, int):
                cell.alignment = Alignment(horizontal="center", vertical="center")
                if v == 1:
                    cell.fill = PatternFill("solid", fgColor="FFD700")
                    cell.font = Font(bold=True, name="Arial", size=9, color="000000")
                elif v == 2:
                    cell.fill = PatternFill("solid", fgColor="C0C0C0")
                    cell.font = Font(bold=True, name="Arial", size=9, color="000000")
                elif v == 3:
                    cell.fill = PatternFill("solid", fgColor="CD7F32")
                    cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                elif v <= 10:
                    cell.font = Font(bold=True, name="Arial", size=9)

        ws.row_dimensions[start_row].height = 16
        start_row += 1

    # Column widths
    if widths:
        for ci, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(ci)].width = w

    if freeze:
        ws.freeze_panes = f"A{hdr_row + 1}"
    return start_row


def format_stock_row(s: dict) -> dict:
    """Convert assembled stock data to display-ready row."""
    ins_str = ""
    if s.get("insiderBuys", 0) >= 3:
        ins_str = f"🏦 {s['insiderBuys']}x ${s['insiderValue'] / 1000:.0f}K"
    elif s.get("insiderBuys", 0) >= 1:
        ins_str = f"🏦 {s['insiderBuys']}x"

    return {
        "Ticker": s["ticker"],
        "Company": s["name"],
        "Sector": s["sector"],
        "Price": s["price"],
        "IV": s.get("iv"),
        "MoS": s.get("mos"),
        "PEG": s.get("peg"),
        "Fwd PEG": s.get("fwdPEG"),
        "Fwd P/E": s.get("fwdPE"),
        "P/E": s.get("pe"),
        "P/B": s.get("pb"),
        "EV/EBITDA": s.get("evEbitda"),
        "P/FCF": s.get("pFcf"),
        "ROE": s.get("roe"),
        "ROIC": s.get("roic"),
        "ROA": s.get("roa"),
        "FCF Yield": s.get("fcfYield"),
        "Div Yield": s.get("divYield"),
        "D/E": s.get("de"),
        "Net Debt/EBITDA": s.get("netDebtEbitda"),
        "Curr Ratio": s.get("currentRatio"),
        "Gross Margin": s.get("grossMargin"),
        "Oper Margin": s.get("operatingMargin"),
        "Tangible Book": s.get("tangibleBook"),
        "Net Cash/Sh": s.get("netCashPerShare"),
        "Graham NN": s.get("grahamNetNet"),
        "Beat Rate": s.get("beatRate"),
        "Beat Streak": s.get("epsBeatStreak"),
        "52w Pos": s.get("priceVs52H"),   # price / 52wk high; 0.65 = 35% off high
        "52w vs High": s.get("priceVs52H"),  # alias used in Fast Growers / QC / Stalwarts columns
        "52w vs Low": s.get("priceVs52L"),   # price / 52wk low; 1.30 = 30% off low
        "Net D/E": s.get("netDebtEbitda"),   # net debt / EBITDA; displayed in Turnarounds
        "Rev Consist.": s.get("revConsistency"),
        "Shares Δ": s.get("sharesGrowth"),
        "FCF Conv.": s.get("fcfConversion"),
        "FCF Margin": s.get("fcfMargin"),
        "FCF Consist.": s.get("fcfGrowthConsistency"),
        "Grwth Gap": s.get("growthOptimism"),
        "EPS Gap": s.get("epsGrowthOptimism"),
        "Rev Growth": s.get("revGrowth"),
        "Rev Gr Prev": s.get("revGrowthPrev"),
        "EPS Growth": s.get("epsGrowth"),
        "Rev Growth 5Y": s.get("revGrowth5y"),
        "EPS Growth 5Y": s.get("epsGrowth5y"),
        "5Y Rev/Sh Gr": s.get("fiveYRevGrowth"),
        "Piotroski": s.get("piotroski"),
        "Altman Z": s.get("altmanZ"),
        "Rating": s.get("recommendation"),
        "MktCap ($B)": s.get("mktCapB"),
        "Cap Size": _cap_size_label(s.get("mktCap", 0)),
        "🏦 Insider": ins_str,
    }


# ─────────────────────────────────────────────
# TAB BUILDERS
# ─────────────────────────────────────────────

def build_iv_discount(wb, stocks):
    """Tab 2: Intrinsic Value Discount — Buffett style: good businesses at fair/cheap prices.
    Primary signal: DCF Margin of Safety. Confirmed by: FCF yield, ROE, ROIC, low P/E.
    Tighter quality gates than before — avoids value traps.
    """
    print("\n📊 Building Tab: IV Discount Picks...")
    qualified = []
    for t, s in stocks.items():
        if not _is_common_stock(s):
            continue
        iv    = s.get("iv")
        price = s.get("price", 0)
        mos   = s.get("mos")
        pio   = s.get("piotroski")
        fcf   = s.get("fcfYield")
        roe   = s.get("roe")
        az    = s.get("altmanZ")

        # Must have a valid DCF-based margin of safety
        if not iv or not price or price <= 0 or not mos:
            continue
        # Only undervalued stocks (positive MoS = DCF > price)
        if mos < 0.05:   # at least 5% discount — noise floor
            continue
        # Piotroski quality gate — stronger than before
        if pio is not None and pio < 6:
            continue
        # Altman Z — exclude financially distressed (bankruptcy zone)
        if az is not None and az < 1.5:
            continue
        # Earnings/cash quality gate — must have at least one positive quality signal
        # Avoids pure speculative DCF plays with no real cash flows
        has_fcf    = (fcf is not None and fcf > 0)
        has_roe    = (roe is not None and roe > 0.08)
        has_roic   = (s.get("roic") is not None and s.get("roic") > 0.08)
        has_pe     = (s.get("pe") and 0 < s.get("pe") < 40)
        if not (has_fcf or has_roe or has_roic or has_pe):
            continue

        # ── Scoring: pure IV-focus, no insider influence on rank ──────────
        score = 0

        # 1. Margin of Safety — the primary signal (40 pts max at 60% MoS)
        score += min(mos * 60, 40)

        # 2. Piotroski financial health — confirms the DCF story is real
        if pio and pio >= 9:   score += 14
        elif pio and pio >= 8: score += 10
        elif pio and pio >= 7: score += 7
        elif pio and pio >= 6: score += 3

        # 3. FCF yield — cash generation confirms DCF assumptions are credible
        if fcf and fcf > 0.12:   score += 12
        elif fcf and fcf > 0.08: score += 9
        elif fcf and fcf > 0.05: score += 6
        elif fcf and fcf > 0.02: score += 3

        # 4. ROIC — Buffett's favourite: only buy businesses that earn above cost of capital
        roic = s.get("roic")
        if roic and roic > 0.25:   score += 10
        elif roic and roic > 0.15: score += 7
        elif roic and roic > 0.10: score += 4

        # 5. ROE — return on equity confirms quality
        if roe and roe > 0.25:   score += 7
        elif roe and roe > 0.15: score += 4
        elif roe and roe > 0.08: score += 2

        # 6. Valuation confirmation: cheap on multiple metrics (multi-metric value = lower risk)
        valuation_confirms = 0
        pe  = s.get("pe")
        peg = s.get("peg")
        ps  = s.get("ps")
        if pe  and 0 < pe  < 15:   valuation_confirms += 1
        if pe  and 0 < pe  < 25:   valuation_confirms += 1
        if peg and 0 < peg < 1.0:  valuation_confirms += 1
        if peg and 0 < peg < 1.5:  valuation_confirms += 1
        if ps  and 0 < ps  < 1.0:  valuation_confirms += 1
        if s.get("pb") and 0 < s.get("pb") < 1.5: valuation_confirms += 1
        # EV/EBITDA < 12 = additional multi-metric cheap confirm
        ev_eb = s.get("evEbitda")
        if ev_eb and 0 < ev_eb < 12: valuation_confirms += 1
        score += valuation_confirms * 3   # up to 21 pts from multi-metric cheapness

        # 7. Low debt — clean balance sheet supports Buffett style hold
        de = s.get("de")
        if de is not None and de < 0.3:   score += 4
        elif de is not None and de < 0.8: score += 2

        # 8. Earnings beat rate — management consistently delivers
        br = s.get("beatRate")
        if br and br >= 0.875: score += 5
        elif br and br >= 0.75: score += 3

        row = format_stock_row(s)
        row["Score"] = round(score, 1)
        qualified.append(row)

    qualified.sort(key=lambda x: -x["Score"])
    for i, row in enumerate(qualified):
        row["Rank"] = i + 1

    # Main tab
    ws = wb.create_sheet("2. IV Discount Picks")
    ws.sheet_view.showGridLines = False
    sr = add_title(ws,
                   "💎 Intrinsic Value Discount — Good Businesses Trading Below DCF Value",
                   f"Filter: MoS≥5%, Piotroski≥6, Altman Z≥1.5, positive FCF/ROE/ROIC/PE. "
                   f"Score: MoS+Piotroski+FCF+ROIC+ROE+multi-metric value+beat rate. {datetime.date.today()}")

    headers = [
        "Rank", "Ticker", "Company", "Sector", "Price", "IV", "MoS",
        "P/E", "EV/EBITDA", "PEG", "P/B", "FCF Yield", "ROIC", "ROE", "D/E",
        "Beat Rate", "Piotroski", "Rev Growth", "EPS Growth 5Y",
        "MktCap ($B)", "Score", "🏦 Insider",
    ]
    widths = [5, 8, 22, 15, 8, 8, 7, 7, 9, 6, 6, 8, 7, 7, 7, 8, 7, 8, 9, 10, 6, 14]
    write_table(ws, qualified[:TOP_N], headers, sr, header_color="1A237E", widths=widths)
    print(f"  ✅ IV Discount tab done — {min(len(qualified), TOP_N)} stocks (from {len(qualified)} qualifying)")

    # By sector (exclude Rank from sector view for cleaner layout)
    sec_headers = headers[1:]  # drop Rank
    sec_widths = widths[1:]
    build_by_sector(wb, qualified, "2b. IV by Sector",
                    "💎 IV Discount by Sector", "1A237E", sec_headers, sec_widths)

    return qualified


def build_lynch_tab(wb, stocks, category_name, tab_number, filter_fn, sort_key,
                    color, description, custom_headers=None, custom_widths=None):
    """Generic Lynch category tab builder. Pass custom_headers/custom_widths to override defaults."""
    print(f"\n📊 Building Tab {tab_number}: {category_name}...")
    qualified = []
    for t, s in stocks.items():
        if filter_fn(s):
            row = format_stock_row(s)
            row["Score"] = round(sort_key(s), 1)
            qualified.append(row)

    qualified.sort(key=lambda x: -x["Score"])
    for i, row in enumerate(qualified):
        row["Rank"] = i + 1

    ws = wb.create_sheet(f"{tab_number}. {category_name}")
    ws.sheet_view.showGridLines = False
    sr = add_title(ws, f"📚 Lynch: {category_name}", description + f" — {datetime.date.today()}")

    headers = custom_headers or [
        "Rank", "Ticker", "Company", "Sector", "Price", "PEG", "P/E", "P/B", "IV", "MoS",
        "ROE", "FCF Yield", "Rev Growth", "EPS Growth 5Y", "Piotroski",
        "Div Yield", "MktCap ($B)", "Score", "🏦 Insider",
    ]
    widths = custom_widths or [5, 8, 22, 15, 8, 6, 7, 6, 8, 7, 7, 8, 8, 8, 7, 7, 10, 6, 14]
    write_table(ws, qualified[:TOP_N], headers, sr, header_color=color, widths=widths)

    print(f"  ✅ {category_name} tab done — {min(len(qualified), TOP_N)} (from {len(qualified)})")
    return qualified


def build_by_sector(wb, all_rows, sheet_name, title, color, headers, widths):
    """Build a per-sector breakdown sheet."""
    ws = wb.create_sheet(sheet_name)
    ws.sheet_view.showGridLines = False
    sectors = defaultdict(list)
    for r in all_rows:
        sectors[r.get("Sector", "Unknown")].append(r)

    sr = 1
    ws.cell(row=sr, column=1, value=title).font = Font(bold=True, name="Arial", size=13, color="FFFFFF")
    ws.cell(row=sr, column=1).fill = PatternFill("solid", fgColor=color)
    ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(headers))
    sr += 2

    for sector in sorted(sectors.keys(), key=lambda s: -len(sectors[s])):
        rows = sectors[sector][:SECTOR_N]
        if not rows: continue
        sc = ws.cell(row=sr, column=1, value=f"📊 {sector} — {len(sectors[sector])} qualifying (top {len(rows)})")
        sc.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
        sc.fill = PatternFill("solid", fgColor="37474F")
        ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(headers))
        sr += 1
        sr = write_table(ws, rows, headers, sr, header_color="455A64", widths=widths, freeze=False)
        sr += 1


def _repair_truncated_json(text: str) -> dict:
    """Try to salvage a JSON response that was cut off mid-stream.
    Extracts whatever complete top-level fields and pick objects exist.
    """
    import re
    result = {}

    # Extract simple string fields (synopsis, sector_rotation, macro_context, disclaimer)
    for field in ("synopsis", "sector_rotation", "macro_context", "disclaimer"):
        m = re.search(rf'"{field}"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
        if m:
            result[field] = m.group(1)

    # Extract attention array items
    attn_m = re.search(r'"attention"\s*:\s*\[(.*?)(?:\]|$)', text, re.DOTALL)
    if attn_m:
        items = re.findall(r'"((?:[^"\\]|\\.)*)"', attn_m.group(1))
        if items:
            result["attention"] = items

    # Extract complete pick objects — each pick ends with a closing }
    picks = []
    pick_blocks = re.findall(r'\{[^{}]*"ticker"[^{}]*\}', text, re.DOTALL)
    for block in pick_blocks:
        try:
            p = json.loads(block)
            if p.get("ticker"):
                picks.append(p)
        except Exception:
            pass
    if picks:
        result["picks"] = picks

    return result if result.get("picks") else {}


def call_claude_analysis(picks_data: dict, stocks: dict, macro: dict = None) -> dict:
    """Multi-agent AI stock analysis: 4 specialist agents (parallel) + 1 judge.
    - Quality Growth: sustained compounders with ROIC leadership
    - Special Situation: event-driven, misunderstood, inflection-point plays
    - Capital Appreciation: near-term re-rating candidates with improving momentum
    - Emerging Growth: smaller fast-growers with large TAM and rising ROIC
    - Judge: synthesises all 4 reports into final 3-10 picks
    Returns {} if ANTHROPIC_KEY not set or all calls fail.
    """
    if not ANTHROPIC_KEY:
        print("  ⏭️ No ANTHROPIC_KEY — skipping AI overview")
        return {}

    print("\n  🤖 Running multi-agent AI analysis (4 specialists + judge)...")

    # ── Step 1: Cross-strategy meta-ranking ────────────────────────────────
    # A stock appearing in multiple strategies is cross-validated — stronger signal.
    # We pool the top 15 from each strategy, score by breadth + depth, deduplicate.
    meta = {}  # ticker -> {strategies, best_rank, max_score, row}
    strategy_short = {
        "IV Discount (Buffett/DCF)":        "IV Discount",
        "Quality Compounders (Buffett)":     "Quality Compounder",
        "Stalwarts (Lynch)":                 "Stalwart",
        "Fast Growers (Lynch)":              "Fast Grower",
        "Turnarounds (Lynch)":               "Turnaround",
        "Slow Growers / Income (Lynch)":     "Slow Grower",
        "Cyclicals (Lynch)":                 "Cyclical",
        "Asset Plays (Lynch)":               "Asset Play",
    }
    for tab_name, rows in picks_data.items():
        short = strategy_short.get(tab_name, tab_name)
        for rank_i, r in enumerate(rows[:15]):
            ticker = r.get("Ticker", "?")
            score  = r.get("Score", 0) or 0
            if ticker not in meta:
                meta[ticker] = {
                    "strategies": [], "best_rank": rank_i + 1,
                    "max_score": score, "row": r,
                }
            meta[ticker]["strategies"].append(short)
            if score > meta[ticker]["max_score"]:
                meta[ticker]["max_score"] = score
            if rank_i + 1 < meta[ticker]["best_rank"]:
                meta[ticker]["best_rank"] = rank_i + 1
                meta[ticker]["row"] = r   # keep the row from its best ranking

    # Meta-score: multi-strategy breadth is the strongest signal
    for t, m in meta.items():
        n   = len(m["strategies"])
        top = max(0, 20 - m["best_rank"])          # rank 1 = 19 pts, rank 13 = 7 pts, rank 20 = 0 pts
        m["meta_score"] = n * 25 + m["max_score"] * 0.4 + top

    # Top 35 unique stocks by meta-score — wider pool so large-cap quality names aren't squeezed out
    top_stocks = sorted(meta.values(), key=lambda x: -x["meta_score"])[:35]

    # ── Step 2: Rich per-stock formatter ───────────────────────────────────
    def fmt_stock(m):
        r  = m["row"]
        t  = r.get("Ticker", "?")
        s  = stocks.get(t, {})   # raw stock data for extra fields not in row
        strats = " + ".join(m["strategies"])
        rank_tag = f"★×{len(m['strategies'])}" if len(m["strategies"]) > 1 else f"#{m['best_rank']}"

        # Core valuation
        parts = [f"{t} [{rank_tag}] ({r.get('Company','')[:20]}) | {strats}"]
        for k, lbl in [("P/E","PE"), ("PEG","PEG"), ("EV/EBITDA","EV/EB"),
                        ("P/B","PB")]:
            v = r.get(k) or s.get(k.lower().replace("/","").replace(" ",""))
            if v is not None:
                parts.append(f"{lbl}={v:.1f}" if isinstance(v, float) else f"{lbl}={v}")
        # MoS is a decimal (0.81 = 81%) — display as percentage for AI readability
        _mos_v = r.get("MoS")
        if _mos_v is not None and isinstance(_mos_v, float):
            parts.append(f"MoS={_mos_v*100:.0f}%")
        elif _mos_v is not None:
            parts.append(f"MoS={_mos_v}")

        # Quality — ROE/ROIC/FCF Yield are decimals (0.15 = 15%) — multiply by 100 for AI readability
        for k, lbl in [("ROE","ROE"), ("ROIC","ROIC"), ("FCF Yield","FCF")]:
            v = r.get(k)
            if v is not None:
                parts.append(f"{lbl}={v*100:.0f}%" if isinstance(v, float) else f"{lbl}={v}")
        for k, lbl in [("Piotroski","Pio"), ("Altman Z","AltZ")]:
            v = r.get(k)
            if v is not None:
                parts.append(f"{lbl}={v:.2f}" if isinstance(v, float) else f"{lbl}={v}")

        # Growth
        for k, lbl in [("Rev Growth","RG"), ("Rev Gr Prev","RGprev"),
                        ("EPS Growth 5Y","EG5y"), ("Rev Growth 5Y","RG5y")]:
            v = r.get(k)
            if v is not None:
                parts.append(f"{lbl}={v:+.0%}")

        # New enrichment
        ncr = s.get("netCashRatio")
        if ncr is not None:
            parts.append(f"NetCash={ncr:+.0%}")
        pvs52h = s.get("priceVs52H")
        if pvs52h is not None:
            parts.append(f"52wPos={pvs52h:.0%}")
        de = r.get("D/E") or s.get("de")
        if de is not None:
            parts.append(f"DE={de:.1f}")
        # Filter 1: ROIC quality badge — moat confirmation
        _roic_ai = s.get("roic")
        if _roic_ai and _roic_ai > 0.15:
            parts.append(f"✅ROIC>{_roic_ai*100:.0f}%")
        # Change 4: Flag when analysts project >150% EPS growth in one year (fwdPE < 40% of trailing)
        _fpe_ai = s.get("fwdPE")
        _pe_ai  = s.get("pe")
        if _fpe_ai and _pe_ai and _pe_ai > 0 and _fpe_ai < _pe_ai * 0.40:
            parts.append(f"⚠FwdPElook(fwd={_fpe_ai:.1f}vs{_pe_ai:.1f}trail)")
        # Filter 3: FCF quality checks
        _fcc_ai = s.get("fcfConversion")
        if _fcc_ai is not None:
            if _fcc_ai < 0.50:
                parts.append(f"⚠FCFConv={_fcc_ai:.1f}(low)")
            elif _fcc_ai >= 0.80:
                parts.append(f"FCFConv={_fcc_ai:.1f}(✓)")
        _fcm_ai = s.get("fcfMargin")
        if _fcm_ai is not None:
            parts.append(f"FCFMargin={_fcm_ai:.0%}")
        _fgc_ai = s.get("fcfGrowthConsistency")
        if _fgc_ai is not None and _fgc_ai < 0.50:
            parts.append(f"⚠FCFConsist={_fgc_ai:.0%}")
        # Filter 4: Growth optimism flags — forward vs historical (revenue + EPS)
        _go_ai  = s.get("growthOptimism")
        _ego_ai = s.get("epsGrowthOptimism")
        if _go_ai is not None and _go_ai > 0.50:
            parts.append(f"⚠RevGap=+{_go_ai:.0%}")
        if _ego_ai is not None and _ego_ai > 0.50:
            parts.append(f"⚠EpsGap=+{_ego_ai:.0%}")
        if (_go_ai is not None and _go_ai > 0.50) and (_ego_ai is not None and _ego_ai > 0.50):
            parts.append("🚩BOTH Rev+EPS estimates far above history")

        # Change 3: Beat rate — management execution quality
        _br_ai = s.get("beatRate")
        if _br_ai is not None:
            if _br_ai >= 0.75:
                parts.append(f"✅BeatRate={_br_ai:.0%}")
            elif _br_ai < 0.50:
                parts.append(f"⚠BeatRate={_br_ai:.0%}(low)")

        # Change 3: Gross margin — pricing power / business model quality
        _gm_ai = s.get("grossMargin")
        if _gm_ai is not None:
            if _gm_ai >= 0.60:
                parts.append(f"✅GrMgn={_gm_ai:.0%}")
            elif _gm_ai < 0.25:
                parts.append(f"⚠GrMgn={_gm_ai:.0%}(low)")
            else:
                parts.append(f"GrMgn={_gm_ai:.0%}")

        # Change 5: Operating margin — asset-light vs capital-heavy distinction
        _om_ai = s.get("operatingMargin")
        if _om_ai is not None:
            if _om_ai >= 0.25:
                parts.append(f"✅OprMgn={_om_ai:.0%}")
            elif _om_ai < 0.05:
                parts.append(f"⚠OprMgn={_om_ai:.0%}(thin)")
            else:
                parts.append(f"OprMgn={_om_ai:.0%}")

        # Size + sector
        mc = s.get("mktCapB") or r.get("MktCap ($B)")
        if mc:
            parts.append(f"Mcap=${mc:.1f}B")
        sector = s.get("sector") or r.get("Sector", "")
        if sector:
            parts.append(f"[{sector}]")

        return "  " + " | ".join(parts)

    candidate_lines = [fmt_stock(m) for m in top_stocks]

    # ── Step 3: Sector context ──────────────────────────────────────────────
    sector_stats = {}
    for s in stocks.values():
        sect = s.get("sector", "Unknown")
        if sect not in sector_stats:
            sector_stats[sect] = {"pegs": [], "fcfs": [], "rgs": [], "count": 0}
        sector_stats[sect]["count"] += 1
        peg = s.get("peg")
        if peg and 0 < peg < 15: sector_stats[sect]["pegs"].append(peg)
        fcf = s.get("fcfYield")
        if fcf is not None: sector_stats[sect]["fcfs"].append(fcf)
        rg = s.get("revGrowth")
        if rg is not None: sector_stats[sect]["rgs"].append(rg)

    def _med(lst): return sorted(lst)[len(lst)//2] if lst else None
    sector_lines = []
    for sect, d in sorted(sector_stats.items(),
                          key=lambda x: _med(x[1]["pegs"]) or 99):
        if d["count"] < 5: continue
        med_peg = _med(d["pegs"])
        avg_fcf = sum(d["fcfs"]) / len(d["fcfs"]) if d["fcfs"] else None
        avg_rg  = sum(d["rgs"])  / len(d["rgs"])  if d["rgs"]  else None
        line = f"  {sect}: "
        if med_peg: line += f"PEG={med_peg:.1f} "
        if avg_fcf is not None: line += f"FCF={avg_fcf:.1%} "
        if avg_rg  is not None: line += f"RevGr={avg_rg:.1%} "
        line += f"({d['count']} stocks)"
        sector_lines.append(line)

    # ── Step 4: Shared helpers ───────────────────────────────────────────────
    def _parse_response(text):
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return _repair_truncated_json(text)

    def _post(sys_p, usr_p, max_tok, timeout_s):
        return requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-sonnet-4-6",
                  "max_tokens": max_tok,
                  "system": sys_p,
                  "messages": [{"role": "user", "content": usr_p}]},
            timeout=timeout_s,
        )

    candidates_block = (
        "Legend: RG=RevGrowth, RGprev=prior yr RevGrowth (trend), MoS=DCF margin of safety, "
        "NetCash=net cash/price, 52wPos=price vs 52wk high, ★×N=appears in N strategies\n"
        + chr(10).join(candidate_lines)
    )
    sector_block = chr(10).join(sector_lines[:10])

    # ── Step 5: Three specialist agents (parallel) ──────────────────────────
    SPECIALIST_JSON_SCHEMA = (
        '{"picks":['
        '{"ticker":"X","company":"Name","brief_case":"one sentence why","'
        'key_metric":"the single most compelling number","conviction":"HIGH|MEDIUM"}'
        ',...]}'
    )

    specialists_cfg = [
        (
            "QualityGrowth",
            "🌱 Quality Growth",
            f"""You are a quality-growth equity analyst. Today is {datetime.date.today()}.
YOUR LENS — find durable compounders growing consistently at above-average rates:
- ROIC > 15% sustained over multiple years = the clearest moat signal available
- Revenue consistency (5/5 positive years) + FCF conversion > 0.8 = earnings are real and repeatable
- PEG < 1.5 with ROIC > 20% = growth at a reasonable price with moat confirmation
- Multi-strategy validation (★×2, ★×3) = cross-validated quality signal
- Market cap is irrelevant — a $100B company with 37% ROIC and PEG 0.8 is a better pick than a $2B company with 12% ROIC
- Quality Compounders, Stalwarts, and high-ROIC Fast Growers are your natural habitat
QUALITY FILTER:
- Does the company have structural pricing power, network effects, or switching costs that competitors cannot easily replicate?
- Is ROIC structurally high (moat-driven) or cyclically high (commodity peak, one-time)?
- Would this business sustain its economics through a recession or an aggressive well-funded competitor?""",
            f"""SECTOR CONTEXT:\n{sector_block}\n\nCANDIDATE STOCKS:\n{candidates_block}

Pick your TOP 7 stocks through a QUALITY GROWTH lens.
Prioritise: ROIC > 15%, consistent multi-year revenue growth, FCF conversion, PEG < 1.5, durable competitive moats.
For each pick, explain: what structural advantage drives the high ROIC, and why can this compound for 3-5 more years?
Respond ONLY with valid JSON (no markdown): {SPECIALIST_JSON_SCHEMA}""",
        ),
        (
            "SpecialSit",
            "⚡ Special Situation",
            f"""You are a special-situations equity analyst. Today is {datetime.date.today()}.
YOUR LENS — find event-driven, misunderstood, and inflection-point opportunities:
- Business model misclassification: market prices as one thing, fundamentals prove another (e.g. royalty biz priced as biotech)
- Restructurings / spinoffs / strategy pivots where the new economics are not yet priced in
- Companies at clear financial inflection points: first profitable year, FCF turning positive, debt paydown completing
- Regulatory or approval catalysts imminent but not yet in price
- Hidden assets or recurring revenue streams the market completely ignores
- IV Discount, Turnarounds, and misunderstood Asset Plays are your natural habitat
QUALITY FILTER:
- The "special" must be real and verifiable — not a story, but a quantifiable mis-pricing
- Balance sheet must be clean enough to survive until the thesis plays out (D/E < 2.0 preferred)
- The business underneath must have durable economics once the special situation resolves""",
            f"""SECTOR CONTEXT:\n{sector_block}\n\nCANDIDATE STOCKS:\n{candidates_block}

Pick your TOP 7 stocks through a SPECIAL SITUATION lens.
Identify: misclassified business models, inflection points, regulatory catalysts, hidden assets, structural mis-pricings.
For each pick, explain: WHAT is the specific special situation, and WHY has the market not yet priced it in?
Respond ONLY with valid JSON (no markdown): {SPECIALIST_JSON_SCHEMA}""",
        ),
        (
            "CapAppreciation",
            "📈 Capital Appreciation",
            f"""You are a capital-appreciation equity analyst. Today is {datetime.date.today()}.
YOUR LENS — find near-term re-rating candidates where a specific catalyst drives price appreciation:
- 52wPos < 0.65 + improving fundamentals = beaten-down quality at the best entry point
- Revenue re-acceleration (RG > RGprev) = early signal of an earnings upgrade cycle starting
- Sector rotation beneficiaries: sectors just turning from trough to recovery phase
- Cyclicals with trough P/E (high P/E = trough earnings) where the cycle is bottoming
- Companies with analyst estimate beats ahead — beat rate > 75% means guidance is conservative
- Turnarounds where the bad news is fully priced and forward metrics are improving
QUALITY & CATALYST FILTER:
- Every pick needs a SPECIFIC catalyst in the next 1-6 months — not vague "recovery"
- The business must be fundamentally sound (positive FCF, manageable debt) to act on the catalyst
- Distinguish genuine re-ratings (earnings power restoring) from dead-cat bounces (no earnings recovery)""",
            f"""SECTOR CONTEXT:\n{sector_block}\n\nCANDIDATE STOCKS:\n{candidates_block}

Pick your TOP 7 stocks through a CAPITAL APPRECIATION lens.
Focus on: beaten-down entries (52wPos), re-acceleration signals (RG > RGprev), cycle troughs, specific near-term catalysts.
For each pick, name the SPECIFIC catalyst (not just "recovery") and the timeframe you expect it to play out.
Respond ONLY with valid JSON (no markdown): {SPECIALIST_JSON_SCHEMA}""",
        ),
        (
            "EmergingGrowth",
            "🚀 Emerging Growth",
            f"""You are an emerging-growth equity analyst. Today is {datetime.date.today()}.
YOUR LENS — find smaller, faster-growing companies at the early stage of becoming compounders:
- Market cap $100M–$15B with revenue growing > 20% — still small enough to 5–10x but profitable enough to validate the model
- ROIC rising YoY (even if not yet at 15%) = the moat is forming, not yet priced by market
- Large addressable market (TAM) that the company is capturing faster than competitors
- Gross margin > 50% = scalable business model; as revenue grows, FCF will compound
- Network effects or platform dynamics forming — the business gets stronger as it grows
- Fast Growers and early-stage Turnarounds are your natural habitat
QUALITY FILTER:
- Positive or near-positive FCF — burning cash for growth is fine, but the unit economics must work
- Revenue consistency must be improving (not lumpy or unpredictable)
- The competitive advantage must be clear: why can THIS company win in this market vs. larger incumbents?
- Reject growth stories with no visible path to profitability, or where the TAM is contested by better-capitalised rivals""",
            f"""SECTOR CONTEXT:\n{sector_block}\n\nCANDIDATE STOCKS:\n{candidates_block}

Pick your TOP 7 stocks through an EMERGING GROWTH lens.
Focus on: $100M–$15B market cap, revenue growth > 20%, rising ROIC, large TAM, scalable economics, network effects.
For each pick, explain: WHY is this company in a position to become the dominant player in its market over 3-5 years?
Respond ONLY with valid JSON (no markdown): {SPECIALIST_JSON_SCHEMA}""",
        ),
    ]

    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    def _call_specialist(cfg):
        name, label, sys_p, usr_p = cfg
        try:
            resp = _post(sys_p, usr_p, 2000, 90)
            if resp.status_code == 200:
                data = _parse_response(resp.json()["content"][0]["text"])
                if data and data.get("picks"):
                    print(f"    ✅ {label} specialist done — {len(data['picks'])} picks")
                    return name, label, data["picks"]
            print(f"    ⚠️ {label} specialist failed (HTTP {resp.status_code})")
        except Exception as exc:
            print(f"    ⚠️ {label} specialist error: {str(exc)[:80]}")
        return name, label, []

    print("    Launching 4 specialist agents in parallel...")
    specialist_results = {}
    with ThreadPoolExecutor(max_workers=4) as _pool:
        _futs = {_pool.submit(_call_specialist, cfg): cfg[0] for cfg in specialists_cfg}
        for _fut in _as_completed(_futs):
            s_name, s_label, s_picks = _fut.result()
            specialist_results[s_name] = {"label": s_label, "picks": s_picks}

    # ── Step 6: Format specialist reports for the judge ─────────────────────
    specialist_report_lines = []
    all_endorsed = {}   # ticker -> list of specialist labels that picked it
    for s_name, sr in specialist_results.items():
        specialist_report_lines.append(f"\n{sr['label']} picks:")
        for p in sr["picks"]:
            t = p.get("ticker", "?")
            line = (f"  {t} — {p.get('brief_case','')}"
                    f" [{p.get('key_metric','')}] [{p.get('conviction','')}]")
            specialist_report_lines.append(line)
            all_endorsed.setdefault(t, []).append(sr["label"])

    # Highlight consensus (picked by 2+ specialists)
    consensus_note = []
    for t, labels in sorted(all_endorsed.items(), key=lambda x: -len(x[1])):
        if len(labels) >= 2:
            consensus_note.append(f"  {t} endorsed by: {' + '.join(labels)}")

    specialist_block = chr(10).join(specialist_report_lines)
    consensus_block  = (
        "CROSS-SPECIALIST CONSENSUS (picked by 2+ agents — highest conviction):\n"
        + (chr(10).join(consensus_note) if consensus_note else "  (no cross-picks this run)")
    )

    # ── Step 7: Judge agent — final synthesis ───────────────────────────────
    judge_system = f"""You are a chief investment officer synthesising recommendations from four specialist analysts.
Today is {datetime.date.today()}.
Your four specialists are: Quality Growth, Special Situation, Capital Appreciation, Emerging Growth.

YOUR INVESTMENT PHILOSOPHY:
- Quality first: ROIC > 15% sustained is the clearest indicator of durable competitive advantage
- PEG < 1.5 = growth at a reasonable price; confirm with ROIC before acting
- Multi-specialist consensus = highest conviction — when 2-3 analysts independently converge on the same name, that is institutional-grade signal
- A great business at a fair price beats a fair business at a great price
- Catalyst matters: prefer picks where a specific event in 1-6 months can unlock value
- Size is irrelevant: a $100B compounder at PEG 0.8 beats a $1B name at PEG 0.8 with half the ROIC

QUALITY STANDARD — hard filter before including any pick:
- Structural moat: pricing power, network effects, switching costs, brand, or cost leadership — NOT cyclical tailwind
- Competitive position: market leader or dominant niche, not a commoditised also-ran
- Survivability: would this business remain competitively relevant through a recession and an aggressive new entrant?

YOUR ROLE: Synthesise the four specialist reports into a final 3-10 pick list that is diversified across lenses (quality + special situations + appreciation + emerging), prioritises consensus names, and includes at least one pick from each specialist where quality meets the bar."""

    # ── Build macro context block for judge (from live FRED data) ──────────
    macro_block = ""
    if macro:
        mc = macro
        macro_block = f"""
LIVE MACRO INDICATORS (FRED data as of {mc.get('as_of', 'recent')}):
  10Y Treasury Yield : {mc.get('dgs10', 'N/A')}%  |  2Y Treasury Yield: {mc.get('dgs2', 'N/A')}%
  Yield Curve (10Y-2Y): {mc.get('yield_curve', 'N/A')}% -> {mc.get('curve_signal', '?')}
  VIX Fear Index     : {mc.get('vix', 'N/A')} -> {mc.get('vix_signal', '?')}
  Fed Funds Rate     : {mc.get('fedfunds', 'N/A')}% -> {mc.get('rate_signal', '?')}
  CPI YoY Inflation  : {mc.get('cpi_yoy', 'N/A')}% -> {mc.get('inflation_signal', '?')}
  Unemployment Rate  : {mc.get('unrate', 'N/A')}% -> {mc.get('labor_signal', '?')}

Use these REAL numbers to anchor your macro_context, market_outlook, and crash_risk assessments.
YIELD CURVE INVERTED (<0) historically precedes recession 6-18 months out. VIX>25 = genuine fear.
Rates ELEVATED (>4%) compress growth multiples — prefer quality cash generators over pure-growth names."""

    judge_user = f"""THREE SPECIALIST REPORTS:
{specialist_block}

{consensus_block}

FULL CANDIDATE DATA (for your reference when writing detailed analysis):
{candidates_block}

SECTOR VALUATIONS (cheapest → most expensive by PEG):
{sector_block}
{macro_block}
YOUR TASK:
1. Assess the macro environment using the LIVE indicators above (rates, yield curve, VIX, CPI, unemployment) — what does the market misunderstand?
2. Select 3-10 of the BEST investments — quality over quantity. Do NOT fill slots.
   If only 3-4 stocks truly meet the quality bar this week, output just those 3-4.
   Lynch never forced picks — sometimes there were 2 great ideas, sometimes 8.
   Only include a pick if you would genuinely invest your own money in it today.
   Prioritise consensus picks (endorsed by 2+ specialists). Balance across strategies.
   Include a contrarian pick ONLY IF one genuinely meets the quality bar — do not force it.
3. For each pick: write a Lynch-style story — what does the market NOT understand? What is the catalyst?
4. Assess competitive position: who are their main competitors, and what makes this company hard to displace?
5. Survivability check: how would this business hold up through a recession or a well-funded new competitor?

Key principles:
- Consensus picks (★ from multiple specialists) get priority unless fundamentals are broken
- Differentiate urgency meaningfully — not everything can be ACT NOW
- 52wPos < 0.75 = beaten down = more bad news priced in = lower risk entry
- Avoid: high D/E + no FCF + decelerating growth = value trap
- Avoid: commodity businesses with no pricing power, or companies losing market share to better competitors
- Do NOT ignore large-cap quality compounders — a $100B company at Fwd PEG 0.8 with 35% ROIC and 10% FCF yield is a better risk-adjusted pick than a small cap at the same PEG with half the ROIC. Size is not a disqualifier.
- QUALITY FILTERS — apply these before including any pick:
  1. ROIC > 15% preferred (✅ROIC flag in data) — HIGH ROIC first, then check PEG; ROIC is the core decision-maker
  2. PEG < 1.5 preferred — valuation confirms ROIC is priced fairly; PEG alone without ROIC is unreliable
  3. FCF conversion ≥ 0.6 preferred (FCFConv in data) — earnings must convert to real cash; growth without cash = illusion
  4. Flag ⚠GrwthGap and ⚠EpsGap picks explicitly — analyst estimates significantly more optimistic than track record
- KILL CRITERIA — hard rejections (do NOT include picks that fail these):
  ❌ FCF negative → reject (cash-burning growth is not investable)
  ❌ ROIC < 8% → reject unless extraordinary turnaround case with explicit justification
  ❌ Revenue declining majority of years (revConsistency < 0.40) → reject

Respond with ONLY valid JSON (no markdown, no preamble):
{{
  "synopsis": "2-3 sentences: what does the market get wrong right now? Where is the real opportunity?",
  "sector_rotation": "1-2 sentences: which sectors are at trough/peak and why — be specific with data",
  "macro_context": "2-3 sentences: how do current rates, yield curve, VIX, and inflation create specific opportunities or risks?",
  "macro_dashboard": {{
    "rate_environment": "1 sentence: what do current Treasury yields mean for equity valuations right now",
    "recession_risk": "LOW | MODERATE | HIGH — cite specific yield curve + unemployment evidence",
    "fed_policy": "HAWKISH | NEUTRAL | DOVISH — based on current fed funds rate vs inflation"
  }},
  "market_outlook": {{
    "near_term_bias": "BULLISH | NEUTRAL | CAUTIOUS | BEARISH",
    "long_term_bias": "BULLISH | NEUTRAL | CAUTIOUS | BEARISH",
    "crash_risk": "LOW | ELEVATED | HIGH",
    "rationale": "2 sentences citing specific macro numbers (e.g. 10Y at X%, VIX at Y, curve at Z%) to justify this view"
  }},
  "attention": ["specific risk #1 with ticker impact", "specific risk #2", "specific risk #3"],
  "specialist_consensus": "{'; '.join(consensus_note[:5]) if consensus_note else 'none this run'}",
  "picks": [
    {{
      "ticker": "TICKER",
      "company": "Company Name",
      "sector": "Sector",
      "strategy": "Fast Grower | Stalwart | Turnaround | Asset Play | Cyclical | Slow Grower | IV Discount | Quality Compounder",
      "endorsed_by": "QualityGrowth + SpecialSit | EmergingGrowth only | CapAppreciation + QualityGrowth | etc.",
      "headline": "Lynch-style one-liner anyone can understand in 10 seconds",
      "story": "2-3 sentences: WHAT DOES THE MARKET NOT UNDERSTAND? Why cheap or overlooked?",
      "industry_context": "1-2 sentences: where is this industry in its cycle?",
      "competitive_position": "1 sentence: market position vs peers — what makes them hard to displace?",
      "survivability": "1 sentence: how durable is this business under recession or competitive pressure?",
      "catalyst": "The specific event or metric shift in next 1-6 months that unlocks value",
      "watch": "The single biggest risk that breaks this thesis — be specific",
      "conviction": "HIGH | MEDIUM",
      "urgency": "ACT NOW | WITHIN WEEKS | WITHIN MONTHS | WATCH | AVOID"
    }}
  ],
  "disclaimer": "Brief disclaimer"
}}

Urgency guide: ACT NOW=catalyst imminent + entry compelling; WITHIN WEEKS=good window 1-4wks; WITHIN MONTHS=patient build; WATCH=need confirmation; AVOID=thesis broken."""

    try:
        print("    Calling judge agent for final synthesis...")
        resp = _post(judge_system, judge_user, 7000, 240)
        if resp.status_code != 200:
            print(f"  ⚠️ Judge agent error {resp.status_code}: {resp.text[:200]}")
            return {}
        result = _parse_response(resp.json()["content"][0]["text"])
        if not result:
            print("  ⚠️ Judge JSON unrecoverable")
            return {}
        n_picks = len(result.get("picks", []))
        n_consensus = len(consensus_note)
        print(f"  ✅ Multi-agent analysis complete — {n_picks} picks ({n_consensus} cross-specialist consensus)")
        result["_specialist_picks"] = specialist_results   # for performance tracking
        return result

    except Exception as e:
        err = str(e)
        if "timed out" in err.lower() or "timeout" in err.lower():
            # Fallback: judge with just specialist reports, no full candidate data
            print("  ⚠️ Judge timed out — retrying with compact input...")
            compact_user = (
                f"Specialist reports:\n{specialist_block}\n\n{consensus_block}\n\n"
                f"Pick the best 6 investments. JSON only:\n"
                '{"synopsis":"...","sector_rotation":"...","macro_context":"...",'
                '"market_outlook":{"near_term_bias":"CAUTIOUS","long_term_bias":"NEUTRAL",'
                '"crash_risk":"ELEVATED","rationale":"..."},'
                '"attention":["risk1","risk2","risk3"],"specialist_consensus":"see above",'
                '"picks":[{"ticker":"T","company":"C","sector":"S","strategy":"S","endorsed_by":"...",'
                '"headline":"...","story":"...","industry_context":"...","competitive_position":"...",'
                '"survivability":"...","catalyst":"...","watch":"...","conviction":"HIGH","urgency":"WITHIN MONTHS"}],'
                '"disclaimer":"Not investment advice."}'
            )
            try:
                r2 = _post(judge_system, compact_user, 3500, 90)
                if r2.status_code == 200:
                    result2 = _parse_response(r2.json()["content"][0]["text"])
                    if result2:
                        print(f"  ✅ Judge retry succeeded — {len(result2.get('picks', []))} picks")
                        result2["_specialist_picks"] = specialist_results
                        return result2
            except Exception as e2:
                print(f"  ⚠️ Judge retry also failed: {str(e2)[:80]}")
        else:
            print(f"  ⚠️ Judge agent failed: {err[:120]}")
        return {}


def log_ai_picks(ai_result: dict, stocks: dict):
    """Auto-log AI picks to fmp_ai_picks_log.csv for performance tracking.
    Logs judge picks (source=AI-Judge) AND specialist picks (source=AI-Bull/Value/Contrarian).
    CSV fields: date, source, ticker, company, strategy, conviction, entry_price, headline
    """
    today = datetime.date.today().isoformat()
    rows = []

    def _valid_ticker(ticker_str):
        """Reject agent labels, empty strings, or anything that looks like non-ticker."""
        if not ticker_str:
            return False
        if ticker_str.startswith("AI-"):   # agent labels: AI-Judge, AI-Bull etc.
            return False
        if len(ticker_str) > 10:           # real tickers are ≤ 5 chars (BRK.B = 5 + dot)
            return False
        return True

    # ── Judge picks — top 5 only (judge already ranks by conviction/urgency) ──
    for p in ai_result.get("picks", [])[:5]:
        t = p.get("ticker", "").upper()
        s = stocks.get(t, {})
        price = s.get("price")
        if _valid_ticker(t) and price:
            rows.append({
                "date": today, "source": "AI-Judge",
                "ticker": t,
                "company": p.get("company", s.get("name", ""))[:30],
                "strategy": p.get("strategy", ""),
                "conviction": p.get("conviction", ""),
                "entry_price": round(price, 2),
                "headline": p.get("headline", "")[:80],
            })

    # ── Specialist picks ───────────────────────────────────────────────────
    for spec_name, sr in ai_result.get("_specialist_picks", {}).items():
        source = f"AI-{spec_name}"   # "AI-Bull", "AI-Value", "AI-Contrarian"
        for p in sr.get("picks", []):
            t = p.get("ticker", "").upper()
            s = stocks.get(t, {})
            price = s.get("price")
            if _valid_ticker(t) and price:
                rows.append({
                    "date": today, "source": source,
                    "ticker": t,
                    "company": s.get("name", t)[:30],
                    "strategy": p.get("brief_case", "")[:50],
                    "conviction": p.get("conviction", ""),
                    "entry_price": round(price, 2),
                    "headline": p.get("key_metric", "")[:80],
                })

    if not rows:
        return

    NEW_FIELDS = ["date", "source", "ticker", "company", "strategy",
                  "conviction", "entry_price", "headline"]

    # Auto-migrate old schema (no 'source' column) → new schema
    file_exists = os.path.exists(AI_PICKS_LOG)
    if file_exists:
        with open(AI_PICKS_LOG, "r", encoding="utf-8") as _f:
            _reader = csv.DictReader(_f)
            _old_headers = _reader.fieldnames or []
            if "source" not in _old_headers:
                _old_rows = list(_reader)
                _migrated = [{**{k: "" for k in NEW_FIELDS},
                              "date": r.get("date", ""), "source": "AI-Judge",
                              "ticker": r.get("ticker", ""), "company": r.get("company", ""),
                              "strategy": r.get("strategy", ""), "conviction": r.get("conviction", ""),
                              "entry_price": r.get("entry_price", ""), "headline": r.get("headline", "")}
                             for r in _old_rows]
                with open(AI_PICKS_LOG, "w", newline="", encoding="utf-8") as _fw:
                    _w = csv.DictWriter(_fw, fieldnames=NEW_FIELDS)
                    _w.writeheader()
                    _w.writerows(_migrated)

    # De-duplicate: skip source+ticker combos already logged today
    file_exists = os.path.exists(AI_PICKS_LOG)
    existing_today = set()
    if file_exists:
        with open(AI_PICKS_LOG, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("date") == today:
                    existing_today.add((row.get("source", "AI-Judge"), row.get("ticker", "")))
    new_rows = [r for r in rows if (r["source"], r["ticker"]) not in existing_today]
    if not new_rows:
        print(f"  ℹ️ AI picks already logged for today")
        return
    with open(AI_PICKS_LOG, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["date", "source", "ticker", "company", "strategy",
                      "conviction", "entry_price", "headline"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)
    n_judge = sum(1 for r in new_rows if r["source"] == "AI-Judge")
    n_spec  = len(new_rows) - n_judge
    print(f"  📝 AI picks logged: {n_judge} judge + {n_spec} specialist → {AI_PICKS_LOG}")


def build_ai_picks_tab(wb, ai_result: dict, stocks: dict, ws=None):
    """Tab 1b: AI Top Picks — Claude's curated picks with collapsible full stories."""
    print("\n📊 Building Tab: AI Top Picks...")
    if ws is None:
        ws = wb.create_sheet("1b. AI Top Picks")
    ws.sheet_view.showGridLines = False
    ws.sheet_properties.outlinePr.summaryBelow = False
    today = datetime.date.today()

    if not ai_result:
        ws.cell(row=1, column=1,
                value="Set ANTHROPIC_API_KEY environment variable to enable AI analysis.").font = \
            Font(italic=True, name="Arial", size=11, color="888888")
        return

    picks = ai_result.get("picks", [])
    TC = 16

    # Column widths: #, Ticker, Company, Sector, Strategy, Conv, Agents, Urgency, Price, PEG, P/E, MoS, ROE, FCF, Pio, Headline
    for col, w in zip("ABCDEFGHIJKLMNOP",
                      [5, 9, 22, 14, 14, 7, 9, 13, 8, 6, 6, 7, 7, 7, 5, 50]):
        ws.column_dimensions[col].width = w

    def _agents_short(endorsed_by_str):
        """Convert agent name string to short display label."""
        s = (endorsed_by_str or "").lower()
        parts = []
        if "qualitygrowth" in s or "quality growth" in s: parts.append("QGrwth")
        if "specialsit"    in s or "special sit"    in s: parts.append("SpSit")
        if "capappreciation" in s or "capital appreciation" in s: parts.append("CapAp")
        if "emerginggrowth" in s or "emerging growth" in s: parts.append("EmGrwth")
        # legacy fallback
        if not parts:
            if "bull"    in s: parts.append("Bull")
            if "value"   in s: parts.append("Val")
            if "contra"  in s: parts.append("Cont")
        return "+".join(parts) if parts else "Judge"

    # Consensus fill: 3 agents=dark green, 2=amber, 1=blue, judge only=grey
    _AGENTS_FILL = {3: "1B5E20", 2: "E65100", 1: "1565C0", 0: "546E7A"}

    HIGH_FILL = PatternFill("solid", fgColor="1B5E20")
    MED_FILL  = PatternFill("solid", fgColor="1565C0")

    def _hdr(row, value, fill_hex, font_size=9, height=18):
        c = ws.cell(row=row, column=1, value=value)
        c.font = Font(bold=True, name="Arial", size=font_size, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor=fill_hex)
        c.alignment = Alignment(horizontal="left", vertical="center", indent=1)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=TC)
        ws.row_dimensions[row].height = height
        return row + 1

    def _col_hdrs(row, headers, fill_hex):
        for ci, h in enumerate(headers, 1):
            c = ws.cell(row=row, column=ci, value=h)
            c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
            c.fill = PatternFill("solid", fgColor=fill_hex)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = THIN_BORDER
        ws.row_dimensions[row].height = 26
        return row + 1

    r = 1
    r = _hdr(r, f"  🤖  AI Top Picks  —  Claude Sonnet Analysis    {today}",
             "0D1B2A", font_size=13, height=30)

    # ── Synopsis + Sector rotation side by side ────────────────────
    synopsis = ai_result.get("synopsis", "")
    sr_text  = ai_result.get("sector_rotation", "")
    SL = 9
    if synopsis or sr_text:
        r = _hdr(r, "  MARKET SYNOPSIS  &  SECTOR ROTATION", "1A237E", font_size=8, height=16)
        if synopsis:
            c = ws.cell(row=r, column=1, value=synopsis)
            c.font = Font(name="Arial", size=9, italic=True, color="1A237E")
            c.fill = PatternFill("solid", fgColor="E8EAF6")
            c.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
            ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=SL)
        if sr_text:
            c2 = ws.cell(row=r, column=SL+1, value=f"SECTOR ROTATION\n{sr_text}")
            c2.font = Font(name="Arial", size=8, italic=True, color="1B5E20")
            c2.fill = PatternFill("solid", fgColor="E8F5E9")
            c2.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
            ws.merge_cells(start_row=r, start_column=SL+1, end_row=r, end_column=TC)
        ws.row_dimensions[r].height = 42
        r += 1

    # ── Macro context ──────────────────────────────────────────────
    macro_ctx = ai_result.get("macro_context", "")
    if macro_ctx:
        r = _hdr(r, "  🌍  GEOPOLITICAL & MACRO CONTEXT", "4A148C", font_size=8, height=16)
        mc = ws.cell(row=r, column=1, value=macro_ctx)
        mc.font = Font(name="Arial", size=9, italic=True, color="4A148C")
        mc.fill = PatternFill("solid", fgColor="F3E5F5")
        mc.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=TC)
        ws.row_dimensions[r].height = 36
        r += 1

    # ── Market outlook / crash risk banner ────────────────────────
    mo = ai_result.get("market_outlook", {})
    if mo:
        nt  = (mo.get("near_term_bias") or "NEUTRAL").upper()
        lt  = (mo.get("long_term_bias") or "NEUTRAL").upper()
        cr  = (mo.get("crash_risk") or "LOW").upper()
        mo_rationale = mo.get("rationale", "")
        # Color scheme: BEARISH/HIGH = red; CAUTIOUS/ELEVATED = amber; BULLISH = green; NEUTRAL = teal
        _BIAS_FILL  = {"BULLISH": "1B5E20", "NEUTRAL": "006064",
                       "CAUTIOUS": "E65100", "BEARISH": "B71C1C"}
        _CRASH_FILL = {"LOW": "1B5E20", "ELEVATED": "E65100", "HIGH": "B71C1C"}
        nt_fill  = _BIAS_FILL.get(nt,  "006064")
        lt_fill  = _BIAS_FILL.get(lt,  "006064")
        cr_fill  = _CRASH_FILL.get(cr, "E65100")
        # Header
        r = _hdr(r, "  📊  MARKET OUTLOOK  &  CRASH RISK ASSESSMENT", "263238",
                 font_size=8, height=16)
        # Three badge cells + rationale
        BADGE_W = 4  # columns per badge (~4 cols each = 12 total, rationale in last 3)
        badge_data = [
            (f"NEAR-TERM\n{nt}",  nt_fill,  1,             BADGE_W),
            (f"LONG-TERM\n{lt}",  lt_fill,  BADGE_W+1,     BADGE_W),
            (f"CRASH RISK\n{cr}", cr_fill,  BADGE_W*2+1,   BADGE_W),
        ]
        for label, fill_hex, start_col, span in badge_data:
            bc = ws.cell(row=r, column=start_col, value=label)
            bc.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
            bc.fill = PatternFill("solid", fgColor=fill_hex)
            bc.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            ws.merge_cells(start_row=r, start_column=start_col,
                           end_row=r, end_column=start_col + span - 1)
        if mo_rationale:
            rc = ws.cell(row=r, column=BADGE_W*3+1, value=mo_rationale)
            rc.font = Font(name="Arial", size=8, italic=True, color="37474F")
            rc.fill = PatternFill("solid", fgColor="ECEFF1")
            rc.alignment = Alignment(wrap_text=True, vertical="center", indent=1)
            ws.merge_cells(start_row=r, start_column=BADGE_W*3+1,
                           end_row=r, end_column=TC)
        ws.row_dimensions[r].height = 30
        r += 1

    # ── Key themes (single compact row) ───────────────────────────
    themes = ai_result.get("attention", [])
    if themes:
        r = _hdr(r, "  ⚠  KEY THEMES & RISKS", "B71C1C", font_size=8, height=16)
        bullets = "     ".join(f"▶ {t}" for t in themes)
        bc = ws.cell(row=r, column=1, value=bullets)
        bc.font = Font(name="Arial", size=8)
        bc.fill = PatternFill("solid", fgColor="FFEBEE")
        bc.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=TC)
        ws.row_dimensions[r].height = 20
        r += 1

    r += 1  # spacer

    # ── Picks table ────────────────────────────────────────────────
    r = _hdr(r,
             f"  TOP {len(picks)} AI PICKS  —  click ▶ row numbers to expand full story",
             "0D47A1", font_size=9, height=20)

    col_hdrs = ["#", "Ticker", "Company", "Sector", "Strategy", "Conv.",
                "Agents", "Urgency", "Price", "PEG", "P/E", "MoS%", "ROE%", "FCF%", "Pio",
                "Headline  (expand row ▶ for full story)"]
    r = _col_hdrs(r, col_hdrs, "1A237E")

    fmts = [None, None, None, None, None, None, None, None,
            "$#,##0.00", "0.00", "0.0", "0%", "0%", "0%", None, None]

    # Urgency colour map
    URGENCY_STYLES = {
        "ACT NOW":       ("B71C1C", "FFFFFF"),  # deep red
        "WITHIN WEEKS":  ("E65100", "FFFFFF"),  # deep orange
        "WITHIN MONTHS": ("1565C0", "FFFFFF"),  # blue
        "WATCH":         ("37474F", "FFFFFF"),  # dark grey
        "AVOID":         ("757575", "FFFFFF"),  # grey
    }

    # Fetch live prices for AI picks via v3 quote (stable API doesn't reliably serve live quotes)
    live_prices = {}
    _pick_tickers = [p.get("ticker","").upper() for p in picks if p.get("ticker")]
    for _t in _pick_tickers:
        _p = fetch_live_price(_t)
        if _p:
            live_prices[_t] = _p
        time.sleep(0.15)

    for i, p in enumerate(picks, 1):
        t        = p.get("ticker", "").upper()
        s        = stocks.get(t, {})
        conv     = (p.get("conviction") or "MEDIUM").upper()
        urgency  = (p.get("urgency") or "WATCH").upper()
        agents   = _agents_short(p.get("endorsed_by", ""))
        n_agents = agents.count("+") + (0 if agents == "Judge" else 1)
        rf       = ALT_FILL if i % 2 == 0 else PLAIN_FILL
        # Use live price if available, fall back to cached
        live_price = live_prices.get(t) or s.get("price")

        # ── Summary row (always visible) ──────────────────────────
        sum_vals = [
            i, t,
            p.get("company", s.get("name",""))[:22],
            p.get("sector", s.get("sector",""))[:14],
            p.get("strategy","")[:14],
            conv,
            agents,    # col 7 — new Agents column
            urgency,
            live_price, s.get("peg"), s.get("pe"),
            s.get("mos"), s.get("roe"), s.get("fcfYield"),
            s.get("piotroski"),
            p.get("headline",""),
        ]
        for ci, (v, fmt) in enumerate(zip(sum_vals, fmts), 1):
            cell = ws.cell(row=r, column=ci, value=v)
            cell.fill = rf
            cell.border = THIN_BORDER
            cell.font = Font(name="Arial", size=9)
            cell.alignment = Alignment(horizontal="center", vertical="center",
                                       wrap_text=(ci == 16))
            if fmt and isinstance(v, (int, float)):
                cell.number_format = fmt
            if ci == 2:
                cell.font = Font(bold=True, name="Arial", size=10, color="0D47A1")
            elif ci == 6:  # conviction badge
                cell.fill = HIGH_FILL if conv == "HIGH" else MED_FILL
                cell.font = Font(bold=True, name="Arial", size=8, color="FFFFFF")
            elif ci == 7:  # agents badge — color by consensus count
                _af = _AGENTS_FILL.get(n_agents, "546E7A")
                cell.fill = PatternFill("solid", fgColor=_af)
                cell.font = Font(bold=True, name="Arial", size=8, color="FFFFFF")
            elif ci == 8:  # urgency badge
                ufg, utf = URGENCY_STYLES.get(urgency, ("757575", "FFFFFF"))
                cell.fill = PatternFill("solid", fgColor=ufg)
                cell.font = Font(bold=True, name="Arial", size=8, color=utf)
            elif ci == 10 and isinstance(v, (int, float)):   # PEG
                if 0 < v < 1.0:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
                    cell.font = Font(bold=True, name="Arial", size=9)
                elif 0 < v < 1.5:
                    cell.fill = PatternFill("solid", fgColor="FFF9C4")
            elif ci in (12, 13, 14) and isinstance(v, (int, float)):  # MoS, ROE, FCF
                cell.font = Font(name="Arial", size=9,
                                 color="1B5E20" if v > 0.01 else ("B71C1C" if v < -0.01 else "000000"))
            elif ci == 15 and isinstance(v, (int, float)):  # Piotroski
                cell.font = Font(bold=True, name="Arial", size=9)
                if v >= 8:
                    cell.fill = PatternFill("solid", fgColor="1B5E20")
                    cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                elif v >= 7:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
            elif ci == 16:
                cell.alignment = Alignment(wrap_text=True, vertical="center",
                                           horizontal="left")
        ws.row_dimensions[r].height = 16
        r += 1

        # ── Detail row (collapsible, hidden by default) ────────────
        story       = p.get("story","")
        ind_ctx     = p.get("industry_context","")
        comp_pos    = p.get("competitive_position","")
        survivabil  = p.get("survivability","")
        catalyst    = p.get("catalyst","")
        watch       = p.get("watch","")
        detail   = (f"  📖 MARKET EDGE: {story}"
                    + (f"\n\n  🏭 INDUSTRY: {ind_ctx}" if ind_ctx else "")
                    + (f"\n\n  🏆 COMPETITIVE POSITION: {comp_pos}" if comp_pos else "")
                    + (f"\n\n  🛡 SURVIVABILITY: {survivabil}" if survivabil else "")
                    + f"\n\n  ⚡ CATALYST: {catalyst}\n\n  ⚠ WATCH: {watch}")
        dc = ws.cell(row=r, column=1, value=detail)
        dc.font = Font(name="Arial", size=9, italic=True, color="37474F")
        dc.fill = PatternFill("solid", fgColor="F5F5F5")
        dc.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=TC)
        ws.row_dimensions[r].height = 105
        ws.row_dimensions[r].outline_level = 1
        ws.row_dimensions[r].hidden = True
        r += 1

    disc = ai_result.get("disclaimer", "")
    if disc:
        dc2 = ws.cell(row=r, column=1, value=disc)
        dc2.font = Font(name="Arial", size=7, italic=True, color="AAAAAA")
        dc2.fill = PatternFill("solid", fgColor="FAFAFA")
        dc2.alignment = Alignment(indent=1, vertical="center")
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=TC)
        ws.row_dimensions[r].height = 13
        r += 1

    # ── Performance history (from log) ────────────────────────────
    r += 1
    if os.path.exists(AI_PICKS_LOG):
        r = _hdr(r, "  📈  AI PICKS PERFORMANCE HISTORY", "263238", font_size=9, height=20)
        logged = []
        with open(AI_PICKS_LOG, "r", encoding="utf-8") as f:
            for row_data in csv.DictReader(f):
                logged.append(row_data)

        if logged:
            ph2 = ["Date", "Ticker", "Company", "Strategy", "Conv.",
                   "Entry $", "Current $", "Return", "Days", "Status"]
            r = _col_hdrs(r, ph2, "263238")

            for ri, row_data in enumerate(logged):
                t2    = row_data.get("ticker", "")
                try:
                    entry = float(row_data.get("entry_price") or 0)
                except (ValueError, TypeError):
                    entry = 0
                curr  = stocks.get(t2, {}).get("price", 0) or 0
                ret = (curr - entry) / entry if entry > 0 and curr > 0 else None
                try:
                    days = (datetime.date.today() - datetime.date.fromisoformat(row_data["date"])).days
                except Exception:
                    days = None
                if ret is None:   status = "N/A"
                elif ret > 0.20:  status = "WINNER"
                elif ret > 0.05:  status = "UP"
                elif ret > -0.05: status = "FLAT"
                elif ret > -0.15: status = "DOWN"
                else:             status = "LOSS"

                _src = row_data.get("source", "AI-Judge")
                row_vals = [row_data.get("date",""), t2,
                            row_data.get("company","")[:20],
                            _src[:12],
                            row_data.get("conviction","")[:4],
                            entry, curr if curr else None, ret, days, status]
                alt = ri % 2 == 0
                for ci, v in enumerate(row_vals, 1):
                    cell = ws.cell(row=r, column=ci, value=v)
                    cell.border = THIN_BORDER
                    cell.font = Font(name="Arial", size=9)
                    cell.fill = ALT_FILL if alt else PLAIN_FILL
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                    if ci in (6, 7) and isinstance(v, (int, float)):
                        cell.number_format = "$#,##0.00"
                    elif ci == 8 and isinstance(v, float):
                        cell.number_format = "0.0%"
                        cell.font = Font(name="Arial", size=9,
                                         color="1B5E20" if v > 0 else "B71C1C")
                    elif ci == 10:
                        if status == "WINNER":
                            cell.fill = PatternFill("solid", fgColor="1B5E20")
                            cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                        elif status == "UP":
                            cell.fill = PatternFill("solid", fgColor="C8E6C9")
                        elif status in ("DOWN", "LOSS"):
                            cell.fill = PatternFill("solid", fgColor="FFCDD2")
                ws.row_dimensions[r].height = 16
                r += 1

    ws.freeze_panes = "A3"
    print(f"  ✅ AI Top Picks tab done — {len(picks)} picks")


def build_overview_tab(ws, stocks, iv_rows, stalwarts, fast_growers, turnarounds,
                       slow_growers, cyclicals, asset_plays, quality_compounders,
                       fmp_call_count, ai=None, portfolio=None,
                       portfolio_nav=None, portfolio_ret=None, portfolio_spy_ret=None,
                       macro=None):
    """Build the Overview tab: AI analysis at top, strategy tables below."""
    ws.sheet_view.showGridLines = False
    today = datetime.date.today()
    TC = 15  # total columns A–O

    # Enable row outline controls (for collapsible story rows)
    ws.sheet_properties.outlinePr.summaryBelow = False

    # Column widths: #, Ticker, Company, Sector, Strategy, Conv, Urgency, Price, PEG, P/E, MoS, ROE, Pio, _, Headline
    for col, w in zip("ABCDEFGHIJKLMNO",
                      [5, 9, 22, 14, 14, 7, 13, 8, 6, 6, 7, 7, 5, 5, 45]):
        ws.column_dimensions[col].width = w

    # ── Helpers ─────────────────────────────────────────────────────
    def _hdr(row, value, fill_hex, font_size=9, height=18):
        """Full-width dark header row (matches write_table header style)."""
        c = ws.cell(row=row, column=1, value=value)
        c.font = Font(bold=True, name="Arial", size=font_size, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor=fill_hex)
        c.alignment = Alignment(horizontal="left", vertical="center", indent=1)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=TC)
        ws.row_dimensions[row].height = height
        return row + 1

    def _text_row(row, value, fill_hex, font_color="222222", font_size=9,
                  italic=False, height=36, cols=TC):
        """Full-width wrapped text content row."""
        c = ws.cell(row=row, column=1, value=value)
        c.font = Font(bold=False, italic=italic, name="Arial",
                      size=font_size, color=font_color)
        c.fill = PatternFill("solid", fgColor=fill_hex)
        c.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=cols)
        ws.row_dimensions[row].height = height
        return row + 1

    def _col_hdrs(row, headers, fill_hex):
        """Write a column-header row matching write_table style."""
        for ci, h in enumerate(headers, 1):
            c = ws.cell(row=row, column=ci, value=h)
            c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
            c.fill = PatternFill("solid", fgColor=fill_hex)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = THIN_BORDER
        ws.row_dimensions[row].height = 26
        return row + 1

    r = 1

    # ── Title bar ──────────────────────────────────────────────────
    r = _hdr(r, f"  FMP Stock Screener  —  Lynch + Buffett Strategies    {today}",
             "0D1B2A", font_size=13, height=30)
    r = _text_row(r,
                  f"Universe: {len(stocks):,} US stocks  |  FMP API calls: {fmp_call_count}"
                  f"  |  Data: Financial Modeling Prep  |  AI: Anthropic Claude Sonnet",
                  "0D1B2A", font_color="AAAAAA", font_size=8, height=14)

    # ── MACRO DASHBOARD ────────────────────────────────────────────
    # Always shown (doesn't depend on AI running) — displays real FRED data
    if macro:
        mc = macro
        r = _hdr(r, f"  🌍  MACRO DASHBOARD  —  Live FRED Data  (as of {mc.get('as_of', '?')})",
                 "1B2838", font_size=9, height=18)

        # Color helpers for metric tiles
        def _tile_fill(signal):
            """Return fill hex based on severity signal (GREEN/YELLOW/RED)."""
            sig = signal.upper() if signal else ""
            if sig in ("CALM", "NORMAL", "STEEP", "NEAR_TARGET", "LOW",
                       "HEALTHY", "TIGHT"):
                return "1B5E20"   # green
            if sig in ("CAUTION", "FLAT", "ABOVE_TARGET", "ELEVATED",
                       "SOFTENING"):
                return "E65100"   # orange/yellow
            if sig in ("PANIC", "FEAR", "INVERTED", "HOT", "HIGH",
                       "WEAK"):
                return "B71C1C"   # red
            return "455A64"       # grey for UNKNOWN / N/A

        # ── Per-metric signal helpers (each tile uses its own value's thresholds) ──
        def _dgs2_signal(v):
            # 2Y yield signal: same thresholds as 10Y (both drive real rates)
            if v is None: return "UNKNOWN"
            if v > 5.0: return "HIGH"
            if v > 4.0: return "ELEVATED"
            if v > 2.0: return "NORMAL"
            return "LOW"

        def _fedfunds_signal(v):
            # Fed Funds: HAWKISH when above 4.5%, ELEVATED 3.5-4.5%, NEUTRAL below
            if v is None: return "UNKNOWN"
            if v > 4.5: return "HAWKISH"
            if v > 3.5: return "ELEVATED"
            if v > 2.0: return "NORMAL"
            return "DOVISH"

        # ── Row A: 7 metric tiles ──────────────────────────────────
        tiles = [
            # (label, value_str, signal_str, col_start, col_span)
            ("10Y YIELD",
             f"{mc['dgs10']}%" if mc.get('dgs10') is not None else "N/A",
             mc.get('rate_signal', 'UNKNOWN'), 1, 2),
            ("2Y YIELD",
             f"{mc['dgs2']}%" if mc.get('dgs2') is not None else "N/A",
             _dgs2_signal(mc.get('dgs2')), 3, 2),
            ("YIELD CURVE",
             (f"+{mc['yield_curve']}%" if mc.get('yield_curve', 0) >= 0
              else f"{mc['yield_curve']}%") if mc.get('yield_curve') is not None else "N/A",
             mc.get('curve_signal', 'UNKNOWN'), 5, 3),
            ("VIX",
             str(mc['vix']) if mc.get('vix') is not None else "N/A",
             mc.get('vix_signal', 'UNKNOWN'), 8, 2),
            ("FED FUNDS",
             f"{mc['fedfunds']}%" if mc.get('fedfunds') is not None else "N/A",
             _fedfunds_signal(mc.get('fedfunds')), 10, 2),
            ("CPI YoY",
             f"{mc['cpi_yoy']}%" if mc.get('cpi_yoy') is not None else "N/A",
             mc.get('inflation_signal', 'UNKNOWN'), 12, 2),
            ("UNEMPLOYMT",
             f"{mc['unrate']}%" if mc.get('unrate') is not None else "N/A",
             mc.get('labor_signal', 'UNKNOWN'), 14, 2),
        ]

        for label, val_str, signal, col_start, col_span in tiles:
            fill_hex = _tile_fill(signal)
            # Label cell (top mini-row in merged area — we use two lines in one cell)
            tc = ws.cell(row=r, column=col_start,
                         value=f"{label}\n{val_str}")
            tc.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
            tc.fill = PatternFill("solid", fgColor=fill_hex)
            tc.alignment = Alignment(horizontal="center", vertical="center",
                                     wrap_text=True)
            if col_span > 1:
                ws.merge_cells(start_row=r, start_column=col_start,
                               end_row=r, end_column=col_start + col_span - 1)
        ws.row_dimensions[r].height = 28
        r += 1

        # ── Row B: signal tags row ─────────────────────────────────
        for label, val_str, signal, col_start, col_span in tiles:
            fill_hex = _tile_fill(signal)
            # Slightly lighter shade: append "99" alpha-equivalent via a fixed light shade
            light_fills = {
                "1B5E20": "C8E6C9",   # green light
                "E65100": "FFE0B2",   # orange light
                "B71C1C": "FFCDD2",   # red light
                "455A64": "ECEFF1",   # grey light
            }
            light = light_fills.get(fill_hex, "ECEFF1")
            sc = ws.cell(row=r, column=col_start, value=signal)
            sc.font = Font(bold=True, name="Arial", size=8,
                           color=fill_hex)
            sc.fill = PatternFill("solid", fgColor=light)
            sc.alignment = Alignment(horizontal="center", vertical="center")
            if col_span > 1:
                ws.merge_cells(start_row=r, start_column=col_start,
                               end_row=r, end_column=col_start + col_span - 1)
        ws.row_dimensions[r].height = 14
        r += 1

        # ── Row C: interpretation line ─────────────────────────────
        interp_parts = []
        vc = mc.get("yield_curve")
        if vc is not None:
            cs = mc.get("curve_signal", "")
            if cs == "INVERTED":
                interp_parts.append(f"Yield curve INVERTED ({vc:+.2f}%) - historically precedes recession 6-18mo")
            elif cs == "FLAT":
                interp_parts.append(f"Yield curve FLAT ({vc:+.2f}%) - slowdown risk, watch carefully")
            else:
                interp_parts.append(f"Yield curve {cs.lower()} ({vc:+.2f}%) - no imminent recession signal")
        vv = mc.get("vix")
        if vv is not None:
            vs = mc.get("vix_signal", "")
            interp_parts.append(f"VIX {vv} ({vs.lower()} fear)")
        ci = mc.get("cpi_yoy")
        if ci is not None:
            inf_s = mc.get("inflation_signal", "")
            interp_parts.append(f"CPI {ci}% YoY ({inf_s.lower().replace('_', ' ')})")
        ff = mc.get("fedfunds")
        if ff is not None:
            rs = mc.get("rate_signal", "")
            interp_parts.append(f"Fed Funds {ff}% ({rs.lower()})")

        interp_text = "  |  ".join(interp_parts) if interp_parts else "Macro data unavailable"
        r = _text_row(r, interp_text, "1B2838", font_color="90A4AE",
                      font_size=8, height=14)

    # ── AI SECTION ─────────────────────────────────────────────────
    if ai:
        r = _hdr(r, "  🤖  AI MARKET OVERVIEW  —  Claude Sonnet Analysis",
                 "1A237E", font_size=10, height=20)

        # Synopsis + sector rotation side by side
        synopsis = ai.get("synopsis", "")
        sr_text  = ai.get("sector_rotation", "")
        SL = 9  # left columns for synopsis
        if synopsis or sr_text:
            if synopsis:
                c = ws.cell(row=r, column=1, value=synopsis)
                c.font = Font(name="Arial", size=9, italic=True, color="1A237E")
                c.fill = PatternFill("solid", fgColor="E8EAF6")
                c.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
                ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=SL)
            if sr_text:
                c2 = ws.cell(row=r, column=SL+1,
                             value=f"SECTOR ROTATION\n{sr_text}")
                c2.font = Font(name="Arial", size=8, italic=True, color="1B5E20")
                c2.fill = PatternFill("solid", fgColor="E8F5E9")
                c2.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
                ws.merge_cells(start_row=r, start_column=SL+1, end_row=r, end_column=TC)
            ws.row_dimensions[r].height = 42
            r += 1

        # Macro context
        macro_ctx = ai.get("macro_context", "")
        if macro_ctx:
            r = _hdr(r, "  🌍  GEOPOLITICAL & MACRO CONTEXT", "4A148C", font_size=8, height=16)
            r = _text_row(r, macro_ctx, "F3E5F5", font_color="4A148C",
                          italic=True, height=36)

        # Market outlook / crash risk banner
        mo = ai.get("market_outlook", {})
        if mo:
            nt  = (mo.get("near_term_bias") or "NEUTRAL").upper()
            lt  = (mo.get("long_term_bias") or "NEUTRAL").upper()
            cr  = (mo.get("crash_risk") or "LOW").upper()
            mo_rationale = mo.get("rationale", "")
            _BIAS_FILL  = {"BULLISH": "1B5E20", "NEUTRAL": "006064",
                           "CAUTIOUS": "E65100", "BEARISH": "B71C1C"}
            _CRASH_FILL = {"LOW": "1B5E20", "ELEVATED": "E65100", "HIGH": "B71C1C"}
            nt_fill  = _BIAS_FILL.get(nt,  "006064")
            lt_fill  = _BIAS_FILL.get(lt,  "006064")
            cr_fill  = _CRASH_FILL.get(cr, "E65100")
            r = _hdr(r, "  📊  MARKET OUTLOOK  &  CRASH RISK ASSESSMENT", "263238",
                     font_size=8, height=16)
            BADGE_W = 4
            badge_data = [
                (f"NEAR-TERM\n{nt}",  nt_fill,  1,           BADGE_W),
                (f"LONG-TERM\n{lt}",  lt_fill,  BADGE_W+1,   BADGE_W),
                (f"CRASH RISK\n{cr}", cr_fill,  BADGE_W*2+1, BADGE_W),
            ]
            for label, fill_hex, start_col, span in badge_data:
                bc = ws.cell(row=r, column=start_col, value=label)
                bc.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
                bc.fill = PatternFill("solid", fgColor=fill_hex)
                bc.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                ws.merge_cells(start_row=r, start_column=start_col,
                               end_row=r, end_column=start_col + span - 1)
            if mo_rationale:
                rc = ws.cell(row=r, column=BADGE_W*3+1, value=mo_rationale)
                rc.font = Font(name="Arial", size=8, italic=True, color="37474F")
                rc.fill = PatternFill("solid", fgColor="ECEFF1")
                rc.alignment = Alignment(wrap_text=True, vertical="center", indent=1)
                ws.merge_cells(start_row=r, start_column=BADGE_W*3+1,
                               end_row=r, end_column=TC)
            ws.row_dimensions[r].height = 30
            r += 1

        # AI macro_dashboard interpretation (rate env, recession risk, fed policy)
        md = ai.get("macro_dashboard", {})
        if md:
            _RECV_FILL = {"LOW": "1B5E20", "MODERATE": "E65100", "HIGH": "B71C1C"}
            _FED_FILL  = {"DOVISH": "1B5E20", "NEUTRAL": "006064", "HAWKISH": "B71C1C"}
            recv_risk = (md.get("recession_risk") or "").split()[0].upper()  # first word only
            fed_pol   = (md.get("fed_policy") or "NEUTRAL").upper()
            r_fill = _RECV_FILL.get(recv_risk, "455A64")
            f_fill = _FED_FILL.get(fed_pol, "006064")
            # Badges for recession risk + fed policy
            MBADGE_W = 3
            mb_data = [
                (f"RECESSION RISK\n{recv_risk}", r_fill, 1,            MBADGE_W),
                (f"FED POLICY\n{fed_pol}",       f_fill, MBADGE_W+1,   MBADGE_W),
            ]
            r = _hdr(r, "  🏦  AI MACRO INTERPRETATION", "37474F", font_size=8, height=16)
            for label, fill_hex, start_col, span in mb_data:
                bc = ws.cell(row=r, column=start_col, value=label)
                bc.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                bc.fill = PatternFill("solid", fgColor=fill_hex)
                bc.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                ws.merge_cells(start_row=r, start_column=start_col,
                               end_row=r, end_column=start_col + span - 1)
            rate_env = md.get("rate_environment", "")
            if rate_env:
                re_c = ws.cell(row=r, column=MBADGE_W*2+1, value=rate_env)
                re_c.font = Font(name="Arial", size=8, italic=True, color="37474F")
                re_c.fill = PatternFill("solid", fgColor="ECEFF1")
                re_c.alignment = Alignment(wrap_text=True, vertical="center", indent=1)
                ws.merge_cells(start_row=r, start_column=MBADGE_W*2+1,
                               end_row=r, end_column=TC)
            ws.row_dimensions[r].height = 28
            r += 1

        # Key themes (compact horizontal list in one row)
        themes = ai.get("attention", [])
        if themes:
            r = _hdr(r, "  ⚠  KEY THEMES & RISKS", "B71C1C", font_size=8, height=16)
            bullets = "     ".join(f"▶ {t}" for t in themes)
            r = _text_row(r, bullets, "FFEBEE", font_size=8, height=20)

        # AI Picks table
        picks = ai.get("picks", [])
        if picks:
            r += 1
            r = _hdr(r,
                     f"  TOP {len(picks)} AI PICKS  —  click ▶ row numbers to expand full story",
                     "0D47A1", font_size=9, height=20)

            pick_hdrs = ["#", "Ticker", "Company", "Sector", "Strategy",
                         "Conv.", "Urgency", "Price", "PEG", "P/E", "MoS%", "ROE%", "Pio",
                         "Headline  (expand row to see full thesis)"]
            r = _col_hdrs(r, pick_hdrs, "1A237E")

            HIGH_F = PatternFill("solid", fgColor="1B5E20")
            MED_F  = PatternFill("solid", fgColor="1565C0")
            fmts   = [None, None, None, None, None, None, None,
                      "$#,##0.00", "0.00", "0.0", "0%", "0%", None, None]

            URGENCY_STYLES = {
                "ACT NOW":       ("B71C1C", "FFFFFF"),
                "WITHIN WEEKS":  ("E65100", "FFFFFF"),
                "WITHIN MONTHS": ("1565C0", "FFFFFF"),
                "WATCH":         ("37474F", "FFFFFF"),
                "AVOID":         ("757575", "FFFFFF"),
            }

            # Fetch live prices for overview picks (same as AI tab)
            _ov_live = {}
            for _p in picks:
                _t2 = _p.get("ticker", "").upper()
                if _t2:
                    _lp = fetch_live_price(_t2)
                    if _lp:
                        _ov_live[_t2] = _lp
                    time.sleep(0.15)

            for i, p in enumerate(picks, 1):
                t      = p.get("ticker", "").upper()
                s      = stocks.get(t, {})
                conv   = (p.get("conviction") or "MEDIUM").upper()
                urgency = (p.get("urgency") or "WATCH").upper()
                rf     = ALT_FILL if i % 2 == 0 else PLAIN_FILL

                # ── Summary row (always visible) ──────────────────
                _ov_price = _ov_live.get(t) or s.get("price")
                sum_vals = [
                    i, t,
                    p.get("company", s.get("name",""))[:22],
                    p.get("sector", s.get("sector",""))[:14],
                    p.get("strategy","")[:14],
                    conv, urgency,
                    _ov_price, s.get("peg"), s.get("pe"),
                    s.get("mos"), s.get("roe"), s.get("piotroski"),
                    p.get("headline",""),
                ]
                for ci, (v, fmt) in enumerate(zip(sum_vals, fmts), 1):
                    cell = ws.cell(row=r, column=ci, value=v)
                    cell.fill = rf
                    cell.border = THIN_BORDER
                    cell.font = Font(name="Arial", size=9)
                    cell.alignment = Alignment(horizontal="center", vertical="center",
                                               wrap_text=(ci == 14))
                    if fmt and isinstance(v, (int, float)):
                        cell.number_format = fmt
                    if ci == 2:
                        cell.font = Font(bold=True, name="Arial", size=9, color="0D47A1")
                    elif ci == 6:  # conviction
                        cell.fill = HIGH_F if conv == "HIGH" else MED_F
                        cell.font = Font(bold=True, name="Arial", size=8, color="FFFFFF")
                    elif ci == 7:  # urgency
                        ufg, utf = URGENCY_STYLES.get(urgency, ("757575", "FFFFFF"))
                        cell.fill = PatternFill("solid", fgColor=ufg)
                        cell.font = Font(bold=True, name="Arial", size=8, color=utf)
                    elif ci == 10 and isinstance(v, (int, float)):
                        cell.number_format = "0.0"
                    elif ci == 11 and isinstance(v, float):
                        cell.number_format = "0%"
                        if v > 0.2: cell.fill = PatternFill("solid", fgColor="C8E6C9")
                    elif ci == 13 and isinstance(v, (int, float)):
                        if v >= 8:
                            cell.fill = PatternFill("solid", fgColor="1B5E20")
                            cell.font = Font(bold=True, name="Arial", size=8, color="FFFFFF")
                        elif v >= 7:
                            cell.fill = PatternFill("solid", fgColor="C8E6C9")
                    elif ci == 14:
                        cell.alignment = Alignment(wrap_text=True, vertical="center",
                                                   horizontal="left")
                ws.row_dimensions[r].height = 16
                r += 1

                # ── Detail row (collapsible — outline level 1) ────
                story    = p.get("story","")
                ind_ctx  = p.get("industry_context","")
                catalyst = p.get("catalyst","")
                watch    = p.get("watch","")
                detail   = (f"  {t}  |  📖 MARKET EDGE: {story}"
                            + (f"\n\n  🏭 INDUSTRY: {ind_ctx}" if ind_ctx else "")
                            + f"\n\n  ⚡ CATALYST: {catalyst}\n\n  ⚠ WATCH: {watch}")
                dc = ws.cell(row=r, column=1, value=detail)
                dc.font = Font(name="Arial", size=8, italic=True, color="37474F")
                dc.fill = PatternFill("solid", fgColor="F5F5F5")
                dc.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
                ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=TC)
                ws.row_dimensions[r].height = 75
                ws.row_dimensions[r].outline_level = 1
                ws.row_dimensions[r].hidden = True   # collapsed by default
                r += 1

            disc = ai.get("disclaimer", "")
            if disc:
                r = _text_row(r, disc, "FAFAFA", font_color="AAAAAA",
                              font_size=7, height=13)
    else:
        r = _hdr(r,
                 "  Set ANTHROPIC_API_KEY in .env to enable AI market analysis",
                 "37474F", font_size=9, height=20)

    # ── PORTFOLIO MANAGER SUMMARY ──────────────────────────────────
    if portfolio and portfolio.get("holdings") is not None:
        r += 1
        r = _hdr(r, "  💼  AI PORTFOLIO MANAGER  —  Paper Portfolio Summary  (see tab 1c for full detail)",
                 "1B2631", font_size=9, height=20)

        holdings_ov = portfolio.get("holdings", [])
        cash_ov     = portfolio.get("cash", PORTFOLIO_INITIAL_CASH)
        initial_ov  = portfolio.get("initial_cash", PORTFOLIO_INITIAL_CASH)
        nav_ov      = portfolio_nav or (cash_ov + sum(
            h["shares"] * (stocks.get(h["ticker"], {}).get("price") or h["entry_price"])
            for h in holdings_ov))
        ret_ov      = portfolio_ret if portfolio_ret is not None else ((nav_ov - initial_ov) / initial_ov)
        spy_ov      = portfolio_spy_ret
        alpha_ov    = (ret_ov - spy_ov) if spy_ov is not None else None

        # Stats row
        stat_labels = ["NAV", "Total Return", "SPY (same period)", "Alpha",
                       "Cash", f"Positions ({len(holdings_ov)}/{PORTFOLIO_MAX_POSITIONS})", "Started"]
        stat_values = [
            f"${nav_ov:,.0f}",
            f"{ret_ov:+.1%}",
            f"{spy_ov:+.1%}" if spy_ov is not None else "n/a",
            f"{alpha_ov:+.1%}" if alpha_ov is not None else "n/a",
            f"${cash_ov:,.0f}",
            ", ".join(h["ticker"] for h in holdings_ov[:6]) + ("…" if len(holdings_ov) > 6 else ""),
            portfolio.get("started", "—"),
        ]
        for ci, (lbl, val) in enumerate(zip(stat_labels, stat_values), 1):
            lc = ws.cell(row=r, column=ci*2-1, value=lbl)
            lc.font = Font(bold=True, name="Arial", size=8, color="AAAAAA")
            lc.fill = PatternFill("solid", fgColor="1B2631")
            lc.alignment = Alignment(horizontal="right")
            vc = ws.cell(row=r, column=ci*2, value=val)
            is_pos = isinstance(val, str) and val.startswith("+")
            is_neg = isinstance(val, str) and val.startswith("-")
            vc.font = Font(bold=True, name="Arial", size=9,
                           color=("00C853" if is_pos else ("FF1744" if is_neg else "FFFFFF")))
            vc.fill = PatternFill("solid", fgColor="1B2631")
        ws.row_dimensions[r].height = 16
        r += 1

        # Portfolio thesis
        thesis = portfolio.get("_last_thesis", "")
        if thesis:
            tc2 = ws.cell(row=r, column=1, value=f"PM Thesis: {thesis}")
            tc2.font = Font(name="Arial", size=8, italic=True, color="455A64")
            tc2.fill = PatternFill("solid", fgColor="ECEFF1")
            tc2.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
            ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=TC)
            ws.row_dimensions[r].height = 28
            r += 1

    # ── STRATEGY SUMMARY TABLE ─────────────────────────────────────
    r += 1
    r = _hdr(r, "  STRATEGY PICKS SUMMARY", "263238", font_size=10, height=22)

    tab_configs = [
        ("💎 IV Discount",      iv_rows,           "1A237E"),
        ("🏆 Quality Comp.",    quality_compounders,"7B1FA2"),
        ("🏛 Stalwarts",        stalwarts,          "4A148C"),
        ("🚀 Fast Growers",     fast_growers,       "1B5E20"),
        ("🔧 Turnarounds",      turnarounds,        "B71C1C"),
        ("💰 Slow Growers",     slow_growers,       "455A64"),
        ("🔄 Cyclicals",        cyclicals,          "E65100"),
        ("🏗 Asset Plays",      asset_plays,        "0D47A1"),
    ]
    sum_hdrs = ["Strategy", "# Qualifying", "Top 3 Tickers", "Top Pick", "PEG", "MoS", "Piotroski"]
    r = _col_hdrs(r, sum_hdrs, "37474F")

    for label, tab_rows, color in tab_configs:
        top3   = ", ".join(p.get("Ticker","") for p in tab_rows[:3])
        top1   = tab_rows[0] if tab_rows else {}
        vals   = [label, len(tab_rows), top3,
                  top1.get("Company","")[:22],
                  top1.get("PEG"), top1.get("MoS"), top1.get("Piotroski")]
        for ci, v in enumerate(vals, 1):
            cell = ws.cell(row=r, column=ci, value=v)
            cell.border = THIN_BORDER
            cell.fill = ALT_FILL if r % 2 == 0 else PLAIN_FILL
            cell.font = Font(name="Arial", size=9)
            cell.alignment = Alignment(horizontal="left" if ci in (1,3,4) else "center",
                                       vertical="center")
            if ci == 1:
                cell.fill = PatternFill("solid", fgColor=color)
                cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
            elif ci == 5 and isinstance(v, float):
                cell.number_format = "0.00"
                if 0 < v < 1.0: cell.fill = PatternFill("solid", fgColor="C8E6C9")
                elif 0 < v < 1.5: cell.fill = PatternFill("solid", fgColor="FFF9C4")
            elif ci == 6 and isinstance(v, float):
                cell.number_format = "0%"
                if v > 0.2: cell.fill = PatternFill("solid", fgColor="C8E6C9")
            elif ci == 7 and isinstance(v, (int, float)):
                if v >= 8:
                    cell.fill = PatternFill("solid", fgColor="1B5E20")
                    cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                elif v >= 7:
                    cell.fill = PatternFill("solid", fgColor="C8E6C9")
        ws.row_dimensions[r].height = 16
        r += 1

    # ── MINI PICK TABLES PER STRATEGY ─────────────────────────────
    r += 1
    r = _hdr(r, "  STRATEGY DETAIL  —  Top 8 picks per strategy",
             "263238", font_size=10, height=22)

    MINI_HDRS = ["#", "Ticker", "Company", "Sector", "Price",
                 "PEG", "P/E", "MoS%", "ROE%", "FCF%", "Pio", "Score"]
    MINI_FMTS = [None,None,None,None,"$#,##0.00","0.00","0.0","0%","0%","0%",None,"0.0"]

    for label, tab_rows, color in tab_configs:
        r = _hdr(r, f"  {label}", color, font_size=9, height=18)
        r = _col_hdrs(r, MINI_HDRS, color)

        for rank, pick in enumerate(tab_rows[:8], 1):
            rf2 = ALT_FILL if rank % 2 == 0 else PLAIN_FILL
            vals2 = [f"#{rank}", pick.get("Ticker",""), (pick.get("Company") or "")[:22],
                     (pick.get("Sector") or "")[:14], pick.get("Price"),
                     pick.get("PEG"), pick.get("P/E"), pick.get("MoS"),
                     pick.get("ROE"), pick.get("FCF Yield"),
                     pick.get("Piotroski"), pick.get("Score")]
            for ci, (v, fmt) in enumerate(zip(vals2, MINI_FMTS), 1):
                c = ws.cell(row=r, column=ci, value=v)
                c.font = Font(name="Arial", size=9)
                c.fill = rf2
                c.alignment = Alignment(
                    horizontal="left" if ci in (2,3,4) else "center",
                    vertical="center")
                c.border = THIN_BORDER
                if fmt and isinstance(v, (int, float)): c.number_format = fmt
                if ci == 8 and isinstance(v, float):
                    if v > 0.3: c.fill = PatternFill("solid", fgColor="C8E6C9")
                    elif v > 0.1: c.fill = PatternFill("solid", fgColor="E8F5E9")
                if ci == 11 and isinstance(v, (int, float)):
                    if v >= 8:
                        c.fill = PatternFill("solid", fgColor="1B5E20")
                        c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                    elif v >= 7: c.fill = PatternFill("solid", fgColor="C8E6C9")
                if ci == 6 and isinstance(v, float) and v > 0:
                    if v < 1.0: c.fill = PatternFill("solid", fgColor="C8E6C9")
                    elif v < 1.5: c.fill = PatternFill("solid", fgColor="FFF9C4")
            ws.row_dimensions[r].height = 16
            r += 1
        r += 1  # gap between strategies

    ws.freeze_panes = "A3"


def log_picks(picks_by_strategy: dict, prices: dict):
    """Auto-log today's top picks to CSV for performance tracking. Skips duplicates."""
    today = datetime.date.today().isoformat()
    file_exists = os.path.exists(PICKS_LOG)

    # Don't duplicate — collect already-logged ticker+strategy combos for today
    existing_today = set()
    if file_exists:
        with open(PICKS_LOG, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("date") == today:
                    existing_today.add((row.get("ticker"), row.get("strategy")))

    rows = []
    for strategy, pick_rows in picks_by_strategy.items():
        for r in pick_rows[:5]:
            t = r.get("Ticker", "")
            price = prices.get(t, r.get("Price"))
            if t and price and (t, strategy) not in existing_today:
                rows.append({"date": today, "strategy": strategy,
                             "ticker": t, "entry_price": round(price, 2),
                             "company": r.get("Company", "")})
    if not rows:
        if existing_today:
            print(f"  ℹ️ Strategy picks already logged for today")
        return
    with open(PICKS_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "strategy", "ticker", "entry_price", "company"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    print(f"  📝 Strategy picks logged: {len(rows)} entries → {PICKS_LOG}")


def load_portfolio() -> dict:
    """Load the paper portfolio from JSON file. Creates a fresh one if it doesn't exist."""
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "started": datetime.date.today().isoformat(),
        "initial_cash": PORTFOLIO_INITIAL_CASH,
        "cash": PORTFOLIO_INITIAL_CASH,
        "holdings": [],
        "transactions": [],
    }


def save_portfolio(p: dict):
    """Persist portfolio to JSON."""
    with open(PORTFOLIO_FILE, "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2)


def run_portfolio_manager(portfolio: dict, candidates_block: str,
                          sector_block: str, stocks: dict) -> dict:
    """Call Claude as a patient long-term portfolio manager.
    Returns a decisions dict with 'review' (hold/sell) + 'buys' lists, or {} on failure.
    """
    if not ANTHROPIC_KEY:
        return {}

    today = datetime.date.today()

    # Build current-holdings context with live performance
    holdings_lines = []
    for h in portfolio.get("holdings", []):
        t = h["ticker"]
        s = stocks.get(t, {})
        current_price = s.get("price") or h["entry_price"]
        ret = (current_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0
        try:
            days = (today - datetime.date.fromisoformat(h["entry_date"])).days
        except Exception:
            days = 0
        roic_v = s.get("roic")
        fcf_v  = s.get("fcfYield")
        rg_v   = s.get("revGrowth")
        peg_v  = s.get("fwdPEG") or s.get("peg")
        line = (f"  {t} ({h.get('company','')[:20]}) | Held {days}d | "
                f"Entry ${h['entry_price']:.2f} → Now ${current_price:.2f} ({ret:+.1%}) | "
                f"ROIC={roic_v*100:.0f}% " if roic_v else f"  {t} | Held {days}d | Entry ${h['entry_price']:.2f} → Now ${current_price:.2f} ({ret:+.1%}) | ")
        if roic_v:  line += f"ROIC={roic_v*100:.0f}% "
        if fcf_v:   line += f"FCF={fcf_v:.1%} "
        if rg_v:    line += f"RG={rg_v:+.0%} "
        if peg_v:   line += f"PEG={peg_v:.2f} "
        line += f"| Thesis: {h.get('rationale','')[:80]}"
        holdings_lines.append(line)

    n_holdings = len(portfolio.get("holdings", []))
    open_slots  = PORTFOLIO_MAX_POSITIONS - n_holdings
    portfolio_value = portfolio.get("cash", PORTFOLIO_INITIAL_CASH)
    for h in portfolio.get("holdings", []):
        t = h["ticker"]
        s = stocks.get(t, {})
        current_price = s.get("price") or h["entry_price"]
        portfolio_value += h["shares"] * current_price

    holdings_block = "\n".join(holdings_lines) if holdings_lines else "  (no holdings yet — deploy capital)"

    # Sector concentration summary for PM awareness
    _sector_counts: dict = {}
    for h in portfolio.get("holdings", []):
        _sec = stocks.get(h["ticker"], {}).get("sector") or h.get("sector") or "Unknown"
        _sector_counts[_sec] = _sector_counts.get(_sec, 0) + 1
    _conc_lines = [f"  {sec}: {cnt} position{'s' if cnt > 1 else ''}{' ⚠️ AT CAP' if cnt >= 4 else ''}"
                   for sec, cnt in sorted(_sector_counts.items(), key=lambda x: -x[1])]
    sector_conc_block = "\n".join(_conc_lines) if _conc_lines else "  (no holdings)"

    # Recent transactions (last 10) for context
    recent_txns = portfolio.get("transactions", [])[-10:]
    txn_lines = [f"  {tx['date']} {tx['action']} {tx['ticker']} @ ${tx['price']:.2f} — {tx.get('rationale','')[:60]}"
                 for tx in recent_txns]
    txn_block = "\n".join(txn_lines) if txn_lines else "  (no transactions yet)"

    sys_prompt = f"""You are a patient, long-horizon portfolio manager running a paper portfolio of US equities.
Today is {today}. Portfolio started: {portfolio.get('started', today.isoformat())}.

PORTFOLIO RULES:
- Maximum {PORTFOLIO_MAX_POSITIONS} positions, roughly equal-weight (~${PORTFOLIO_TARGET_POSITION:,.0f} per position)
- Time horizon: 1-3 years. You are NOT a trader.
- Hold bias: do NOT sell unless the thesis is clearly broken or a dramatically superior opportunity exists
- A position held <60 days needs strong justification to sell — short-term volatility is NOT a reason
- Every SELL must state the exact reason the original thesis is broken
- Every BUY must have: ROIC > 12%, positive FCF, PEG < 2.5, and a clear moat or catalyst
- Diversify across sectors and strategies — hard cap of 4 positions per sector (Technology included)
- If existing holdings already have 4 in one sector, DO NOT buy another in that sector regardless of quality
- Cash not deployed is drag — fill open slots if quality candidates exist

CURRENT PORTFOLIO STATE:
Cash available: ${portfolio.get('cash', PORTFOLIO_INITIAL_CASH):,.2f}
Holdings ({n_holdings}/{PORTFOLIO_MAX_POSITIONS}):
{holdings_block}

SECTOR CONCENTRATION (current holdings):
{sector_conc_block}

RECENT TRANSACTIONS:
{txn_block}"""

    usr_prompt = f"""SECTOR CONTEXT:
{sector_block}

CANDIDATE STOCKS (quality-ranked pool):
{candidates_block}

YOUR TASK:
1. Review each current holding: HOLD or SELL (with explicit thesis-broken reasoning)
2. Propose BUYs for open slots (currently {open_slots} open) — only if a genuinely compelling candidate exists
3. If a current holding has badly deteriorated AND a clearly superior replacement exists, you may propose a SWAP (SELL + BUY)

Respond ONLY with valid JSON (no markdown):
{{
  "portfolio_thesis": "2-3 sentences: your overall portfolio positioning and market view",
  "review": [
    {{
      "ticker": "QLYS",
      "decision": "HOLD",
      "rationale": "Thesis intact: ROIC 28%, revenue re-accelerating, PEG 1.6 still fair for quality"
    }},
    {{
      "ticker": "XYZ",
      "decision": "SELL",
      "rationale": "Thesis broken: revenue declined 3 consecutive quarters — no longer a quality compounder"
    }}
  ],
  "buys": [
    {{
      "ticker": "HIG",
      "company": "The Hartford Financial",
      "rationale": "Best-in-class insurer at trough valuation; rate environment is a tailwind; ROIC 28%",
      "conviction": "HIGH",
      "sell_trigger": "Combined ratio rises above 100% for 2 consecutive quarters"
    }}
  ]
}}"""

    try:
        import requests as _req
        headers = {
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        body = {
            "model": "claude-sonnet-4-6",
            "max_tokens": 3000,
            "system": sys_prompt,
            "messages": [{"role": "user", "content": usr_prompt}],
        }
        resp = _req.post("https://api.anthropic.com/v1/messages",
                         headers=headers, json=body, timeout=120)
        if resp.status_code == 200:
            raw = resp.json()["content"][0]["text"]
            # strip markdown fences if present
            raw = raw.strip()
            if raw.startswith("```"):
                raw = "\n".join(raw.split("\n")[1:])
            if raw.endswith("```"):
                raw = "\n".join(raw.split("\n")[:-1])
            result = json.loads(raw)
            print(f"  ✅ Portfolio manager done — "
                  f"{len(result.get('review',[]))} holdings reviewed, "
                  f"{len(result.get('buys',[]))} buys proposed")
            return result
        else:
            print(f"  ⚠️ Portfolio manager HTTP {resp.status_code}")
    except Exception as e:
        print(f"  ⚠️ Portfolio manager error: {str(e)[:80]}")
    return {}


def apply_portfolio_decisions(portfolio: dict, decisions: dict, stocks: dict) -> dict:
    """Apply PM decisions to portfolio: execute sells then buys. Returns updated portfolio."""
    if not decisions:
        return portfolio
    today = datetime.date.today().isoformat()
    holdings_by_ticker = {h["ticker"]: h for h in portfolio.get("holdings", [])}

    # ── Process SELLs ────────────────────────────────────────────────
    for rev in decisions.get("review", []):
        if rev.get("decision") != "SELL":
            continue
        t = rev.get("ticker", "").upper()
        if t not in holdings_by_ticker:
            continue
        h = holdings_by_ticker[t]
        s = stocks.get(t, {})
        sell_price = s.get("price") or h["entry_price"]
        proceeds = h["shares"] * sell_price
        ret = (sell_price - h["entry_price"]) / h["entry_price"] if h["entry_price"] > 0 else 0
        portfolio["cash"] = portfolio.get("cash", 0) + proceeds
        portfolio["transactions"].append({
            "date": today,
            "action": "SELL",
            "ticker": t,
            "company": h.get("company", ""),
            "shares": h["shares"],
            "price": round(sell_price, 2),
            "value": round(proceeds, 2),
            "return_pct": round(ret * 100, 2),
            "rationale": rev.get("rationale", ""),
        })
        del holdings_by_ticker[t]
        print(f"    💰 SELL {t} @ ${sell_price:.2f} ({ret:+.1%}) — {rev.get('rationale','')[:60]}")

    # ── Process BUYs ─────────────────────────────────────────────────
    for buy in decisions.get("buys", []):
        t = buy.get("ticker", "").upper()
        if not t or t in holdings_by_ticker:
            continue  # already held
        if len(holdings_by_ticker) >= PORTFOLIO_MAX_POSITIONS:
            break
        s = stocks.get(t, {})
        price = s.get("price")
        if not price or price <= 0:
            continue
        # Use target position size; if not enough cash, skip
        invest = min(PORTFOLIO_TARGET_POSITION, portfolio.get("cash", 0))
        if invest < price:
            continue  # can't afford even 1 share
        shares = max(1, int(invest / price))
        cost = shares * price
        portfolio["cash"] = portfolio.get("cash", 0) - cost
        holdings_by_ticker[t] = {
            "ticker": t,
            "company": buy.get("company", s.get("name", t))[:30],
            "shares": shares,
            "entry_price": round(price, 2),
            "entry_date": today,
            "rationale": buy.get("rationale", "")[:200],
            "conviction": buy.get("conviction", "MEDIUM"),
            "sell_trigger": buy.get("sell_trigger", "")[:150],
        }
        portfolio["transactions"].append({
            "date": today,
            "action": "BUY",
            "ticker": t,
            "company": buy.get("company", s.get("name", t))[:30],
            "shares": shares,
            "price": round(price, 2),
            "value": round(cost, 2),
            "return_pct": 0.0,
            "rationale": buy.get("rationale", "")[:200],
        })
        print(f"    🛒 BUY  {t} {shares}sh @ ${price:.2f} (${cost:,.0f}) — {buy.get('rationale','')[:60]}")

    portfolio["holdings"] = list(holdings_by_ticker.values())
    portfolio["last_updated"] = today
    return portfolio


def build_portfolio_tab(wb, portfolio: dict, stocks: dict, spy_prices: dict = None,
                        spy_today: float = None, ws=None):
    """Tab 1c: AI Portfolio Manager — persistent paper portfolio with full trade log."""
    print("\n📊 Building Tab: AI Portfolio...")
    if ws is None:
        ws = wb.create_sheet("1c. AI Portfolio")
    ws.sheet_view.showGridLines = False

    today = datetime.date.today()
    holdings = portfolio.get("holdings", [])
    transactions = portfolio.get("transactions", [])
    cash = portfolio.get("cash", PORTFOLIO_INITIAL_CASH)
    initial = portfolio.get("initial_cash", PORTFOLIO_INITIAL_CASH)

    # ── Compute portfolio value ──────────────────────────────────────
    total_mkt = 0.0
    for h in holdings:
        t = h["ticker"]
        s = stocks.get(t, {})
        cp = s.get("price") or h["entry_price"]
        total_mkt += h["shares"] * cp
    nav = cash + total_mkt
    total_ret = (nav - initial) / initial if initial > 0 else 0

    # SPY return since portfolio started
    spy_ret_overall = None
    started = portfolio.get("started")
    if started and spy_prices and spy_today:
        for delta in range(8):
            d = (datetime.date.fromisoformat(started) - datetime.timedelta(days=delta)).isoformat()
            if d in spy_prices and spy_prices[d] > 0:
                spy_ret_overall = (spy_today - spy_prices[d]) / spy_prices[d]
                break

    # ── Column widths ────────────────────────────────────────────────
    col_widths = [8, 9, 25, 14, 8, 9, 9, 9, 9, 9, 8, 8, 50, 40]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    TC = len(col_widths)

    def _hdr(row, txt, color, size=9, height=18):
        c = ws.cell(row=row, column=1, value=txt)
        c.font = Font(bold=True, name="Arial", size=size, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor=color)
        c.alignment = Alignment(horizontal="left", vertical="center", indent=1)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=TC)
        ws.row_dimensions[row].height = height
        return row + 1

    r = 1
    r = _hdr(r, f"  💼  AI Portfolio Manager  —  Paper Portfolio  |  {today}", "0D1B2A", size=12, height=28)

    # ── Stats bar ────────────────────────────────────────────────────
    stat_items = [
        ("NAV", f"${nav:,.0f}"),
        ("Cash", f"${cash:,.0f}"),
        ("Invested", f"${total_mkt:,.0f}"),
        ("Total Return", f"{total_ret:+.1%}"),
        ("SPY (same period)", f"{spy_ret_overall:+.1%}" if spy_ret_overall is not None else "n/a"),
        ("Alpha", f"{(total_ret - spy_ret_overall):+.1%}" if spy_ret_overall is not None else "n/a"),
        ("Positions", f"{len(holdings)}/{PORTFOLIO_MAX_POSITIONS}"),
        ("Started", portfolio.get("started", "—")),
    ]
    for ci, (lbl, val) in enumerate(stat_items, 1):
        lc = ws.cell(row=r, column=ci*2-1, value=lbl)
        lc.font = Font(bold=True, name="Arial", size=8, color="AAAAAA")
        lc.fill = PatternFill("solid", fgColor="0D1B2A")
        lc.alignment = Alignment(horizontal="right")
        vc = ws.cell(row=r, column=ci*2, value=val)
        vc.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        vc.fill = PatternFill("solid", fgColor="0D1B2A")
    ws.row_dimensions[r].height = 16
    r += 1

    # ── Holdings table ───────────────────────────────────────────────
    r = _hdr(r, "  📊  Current Holdings", "1A237E")
    hdrs = ["Entry Date", "Ticker", "Company", "Sector", "Shares",
            "Entry $", "Current $", "Return", "SPY Ret", "Rel Ret",
            "Days", "Conviction", "Rationale", "Sell Trigger"]
    for ci, h in enumerate(hdrs, 1):
        c = ws.cell(row=r, column=ci, value=h)
        c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="1A237E")
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        c.border = THIN_BORDER
    ws.row_dimensions[r].height = 26
    r += 1

    for ri, h in enumerate(holdings):
        t = h["ticker"]
        s = stocks.get(t, {})
        cp = s.get("price") or h["entry_price"]
        ep = h["entry_price"]
        ret_h = (cp - ep) / ep if ep > 0 else None
        try:
            days_h = (today - datetime.date.fromisoformat(h["entry_date"])).days
        except Exception:
            days_h = None
        # SPY for same period
        spy_h_entry = None
        if spy_prices and h.get("entry_date"):
            for delta in range(8):
                d = (datetime.date.fromisoformat(h["entry_date"]) - datetime.timedelta(days=delta)).isoformat()
                if d in spy_prices and spy_prices[d] > 0:
                    spy_h_entry = spy_prices[d]
                    break
        spy_h_ret = ((spy_today - spy_h_entry) / spy_h_entry
                     if spy_h_entry and spy_today else None)
        rel_h = (ret_h - spy_h_ret) if ret_h is not None and spy_h_ret is not None else None

        fill = ALT_FILL if ri % 2 == 0 else PLAIN_FILL
        row_vals = [h.get("entry_date",""), t, h.get("company","")[:24],
                    s.get("sector","")[:13], h["shares"],
                    ep, round(cp, 2), ret_h, spy_h_ret, rel_h,
                    days_h, h.get("conviction",""),
                    h.get("rationale","")[:120], h.get("sell_trigger","")[:80]]
        for ci, v in enumerate(row_vals, 1):
            c = ws.cell(row=r, column=ci, value=v)
            c.font = Font(name="Arial", size=9)
            c.fill = fill
            c.border = THIN_BORDER
            c.alignment = Alignment(horizontal="left" if ci >= 3 else "center",
                                    vertical="center", wrap_text=(ci >= 13))
            if hdrs[ci-1] in ("Entry $", "Current $"):
                c.number_format = "$#,##0.00"
            elif hdrs[ci-1] in ("Return", "SPY Ret"):
                c.number_format = "0.0%"
                if isinstance(v, float):
                    c.font = Font(name="Arial", size=9,
                                  color="1B5E20" if v > 0 else "B71C1C")
            elif hdrs[ci-1] == "Rel Ret":
                c.number_format = "0.0%"
                if isinstance(v, float):
                    c.fill = PatternFill("solid", fgColor="C8E6C9" if v > 0 else "FFCDD2")
                    c.font = Font(bold=True, name="Arial", size=9,
                                  color="1B5E20" if v > 0 else "B71C1C")
        ws.row_dimensions[r].height = 15
        r += 1

    if not holdings:
        ws.cell(row=r, column=1,
                value="No holdings yet — portfolio will deploy capital on next run."
                ).font = Font(italic=True, name="Arial", size=9, color="888888")
        r += 1

    # ── Transaction log ──────────────────────────────────────────────
    r += 1
    r = _hdr(r, "  📋  Transaction Log  (all buys & sells)", "263238")
    txn_hdrs = ["Date", "Action", "Ticker", "Company", "Shares", "Price", "Value", "Return", "Rationale"]
    txn_widths_map = [10, 8, 8, 22, 7, 9, 10, 8, 60]
    for ci, (h, w) in enumerate(zip(txn_hdrs, txn_widths_map), 1):
        c = ws.cell(row=r, column=ci, value=h)
        c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="263238")
        c.alignment = Alignment(horizontal="center")
        c.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[r].height = 20
    r += 1

    for ri, tx in enumerate(reversed(transactions)):  # newest first
        fill = ALT_FILL if ri % 2 == 0 else PLAIN_FILL
        is_buy = tx.get("action") == "BUY"
        action_fill = PatternFill("solid", fgColor="C8E6C9" if is_buy else "FFCDD2")
        row_vals = [tx.get("date",""), tx.get("action",""), tx.get("ticker",""),
                    tx.get("company","")[:22], tx.get("shares"),
                    tx.get("price"), tx.get("value"),
                    (tx.get("return_pct", 0) / 100) if tx.get("return_pct") is not None else None,
                    tx.get("rationale","")[:120]]
        for ci, v in enumerate(row_vals, 1):
            c = ws.cell(row=r, column=ci, value=v)
            c.font = Font(name="Arial", size=9)
            c.fill = action_fill if ci == 2 else fill
            c.border = THIN_BORDER
            c.alignment = Alignment(horizontal="left" if ci >= 4 else "center",
                                    vertical="center", wrap_text=(ci == 9))
            if txn_hdrs[ci-1] in ("Price", "Value"):
                c.number_format = "$#,##0.00"
            elif txn_hdrs[ci-1] == "Return" and isinstance(v, float):
                c.number_format = "0.0%"
                c.font = Font(name="Arial", size=9,
                              color="1B5E20" if (v or 0) >= 0 else "B71C1C")
        ws.row_dimensions[r].height = 15
        r += 1

    print(f"  ✅ Portfolio tab done — {len(holdings)} holdings, {len(transactions)} transactions")
    return nav, total_ret, spy_ret_overall


def build_picks_tracking(wb, stocks):
    """Tab 11: Picks Tracking — measure performance of logged picks.
    Reads fmp_picks_log.csv, fetches current prices, shows P&L.
    """
    print("\n📊 Building Tab: Picks Tracking...")
    ws = wb.create_sheet("11. Picks Tracking")
    ws.sheet_view.showGridLines = False

    sr = add_title(ws, "📈 Picks Tracking — Strategy Performance Log",
                   f"Auto-logged every run. Top 5 per strategy tracked at entry price. {datetime.date.today()}")

    if not os.path.exists(PICKS_LOG):
        ws.cell(row=sr, column=1,
                value="No picks logged yet — will appear automatically after first run.").font = \
            Font(italic=True, name="Arial", size=10, color="888888")
        print("  ℹ️ No picks log found")
        return

    # ── Read strategy picks (fmp_picks_log.csv) ────────────────────────────
    logged = []
    with open(PICKS_LOG, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["_kind"] = "strategy"
            row["_display_strategy"] = row.get("strategy", "")[:22]
            logged.append(row)

    # ── Read AI agent picks (fmp_ai_picks_log.csv) — if exists ─────────────
    ai_logged = []
    _agent_icons = {
        "AI-QualityGrowth":    "🌱 Qual.Growth",
        "AI-SpecialSit":       "⚡ Special Sit",
        "AI-CapAppreciation":  "📈 Cap.Apprecn",
        "AI-EmergingGrowth":   "🚀 Emerg.Growth",
        "AI-Judge":            "⚖️ Judge",
        # legacy labels kept for old log entries
        "AI-Bull":             "🐂 Bull (legacy)",
        "AI-Value":            "🛡️ Value (legacy)",
        "AI-Contrarian":       "🔄 Contrarian (legacy)",
    }
    if os.path.exists(AI_PICKS_LOG):
        with open(AI_PICKS_LOG, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                src = row.get("source", "AI-Judge")
                row["_kind"] = "ai"
                row["_display_strategy"] = _agent_icons.get(src, src)
                row["ticker"] = row.get("ticker", "")
                row["entry_price"] = row.get("entry_price", "")
                ai_logged.append(row)

    all_logged = logged + ai_logged
    if not all_logged:
        return

    # ── Fetch LIVE prices for all unique tickers ────────────────────────────
    unique_tickers = list({r["ticker"] for r in all_logged if r.get("ticker")})
    current_prices = {}
    print(f"    Fetching live prices for {len(unique_tickers)} tracked tickers...")
    for _t in unique_tickers:
        _p = fetch_live_price(_t)
        if _p:
            current_prices[_t] = _p
        time.sleep(0.15)
    # Fallback: cached price
    for t in unique_tickers:
        if t not in current_prices:
            s = stocks.get(t, {})
            if s.get("price"):
                current_prices[t] = s["price"]

    # ── Fetch SPY benchmark history (single batch call) ────────────────────
    _all_dates = []
    for r in all_logged:
        try:
            _all_dates.append(datetime.date.fromisoformat(r["date"]))
        except Exception:
            pass
    spy_prices = {}
    spy_today  = None
    if _all_dates:
        _earliest = min(_all_dates).isoformat()
        print(f"    Fetching SPY history from {_earliest} for benchmark...")
        spy_prices = fetch_spy_history(_earliest)
        spy_today  = fetch_live_price("SPY")

    def _spy_entry_price(entry_date_str: str) -> float | None:
        """Find SPY close on or before entry date (handles weekends/holidays)."""
        if not spy_prices:
            return None
        try:
            target = datetime.date.fromisoformat(entry_date_str)
        except Exception:
            return None
        # Scan backwards up to 7 calendar days to find nearest prior trading day
        for delta in range(8):
            d = (target - datetime.timedelta(days=delta)).isoformat()
            if d in spy_prices and spy_prices[d] > 0:
                return spy_prices[d]
        return None

    # ── Build display rows ──────────────────────────────────────────────────
    def _build_row(r):
        t = r.get("ticker", "")
        try:
            entry = float(r.get("entry_price") or 0)
        except (ValueError, TypeError):
            entry = 0
        current = current_prices.get(t, 0)
        ret = ((current - entry) / entry) if entry > 0 and current > 0 else None
        try:
            days = (datetime.date.today() - datetime.date.fromisoformat(r["date"])).days
        except Exception:
            days = None
        # SPY benchmark return for the same holding period
        spy_entry = _spy_entry_price(r.get("date", ""))
        spy_ret = ((spy_today - spy_entry) / spy_entry
                   if spy_entry and spy_today and spy_entry > 0 else None)
        rel_ret = ((ret - spy_ret) if ret is not None and spy_ret is not None else None)
        return {
            "Date Logged": r.get("date", ""),
            "Agent / Strategy": r.get("_display_strategy", "")[:22],
            "Ticker": t,
            "Company": r.get("company", "")[:25],
            "Entry $": round(entry, 2) if entry else None,
            "Current $": round(current, 2) if current else None,
            "Return": ret,
            "SPY Ret": spy_ret,
            "Rel Ret": rel_ret,
            "Days": days,
            "_kind": r.get("_kind", "strategy"),
        }

    strat_rows = [_build_row(r) for r in logged]
    ai_rows    = [_build_row(r) for r in ai_logged]
    strat_rows.sort(key=lambda x: (x.get("Agent / Strategy", ""), x.get("Date Logged", "")))
    ai_rows.sort(key=lambda x: (x.get("Agent / Strategy", ""), x.get("Date Logged", "")))

    headers = ["Date Logged", "Agent / Strategy", "Ticker", "Company",
               "Entry $", "Current $", "Return", "SPY Ret", "Rel Ret", "Days"]
    widths  = [12, 22, 8, 25, 9, 9, 9, 9, 9, 6]

    def _write_section(title_val, title_color, row_list, start_row):
        r = start_row
        # Section title
        tc = ws.cell(row=r, column=1, value=title_val)
        tc.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
        tc.fill = PatternFill("solid", fgColor=title_color)
        ws.row_dimensions[r].height = 18
        r += 1
        # Header
        for ci, h in enumerate(headers, 1):
            c = ws.cell(row=r, column=ci, value=h)
            c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
            c.fill = PatternFill("solid", fgColor="1A237E")
            c.alignment = Alignment(horizontal="center")
            c.border = THIN_BORDER
        for ci, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(ci)].width = w
        r += 1
        # Rows
        for ri, row in enumerate(row_list):
            fill = ALT_FILL if ri % 2 == 0 else PLAIN_FILL
            for ci, h in enumerate(headers, 1):
                v = row.get(h)
                c = ws.cell(row=r, column=ci, value=v)
                c.font = Font(name="Arial", size=9)
                c.border = THIN_BORDER
                c.fill = fill
                c.alignment = Alignment(
                    horizontal="left" if ci in (2, 4) else "center",
                    vertical="center")
                if h in ("Entry $", "Current $") and isinstance(v, (int, float)):
                    c.number_format = "$#,##0.00"
                elif h == "Return" and isinstance(v, (int, float)):
                    c.number_format = "0.0%"
                    if v > 0.10:
                        c.fill = PatternFill("solid", fgColor="C8E6C9")
                        c.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                    elif v > 0:
                        c.fill = PatternFill("solid", fgColor="E8F5E9")
                    elif v < -0.10:
                        c.fill = PatternFill("solid", fgColor="FFCDD2")
                        c.font = Font(name="Arial", size=9, color="B71C1C")
                    elif v < 0:
                        c.fill = PatternFill("solid", fgColor="FFEBEE")
                elif h == "SPY Ret" and isinstance(v, (int, float)):
                    c.number_format = "0.0%"
                elif h == "Rel Ret" and isinstance(v, (int, float)):
                    c.number_format = "0.0%"
                    if v > 0:
                        c.fill = PatternFill("solid", fgColor="C8E6C9")
                        c.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                    elif v < 0:
                        c.fill = PatternFill("solid", fgColor="FFCDD2")
                        c.font = Font(name="Arial", size=9, color="B71C1C")
            ws.row_dimensions[r].height = 15
            r += 1
        return r

    def _write_summary(title_val, title_color, row_list, start_row):
        r = start_row + 1
        tc = ws.cell(row=r, column=1, value=title_val)
        tc.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
        tc.fill = PatternFill("solid", fgColor=title_color)
        ws.row_dimensions[r].height = 18
        r += 1
        # Column headers for summary
        for ci, lbl in enumerate(["Agent / Strategy", "Picks", "Avg Return", "SPY Avg", "Avg Alpha", "Win Rate"], 1):
            hc = ws.cell(row=r, column=ci, value=lbl)
            hc.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
            hc.fill = PatternFill("solid", fgColor="455A64")
            hc.alignment = Alignment(horizontal="center")
        r += 1
        by_agent = {}
        for row in row_list:
            ag = row.get("Agent / Strategy", "Unknown")
            if ag not in by_agent:
                by_agent[ag] = {"returns": [], "spy_rets": [], "rel_rets": []}
            ret = row.get("Return")
            spy_r = row.get("SPY Ret")
            rel_r = row.get("Rel Ret")
            if ret is not None:
                by_agent[ag]["returns"].append(ret)
            if spy_r is not None:
                by_agent[ag]["spy_rets"].append(spy_r)
            if rel_r is not None:
                by_agent[ag]["rel_rets"].append(rel_r)
        for ag, data in sorted(by_agent.items()):
            returns  = data["returns"]
            spy_rets = data["spy_rets"]
            rel_rets = data["rel_rets"]
            avg     = sum(returns)  / len(returns)  if returns  else None
            avg_spy = sum(spy_rets) / len(spy_rets) if spy_rets else None
            avg_rel = sum(rel_rets) / len(rel_rets) if rel_rets else None
            win_rate = (sum(1 for v in rel_rets if v > 0) / len(rel_rets)
                        if rel_rets else None)
            ws.cell(row=r, column=1, value=ag).font = Font(bold=True, name="Arial", size=9)
            ws.cell(row=r, column=2, value=len(returns))
            avg_cell = ws.cell(row=r, column=3, value=avg if avg is not None else "no data")
            if isinstance(avg, float):
                avg_cell.number_format = "0.0%"
                avg_cell.font = Font(bold=True, name="Arial", size=9,
                                     color="1B5E20" if avg > 0 else "B71C1C")
            spy_cell = ws.cell(row=r, column=4, value=avg_spy)
            if isinstance(avg_spy, float):
                spy_cell.number_format = "0.0%"
                spy_cell.font = Font(name="Arial", size=9)
            rel_cell = ws.cell(row=r, column=5, value=avg_rel)
            if isinstance(avg_rel, float):
                rel_cell.number_format = "0.0%"
                rel_cell.font = Font(bold=True, name="Arial", size=9,
                                     color="1B5E20" if avg_rel > 0 else "B71C1C")
            wr_cell = ws.cell(row=r, column=6, value=win_rate)
            if isinstance(win_rate, float):
                wr_cell.number_format = "0%"
                wr_cell.font = Font(bold=True, name="Arial", size=9,
                                    color="1B5E20" if win_rate >= 0.50 else "B71C1C")
            r += 1
        return r

    # ── Write strategy picks section ────────────────────────────────────────
    sr = _write_section("📊  Strategy Picks", "1A237E", strat_rows, sr)
    sr = _write_summary("Strategy Performance Summary", "263238", strat_rows, sr)

    # ── Write AI agent picks section ────────────────────────────────────────
    sr += 1
    if ai_rows:
        sr = _write_section("🤖  AI Agent Picks  (Bull · Value · Contrarian · Judge)",
                            "4A148C", ai_rows, sr)
        sr = _write_summary("AI Agent Performance Summary", "37474F", ai_rows, sr)
    else:
        ws.cell(row=sr, column=1,
                value="AI agent picks will appear here after the first multi-agent run."
                ).font = Font(italic=True, name="Arial", size=9, color="888888")

    print(f"  ✅ Picks Tracking done — {len(logged)} strategy picks, "
          f"{len(ai_logged)} AI agent picks, {len(unique_tickers)} unique tickers")


def build_quality_compounders(wb, stocks):
    """Tab 3b: Quality Compounders — Buffett's 'wonderful businesses at fair prices'.
    ROE > 15%, ROIC > 12%, consistent growth, low debt, Piotroski >= 7.
    These are businesses with durable competitive advantages (moats).
    """
    print("\n📊 Building Tab: Quality Compounders...")
    qualified = []
    for t, s in stocks.items():
        if not _is_common_stock(s): continue
        roe = s.get("roe")
        roic = s.get("roic")
        pio = s.get("piotroski")
        fcf = s.get("fcfYield")
        de = s.get("de")
        rg = s.get("revGrowth")
        pe = s.get("pe")

        # Quality gates: ROIC > 15% (Filter 1), ROE > 15%, Piotroski >= 7, FCF positive (Filter 3)
        # ROIC > ROE: ROIC measures true capital efficiency regardless of leverage
        # Real Estate excluded: land appreciation inflates ROIC; lumpy/asset-heavy; not a moat business
        # Basic Materials excluded: commodity-cycle peaks inflate ROIC/FCF for miners; not durable moats
        _sector_qc = (s.get("sector") or "")
        if _sector_qc == "Real Estate": continue
        if _sector_qc == "Basic Materials": continue
        if not roic or roic < 0.15: continue   # raised from 12% — moat signal
        if not roe or roe < 0.15: continue
        if not pio or pio < 7: continue
        if not fcf or fcf <= 0: continue        # Filter 3: FCF must be positive
        if not s.get("mktCap", 0) > 1e9: continue
        if de is not None and de > 3.0: continue  # not excessively leveraged

        # Kill: RevConsistency floor — compounders must show steady revenue
        rc_kill = s.get("revConsistency")
        if rc_kill is not None and rc_kill < 0.60:
            continue

        # Kill: Declining EPS projection — not a compounder if earnings expected to shrink
        # Override: ROIC > 25% is exception (genuine moat may face temporary headwind)
        eg5_kill = s.get("epsGrowth5y")
        if eg5_kill is not None and eg5_kill < -0.03 and not (roic and roic > 0.25):
            continue

        # Score: ROIC weighted MORE than ROE — avoids rewarding leverage-inflated returns
        sc = 0
        # ROIC is the primary capital efficiency signal (Filter 1)
        if roic > 0.30:      sc += 18
        elif roic > 0.25:    sc += 14
        elif roic > 0.20:    sc += 10
        elif roic > 0.15:    sc += 5

        # ROE is secondary — can be inflated by debt, so lower weight than ROIC
        if roe > 0.30:       sc += 8
        elif roe > 0.25:     sc += 6
        elif roe > 0.20:     sc += 4
        elif roe > 0.15:     sc += 2

        if pio >= 9:         sc += 10
        elif pio >= 8:       sc += 7
        elif pio >= 7:       sc += 4

        if fcf > 0.08:       sc += 8
        elif fcf > 0.05:     sc += 5
        elif fcf > 0.02:     sc += 2

        best_peg_qc = s.get("fwdPEG") or s.get("peg")
        _fin_qc       = "financial" in (s.get("sector") or "").lower()
        _basic_mat_qc = "basic materials" in (s.get("sector") or "").lower()
        _energy_qc    = (_sector_qc) == "Energy"
        peg_sc_qc = 0
        if best_peg_qc and 0 < best_peg_qc < 1.0:   peg_sc_qc = 10
        elif best_peg_qc and 0 < best_peg_qc < 1.5: peg_sc_qc = 6
        elif best_peg_qc and 0 < best_peg_qc < 2.5: peg_sc_qc = 3
        # Quality bonus: ROIC+FCF validates the PEG is moat-backed
        if peg_sc_qc and roic and roic > 0.15 and fcf and fcf > 0.04:
            peg_sc_qc = min(10, round(peg_sc_qc * 1.25))
        # Deceleration penalty
        _rg_qc = rg or 0; _rg_p_qc = s.get("revGrowthPrev")
        if peg_sc_qc and _rg_p_qc is not None and (_rg_qc - _rg_p_qc) < -0.10:
            peg_sc_qc = round(peg_sc_qc * 0.60)
        if peg_sc_qc and _fin_qc and best_peg_qc and best_peg_qc < 1.5:
            peg_sc_qc = round(peg_sc_qc * 0.50)
        if peg_sc_qc and _basic_mat_qc and best_peg_qc and best_peg_qc < 1.5:
            peg_sc_qc = round(peg_sc_qc * 0.60)  # commodity miners: spot-price cycle inflates apparent cheapness
        if peg_sc_qc and _energy_qc and best_peg_qc and best_peg_qc < 1.5:
            peg_sc_qc = round(peg_sc_qc * 0.60)  # E&P: commodity-driven earnings distort PEG reliability
        sc += peg_sc_qc

        if rg and rg > 0.10: sc += 5
        elif rg and rg > 0.05: sc += 2

        if s.get("mos") and s.get("mos") > 0.2: sc += 5

        # Revenue consistency bonus
        rc_qc = s.get("revConsistency")
        if rc_qc is not None:
            if rc_qc >= 0.80:   sc += 6
            elif rc_qc >= 0.60: sc += 2
            elif rc_qc < 0.40:  sc -= 4
        # Share buybacks as capital allocation quality signal
        sg_qc = s.get("sharesGrowth")
        if sg_qc is not None:
            if sg_qc < -0.03:   sc += 5
            elif sg_qc < 0:     sc += 2
            elif sg_qc > 0.05:  sc -= 4
        # Filter 3: FCF conversion — FCF ≈ Net Income = no accounting tricks
        fcc_qc = s.get("fcfConversion")
        if fcc_qc is not None:
            if fcc_qc >= 0.80:  sc += 8   # FCF quality confirmed — earnings are real cash
            elif fcc_qc >= 0.60: sc += 4
            elif fcc_qc < 0.40:  sc -= 6  # FCF << earnings: accruals, capex drain, or tricks
        # FCF Margin: high FCF margin = scalable, asset-light business
        fcm_qc = s.get("fcfMargin")
        if fcm_qc is not None:
            if fcm_qc >= 0.20:   sc += 8
            elif fcm_qc >= 0.12: sc += 5
            elif fcm_qc >= 0.06: sc += 2
            elif fcm_qc < 0.02:  sc -= 4
        # FCF growth consistency
        fgc_qc = s.get("fcfGrowthConsistency")
        if fgc_qc is not None:
            if fgc_qc >= 0.80:   sc += 5
            elif fgc_qc >= 0.60: sc += 2
            elif fgc_qc < 0.40:  sc -= 4
        # Operating margin — scalable, asset-light business model signal
        om_qc = s.get("operatingMargin")
        if om_qc is not None:
            if om_qc >= 0.25:   sc += 5
            elif om_qc >= 0.15: sc += 2
            elif om_qc < 0.05:  sc -= 4
        # Filter 4: Growth quality — penalise over-optimistic analyst estimates
        go_qc = s.get("growthOptimism")
        if go_qc is not None and go_qc > 0.50:
            sc -= 5   # analysts projecting >50% more growth than history suggests

        row = format_stock_row(s)
        row["Score"] = round(sc, 1)
        qualified.append(row)

    qualified.sort(key=lambda x: -x["Score"])
    for i, row in enumerate(qualified):
        row["Rank"] = i + 1

    ws = wb.create_sheet("3b. Quality Compounders")
    ws.sheet_view.showGridLines = False
    sr = add_title(ws,
                   "🏆 Quality Compounders — Buffett's Wonderful Businesses",
                   f"ROE >15% + ROIC >12% + Piotroski ≥7 + Positive FCF. "
                   f"Durable moat businesses at reasonable prices. {datetime.date.today()}")

    headers = ["Rank", "Ticker", "Company", "Sector", "Price", "Fwd PEG", "PEG", "Fwd P/E", "P/E",
               "IV", "MoS", "ROIC", "ROE", "FCF Yield", "FCF Margin", "Oper Margin", "FCF Conv.", "FCF Consist.",
               "Rev Growth", "Grwth Gap", "Rev Consist.", "Shares Δ", "EPS Growth 5Y",
               "Piotroski", "52w vs High", "Div Yield", "MktCap ($B)", "Score", "🏦 Insider"]
    widths = [5, 8, 22, 15, 8, 7, 6, 7, 7, 8, 7, 7, 7, 8, 9, 8, 8, 9, 8, 8, 9, 8, 8, 7, 9, 7, 10, 6, 14]
    write_table(ws, qualified[:TOP_N], headers, sr, header_color="B71C1C", widths=widths)
    print(f"  ✅ Quality Compounders tab done — {min(len(qualified), TOP_N)} (from {len(qualified)})")
    return qualified


def fetch_etf_returns(etf_tickers: list) -> dict:
    """Fetch 1Y of daily closes for each ETF (including SPY) and compute
    1M/3M/6M/1Y returns. Returns {ticker: {"1M": float, "3M": float, "6M": float, "1Y": float}}.
    Uses a single historical-price-eod/light call per ticker.
    """
    global _fmp_call_count
    if not FMP_KEY:
        return {}
    today = datetime.date.today()
    from_date = (today - datetime.timedelta(days=380)).isoformat()
    results = {}
    all_tickers = list(set(etf_tickers + ["SPY"]))
    for ticker in all_tickers:
        try:
            r = requests.get(f"{FMP_BASE}/historical-price-eod/light",
                             params={"symbol": ticker, "from": from_date,
                                     "to": today.isoformat(), "apikey": FMP_KEY},
                             timeout=20)
            _fmp_call_count += 1
            if r.status_code != 200:
                continue
            data = r.json()
            if not isinstance(data, list) or not data:
                continue
            # Build date→price dict; sort by date descending
            # FMP stable endpoint returns "price" field (not "close")
            price_map = {rec["date"]: float(rec.get("price") or rec.get("close") or 0)
                         for rec in data if rec.get("date") and (rec.get("price") or rec.get("close"))}
            if not price_map:
                continue
            sorted_dates = sorted(price_map.keys(), reverse=True)
            latest_price = price_map[sorted_dates[0]]

            def _price_on_or_before(target_date):
                for delta in range(10):
                    d = (target_date - datetime.timedelta(days=delta)).isoformat()
                    if d in price_map and price_map[d] > 0:
                        return price_map[d]
                return None

            p1w  = _price_on_or_before(today - datetime.timedelta(days=7))
            p1m  = _price_on_or_before(today - datetime.timedelta(days=21))
            p3m  = _price_on_or_before(today - datetime.timedelta(days=63))
            p6m  = _price_on_or_before(today - datetime.timedelta(days=126))
            p1y  = _price_on_or_before(today - datetime.timedelta(days=252))

            # ── Technical indicators — computed from same price_map, zero extra API calls ──
            # 52-week high/low (use up to 252 trading days = ~365 calendar days)
            prices_252 = [price_map[d] for d in sorted_dates[:252] if price_map.get(d, 0) > 0]
            hi52  = max(prices_252) if prices_252 else None
            lo52  = min(prices_252) if prices_252 else None
            vs52h = round(latest_price / hi52, 4) if hi52 and hi52 > 0 else None
            vs52l = round(latest_price / lo52, 4) if lo52 and lo52 > 0 else None

            # Simple moving averages
            prices_chrono = [price_map[d] for d in sorted(sorted_dates) if price_map.get(d, 0) > 0]
            ma50  = round(sum(prices_chrono[-50:])  / min(50,  len(prices_chrono)), 2) if len(prices_chrono) >= 10 else None
            ma200 = round(sum(prices_chrono[-200:]) / min(200, len(prices_chrono)), 2) if len(prices_chrono) >= 10 else None
            vs_ma50  = round(latest_price / ma50,  4) if ma50  and ma50  > 0 else None
            vs_ma200 = round(latest_price / ma200, 4) if ma200 and ma200 > 0 else None

            # 14-period Wilder RSI from last 15 daily price changes
            _rsi_prices = prices_chrono[-16:] if len(prices_chrono) >= 16 else []
            rsi14 = None
            if len(_rsi_prices) >= 15:
                _changes = [_rsi_prices[i] - _rsi_prices[i-1] for i in range(1, len(_rsi_prices))]
                _gains = [c for c in _changes if c > 0]; _losses = [abs(c) for c in _changes if c < 0]
                _avg_gain = sum(_gains) / 14 if _gains else 0
                _avg_loss = sum(_losses) / 14 if _losses else 0
                if _avg_loss == 0:
                    rsi14 = 100.0
                else:
                    _rs = _avg_gain / _avg_loss
                    rsi14 = round(100 - (100 / (1 + _rs)), 1)

            # Momentum direction: is 1M return stronger than recent 3M average rate?
            _r1m = (latest_price - p1m) / p1m if p1m else None
            _r3m = (latest_price - p3m) / p3m if p3m else None
            if _r1m is not None and _r3m is not None:
                momentum_dir = "▲ Accel" if _r1m > (_r3m / 3) else "▼ Decel"
            else:
                momentum_dir = None

            results[ticker] = {
                "current": latest_price,
                "1W":  (latest_price - p1w)  / p1w  if p1w  else None,
                "1M":  _r1m,
                "3M":  _r3m,
                "6M":  (latest_price - p6m)  / p6m  if p6m  else None,
                "1Y":  (latest_price - p1y)  / p1y  if p1y  else None,
                # 52-week range
                "52H": hi52, "52L": lo52,
                "vs52H": vs52h, "vs52L": vs52l,
                # Moving averages
                "ma50": ma50, "ma200": ma200,
                "vs_ma50": vs_ma50, "vs_ma200": vs_ma200,
                # Oscillator
                "rsi14": rsi14,
                # Trend direction
                "momentum_dir": momentum_dir,
            }
            time.sleep(0.12)
        except Exception:
            continue

    # Compute vs-SPY alpha for each ticker
    spy = results.get("SPY", {})
    for ticker, ret in results.items():
        if ticker == "SPY":
            continue
        for period in ("1M", "3M", "6M", "1Y"):
            etf_r = ret.get(period)
            spy_r = spy.get(period)
            ret[f"{period}_alpha"] = (etf_r - spy_r) if (etf_r is not None and spy_r is not None) else None
    return results


def build_sector_valuations(wb, stocks):
    """Tab 9: Sector Rotation Dashboard — valuation, momentum, quality, signal."""
    print("\n📊 Building Tab: Sector Valuations...")
    ws = wb.create_sheet("9. Sector Rotation")
    ws.sheet_view.showGridLines = False

    # ── Fetch live sector ETF performance vs SPY ───────────────────────────
    # Reverse map: sector name → ETF ticker
    _sector_to_etf = {v: k for k, v in SECTOR_ETFS.items()}
    print("    Fetching sector ETF performance vs SPY...")
    _etf_returns = fetch_etf_returns(list(SECTOR_ETFS.keys()))
    spy_1m  = (_etf_returns.get("SPY") or {}).get("1M")
    spy_3m  = (_etf_returns.get("SPY") or {}).get("3M")
    spy_6m  = (_etf_returns.get("SPY") or {}).get("6M")
    spy_1y  = (_etf_returns.get("SPY") or {}).get("1Y")
    print(f"    SPY: 1M={spy_1m:.1%} 3M={spy_3m:.1%} 6M={spy_6m:.1%} 1Y={spy_1y:.1%}"
          if all(x is not None for x in [spy_1m, spy_3m, spy_6m, spy_1y]) else
          "    SPY data partially unavailable")

    # ── Economic cycle context (static, well-established rotation pattern) ──
    CYCLE_PHASE = {
        "Technology":             "Mid/Late",
        "Financial Services":     "Early",
        "Financials":             "Early",
        "Consumer Cyclical":      "Early/Mid",
        "Consumer Discretionary": "Early/Mid",
        "Industrials":            "Mid",
        "Basic Materials":        "Mid/Late",
        "Materials":              "Mid/Late",
        "Energy":                 "Late",
        "Healthcare":             "Late/Recession",
        "Consumer Defensive":     "Recession",
        "Consumer Staples":       "Recession",
        "Utilities":              "Recession",
        "Real Estate":            "Early/Mid",
        "Communication Services": "Mid",
    }

    # ── Aggregate per sector ──
    sector_agg = defaultdict(lambda: {
        "pegs": [], "mos": [], "fcf": [], "roe": [], "rg": [], "epsg": [],
        "pe": [], "pb": [], "de": [], "pio": [], "count": 0, "stocks": []
    })
    for t, s in stocks.items():
        if not _is_common_stock(s): continue
        sect = s.get("sector") or "Unknown"
        agg = sector_agg[sect]
        agg["count"] += 1
        agg["stocks"].append(s)
        if s.get("peg") and 0 < s.get("peg") < 30:   agg["pegs"].append(s.get("peg"))
        if s.get("mos") is not None:               agg["mos"].append(s.get("mos"))
        if s.get("fcfYield") is not None:          agg["fcf"].append(s.get("fcfYield"))
        if s.get("roe") is not None:               agg["roe"].append(s.get("roe"))
        if s.get("revGrowth") is not None:         agg["rg"].append(s.get("revGrowth"))
        if s.get("epsGrowth5y") is not None:       agg["epsg"].append(s.get("epsGrowth5y"))
        if s.get("pe") and 0 < s.get("pe") < 200:     agg["pe"].append(s.get("pe"))
        if s.get("pb") and 0 < s.get("pb") < 50:      agg["pb"].append(s.get("pb"))
        if s.get("de") is not None:                agg["de"].append(s.get("de"))
        if s.get("piotroski") is not None:         agg["pio"].append(s.get("piotroski"))

    def _med(lst):
        if not lst: return None
        s = sorted(lst)
        n = len(s)
        return round(s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2, 4)

    def _avg(lst):
        return round(sum(lst) / len(lst), 4) if lst else None

    def _pct_above(lst, threshold):
        if not lst: return None
        return round(sum(1 for x in lst if x > threshold) / len(lst), 3)

    rows = []
    for sector, agg in sorted(sector_agg.items(), key=lambda x: -x[1]["count"]):
        if agg["count"] < 5: continue

        med_peg   = _med(agg["pegs"])
        med_pe    = _med(agg["pe"])
        med_pb    = _med(agg["pb"])
        avg_mos   = _avg(agg["mos"])
        avg_fcf   = _avg(agg["fcf"])
        avg_roe   = _avg(agg["roe"])
        avg_rg    = _avg(agg["rg"])
        avg_epsg  = _avg(agg["epsg"])
        avg_pio   = _avg(agg["pio"])
        pct_under = _pct_above(agg["mos"], 0)   # % stocks with positive MoS
        pct_qual  = _pct_above(agg["pio"], 6.5) # % stocks with Piotroski ≥ 7
        n_qual    = sum(1 for x in agg["pio"] if x >= 7)

        # ── Rotation Score (0-100): value + quality + momentum ──
        score = 0
        # Valuation (40 pts): low PEG + high % undervalued
        if med_peg:
            if med_peg < 1.0:    score += 20
            elif med_peg < 1.5:  score += 15
            elif med_peg < 2.0:  score += 10
            elif med_peg < 3.0:  score += 5
        if pct_under is not None:
            score += round(pct_under * 20)  # up to 20pts for 100% undervalued
        # Quality (30 pts): FCF + ROE + Piotroski breadth
        if avg_fcf and avg_fcf > 0.05:   score += 10
        elif avg_fcf and avg_fcf > 0.02: score += 5
        if avg_roe and avg_roe > 0.15:   score += 10
        elif avg_roe and avg_roe > 0.08: score += 5
        if pct_qual and pct_qual > 0.3:  score += 10
        elif pct_qual and pct_qual > 0.15: score += 5
        # Momentum (30 pts): revenue + EPS growth
        if avg_rg and avg_rg > 0.10:    score += 15
        elif avg_rg and avg_rg > 0.05:  score += 8
        elif avg_rg and avg_rg > 0:     score += 3
        if avg_epsg and avg_epsg > 0.10: score += 15
        elif avg_epsg and avg_epsg > 0.05: score += 8
        elif avg_epsg and avg_epsg > 0:  score += 3

        # ETF performance for this sector
        _etf_tkr  = _sector_to_etf.get(sector)
        _etf_data = _etf_returns.get(_etf_tkr, {}) if _etf_tkr else {}
        rows.append({
            "Sector":        sector,
            "Signal":        None,   # assigned by relative ranking below
            "Rot. Score":    score,
            "# Stocks":      agg["count"],
            "# Quality":     n_qual,
            "% Underval.":   pct_under,
            "Med PEG":       med_peg,
            "Med P/E":       med_pe,
            "Med P/B":       med_pb,
            "ETF":           _etf_tkr or "—",
            "1M vs SPY":     _etf_data.get("1M_alpha"),
            "3M vs SPY":     _etf_data.get("3M_alpha"),
            "6M vs SPY":     _etf_data.get("6M_alpha"),
            "1Y vs SPY":     _etf_data.get("1Y_alpha"),
            "Avg FCF Yield": avg_fcf,
            "Avg ROE":       avg_roe,
            "Avg Rev Grw":   avg_rg,
            "Avg EPS Grw":   avg_epsg,
            "Cycle Phase":   CYCLE_PHASE.get(sector, "—"),
            "_stocks":       agg["stocks"],  # for top picks mini-table
        })

    rows.sort(key=lambda x: -x["Rot. Score"])

    # ── Relative signal assignment ─────────────────────────────────────────
    # Absolute thresholds inflate BUY in any bull market.
    # Sector rotation is about WHERE to put capital, not whether to invest at all.
    # Rule: top 3 → BUY, bottom 3 → AVOID, rest → HOLD.
    # Tie-break: minimum absolute score of 40 required for BUY (prevents BUY in a crash).
    n = len(rows)
    for i, row in enumerate(rows):
        sc = row["Rot. Score"]
        if i < 3 and sc >= 40:
            row["Signal"] = "BUY"
        elif i >= n - 3:
            row["Signal"] = "AVOID"
        else:
            row["Signal"] = "HOLD"

    # ── Main rotation table ──
    sr = add_title(ws,
                   "🔄 Sector Rotation Dashboard",
                   f"Rotation Score = Valuation (40) + Quality (30) + Momentum (30). "
                   f"Medians from {len(stocks)} US common stocks. {datetime.date.today()}")

    main_headers = ["Sector", "Signal", "Rot. Score", "# Stocks", "# Quality",
                    "% Underval.", "Med PEG", "Med P/E", "Med P/B",
                    "ETF", "1M vs SPY", "3M vs SPY", "6M vs SPY", "1Y vs SPY",
                    "Avg FCF Yield", "Avg ROE",
                    "Avg Rev Grw", "Avg EPS Grw", "Cycle Phase"]
    main_widths  = [22, 8, 9, 8, 8, 9, 8, 8, 8, 6, 9, 9, 9, 9, 10, 8, 10, 10, 12]

    # Write header row
    for ci, h in enumerate(main_headers, 1):
        c = ws.cell(row=sr, column=ci, value=h)
        c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="263238")
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = THIN_BORDER
    ws.row_dimensions[sr].height = 26
    sr += 1

    BUY_FILL   = PatternFill("solid", fgColor="1B5E20")
    HOLD_FILL  = PatternFill("solid", fgColor="F57F17")
    AVOID_FILL = PatternFill("solid", fgColor="B71C1C")
    BUY_ROW    = PatternFill("solid", fgColor="E8F5E9")
    HOLD_ROW   = PatternFill("solid", fgColor="FFF8E1")
    AVOID_ROW  = PatternFill("solid", fgColor="FFEBEE")

    for ri, row in enumerate(rows):
        sig = row["Signal"]
        row_fill = BUY_ROW if sig == "BUY" else (HOLD_ROW if sig == "HOLD" else AVOID_ROW)
        for ci, h in enumerate(main_headers, 1):
            v = row.get(h)
            cell = ws.cell(row=sr, column=ci, value=v)
            cell.border = THIN_BORDER
            cell.font = Font(name="Arial", size=9)
            cell.fill = row_fill
            cell.alignment = Alignment(horizontal="center", vertical="center")

            if h == "Signal":
                if sig == "BUY":
                    cell.fill = BUY_FILL
                    cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                elif sig == "HOLD":
                    cell.fill = HOLD_FILL
                    cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
                else:
                    cell.fill = AVOID_FILL
                    cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
            elif h == "Rot. Score":
                cell.font = Font(bold=True, name="Arial", size=9)
                if row["Rot. Score"] >= 55:
                    cell.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                elif row["Rot. Score"] < 35:
                    cell.font = Font(bold=True, name="Arial", size=9, color="B71C1C")
            elif h in ("% Underval.", "Avg FCF Yield", "Avg ROE",
                       "Avg Rev Grw", "Avg EPS Grw") and isinstance(v, float):
                cell.number_format = "0%"
                if v and v > 0.01:
                    cell.font = Font(name="Arial", size=9, color="1B5E20")
                elif v and v < -0.01:
                    cell.font = Font(name="Arial", size=9, color="B71C1C")
            elif h in ("1M vs SPY", "3M vs SPY", "6M vs SPY", "1Y vs SPY"):
                if isinstance(v, float):
                    cell.number_format = "+0.0%;-0.0%;0%"
                    if v >= 0.03:
                        cell.fill = PatternFill("solid", fgColor="C8E6C9")
                        cell.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                    elif v > 0:
                        cell.fill = PatternFill("solid", fgColor="E8F5E9")
                        cell.font = Font(name="Arial", size=9, color="2E7D32")
                    elif v <= -0.03:
                        cell.fill = PatternFill("solid", fgColor="FFCDD2")
                        cell.font = Font(bold=True, name="Arial", size=9, color="B71C1C")
                    elif v < 0:
                        cell.fill = PatternFill("solid", fgColor="FFEBEE")
                        cell.font = Font(name="Arial", size=9, color="C62828")
                else:
                    cell.value = "n/a"
                    cell.font = Font(name="Arial", size=8, color="AAAAAA")
            elif h in ("Med PEG", "Med P/E", "Med P/B") and isinstance(v, float):
                cell.number_format = "0.1"
            elif h == "Sector":
                cell.alignment = Alignment(horizontal="left", vertical="center")
                cell.font = Font(bold=True, name="Arial", size=9)
            elif h == "Cycle Phase":
                cell.alignment = Alignment(horizontal="left", vertical="center")
        ws.row_dimensions[sr].height = 16
        sr += 1

    for ci, w in enumerate(main_widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.freeze_panes = "A4"

    sr += 2  # gap

    # ── Cycle Reference Guide ──
    guide_title = ws.cell(row=sr, column=1, value="📚 Economic Cycle — Sector Rotation Guide")
    guide_title.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
    guide_title.fill = PatternFill("solid", fgColor="37474F")
    ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(main_headers))
    sr += 1

    cycle_data = [
        ("🌱 Early Cycle (Recovery)",     "Financials, Consumer Discretionary, Real Estate, Industrials",
         "Credit loosens, consumer spending rebounds, capex picks up"),
        ("☀️  Mid Cycle (Expansion)",      "Technology, Industrials, Materials, Communication Services",
         "Earnings growth broad, rates stable, business investment peaks"),
        ("🌇 Late Cycle (Slowdown)",       "Energy, Materials, Healthcare, Consumer Staples",
         "Inflation rises, margins compress, defensives outperform"),
        ("🌧️  Recession",                  "Consumer Staples, Healthcare, Utilities, Bonds",
         "Revenue falls, dividends valued, capital preservation focus"),
    ]
    cycle_headers = ["Phase", "Favored Sectors", "Key Dynamic"]
    cycle_widths  = [22, 45, 42]
    for ci, h in enumerate(cycle_headers, 1):
        c = ws.cell(row=sr, column=ci, value=h)
        c.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="546E7A")
        c.border = THIN_BORDER
    sr += 1
    for phase, favored, dynamic in cycle_data:
        ws.cell(row=sr, column=1, value=phase).font = Font(bold=True, name="Arial", size=9)
        ws.cell(row=sr, column=2, value=favored).font = Font(name="Arial", size=9)
        ws.cell(row=sr, column=3, value=dynamic).font = Font(name="Arial", size=9, color="546E7A")
        for ci in range(1, 4):
            ws.cell(row=sr, column=ci).border = THIN_BORDER
            ws.cell(row=sr, column=ci).alignment = Alignment(vertical="center", wrap_text=True)
        ws.row_dimensions[sr].height = 18
        sr += 1
    for ci, w in enumerate(cycle_widths, 1):
        # don't override already-set widths for col 1; use max
        existing = ws.column_dimensions[get_column_letter(ci)].width or 0
        ws.column_dimensions[get_column_letter(ci)].width = max(existing, w)

    sr += 2

    # ── Top 3 picks per sector (sorted by Score) ──
    picks_title = ws.cell(row=sr, column=1, value="🏆 Top Picks per Sector (by Score)")
    picks_title.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
    picks_title.fill = PatternFill("solid", fgColor="1A237E")
    ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(main_headers))
    sr += 1

    pick_headers = ["Ticker", "Company", "Price", "PEG", "P/E", "MoS", "ROE", "FCF Yield",
                    "Piotroski", "Rev Growth", "Score"]
    pick_widths_local = [8, 24, 8, 7, 7, 7, 7, 9, 7, 9, 7]

    for row in rows:
        sector_name = row["Sector"]
        sig = row["Signal"]
        sc_fill = BUY_FILL if sig == "BUY" else (HOLD_FILL if sig == "HOLD" else AVOID_FILL)

        # Section header
        sh = ws.cell(row=sr, column=1,
                     value=f"  {sector_name}  [{sig}] Score: {row['Rot. Score']}")
        sh.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        sh.fill = sc_fill
        ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(main_headers))
        sr += 1

        # Score the stocks in this sector
        sector_stocks = row["_stocks"]
        scored = []
        for s in sector_stocks:
            if not _is_common_stock(s): continue
            sc2 = 0
            peg = s.get("peg")
            mos = s.get("mos")
            pio = s.get("piotroski")
            if peg and 0 < peg < 1.0:   sc2 += 15
            elif peg and 0 < peg < 1.5: sc2 += 10
            elif peg and 0 < peg < 2.0: sc2 += 5
            if mos and mos > 0.2:  sc2 += 10
            elif mos and mos > 0:  sc2 += 5
            if pio and pio >= 8:   sc2 += 10
            elif pio and pio >= 6: sc2 += 5
            if s.get("roe") and s.get("roe") > 0.15: sc2 += 5
            if s.get("fcfYield") and s.get("fcfYield") > 0.05: sc2 += 5
            if s.get("revGrowth") and s.get("revGrowth") > 0.1: sc2 += 5
            scored.append((sc2, s))
        scored.sort(key=lambda x: -x[0])
        top3 = scored[:3]

        if top3:
            # mini header
            for ci, h in enumerate(pick_headers, 1):
                c = ws.cell(row=sr, column=ci, value=h)
                c.font = Font(bold=True, name="Arial", size=8, color="FFFFFF")
                c.fill = PatternFill("solid", fgColor="455A64")
                c.border = THIN_BORDER
                c.alignment = Alignment(horizontal="center")
            sr += 1
            for pick_sc, s in top3:
                vals = [s.get("ticker"), s.get("name", "")[:28],
                        s.get("price"), s.get("peg"), s.get("pe"),
                        s.get("mos"), s.get("roe"), s.get("fcfYield"),
                        s.get("piotroski"), s.get("revGrowth"), round(pick_sc, 1)]
                for ci, (h, v) in enumerate(zip(pick_headers, vals), 1):
                    cell = ws.cell(row=sr, column=ci, value=v)
                    cell.border = THIN_BORDER
                    cell.font = Font(name="Arial", size=8)
                    cell.fill = ALT_FILL
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                    if h == "Price" and isinstance(v, float):
                        cell.number_format = "$#,##0.00"
                    elif h in ("MoS", "ROE", "FCF Yield", "Rev Growth") and isinstance(v, float):
                        cell.number_format = "0%"
                    elif h in ("PEG", "P/E") and isinstance(v, float):
                        cell.number_format = "0.1"
                ws.row_dimensions[sr].height = 14
                sr += 1
        sr += 1  # gap between sectors

    print(f"  ✅ Sector Valuations done — {len(rows)} sectors")
    return rows, _etf_returns  # etf_returns reused by build_sector_etf_rotation (no re-fetch)


def build_sector_etf_rotation(wb, stocks, etf_returns: dict, sector_fund_scores: dict = None):
    """Tab 10: ETF Sector Rotation — technical + fundamental signals for ETF rotation trading.
    Uses same ETF price history already fetched by build_sector_valuations (zero new API calls).
    Adds: 52w positioning, MA50/200, RSI-14, momentum direction, combined rotation signal.
    """
    print("\n📊 Building Tab: ETF Sector Rotation...")
    ws = wb.create_sheet("10. ETF Rotation")
    ws.sheet_view.showGridLines = False

    CYCLE_PHASE = {
        "Technology": "Mid/Late", "Financial Services": "Early",
        "Consumer Cyclical": "Early/Mid", "Industrials": "Mid",
        "Basic Materials": "Mid/Late", "Energy": "Late",
        "Healthcare": "Late/Recession", "Consumer Defensive": "Recession",
        "Utilities": "Recession", "Real Estate": "Early/Mid",
        "Communication Services": "Mid",
    }
    # Reverse map: sector → ETF
    _sec2etf = {v: k for k, v in SECTOR_ETFS.items()}

    # ── Aggregate fundamental quality per sector from stocks dict ─────────────
    _fund_scores = sector_fund_scores or {}
    if not _fund_scores:
        _sec_agg = {}
        for t, s in stocks.items():
            if not _is_common_stock(s): continue
            sec = s.get("sector", "Unknown")
            if sec not in _sec_agg:
                _sec_agg[sec] = {"pegs": [], "mos": [], "fcf": [], "roe": [], "rg": []}
            if s.get("peg") and 0 < s["peg"] < 20:  _sec_agg[sec]["pegs"].append(s["peg"])
            if s.get("mos") is not None:             _sec_agg[sec]["mos"].append(s["mos"])
            if s.get("fcfYield") is not None:        _sec_agg[sec]["fcf"].append(s["fcfYield"])
            if s.get("roe") is not None:             _sec_agg[sec]["roe"].append(s["roe"])
            if s.get("revGrowth") is not None:       _sec_agg[sec]["rg"].append(s["revGrowth"])
        def _med(lst): return sorted(lst)[len(lst)//2] if lst else None
        def _avg(lst): return sum(lst)/len(lst) if lst else None
        for sec, agg in _sec_agg.items():
            sc = 0
            peg = _med(agg["pegs"])
            if peg:
                if peg < 1.0: sc += 20
                elif peg < 1.5: sc += 15
                elif peg < 2.0: sc += 10
                elif peg < 3.0: sc += 5
            pct_val = sum(1 for v in agg["mos"] if v and v > 0) / len(agg["mos"]) if agg["mos"] else 0
            sc += min(20, int(pct_val * 20))
            fcf_avg = _avg(agg["fcf"])
            if fcf_avg:
                if fcf_avg > 0.05: sc += 10
                elif fcf_avg > 0.02: sc += 5
            roe_avg = _avg(agg["roe"])
            if roe_avg:
                if roe_avg > 0.15: sc += 10
                elif roe_avg > 0.08: sc += 5
            rg_avg = _avg(agg["rg"])
            if rg_avg:
                if rg_avg > 0.10: sc += 15
                elif rg_avg > 0.05: sc += 8
                elif rg_avg > 0: sc += 3
            _fund_scores[sec] = sc

    # ── Compute ETF rotation score for each sector ─────────────────────────────
    etf_rows = []
    spy_data = etf_returns.get("SPY", {})

    for etf_ticker, sector_name in SECTOR_ETFS.items():
        ed = etf_returns.get(etf_ticker, {})
        if not ed: continue

        sc = 0
        # Technical momentum (40 pts)
        alpha_3m = ed.get("3M_alpha")
        if alpha_3m is not None:
            if alpha_3m > 0.05:   sc += 15
            elif alpha_3m > 0:    sc += 8
            elif alpha_3m < -0.10: sc -= 12
            elif alpha_3m < -0.05: sc -= 5
        vs_ma50  = ed.get("vs_ma50")
        vs_ma200 = ed.get("vs_ma200")
        if vs_ma50 is not None:
            if vs_ma50 > 1.0: sc += 10
            else:             sc -= 8
        if vs_ma200 is not None:
            if vs_ma200 > 1.0: sc += 10
            else:              sc -= 5
        if ed.get("momentum_dir") == "▲ Accel": sc += 5

        # Mean-reversion opportunity (30 pts)
        vs52h = ed.get("vs52H")
        rsi   = ed.get("rsi14")
        if vs52h is not None:
            if vs52h <= 0.65:   sc += 25
            elif vs52h <= 0.75: sc += 18
            elif vs52h <= 0.85: sc += 10
            elif vs52h >= 0.98: sc -= 12
        if rsi is not None:
            if rsi <= 30:  sc += 10
            elif rsi >= 70: sc -= 10

        # Fundamental quality (30 pts)
        fund_sc = _fund_scores.get(sector_name, 0)
        sc += round(fund_sc / 100 * 30)

        # Signal
        if sc >= 65:   sig = "🟢 ROTATE IN"
        elif sc >= 48: sig = "🟡 HOLD"
        elif sc >= 35: sig = "🟠 TAKE PROFITS"
        else:          sig = "🔴 AVOID"

        etf_rows.append({
            "sector": sector_name, "etf": etf_ticker, "signal": sig, "score": sc,
            "price":    ed.get("current"),
            "vs52H":    vs52h,
            "vs52L":    ed.get("vs52L"),
            "1W":       ed.get("1W"),
            "1M":       ed.get("1M"),
            "3M":       ed.get("3M"),
            "6M":       ed.get("6M"),
            "1Y":       ed.get("1Y"),
            "1M_alpha": ed.get("1M_alpha"),
            "3M_alpha": alpha_3m,
            "6M_alpha": ed.get("6M_alpha"),
            "1Y_alpha": ed.get("1Y_alpha"),
            "vs_ma50":  vs_ma50,
            "vs_ma200": vs_ma200,
            "rsi14":    rsi,
            "trend":    ed.get("momentum_dir"),
            "cycle":    CYCLE_PHASE.get(sector_name, "—"),
            "fund_sc":  fund_sc,
        })

    etf_rows.sort(key=lambda x: -x["score"])

    # ── SECTION A: Title ──────────────────────────────────────────────────────
    sr = add_title(ws,
                   "🔄 ETF Sector Rotation — Technical + Fundamental Signals",
                   f"52w positioning · MA50/200 · RSI-14 · Alpha vs SPY · Zero extra API calls — {datetime.date.today()}")

    # SPY benchmark row
    spy_row_r = sr
    sr += 1
    _spy_lbl_cell = ws.cell(row=spy_row_r, column=1, value="📊 SPY Benchmark")
    _spy_lbl_cell.font = Font(bold=True, name="Arial", size=9, color="FFFFFF")
    _spy_lbl_cell.fill = PatternFill("solid", fgColor="263238")
    _spy_data_map = {
        "1W": spy_data.get("1W"), "1M": spy_data.get("1M"), "3M": spy_data.get("3M"),
        "6M": spy_data.get("6M"), "1Y": spy_data.get("1Y"),
        "vs52H": spy_data.get("vs52H"), "vs52L": spy_data.get("vs52L"),
        "RSI": spy_data.get("rsi14"),
        "vs MA50": spy_data.get("vs_ma50"), "vs MA200": spy_data.get("vs_ma200"),
    }
    _spy_col = 2
    for lbl, val in _spy_data_map.items():
        hc = ws.cell(row=spy_row_r - 1 if spy_row_r > 1 else spy_row_r, column=_spy_col)
        dc = ws.cell(row=spy_row_r, column=_spy_col)
        dc.value = val
        dc.font = Font(bold=True, name="Arial", size=9)
        dc.border = THIN_BORDER
        dc.alignment = Alignment(horizontal="center")
        if isinstance(val, float):
            if lbl in ("1W","1M","3M","6M","1Y","vs MA50","vs MA200"):
                dc.number_format = "+0.0%;-0.0%;0%"
                if val > 0: dc.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                elif val < 0: dc.font = Font(bold=True, name="Arial", size=9, color="B71C1C")
            elif lbl in ("vs52H","vs52L"):
                dc.number_format = "0%"
            elif lbl == "RSI":
                dc.number_format = "0.0"
        _spy_col += 1
    # Market regime note
    spy_vs200 = spy_data.get("vs_ma200")
    spy_rsi   = spy_data.get("rsi14")
    if spy_vs200 is not None:
        if spy_vs200 >= 1.05:   regime = "BULL MARKET — SPY above MA200 (+5%)"
        elif spy_vs200 >= 1.0:  regime = "RECOVERY — SPY above MA200 (marginal)"
        elif spy_vs200 >= 0.93: regime = "CORRECTION — SPY below MA200"
        else:                   regime = "BEAR MARKET — SPY deeply below MA200"
        rc = ws.cell(row=spy_row_r, column=_spy_col, value=regime)
        rc.font = Font(bold=True, name="Arial", size=9,
                       color="1B5E20" if spy_vs200 >= 1.0 else "B71C1C")
        rc.alignment = Alignment(horizontal="left")

    # ── SECTION B: Main ETF Dashboard table ───────────────────────────────────
    sr += 1  # blank row
    hdr_row = sr
    _hdr_cols = [
        "Sector", "ETF", "Signal", "ETF Score",
        "Price", "52w vs High", "52w vs Low",
        "1W", "1M", "3M", "6M", "1Y",
        "1M α", "3M α", "6M α", "1Y α",
        "vs MA50", "vs MA200", "RSI-14", "Trend", "Cycle",
    ]
    _hdr_widths = [22, 6, 14, 9, 8, 11, 10, 7, 8, 8, 8, 8, 8, 8, 8, 8, 9, 10, 7, 10, 14]

    for ci, h in enumerate(_hdr_cols, 1):
        c = ws.cell(row=hdr_row, column=ci, value=h)
        c.font  = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        c.fill  = PatternFill("solid", fgColor="263238")
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = THIN_BORDER
    ws.row_dimensions[hdr_row].height = 28
    for ci, w in enumerate(_hdr_widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.freeze_panes = f"A{hdr_row + 1}"
    sr += 1

    # Row fill by signal
    _SIG_FILL = {
        "🟢 ROTATE IN":    ("E8F5E9", "1B5E20"),   # light green / dark green text
        "🟡 HOLD":         ("FFFDE7", "F57F17"),   # light yellow / amber text
        "🟠 TAKE PROFITS": ("FFF3E0", "E65100"),   # light orange / orange text
        "🔴 AVOID":        ("FFEBEE", "B71C1C"),   # light red / red text
    }

    for row_data in etf_rows:
        sig     = row_data["signal"]
        fill_clr, txt_clr = _SIG_FILL.get(sig, ("FFFFFF", "000000"))
        row_fill = PatternFill("solid", fgColor=fill_clr)

        for ci, h in enumerate(_hdr_cols, 1):
            key_map = {
                "Sector": "sector", "ETF": "etf", "Signal": "signal", "ETF Score": "score",
                "Price": "price", "52w vs High": "vs52H", "52w vs Low": "vs52L",
                "1W": "1W", "1M": "1M", "3M": "3M", "6M": "6M", "1Y": "1Y",
                "1M α": "1M_alpha", "3M α": "3M_alpha", "6M α": "6M_alpha", "1Y α": "1Y_alpha",
                "vs MA50": "vs_ma50", "vs MA200": "vs_ma200",
                "RSI-14": "rsi14", "Trend": "trend", "Cycle": "cycle",
            }
            v = row_data.get(key_map.get(h, ""))
            cell = ws.cell(row=sr, column=ci, value=v)
            cell.fill   = row_fill
            cell.border = THIN_BORDER
            cell.font   = Font(name="Arial", size=9)
            cell.alignment = Alignment(
                horizontal="left" if h in ("Sector", "Signal", "Trend", "Cycle") else "center",
                vertical="center")

            # Column-specific formatting
            if h == "Signal":
                cell.font = Font(bold=True, name="Arial", size=9, color=txt_clr)
            elif h == "ETF Score":
                cell.font = Font(bold=True, name="Arial", size=9,
                                 color="1B5E20" if (v or 0) >= 55 else ("B71C1C" if (v or 0) < 35 else "000000"))
            elif h == "Price" and isinstance(v, float):
                cell.number_format = "$#,##0.00"
            elif h in ("1W","1M","3M","6M","1Y","1M α","3M α","6M α","1Y α") and isinstance(v, float):
                cell.number_format = "+0.0%;-0.0%;0%"
                color = "1B5E20" if v > 0.005 else ("B71C1C" if v < -0.005 else "555555")
                cell.font = Font(name="Arial", size=9, color=color)
            elif h in ("52w vs High","52w vs Low","vs MA50","vs MA200") and isinstance(v, float):
                cell.number_format = "0%"
                # 52w vs High: low = green (beaten down = opportunity)
                if h == "52w vs High":
                    if v <= 0.70:   cell.fill = PatternFill("solid", fgColor="C8E6C9"); cell.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                    elif v >= 0.95: cell.fill = PatternFill("solid", fgColor="FFCDD2"); cell.font = Font(name="Arial", size=9, color="B71C1C")
                elif h == "52w vs Low":
                    if v <= 1.15:   cell.fill = PatternFill("solid", fgColor="C8E6C9")
                    elif v >= 1.75: cell.fill = PatternFill("solid", fgColor="FFF9C4")
                elif h in ("vs MA50","vs MA200"):
                    if v >= 1.02:   cell.font = Font(name="Arial", size=9, color="1B5E20")
                    elif v <= 0.97: cell.font = Font(name="Arial", size=9, color="B71C1C")
            elif h == "RSI-14" and isinstance(v, float):
                cell.number_format = "0.0"
                if v <= 30:   cell.fill = PatternFill("solid", fgColor="C8E6C9"); cell.font = Font(bold=True, name="Arial", size=9, color="1B5E20")
                elif v >= 70: cell.fill = PatternFill("solid", fgColor="FFCDD2"); cell.font = Font(bold=True, name="Arial", size=9, color="B71C1C")
            elif h == "Trend" and v:
                cell.font = Font(name="Arial", size=9, color="1B5E20" if "Accel" in str(v) else "B71C1C")

        ws.row_dimensions[sr].height = 16
        sr += 1

    # ── SECTION C: Rotation Opportunity Matrix ─────────────────────────────────
    sr += 1
    mat_hdr = ws.cell(row=sr, column=1, value="📊 Rotation Opportunity Matrix — Best Risk/Reward by Quadrant")
    mat_hdr.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
    mat_hdr.fill = PatternFill("solid", fgColor="37474F")
    ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=8)
    ws.row_dimensions[sr].height = 18
    sr += 1

    # Sub-header
    _mat_hdrs = ["Sector", "ETF", "Opportunity Type", "ETF Score", "52w vs High", "Fund. Score", "RSI", "Entry Rationale"]
    _mat_wids = [22, 6, 18, 9, 11, 10, 7, 45]
    for ci, (h, w) in enumerate(zip(_mat_hdrs, _mat_wids), 1):
        c = ws.cell(row=sr, column=ci, value=h)
        c.font  = Font(bold=True, name="Arial", size=9, color="FFFFFF")
        c.fill  = PatternFill("solid", fgColor="455A64")
        c.alignment = Alignment(horizontal="center")
        c.border = THIN_BORDER
        ws.column_dimensions[get_column_letter(ci)].width = w
    sr += 1

    # Classify each sector ETF into opportunity quadrants
    _opp_rows = []
    for row_data in etf_rows:
        vs52h   = row_data.get("vs52H")
        fund_sc = row_data.get("fund_sc", 0)
        a3m     = row_data.get("3M_alpha")
        vs200   = row_data.get("vs_ma200")
        rsi     = row_data.get("rsi14")
        sc      = row_data["score"]

        if vs52h is not None and vs52h <= 0.75 and fund_sc >= 40:
            opp_type = "🏆 Turnaround"
            rationale = f"ETF {round(vs52h*100)}% of 52w high — quality sector beaten down, fundamentals intact"
        elif a3m is not None and a3m > 0.03 and vs200 is not None and vs200 >= 1.0:
            top3_alpha = sorted(etf_rows, key=lambda x: -(x.get("3M_alpha") or -99))[:3]
            if row_data in top3_alpha:
                opp_type = "📈 Momentum"
                rationale = f"Top-3 3M alpha vs SPY ({round((a3m or 0)*100,1)}%), above MA200 — ride the trend"
            else:
                opp_type = "🟡 Hold Trend"
                rationale = "Above MA200, positive momentum but not top-ranked"
        elif vs52h is not None and vs52h <= 0.75 and fund_sc < 35:
            opp_type = "⚠️ Value Trap Risk"
            rationale = "Cheap ETF but fundamentals weak — caution, could be structural decline"
        elif vs52h is not None and vs52h >= 0.95 and fund_sc >= 45:
            opp_type = "💰 Extended Quality"
            rationale = f"Strong fundamentals but ETF near 52w high — consider trimming or wait for pullback"
        elif sc >= 48:
            opp_type = "🟡 Hold"
            rationale = "Mixed signals — no strong edge either direction"
        else:
            opp_type = "🔴 Avoid / Rotate Out"
            rationale = "Weak technicals + weak fundamentals — better opportunities elsewhere"
        _opp_rows.append((row_data, opp_type, rationale))

    # Sort: Turnaround first, then Momentum, then Hold, then Avoid
    _opp_priority = {"🏆 Turnaround": 0, "📈 Momentum": 1, "🟡 Hold Trend": 2,
                     "💰 Extended Quality": 3, "🟡 Hold": 4, "⚠️ Value Trap Risk": 5, "🔴 Avoid / Rotate Out": 6}
    _opp_rows.sort(key=lambda x: _opp_priority.get(x[1], 9))

    for row_data, opp_type, rationale in _opp_rows:
        _opp_fill_map = {
            "🏆 Turnaround":        "C8E6C9", "📈 Momentum": "E8F5E9",
            "🟡 Hold Trend":        "FFFDE7", "🟡 Hold": "FFFDE7",
            "💰 Extended Quality":  "FFF3E0", "⚠️ Value Trap Risk": "FFF9C4",
            "🔴 Avoid / Rotate Out":"FFEBEE",
        }
        _fill = PatternFill("solid", fgColor=_opp_fill_map.get(opp_type, "FFFFFF"))
        vals = [
            row_data["sector"], row_data["etf"], opp_type, row_data["score"],
            row_data.get("vs52H"), row_data.get("fund_sc"),
            row_data.get("rsi14"), rationale,
        ]
        for ci, v in enumerate(vals, 1):
            c = ws.cell(row=sr, column=ci, value=v)
            c.fill   = _fill
            c.border = THIN_BORDER
            c.font   = Font(name="Arial", size=9)
            c.alignment = Alignment(
                horizontal="left" if ci in (1, 3, 8) else "center",
                vertical="center", wrap_text=(ci == 8))
            if ci == 5 and isinstance(v, float): c.number_format = "0%"
            elif ci == 7 and isinstance(v, float): c.number_format = "0.0"
            elif ci == 3:
                c.font = Font(bold=True, name="Arial", size=9)
        ws.row_dimensions[sr].height = 16
        sr += 1

    # ── Footer: legend ──────────────────────────────────────────────────────────
    sr += 1
    _legend = [
        "📖  How to use:  ROTATE IN = ETF beaten down + fundamentals support recovery. "
        "TAKE PROFITS = near 52w high, trim position.  AVOID = weak technicals + fundamentals.  "
        "RSI ≤30 = oversold (buy signal).  RSI ≥70 = overbought (sell signal).  "
        "vs MA200 <1.0 = long-term downtrend (caution).  "
        "52w vs High ≤70% = deep discount — highest mean-reversion potential."
    ]
    lc = ws.cell(row=sr, column=1, value=_legend[0])
    lc.font = Font(italic=True, name="Arial", size=8, color="546E7A")
    lc.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(_hdr_cols))
    ws.row_dimensions[sr].height = 36

    print(f"  ✅ ETF Rotation tab done — {len(etf_rows)} sector ETFs")
    return etf_rows


def build_insider_tab(wb, stocks, insider_data):
    """Tab 10: Insider Buying — cluster buys + individual large purchases."""
    print("\n📊 Building Tab: Insider Buying...")
    ws = wb.create_sheet("10. Insider Buying")
    ws.sheet_view.showGridLines = False

    if not insider_data:
        sr = add_title(ws, "🏦 Insider Buying — Recent Open Market Purchases",
                       "No insider data available.")
        ws.cell(row=sr, column=1,
                value="No insider trades loaded. Check FMP plan or openinsider.com connectivity.").font = \
            Font(italic=True, name="Arial", size=10, color="888888")
        print("  ✅ Insider Buying tab done — 0 trades")
        return

    _src_map = {"openinsider": "openinsider.com", "finviz": "finviz.com"}
    source = _src_map.get(insider_data[0].get("_source", ""), "FMP")

    def _trade_value(tr):
        v = tr.get("_value", 0)
        if not v: v = abs((tr.get("securitiesTransacted") or 0) * (tr.get("price") or 0))
        return v

    # ── Title mapping for importance scoring ──
    TITLE_SCORE = {
        "ceo": 10, "chief executive": 10, "president": 8, "coo": 7,
        "cfo": 7, "chief financial": 7, "chairman": 8, "cto": 6,
        "director": 5, "vp": 4, "vice president": 4, "officer": 3,
    }
    def _title_score(title):
        t = title.lower()
        for k, v in TITLE_SCORE.items():
            if k in t: return v
        return 2

    # ── Group by ticker ──
    ticker_trades = defaultdict(list)
    for trade in insider_data:
        t = trade.get("symbol", "")
        ticker_trades[t].append(trade)

    # ── Score each company ──
    company_rows = []
    for t, trades in ticker_trades.items():
        s = stocks.get(t, {})
        total_val  = sum(_trade_value(tr) for tr in trades)
        latest     = max((tr.get("transactionDate") or "") for tr in trades)
        n_insiders = len({tr.get("reportingName", "") for tr in trades})
        n_buys     = len(trades)
        max_title  = max((_title_score(tr.get("typeOfOwner") or tr.get("reportingCik","")) for tr in trades), default=2)

        # Conviction score
        score = 0
        score += min(n_buys, 5) * 8           # up to 40 pts for 5+ buys
        score += min(n_insiders - 1, 4) * 10   # cluster bonus: +10 per additional insider
        score += max_title * 3                  # senior insider bonus
        if total_val > 1_000_000: score += 20
        elif total_val > 500_000: score += 12
        elif total_val > 100_000: score += 5
        if s.get("piotroski") and s.get("piotroski") >= 7: score += 5
        if s.get("mos") and s.get("mos") > 0.1: score += 5

        # Cluster label
        if n_insiders >= 3:   cluster = "CLUSTER"
        elif n_insiders == 2: cluster = "DOUBLE"
        else:                 cluster = "SINGLE"

        names = "; ".join(
            f"{tr.get('reportingName','?')[:18]} ({(tr.get('typeOfOwner') or '')[:8]})"
            for tr in sorted(trades, key=lambda x: -_trade_value(x))[:3]
        )

        row = format_stock_row(s) if s else {"Ticker": t, "Company": t}
        row["# Buys"]   = n_buys
        row["# Insiders"] = n_insiders
        row["Cluster"]  = cluster
        row["Total $M"] = round(total_val / 1_000_000, 3)
        row["Latest"]   = latest
        row["Who Bought"] = names
        row["Score"]    = score
        company_rows.append(row)

    company_rows.sort(key=lambda x: -x["Score"])

    sr = add_title(ws,
                   "🏦 Insider Buying — Recent Open Market Purchases",
                   f"Source: {source}. {len(insider_data)} total purchases across "
                   f"{len(company_rows)} companies. "
                   f"Cluster = 3+ insiders buying same stock. {datetime.date.today()}")

    # ── Cluster buys section ──
    clusters = [r for r in company_rows if r["Cluster"] in ("CLUSTER", "DOUBLE")]
    singles  = [r for r in company_rows if r["Cluster"] == "SINGLE"]

    headers = ["Score", "Cluster", "Ticker", "Company", "Sector", "Price",
               "PEG", "P/E", "MoS", "Piotroski", "# Buys", "# Insiders",
               "Total $M", "Latest", "Who Bought"]
    widths = [7, 8, 8, 22, 15, 8, 6, 7, 7, 7, 6, 8, 9, 10, 45]

    if clusters:
        ch = ws.cell(row=sr, column=1,
                     value=f"🔥 CLUSTER BUYS — {len(clusters)} companies with 2+ insiders buying")
        ch.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
        ch.fill = PatternFill("solid", fgColor="B71C1C")
        ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(headers))
        ws.row_dimensions[sr].height = 18
        sr += 1
        sr = write_table(ws, clusters[:30], headers, sr, header_color="C62828", widths=widths)
        sr += 2

    sh = ws.cell(row=sr, column=1,
                 value=f"📋 ALL INSIDER PURCHASES — top {min(len(singles), TOP_N)} by conviction score")
    sh.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
    sh.fill = PatternFill("solid", fgColor="1B5E20")
    ws.merge_cells(start_row=sr, start_column=1, end_row=sr, end_column=len(headers))
    ws.row_dimensions[sr].height = 18
    sr += 1
    write_table(ws, singles[:TOP_N], headers, sr, header_color="2E7D32", widths=widths)

    # Extra formatting: color Cluster column
    # (write_table already handled; add cluster color override here)
    for row in ws.iter_rows(min_row=sr, max_row=ws.max_row):
        for cell in row:
            if cell.value == "CLUSTER":
                cell.fill = PatternFill("solid", fgColor="B71C1C")
                cell.font = Font(bold=True, name="Arial", size=8, color="FFFFFF")
            elif cell.value == "DOUBLE":
                cell.fill = PatternFill("solid", fgColor="E57373")
                cell.font = Font(bold=True, name="Arial", size=8, color="FFFFFF")

    print(f"  ✅ Insider Buying tab done — {len(clusters)} cluster + {len(singles)} single buys "
          f"({len(insider_data)} trades total)")


# ─────────────────────────────────────────────
# HTML DASHBOARD
# ─────────────────────────────────────────────

def build_html_report(stocks, iv_rows, stalwarts, fast_growers, turnarounds,
                      slow_growers, cyclicals, asset_plays, quality_compounders,
                      sector_rows=None, etf_rows=None, ai=None, macro=None,
                      portfolio=None, fmp_call_count=0) -> str:
    """Generate a self-contained mobile-responsive HTML dashboard.
    Reads the same data structures that feed the Excel — no extra computation.
    Returns the full HTML string.
    """
    today = datetime.date.today().strftime("%Y-%m-%d")
    now   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── CSS ───────────────────────────────────────────────────────────────
    css = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0f1117; color: #e0e0e0; font-size: 14px; }
a { color: #90caf9; }
h1 { font-size: 1.1rem; font-weight: 700; color: #fff; }
h2 { font-size: .85rem; font-weight: 700; color: #90caf9;
     text-transform: uppercase; letter-spacing: .05em; }
.header { background: #1a237e; padding: 12px 16px;
          display: flex; justify-content: space-between; align-items: center; }
.header small { color: #9fa8da; font-size: .75rem; }
.nav { background: #1e1e2e; display: flex; flex-wrap: wrap; gap: 2px;
       padding: 6px 8px; position: sticky; top: 0; z-index: 99;
       border-bottom: 1px solid #333; }
.nav button { background: #2a2a3e; color: #aaa; border: none; border-radius: 4px;
              padding: 5px 10px; font-size: .75rem; cursor: pointer; }
.nav button.active, .nav button:hover { background: #1a237e; color: #fff; }
section { padding: 12px 16px; display: none; }
section.active { display: block; }
.section-title { margin: 0 0 10px; padding: 6px 10px; background: #1a237e;
                 border-radius: 4px; font-size: .8rem; font-weight: 700;
                 color: #fff; text-transform: uppercase; }
.tbl-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
table { border-collapse: collapse; width: 100%; font-size: .78rem;
        white-space: nowrap; }
th { background: #1a237e; color: #fff; padding: 6px 8px;
     text-align: left; cursor: pointer; user-select: none; }
th:hover { background: #283593; }
td { padding: 5px 8px; border-bottom: 1px solid #2a2a2a; }
tr:hover td { background: #1a1a2e !important; }
tr.alt td { background: #161622; }
.g  { background: #1b3a1e !important; color: #a5d6a7; }
.a  { background: #3e2a00 !important; color: #ffe082; }
.r  { background: #3e1111 !important; color: #ef9a9a; }
.badge { display: inline-block; border-radius: 3px; padding: 2px 6px;
         font-size: .7rem; font-weight: 700; }
.badge-high   { background: #1b5e20; color: #a5d6a7; }
.badge-med    { background: #0d47a1; color: #90caf9; }
.badge-actnow { background: #b71c1c; color: #fff; }
.badge-weeks  { background: #e65100; color: #fff; }
.badge-months { background: #1565c0; color: #fff; }
.badge-watch  { background: #37474f; color: #cfd8dc; }
.badge-bull   { background: #1b5e20; color: #a5d6a7; }
.badge-bear   { background: #b71c1c; color: #ef9a9a; }
.badge-neut   { background: #006064; color: #b2ebf2; }
.badge-caut   { background: #e65100; color: #ffe0b2; }
.badge-buy    { background: #1b5e20; color: #a5d6a7; }
.badge-hold   { background: #006064; color: #b2ebf2; }
.badge-avoid  { background: #b71c1c; color: #ef9a9a; }
.badge-elev   { background: #e65100; color: #ffe0b2; }
.macro-grid { display: grid;
              grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
              gap: 8px; margin: 10px 0; }
.macro-tile { background: #1e1e2e; border-radius: 6px; padding: 10px;
              text-align: center; }
.macro-tile .lbl { font-size: .65rem; color: #9e9e9e; text-transform: uppercase; }
.macro-tile .val { font-size: 1.1rem; font-weight: 700; color: #fff;
                   margin: 4px 0 2px; }
.macro-tile .sig { font-size: .65rem; font-weight: 700; border-radius: 3px;
                   padding: 1px 5px; display: inline-block; }
.sig-g { background: #1b3a1e; color: #a5d6a7; }
.sig-a { background: #3e2a00; color: #ffe082; }
.sig-r { background: #3e1111; color: #ef9a9a; }
.pick-card { background: #1e1e2e; border-radius: 6px; padding: 12px;
             margin-bottom: 10px; border-left: 3px solid #1a237e; }
.pick-card .pick-hdr { display: flex; align-items: center; gap: 8px;
                       flex-wrap: wrap; margin-bottom: 6px; }
.pick-ticker { font-size: 1.1rem; font-weight: 700; color: #fff; }
.pick-co     { color: #9e9e9e; font-size: .82rem; }
.pick-hl     { color: #e0e0e0; margin: 6px 0 4px; font-style: italic; }
.pick-story  { color: #bdbdbd; font-size: .8rem; line-height: 1.5; }
.pick-meta   { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 8px;
               font-size: .75rem; }
.pick-meta span { color: #9e9e9e; }
.pick-meta b   { color: #e0e0e0; }
.interp { background: #1a1a2e; padding: 8px 12px; border-radius: 4px;
          font-size: .78rem; color: #9e9e9e; margin-top: 8px; }
.synopsis { background: #1a237e22; border-left: 3px solid #1a237e;
            padding: 10px 14px; border-radius: 0 4px 4px 0;
            margin-bottom: 12px; font-style: italic; color: #c5cae9;
            font-size: .85rem; line-height: 1.6; }
.outlook-row { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
.outlook-tile { border-radius: 6px; padding: 10px 14px; text-align: center;
                min-width: 90px; }
.outlook-tile .o-lbl { font-size: .65rem; text-transform: uppercase; opacity: .8; }
.outlook-tile .o-val { font-size: .95rem; font-weight: 700; margin-top: 3px; }
.agent-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
              gap: 8px; margin: 10px 0 16px; }
.agent-card { background: #1e1e2e; border-radius: 6px; padding: 10px 12px;
              border-left: 3px solid #1a237e; }
.agent-card .ag-name { font-size: .78rem; font-weight: 700; color: #fff; margin-bottom: 6px; }
.agent-card .ag-stat { font-size: .72rem; color: #9e9e9e; line-height: 1.7; }
.agent-card .ag-stat b { color: #e0e0e0; }
.agent-card.win { border-left-color: #1b5e20; }
.agent-card.loss { border-left-color: #b71c1c; }
"""

    # ── JS (tab switch + table sort) ──────────────────────────────────────
    js = """
function showTab(id, btn) {
  document.querySelectorAll('section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.nav button').forEach(b => b.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  if (btn) btn.classList.add('active');
}
function sortTable(th) {
  var table = th.closest('table');
  var tbody = table.querySelector('tbody');
  var rows  = Array.from(tbody.querySelectorAll('tr'));
  var col   = Array.from(th.parentNode.children).indexOf(th);
  var asc   = th.dataset.asc !== 'true';
  var dateRe = /^\\d{4}-\\d{2}-\\d{2}$/;
  rows.sort(function(a, b) {
    var av = a.children[col] ? a.children[col].innerText.replace(/[%$,]/g,'').trim() : '';
    var bv = b.children[col] ? b.children[col].innerText.replace(/[%$,]/g,'').trim() : '';
    if (dateRe.test(av) && dateRe.test(bv)) return asc ? av.localeCompare(bv) : bv.localeCompare(av);
    var an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });
  rows.forEach(r => tbody.appendChild(r));
  th.dataset.asc = asc ? 'true' : 'false';
}
document.querySelectorAll('th').forEach(th => th.addEventListener('click', function(){sortTable(this);}));
"""

    # ── Helpers ───────────────────────────────────────────────────────────
    def _pct(v, decimals=1):
        if v is None: return "—"
        return f"{v*100:.{decimals}f}%"

    def _num(v, decimals=1):
        if v is None: return "—"
        return f"{v:.{decimals}f}"

    def _money(v):
        if v is None: return "—"
        return f"${v:.2f}"

    def _cell(val, raw, cls=""):
        """Format a table cell with optional color class."""
        display = "—" if val == "—" or val is None else val
        if cls:
            return f'<td class="{cls}">{display}</td>'
        return f"<td>{display}</td>"

    def _peg_cls(v):
        if v is None: return ""
        if v < 1.0:  return "g"
        if v < 1.5:  return "a"
        if v > 2.5:  return "r"
        return ""

    def _roic_cls(v):
        if v is None: return ""
        if v > 0.20: return "g"
        if v > 0.10: return "a"
        return "r"

    def _roe_cls(v):
        if v is None: return ""
        if v > 0.15: return "g"
        if v > 0.08: return "a"
        return "r"

    def _mos_cls(v):
        if v is None: return ""
        if v > 0.30: return "g"
        if v > 0.10: return "a"
        if v < 0:    return "r"
        return ""

    def _sig_cls(sig):
        s = (sig or "").upper()
        if s in ("CALM", "NORMAL", "STEEP", "NEAR_TARGET", "LOW",
                 "HEALTHY", "TIGHT", "DOVISH"):
            return "sig-g"
        if s in ("CAUTION", "FLAT", "ABOVE_TARGET", "ELEVATED",
                 "SOFTENING", "NEUTRAL"):
            return "sig-a"
        if s in ("PANIC", "FEAR", "INVERTED", "HOT", "HIGH",
                 "WEAK", "HAWKISH"):
            return "sig-r"
        return "sig-a"

    def _bias_badge(v):
        v = (v or "NEUTRAL").upper()
        cls = {"BULLISH": "badge-bull", "BEARISH": "badge-bear",
               "CAUTIOUS": "badge-caut"}.get(v, "badge-neut")
        return f'<span class="badge {cls}">{v}</span>'

    def _crash_badge(v):
        v = (v or "ELEVATED").upper()
        cls = {"LOW": "badge-buy", "HIGH": "badge-bear"}.get(v, "badge-elev")
        return f'<span class="badge {cls}">{v}</span>'

    def _signal_badge(v):
        v = (v or "HOLD").upper()
        cls = {"BUY": "badge-buy", "AVOID": "badge-avoid"}.get(v, "badge-hold")
        return f'<span class="badge {cls}">{v}</span>'

    def _urgency_badge(u):
        u = (u or "WATCH").upper()
        cls = {"ACT NOW": "badge-actnow", "WITHIN WEEKS": "badge-weeks",
               "WITHIN MONTHS": "badge-months"}.get(u, "badge-watch")
        return f'<span class="badge {cls}">{u}</span>'

    def _conv_badge(c):
        c = (c or "MEDIUM").upper()
        return f'<span class="badge {"badge-high" if c=="HIGH" else "badge-med"}">{c}</span>'

    def _strategy_table(rows, cols, title_id, title_label, description=""):
        """Render a strategy tab as a sortable table."""
        if not rows:
            return f'<section id="{title_id}"><p style="color:#666">No data</p></section>'
        desc_html = (f'<p style="background:#1a1a2e;padding:8px 12px;border-radius:4px;'
                     f'font-size:.75rem;color:#9e9e9e;margin-bottom:10px;line-height:1.6">'
                     f'<b style="color:#90caf9">Filter: </b>{description}</p>') if description else ""
        hdr_html = "".join(f"<th>{c}</th>" for c in cols)
        body_rows = []
        for i, r in enumerate(rows):
            alt = ' class="alt"' if i % 2 == 0 else ''
            cells = []
            for c in cols:
                v = r.get(c)
                # Apply color logic per column
                if c in ("PEG", "Fwd PEG"):
                    cells.append(_cell(_num(v), v, _peg_cls(v)))
                elif c == "ROIC":
                    cells.append(_cell(_pct(v), v, _roic_cls(v)))
                elif c in ("ROE", "ROE%"):
                    cells.append(_cell(_pct(v), v, _roe_cls(v)))
                elif c in ("MoS", "MoS%"):
                    cells.append(_cell(_pct(v), v, _mos_cls(v)))
                elif c == "Price":
                    cells.append(_cell(_money(v), v))
                elif c in ("FCF Yield", "Rev Growth", "EPS Growth", "Rev Growth 5Y",
                           "EPS Growth 5Y", "Div Yield"):
                    cells.append(_cell(_pct(v), v))
                elif c == "Conviction":
                    cells.append(f"<td>{_conv_badge(v)}</td>")
                elif c == "Urgency":
                    cells.append(f"<td>{_urgency_badge(v)}</td>")
                elif c in ("P/E", "P/B", "P/FCF", "EV/EBITDA", "Fwd P/E", "Score", "Rank",
                           "Piotroski", "MktCap ($B)", "Beta"):
                    cells.append(_cell(_num(v, 1) if isinstance(v, float) else (str(v) if v is not None else "—"), v))
                else:
                    cells.append(f"<td>{v if v is not None else '—'}</td>")
            body_rows.append(f"<tr{alt}>{''.join(cells)}</tr>")
        return f"""
<section id="{title_id}">
  <div class="section-title">{title_label}</div>
  {desc_html}
  <div class="tbl-wrap">
    <table>
      <thead><tr>{hdr_html}</tr></thead>
      <tbody>{"".join(body_rows)}</tbody>
    </table>
  </div>
</section>"""

    # ── NAV TABS ──────────────────────────────────────────────────────────
    tabs = [
        ("ai",       "🤖 AI Picks"),
        ("macro",    "🌍 Macro"),
        ("iv",       "📊 IV Discount"),
        ("stalwart", "🏛 Stalwarts"),
        ("fastg",    "🚀 Fast Growers"),
        ("slowg",    "🐢 Slow Growers"),
        ("cycl",     "🔄 Cyclicals"),
        ("turn",     "🔁 Turnarounds"),
        ("asset",    "🏗 Asset Plays"),
        ("qual",     "💎 Quality"),
        ("sector",   "🗺 Sectors"),
        ("etf",      "📡 ETF Rotation"),
        ("perf",     "📈 Performance"),
    ]
    nav_html = "\n".join(
        f'<button onclick="showTab(\'{tid}\', this)" class="{"active" if i==0 else ""}">{label}</button>'
        for i, (tid, label) in enumerate(tabs)
    )

    # ── MACRO SECTION ─────────────────────────────────────────────────────
    def _macro_section():
        if not macro:
            return '<section id="macro"><p style="color:#666">Macro data unavailable</p></section>'
        mc = macro

        def tile(lbl, val_str, sig):
            sc = _sig_cls(sig)
            return f'''<div class="macro-tile">
  <div class="lbl">{lbl}</div>
  <div class="val">{val_str}</div>
  <span class="sig {sc}">{sig}</span>
</div>'''

        def _dgs2_sig(v):
            if v is None: return "UNKNOWN"
            return "HIGH" if v>5 else "ELEVATED" if v>4 else "NORMAL" if v>2 else "LOW"
        def _ff_sig(v):
            if v is None: return "UNKNOWN"
            return "HAWKISH" if v>4.5 else "ELEVATED" if v>3.5 else "NORMAL" if v>2 else "DOVISH"

        tiles_html = "".join([
            tile("10Y YIELD", f"{mc.get('dgs10','—')}%", mc.get('rate_signal','?')),
            tile("2Y YIELD",  f"{mc.get('dgs2','—')}%",  _dgs2_sig(mc.get('dgs2'))),
            tile("YIELD CURVE",
                 (f"+{mc['yield_curve']}%" if mc.get('yield_curve',0)>=0
                  else f"{mc.get('yield_curve','—')}%") if mc.get('yield_curve') is not None else "—",
                 mc.get('curve_signal','?')),
            tile("VIX",       str(mc.get('vix','—')),    mc.get('vix_signal','?')),
            tile("FED FUNDS", f"{mc.get('fedfunds','—')}%", _ff_sig(mc.get('fedfunds'))),
            tile("CPI YoY",   f"{mc.get('cpi_yoy','—')}%", mc.get('inflation_signal','?')),
            tile("UNEMPLOYMT",f"{mc.get('unrate','—')}%",  mc.get('labor_signal','?')),
        ])

        # Interpretation
        parts = []
        vc = mc.get("yield_curve")
        cs = mc.get("curve_signal","")
        if vc is not None:
            if cs == "INVERTED":
                parts.append(f"⚠️ Yield curve INVERTED ({vc:+.2f}%) — historically precedes recession 6-18mo")
            elif cs == "FLAT":
                parts.append(f"⚠️ Yield curve flat ({vc:+.2f}%) — slowdown risk")
            else:
                parts.append(f"✅ Yield curve {cs.lower()} ({vc:+.2f}%)")
        if mc.get("vix"): parts.append(f"VIX {mc['vix']} ({mc.get('vix_signal','?').lower()})")
        if mc.get("cpi_yoy"): parts.append(f"CPI {mc['cpi_yoy']}% YoY")
        if mc.get("fedfunds"): parts.append(f"Fed Funds {mc['fedfunds']}%")
        interp = "  ·  ".join(parts)

        # AI macro interpretation
        ai_md = (ai or {}).get("macro_dashboard", {})
        ai_rows = ""
        if ai_md:
            rr = (ai_md.get("recession_risk") or "").split()[0].upper()
            fp = (ai_md.get("fed_policy") or "NEUTRAL").upper()
            re = ai_md.get("rate_environment", "")
            rr_cls = {"LOW":"badge-buy","HIGH":"badge-bear"}.get(rr,"badge-elev")
            fp_cls = {"DOVISH":"badge-buy","HAWKISH":"badge-bear"}.get(fp,"badge-neut")
            ai_rows = f"""
<div style="margin-top:12px">
  <span style="font-size:.75rem;color:#9e9e9e">AI INTERPRETATION &nbsp;</span>
  <span class="badge {rr_cls}">RECESSION RISK: {rr}</span> &nbsp;
  <span class="badge {fp_cls}">FED POLICY: {fp}</span>
  {f'<p class="interp" style="margin-top:8px">{re}</p>' if re else ''}
</div>"""

        # AI market outlook block (from ai dict in outer scope)
        _mo = (ai or {}).get("market_outlook", {})
        _outlook_block = ""
        if _mo:
            _nt  = _mo.get("near_term_bias", "NEUTRAL")
            _lt  = _mo.get("long_term_bias",  "NEUTRAL")
            _cr  = _mo.get("crash_risk",      "ELEVATED")
            _rat = _mo.get("rationale", "")
            _outlook_block = f"""
<div style="margin-bottom:14px">
  <h2 style="margin-bottom:8px;font-size:.75rem">AI MARKET OUTLOOK</h2>
  <div class="outlook-row">
    <div class="outlook-tile" style="background:#1a237e22">
      <div class="o-lbl">NEAR-TERM</div>
      <div class="o-val">{_bias_badge(_nt)}</div>
    </div>
    <div class="outlook-tile" style="background:#1a237e22">
      <div class="o-lbl">LONG-TERM</div>
      <div class="o-val">{_bias_badge(_lt)}</div>
    </div>
    <div class="outlook-tile" style="background:#1a237e22">
      <div class="o-lbl">CRASH RISK</div>
      <div class="o-val">{_crash_badge(_cr)}</div>
    </div>
  </div>
  {f'<p class="interp" style="margin-top:6px">{_rat}</p>' if _rat else ''}
</div>"""

        # AI synopsis block (market overview text)
        _synopsis = (ai or {}).get("synopsis", "")
        _synopsis_block = (f'<div style="margin-bottom:14px">'
                           f'<h2 style="margin-bottom:6px;font-size:.75rem">AI MARKET SYNOPSIS</h2>'
                           f'<div class="synopsis">{_synopsis}</div>'
                           f'</div>') if _synopsis else ""

        # Geopolitical & macro context block
        _macro_ctx_text = (ai or {}).get("macro_context", "")
        _geo_block = (
            f'<div style="margin-bottom:14px;background:#f3e5f5;border-left:4px solid #7b1fa2;'
            f'border-radius:4px;padding:10px 14px">'
            f'<h2 style="margin-bottom:6px;font-size:.75rem;color:#4a148c">'
            f'🌍 GEOPOLITICAL &amp; MACRO CONTEXT</h2>'
            f'<p style="color:#4a148c;font-style:italic;margin:0;font-size:.82rem;line-height:1.6">'
            f'{_macro_ctx_text}</p>'
            f'</div>'
        ) if _macro_ctx_text else ""

        return f"""
<section id="macro">
  <div class="section-title">🌍 Macro Dashboard — FRED data as of {mc.get('as_of','?')}</div>
  {_outlook_block}
  {_synopsis_block}
  {_geo_block}
  <h2 style="margin-bottom:8px;margin-top:4px;font-size:.75rem">LIVE MACRO INDICATORS</h2>
  <div class="macro-grid">{tiles_html}</div>
  <p class="interp">{interp}</p>
  {ai_rows}
</section>"""

    # ── AI PICKS SECTION ──────────────────────────────────────────────────
    def _ai_section():
        if not ai:
            return '<section id="ai" class="active"><p style="color:#666">AI analysis not available</p></section>'
        picks = ai.get("picks", [])

        cards = []
        for i, p in enumerate(picks):
            t = p.get("ticker","?")
            co = p.get("company","")
            sec = p.get("sector","")
            strat = p.get("strategy","")
            hl = p.get("headline","")
            story = p.get("story","")
            catalyst = p.get("catalyst","")
            watch = p.get("watch","")
            conv = p.get("conviction","MEDIUM")
            urg = p.get("urgency","WATCH")
            s = stocks.get(t, {})
            price_str = _money(s.get("price"))
            peg_str   = _num(s.get("peg"))
            pe_str    = _num(s.get("pe"))
            roic_str  = _pct(s.get("roic"))
            cards.append(f"""
<div class="pick-card">
  <div class="pick-hdr">
    <span class="pick-ticker">{i+1}. {t}</span>
    <span class="pick-co">{co} · {sec}</span>
    {_conv_badge(conv)} {_urgency_badge(urg)}
    <span class="badge badge-hold" style="font-size:.65rem">{strat}</span>
  </div>
  <p class="pick-hl">"{hl}"</p>
  <p class="pick-story">{story}</p>
  <div class="pick-meta">
    <span><b>Price</b> {price_str}</span>
    <span><b>PEG</b> {peg_str}</span>
    <span><b>P/E</b> {pe_str}</span>
    <span><b>ROIC</b> {roic_str}</span>
    {f'<span><b>Catalyst</b> {catalyst}</span>' if catalyst else ''}
    {f'<span><b>Watch</b> {watch}</span>' if watch else ''}
  </div>
</div>""")

        attn = ai.get("attention",[])
        risks_html = ""
        if attn:
            risk_items = "".join(f"<li>{r}</li>" for r in attn)
            risks_html = f'<div style="margin-top:12px"><h2 style="margin-bottom:6px">⚠ Key Risks</h2><ul style="padding-left:1.2em;color:#ef9a9a;font-size:.8rem;line-height:1.8">{risk_items}</ul></div>'

        return f"""
<section id="ai" class="active">
  <div class="section-title">🤖 AI Top Picks — Claude Sonnet</div>
  {"".join(cards)}
  {risks_html}
</section>"""

    # ── SECTOR SECTION ────────────────────────────────────────────────────
    def _sector_section():
        if not sector_rows:
            return '<section id="sector"><p style="color:#666">No sector data</p></section>'
        cols = ["Sector","Signal","Rot. Score","# Stocks","Med PEG","Med P/E","ETF",
                "1M vs SPY","3M vs SPY","Avg FCF Yield","Avg ROE"]
        hdr = "".join(f"<th>{c}</th>" for c in cols)
        rows_html = []
        for i, r in enumerate(sector_rows):
            alt = ' class="alt"' if i%2==0 else ''
            sig = r.get("Signal","HOLD")
            sig_cls = {"BUY":"g","AVOID":"r"}.get(sig,"")
            cells = []
            for c in cols:
                v = r.get(c)
                if c == "Signal":
                    cells.append(f"<td>{_signal_badge(v)}</td>")
                elif c in ("1M vs SPY","3M vs SPY","Avg FCF Yield","Avg ROE"):
                    cells.append(_cell(_pct(v), v))
                elif c in ("Med PEG","Med P/E","Rot. Score","# Stocks"):
                    cells.append(_cell(_num(v,1) if isinstance(v,float) else str(v or "—"), v))
                else:
                    cells.append(f"<td>{v or '—'}</td>")
            rows_html.append(f"<tr{alt}>{''.join(cells)}</tr>")
        return f"""
<section id="sector">
  <div class="section-title">Sector Rotation</div>
  <div class="tbl-wrap">
    <table><thead><tr>{hdr}</tr></thead>
    <tbody>{"".join(rows_html)}</tbody></table>
  </div>
</section>"""

    # ── ETF SECTION ───────────────────────────────────────────────────────
    def _etf_badge(sig):
        """Badge for ETF rotation signals (emoji-prefixed)."""
        s = (sig or "").upper()
        if "ROTATE IN" in s: return f'<span class="badge badge-buy">{sig}</span>'
        if "AVOID" in s:     return f'<span class="badge badge-avoid">{sig}</span>'
        if "TAKE PROFIT" in s: return f'<span class="badge badge-caut">{sig}</span>'
        return f'<span class="badge badge-hold">{sig}</span>'

    def _etf_section():
        if not etf_rows:
            return '<section id="etf"><p style="color:#666">No ETF data</p></section>'
        # etf_rows uses lowercase keys: sector, etf, signal, score, price, vs52H, 1M, 3M, 1Y, etc.
        cols = ["Sector","ETF","Signal","Score","Price","52w Hi","1W","1M","3M","6M","1Y",
                "1M α","3M α","vs MA50","vs MA200","RSI","Trend","Cycle"]
        key_map = {
            "Sector":"sector","ETF":"etf","Signal":"signal","Score":"score","Price":"price",
            "52w Hi":"vs52H","1W":"1W","1M":"1M","3M":"3M","6M":"6M","1Y":"1Y",
            "1M α":"1M_alpha","3M α":"3M_alpha",
            "vs MA50":"vs_ma50","vs MA200":"vs_ma200","RSI":"rsi14","Trend":"trend","Cycle":"cycle",
        }
        pct_cols  = {"52w Hi","1W","1M","3M","6M","1Y","1M α","3M α","vs MA50","vs MA200"}
        hdr = "".join(f"<th>{c}</th>" for c in cols)
        rows_html = []
        for i, r in enumerate(etf_rows):
            alt = ' class="alt"' if i%2==0 else ''
            cells = []
            for c in cols:
                v = r.get(key_map.get(c, c))
                if c == "Signal":
                    cells.append(f"<td>{_etf_badge(v)}</td>")
                elif c in pct_cols:
                    # color green/red for return columns
                    disp = _pct(v) if v is not None else "—"
                    if v is not None and c not in ("52w Hi",):
                        cls = "g" if v > 0 else ("r" if v < 0 else "")
                        cells.append(f'<td class="{cls}">{disp}</td>' if cls else f"<td>{disp}</td>")
                    else:
                        cells.append(f"<td>{disp}</td>")
                elif c == "Score":
                    sc_v = v or 0
                    cls = "g" if sc_v >= 65 else ("a" if sc_v >= 48 else "r")
                    cells.append(f'<td class="{cls}">{sc_v}</td>')
                elif c == "Price":
                    cells.append(f"<td>{_money(v)}</td>")
                elif c == "RSI":
                    rsi_v = v
                    cls = "g" if rsi_v and rsi_v <= 35 else ("r" if rsi_v and rsi_v >= 65 else "")
                    cells.append(f'<td class="{cls}">{_num(rsi_v)}</td>' if cls else f"<td>{_num(rsi_v)}</td>")
                else:
                    cells.append(f"<td>{v if v is not None else '—'}</td>")
            rows_html.append(f"<tr{alt}>{''.join(cells)}</tr>")
        return f"""
<section id="etf">
  <div class="section-title">📡 ETF Sector Rotation — Technical + Fundamental</div>
  <p style="background:#1a1a2e;padding:8px 12px;border-radius:4px;font-size:.75rem;color:#9e9e9e;margin-bottom:10px">
    <b style="color:#90caf9">Signals: </b>Score combines momentum (MA50/200, 3M alpha), mean-reversion (52W positioning, RSI), and sector fundamentals.
    α = excess return vs SPY. <b>ROTATE IN ≥65 · HOLD 48–64 · TAKE PROFITS 35–47 · AVOID &lt;35</b>
  </p>
  <div class="tbl-wrap">
    <table><thead><tr>{hdr}</tr></thead>
    <tbody>{"".join(rows_html)}</tbody></table>
  </div>
</section>"""

    # ── PERFORMANCE SECTION ───────────────────────────────────────────────
    def _perf_section():
        """Performance tab: agent scorecard + AI picks P&L + portfolio holdings."""
        import csv as _csv

        _AGENT_ICONS = {
            "AI-Judge":           "⚖️ Judge",
            "AI-QualityGrowth":   "🌱 Qual.Growth",
            "AI-EmergingGrowth":  "🚀 Emerg.Growth",
            "AI-CapAppreciation": "📈 Cap.Apprecn",
            "AI-SpecialSit":      "⚡ Special Sit",
        }
        _AGENT_ORDER = list(_AGENT_ICONS.keys())

        # ── Read AI picks log ─────────────────────────────────────────────
        ai_perf_rows = []
        if os.path.exists(AI_PICKS_LOG):
            try:
                with open(AI_PICKS_LOG, "r", encoding="utf-8") as _f:
                    for row in _csv.DictReader(_f):
                        t = row.get("ticker", "")
                        try: entry = float(row.get("entry_price") or 0)
                        except (ValueError, TypeError): entry = 0
                        s = stocks.get(t, {})
                        curr = s.get("price") or 0
                        ret  = ((curr - entry) / entry) if entry > 0 and curr > 0 else None
                        try:
                            days = (datetime.date.today()
                                    - datetime.date.fromisoformat(row["date"])).days
                        except Exception:
                            days = None
                        src = row.get("source", "")
                        ai_perf_rows.append({
                            "_src":      src,
                            "Date":      row.get("date",""),
                            "Agent":     _AGENT_ICONS.get(src, src),
                            "Ticker":    t,
                            "Company":   row.get("company","")[:28],
                            "Strategy":  row.get("strategy","")[:22],
                            "Entry $":   entry or None,
                            "Current $": curr or None,
                            "Return":    ret,
                            "Days":      days,
                            "Conviction":row.get("conviction",""),
                            "Headline":  row.get("headline","")[:60],
                        })
            except Exception:
                pass

        # ── Build per-agent scorecard ─────────────────────────────────────
        _agent_stats = {}  # src → {picks, rets, wins, best_t, best_r}
        for r in ai_perf_rows:
            src = r["_src"]
            if src not in _agent_stats:
                _agent_stats[src] = {"picks": 0, "rets": [], "wins": 0,
                                     "best_t": "—", "best_r": None}
            st = _agent_stats[src]
            st["picks"] += 1
            ret = r.get("Return")
            if ret is not None:
                st["rets"].append(ret)
                if ret > 0:
                    st["wins"] += 1
                if st["best_r"] is None or ret > st["best_r"]:
                    st["best_r"] = ret
                    st["best_t"] = r["Ticker"]

        scorecard_tiles = []
        # Sort: judge first, then specialists in fixed order, then any unknown
        _sorted_srcs = (
            [s for s in _AGENT_ORDER if s in _agent_stats] +
            [s for s in _agent_stats if s not in _AGENT_ORDER]
        )
        for src in _sorted_srcs:
            st = _agent_stats[src]
            n     = st["picks"]
            wins  = st["wins"]
            rets  = st["rets"]
            avg_r = sum(rets)/len(rets) if rets else None
            win_r = wins/n if n else None
            best_r = st["best_r"]
            best_t = st["best_t"]
            name  = _AGENT_ICONS.get(src, src)
            # Card colour: green if winning >50%, red if not
            card_cls = "win" if win_r and win_r > 0.5 else ("loss" if win_r is not None else "")
            avg_color = "#a5d6a7" if avg_r and avg_r > 0 else "#ef9a9a"
            scorecard_tiles.append(f"""
<div class="agent-card {card_cls}">
  <div class="ag-name">{name}</div>
  <div class="ag-stat">
    <b>{n}</b> picks &nbsp;·&nbsp;
    <b style="color:{'#a5d6a7' if win_r and win_r>0.5 else '#ef9a9a'}">{f"{win_r*100:.0f}%" if win_r is not None else "—"}</b> win rate<br>
    Avg: <b style="color:{avg_color}">{_pct(avg_r) if avg_r is not None else "—"}</b><br>
    Best: <b>{best_t}</b> {f'<span style="color:#a5d6a7">{_pct(best_r)}</span>' if best_r else ''}
  </div>
</div>""")

        scorecard_html = ""
        if scorecard_tiles:
            scorecard_html = f"""
<h2 style="margin:0 0 8px;font-size:.75rem">AGENT SCORECARD</h2>
<div class="agent-grid">{"".join(scorecard_tiles)}</div>"""

        # ── Picks table (sorted: judge first, then by agent, then date) ───
        def _sort_key(r):
            src = r["_src"]
            try: order = _AGENT_ORDER.index(src)
            except ValueError: order = 99
            return (order, r.get("Date",""))

        ai_perf_rows.sort(key=_sort_key)
        ai_hdr = ["Date","Agent","Ticker","Company","Strategy",
                  "Entry $","Current $","Return","Days","Conviction","Headline"]
        ai_body = []
        for i, r in enumerate(ai_perf_rows):
            ret = r.get("Return")
            ret_cls = "g" if ret and ret > 0 else ("r" if ret and ret < 0 else "")
            alt = ' class="alt"' if i % 2 == 0 else ''
            cells = []
            for c in ai_hdr:
                v = r.get(c)
                if c == "Return":
                    disp = _pct(v) if v is not None else "—"
                    cells.append(f'<td class="{ret_cls}">{disp}</td>' if ret_cls
                                 else f"<td>{disp}</td>")
                elif c in ("Entry $", "Current $"):
                    cells.append(f"<td>{_money(v)}</td>")
                elif c == "Conviction":
                    cells.append(f"<td>{_conv_badge(v) if v else '—'}</td>")
                else:
                    cells.append(f"<td>{v if v is not None else '—'}</td>")
            ai_body.append(f"<tr{alt}>{''.join(cells)}</tr>")

        ai_table = f"""
<h2 style="margin:14px 0 8px;font-size:.75rem">ALL PICKS — DETAIL</h2>
<div class="tbl-wrap">
  <table>
    <thead><tr>{"".join(f"<th>{c}</th>" for c in ai_hdr)}</tr></thead>
    <tbody>{"".join(ai_body)}</tbody>
  </table>
</div>""" if ai_perf_rows else ""

        # ── Portfolio holdings ────────────────────────────────────────────
        port_rows = []
        total_mkt = 0.0
        port_total_ret = None
        if portfolio and isinstance(portfolio.get("holdings"), list):
            init_cash = portfolio.get("initial_cash", 100000)
            cash      = portfolio.get("cash", 0)
            for h in portfolio["holdings"]:
                t      = h.get("ticker","")
                entry  = float(h.get("entry_price") or 0)
                shares = int(h.get("shares") or 0)
                curr   = (stocks.get(t, {}).get("price") or 0)
                ret    = ((curr - entry) / entry) if entry > 0 and curr > 0 else None
                mktval = curr * shares
                total_mkt += mktval
                port_rows.append({
                    "Entry Date":  h.get("entry_date",""),
                    "Ticker":      t,
                    "Company":     h.get("company","")[:26],
                    "Shares":      shares,
                    "Entry $":     entry or None,
                    "Current $":   curr or None,
                    "Cost Basis":  entry * shares or None,
                    "Mkt Value":   mktval or None,
                    "Return":      ret,
                    "Conviction":  h.get("conviction",""),
                })
            port_total_ret = ((total_mkt + cash - init_cash) / init_cash) if init_cash else None

        if not ai_perf_rows and not port_rows:
            return ('<section id="perf"><p style="color:#666;padding:16px">'
                    'No performance data yet — will populate after first logged picks.'
                    '</p></section>')

        # Portfolio summary bar
        summary_bar = ""
        if port_rows and portfolio:
            cash      = portfolio.get("cash", 0)
            init_cash = portfolio.get("initial_cash", 100000)
            tot_val   = total_mkt + cash
            ret_color = "#a5d6a7" if tot_val >= init_cash else "#ef9a9a"
            summary_bar = f"""
<div style="background:#1a1a2e;padding:10px 14px;border-radius:4px;margin-bottom:12px;
            display:flex;gap:20px;flex-wrap:wrap;font-size:.8rem">
  <span>💰 <b>Cash:</b> {_money(cash)}</span>
  <span>📊 <b>Holdings:</b> {_money(total_mkt)}</span>
  <span>📊 <b>Total value:</b> {_money(tot_val)}</span>
  <span>📈 <b>Total return:</b>
    <b style="color:{ret_color}">{_pct(port_total_ret) if port_total_ret is not None else "—"}</b>
  </span>
  <span style="color:#666">Started: {portfolio.get('started','?')}</span>
</div>"""

        port_hdr = ["Entry Date","Ticker","Company","Shares","Entry $","Current $",
                    "Cost Basis","Mkt Value","Return","Conviction"]
        port_body = []
        for i, r in enumerate(port_rows):
            ret = r.get("Return")
            ret_cls = "g" if ret and ret > 0 else ("r" if ret and ret < 0 else "")
            alt = ' class="alt"' if i % 2 == 0 else ''
            cells = []
            for c in port_hdr:
                v = r.get(c)
                if c == "Return":
                    disp = _pct(v) if v is not None else "—"
                    cells.append(f'<td class="{ret_cls}">{disp}</td>' if ret_cls
                                 else f"<td>{disp}</td>")
                elif c in ("Entry $","Current $","Cost Basis","Mkt Value"):
                    cells.append(f"<td>{_money(v)}</td>")
                elif c == "Conviction":
                    cells.append(f"<td>{_conv_badge(v) if v else '—'}</td>")
                else:
                    cells.append(f"<td>{v if v is not None else '—'}</td>")
            port_body.append(f"<tr{alt}>{''.join(cells)}</tr>")

        port_table = f"""
<h2 style="margin:14px 0 8px;font-size:.75rem">AI PORTFOLIO — PAPER TRADING</h2>
{summary_bar}
<div class="tbl-wrap">
  <table>
    <thead><tr>{"".join(f"<th>{c}</th>" for c in port_hdr)}</tr></thead>
    <tbody>{"".join(port_body)}</tbody>
  </table>
</div>""" if port_rows else ""

        return f"""
<section id="perf">
  <div class="section-title">📈 Performance Tracking</div>
  {scorecard_html}
  {ai_table}
  {port_table}
</section>"""

    # ── STRATEGY TABLE COLS ───────────────────────────────────────────────
    STRAT_COLS = ["Rank","Ticker","Company","Sector","Price","Score",
                  "PEG","Fwd PEG","P/E","ROIC","ROE","FCF Yield",
                  "MoS","Rev Growth","Piotroski","MktCap ($B)"]

    # ── ASSEMBLE ──────────────────────────────────────────────────────────
    body = f"""
<div class="header">
  <div>
    <h1>📈 FMP Stock Screener</h1>
    <small>{now} · {len(stocks):,} stocks · {fmp_call_count} API calls</small>
  </div>
  <small style="color:#9fa8da">Lynch + Buffett</small>
</div>
<nav class="nav">{nav_html}</nav>
{_ai_section()}
{_macro_section()}
{_strategy_table(iv_rows,   STRAT_COLS + ["IV","D/E","EV/EBITDA"],   "iv",       "📊 IV Discount Picks",
    "DCF intrinsic value discount ≥15% · Piotroski ≥7 · MktCap >$100M · "
    "Scored on: value gap, quality (ROIC/FCF/Piotroski), growth. Top 50.")}
{_strategy_table(stalwarts,  STRAT_COLS,                               "stalwart", "🏛 Stalwarts",
    "Revenue growth 5–25% · MktCap >$2B · P/E <50 · FCF positive · Piotroski ≥5 · "
    "Rev consistency ≥60% · Excl. Basic Materials. Lynch 'boring but reliable' category.")}
{_strategy_table(fast_growers, STRAT_COLS + ["Rev Growth 5Y","EPS Growth 5Y"], "fastg",    "🚀 Fast Growers",
    "Revenue growth >20% · PEG <1.5 · ROIC >10% · FCF positive or high-growth exception. "
    "Lynch's highest-return category — find the next 10-bagger before it's obvious.")}
{_strategy_table(slow_growers, STRAT_COLS + ["Div Yield"],              "slowg",    "🐢 Slow Growers",
    "Dividend yield ≥2% · Stable multi-year earnings · Large established companies. "
    "Lynch category: own for income + capital preservation, not growth.")}
{_strategy_table(cyclicals,  STRAT_COLS,                               "cycl",     "🔄 Cyclicals",
    "Low P/E in cyclical sectors (Industrials, Energy, Materials, Consumer Cyclical). "
    "Lynch: buy cyclicals when P/E is LOW (trough earnings) — sell when P/E looks cheap at peak.")}
{_strategy_table(turnarounds, STRAT_COLS,                              "turn",     "🔁 Turnarounds",
    "Down ≥40% from 52W high · Revenue recovering (positive growth trend) · Piotroski ≥4. "
    "Lynch: near-bankrupt companies that survive can be 10x — but require highest conviction.")}
{_strategy_table(asset_plays, STRAT_COLS + ["P/B","Graham NN"],        "asset",    "🏗 Asset Plays",
    "P/B <1 · Hidden tangible asset value (real estate, cash, IP) · FCF positive. "
    "Lynch/Graham: buy $1 of assets for <$1. Best in Financial Services, Real Estate, Industrials.")}
{_strategy_table(quality_compounders, STRAT_COLS + ["P/FCF","EV/EBITDA"], "qual",  "💎 Quality Compounders",
    "ROIC >15% · PEG <2 · FCF positive · Operating margin >20% · Revenue growth >8%. "
    "Buffett category: wonderful companies at fair prices — hold forever, let compounding work.")}
{_sector_section()}
{_etf_section()}
{_perf_section()}
<footer style="padding:16px;color:#555;font-size:.72rem;text-align:center">
  FMP Screener · {now} · Not investment advice.
</footer>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FMP Screener — {today}</title>
<style>{css}</style>
</head>
<body>
{body}
<script>{js}</script>
</body>
</html>"""


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    global _fmp_call_count, _run_start

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Debug mode: 20 stocks only, fast iteration")
    parser.add_argument("--log-picks", action="store_true", help="Log today's top picks to CSV for performance tracking")
    args = parser.parse_args()

    DEBUG = args.debug
    _run_start = time.time()

    print("=" * 65)
    print("  📈 FMP STOCK SCREENER — Professional Fundamentals Edition")
    if DEBUG:
        print("  ⚡ DEBUG MODE — 20 stocks only for fast testing")
    if NTFY_TOPIC:
        print(f"  🔔 Phone notifications: ntfy.sh/{NTFY_TOPIC}")
    print("=" * 65)

    # ── API key status ──
    fmp_status  = "✅ loaded" if FMP_KEY  else "❌ NOT FOUND"
    ai_status   = "✅ loaded" if ANTHROPIC_KEY else "⚠️  not set  (AI analysis disabled)"
    print(f"  🔑 FMP_API_KEY:        {fmp_status}")
    print(f"  🔑 ANTHROPIC_API_KEY:  {ai_status}")
    if not FMP_KEY or not ANTHROPIC_KEY:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if not os.path.exists(env_path):
            print(f"\n  💡 Tip: create {env_path} with:")
            print("     FMP_API_KEY=your_fmp_key")
            print("     ANTHROPIC_API_KEY=sk-ant-api03-...")
    print()

    if not FMP_KEY:
        print("\n  ❌ FMP_API_KEY not set! Add it to .env or environment variables.")
        sys.exit(1)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    output_file = os.path.join(OUTPUT_DIR, f"fmp_screener_{ts}.xlsx")

    # Load cache
    load_cache()

    # Start notification — sent before the heavy lifting begins
    if NTFY_TOPIC:
        _cache_warm = bool(_cache.get("universe"))
        _eta_start = "~10–15 min (cache warm)" if _cache_warm else "~2.5–3 h (full fetch)"
        _purl_start = (f"https://{GITHUB_REPO.split('/', 1)[0]}.github.io/"
                       f"{GITHUB_REPO.split('/', 1)[1]}/"
                       if GITHUB_REPO and '/' in GITHUB_REPO else "")
        notify_phone(
            title="FMP Screener starting 🚀",
            message=(f"Run started {datetime.datetime.now().strftime('%H:%M')} · ETA {_eta_start}"
                     + (f"\n📱 When done: {_purl_start}" if _purl_start else "")),
            tags="rocket",
        )

    # Fetch macro indicators early (fast, cached, free via FRED)
    phase_start("macro", "Fetching macro indicators (FRED)")
    macro_data = fetch_macro_indicators()

    # Phase 1: Universe from screener (already has price, mktCap, sector)
    phase_start("universe", "Fetching US stock universe (live prices)")
    universe = fetch_us_universe()

    # Phase 2: Skip bulk profiles — screener data is sufficient for filtering
    # Use screener data directly as our "profiles"
    profiles = {}
    for t, u in universe.items():
        if u.get("price") and u.get("mktCap") and u["mktCap"] > 0:
            profiles[t] = u  # screener data IS the profile

    # Filter: stocks with positive price and market cap > $50M for enrichment
    candidates = [t for t in profiles if profiles[t].get("mktCap", 0) > 50_000_000
                  and profiles[t].get("price", 0) > 1]
    # Sort by market cap, take top N for detailed analysis
    candidates.sort(key=lambda t: -(profiles.get(t, {}).get("mktCap") or 0))
    enrich_count = 20 if DEBUG else 4000
    top_for_enrichment = candidates[:enrich_count]
    print(f"\n  📊 {len(candidates)} stocks > $50M mktcap, enriching top {len(top_for_enrichment)}")
    phase_start("enrichment", f"Enriching top {len(top_for_enrichment)} stocks (fundamentals)")

    key_metrics = fetch_key_metrics(top_for_enrichment)
    ratios_ttm = fetch_ratios_ttm(top_for_enrichment)
    dcf_data = fetch_dcf_bulk(top_for_enrichment)
    estimates = fetch_growth_estimates(top_for_enrichment)
    scores = fetch_financial_scores(top_for_enrichment)
    ratings = fetch_ratings(top_for_enrichment)
    growth_data = fetch_financial_growth(top_for_enrichment)
    balance_sheet = fetch_balance_sheet(top_for_enrichment)
    earnings_surp = fetch_earnings_surprises(top_for_enrichment)
    insider_data = fetch_insider_trading(tickers=top_for_enrichment)

    # Fetch 52-week high/low (not returned by screener on Starter plan; batched via /quote)
    phase_start("52w_ranges", "Fetching 52-week ranges (parallel)")
    w52 = fetch_52w_ranges(top_for_enrichment)
    for _t, _rng in w52.items():
        if _t in universe:
            universe[_t]["yearHigh"] = _rng.get("yearHigh")
            universe[_t]["yearLow"]  = _rng.get("yearLow")

    # Save cache after all fetches
    save_cache()

    # Assemble
    stocks = assemble_stock_data(universe, profiles, key_metrics, ratios_ttm, dcf_data,
                                 estimates, scores, ratings, growth_data, insider_data,
                                 balance_sheet, earnings_surp)

    print(f"\n  📊 FMP API calls this run: {_fmp_call_count}")

    # Build Excel
    wb = Workbook()
    ws_overview = wb.active
    ws_overview.title = "Overview"
    # Pre-create AI picks sheet in position 2 and portfolio sheet in position 3
    ws_ai_picks   = wb.create_sheet("1b. AI Top Picks")
    ws_ai_portf   = wb.create_sheet("1c. AI Portfolio")

    phase_start("tabs", "Building strategy tabs (IV Discount, Lynch, Sector, AI)")
    # Tab 2: IV Discount
    iv_rows = build_iv_discount(wb, stocks)

    # Lynch tabs
    # ─── Stalwarts: consistent 5-25% revenue growers, large cap, quality gate ───
    def _stalwart_filter(s):
        if not _is_common_stock(s): return False
        rg  = s.get("revGrowth")
        pe  = s.get("pe")
        pio = s.get("piotroski")
        fcf = s.get("fcfYield")
        # Kill criteria: FCF must be positive (real business, not accounting mirage)
        if fcf is not None and fcf <= 0:
            return False
        # Kill: Basic Materials excluded — commodity cycle drives revenue, not business quality
        if (s.get("sector") or "") == "Basic Materials":
            return False
        # Kill: RevConsistency < 0.60 — Stalwarts must show steady multi-year growth
        rc = s.get("revConsistency")
        if rc is not None and rc < 0.60:
            return False
        return (rg and 0.05 <= rg <= 0.25
                and s.get("mktCap", 0) > 2e9
                and (pe is None or (0 < pe < 50))
                and (pio is None or pio >= 5))

    def _stalwart_score(s):
        sc = 0
        _roic_s  = s.get("roic")
        _fcf_s   = s.get("fcfYield")
        _rg_s    = s.get("revGrowth") or 0
        _rg_p_s  = s.get("revGrowthPrev")
        _fin_s       = "financial" in (s.get("sector") or "").lower()
        _basic_mat_s = "basic materials" in (s.get("sector") or "").lower()
        _energy_s    = (s.get("sector") or "") == "Energy"

        # ── ROIC first: is this a quality business worth owning? ─────────────
        if _roic_s and _roic_s > 0.25:   sc += 18
        elif _roic_s and _roic_s > 0.20: sc += 14
        elif _roic_s and _roic_s > 0.15: sc += 10
        elif _roic_s and _roic_s > 0.10: sc += 5
        elif _roic_s is not None and _roic_s < 0.08:
            sc -= 4  # low ROIC Stalwart = growing revenue but not compounding capital

        # ── PEG second: is the quality priced fairly? ────────────────────────
        # Max 12pts (ROIC leads; PEG confirms entry price)
        best_peg = s.get("fwdPEG") or s.get("peg")
        peg_sc = 0
        if best_peg and 0 < best_peg < 1:    peg_sc = 12
        elif best_peg and 0 < best_peg < 1.5: peg_sc = 8
        elif best_peg and 0 < best_peg < 2:   peg_sc = 4
        # Quality bonus: ROIC validates the PEG is moat-backed
        if peg_sc and _roic_s and _roic_s > 0.15 and _fcf_s and _fcf_s > 0.04:
            peg_sc = min(12, round(peg_sc * 1.25))
        # Deceleration penalty
        if peg_sc and _rg_p_s is not None and (_rg_s - _rg_p_s) < -0.10:
            peg_sc = round(peg_sc * 0.60)
        # Cyclical sector penalty: financials, commodity miners, E&P energy
        if peg_sc and _fin_s and best_peg and best_peg < 1.5:
            peg_sc = round(peg_sc * 0.50)  # insurance float / cycle distorts PEG
        if peg_sc and _basic_mat_s and best_peg and best_peg < 1.5:
            peg_sc = round(peg_sc * 0.60)  # gold/commodity miners: trough-earnings PEG unreliable
        if peg_sc and _energy_s and best_peg and best_peg < 1.5:
            peg_sc = round(peg_sc * 0.60)  # E&P revenue/earnings move with oil/gas price, not business quality
        sc += peg_sc
        roe = s.get("roe")
        if roe and roe > 0.20:   sc += 8
        elif roe and roe > 0.15: sc += 5
        elif roe and roe > 0.08: sc += 2
        fcf = s.get("fcfYield")
        if fcf and fcf > 0.05: sc += 5
        elif fcf and fcf > 0.02: sc += 2
        pio = s.get("piotroski")
        if pio and pio >= 8: sc += 8
        elif pio and pio >= 7: sc += 5
        elif pio and pio >= 5: sc += 2
        if s.get("mos") and s.get("mos") > 0.2: sc += 5
        if s.get("pe") and 0 < s.get("pe") < 20: sc += 3
        roic = s.get("roic")
        if roic and roic > 0.15: sc += 4
        # Earnings beat rate — consistent executors are the best Stalwarts
        br = s.get("beatRate")
        if br and br >= 0.875: sc += 7
        elif br and br >= 0.75: sc += 4
        elif br and br >= 0.625: sc += 2
        # Revenue acceleration — growth improving YoY = re-accelerating Stalwart
        rg = s.get("revGrowth") or 0
        rg_prev = s.get("revGrowthPrev")
        if rg_prev is not None and rg > 0.05:
            rg_delta = rg - rg_prev
            if rg_delta > 0.05:   sc += 5   # re-accelerating = high conviction
            elif rg_delta < -0.08: sc -= 3  # decelerating toward slow grower zone
        # Revenue consistency — steady multi-year growers carry lower risk
        rc = s.get("revConsistency")
        if rc is not None:
            if rc >= 0.80:   sc += 7   # 4+ of 5 years positive
            elif rc >= 0.60: sc += 3
            elif rc < 0.40:  sc -= 4   # majority of years declining = structural issue
        return sc

    _st_headers = [
        "Rank", "Ticker", "Company", "Sector", "Price",
        "Fwd PEG", "PEG", "Fwd P/E", "P/E", "ROIC",
        "ROE", "FCF Yield", "FCF Conv.", "Oper Margin", "Rev Growth", "Rev Consist.", "Grwth Gap",
        "EPS Growth 5Y", "Piotroski", "52w vs High", "Div Yield", "MktCap ($B)", "Score", "🏦 Insider",
    ]
    _st_widths = [5, 8, 22, 15, 8, 7, 6, 7, 7, 8, 7, 8, 8, 8, 9, 9, 8, 11, 7, 9, 7, 10, 6, 14]

    stalwarts = build_lynch_tab(wb, stocks, "Stalwarts", "3", _stalwart_filter, _stalwart_score,
                                "4A148C", "Consistent 5-25% revenue growers, >$2B. Buy on dips.",
                                custom_headers=_st_headers, custom_widths=_st_widths)

    # ─── Fast Growers: Peter Lynch 10-bagger style ─────────────────────────
    # Lynch: "Find a company growing 20-25%/yr with a low PEG — that's a 10-bagger."
    # Key: consistent multi-year growth + reasonable PEG + profitability improving
    # NOT primarily Piotroski — Lynch focused on growth quality, not balance sheet scores
    def _fast_grower_filter(s):
        if not _is_common_stock(s): return False
        # Real Estate excluded: ROIC is inflated by land-at-cost book values; revenue growth
        # is asset-cycle driven, not business quality compounding
        if (s.get("sector") or "") == "Real Estate": return False
        # Basic Materials excluded: commodity prices (gold, copper, fertilizer) drive revenue
        # growth, not underlying business quality — miners are not Lynch fast growers
        if (s.get("sector") or "") == "Basic Materials": return False
        rg  = s.get("revGrowth")       # trailing 1-yr revenue growth
        rg5 = s.get("revGrowth5y")     # forward 5-yr revenue CAGR (analyst estimates)
        eg5 = s.get("epsGrowth5y")     # forward 5-yr EPS CAGR
        rg5h = s.get("fiveYRevGrowth") # historical 5-yr rev/share growth
        pe  = s.get("pe")
        mktcap = s.get("mktCap", 0)

        # Size: Lynch preferred small-mid caps — lots of runway to grow
        if mktcap < 100e6 or mktcap > 150e9:
            return False

        # Primary qualifier — at least ONE strong growth signal must exist:
        # A) Current year revenue growing >20% (classic fast grower)
        # B) Multi-year revenue growth >15% (steadier compounder trajectory)
        # C) Forward EPS CAGR >15% + any positive current growth (earnings-led growth)
        strong_current = (rg and rg > 0.20)
        strong_5yr     = (rg5 and rg5 > 0.15 and (rg is None or rg > 0.10))
        strong_5yr_h   = (rg5h and rg5h > 0.15 and (rg is None or rg > 0.10))
        eps_led_growth = (eg5 and eg5 > 0.15 and (rg is None or rg > 0.08))
        if not (strong_current or strong_5yr or strong_5yr_h or eps_led_growth):
            return False

        # A1: Kill isolated single-year spike — >50% growth needs prior-year support
        # Prevents one-time events (pharma milestones, asset sales) from inflating rankings
        if rg is not None and rg > 0.50:
            _rg_prev_a1 = s.get("revGrowthPrev"); _rg_yr2_a1 = s.get("revGrowthYr2")
            if not ((_rg_prev_a1 and _rg_prev_a1 > 0.15) or (_rg_yr2_a1 and _rg_yr2_a1 > 0.15)):
                return False  # isolated spike — not a structural fast grower

        # ── KILL CRITERIA: hard rejections — mediocre businesses filtered out ──
        # Kill 1: FCF must be positive — with one exception for genuine high-growth small caps.
        # Lynch would not have excluded early Amazon/Salesforce for reinvesting aggressively.
        # Exception: mktcap < $2B AND rev growth > 25% AND FCF yield > -5% (not deeply burning).
        fcf = s.get("fcfYield")
        if fcf is not None and fcf <= 0:
            _small = mktcap < FG_SMALL_CAP_MAX
            _fast  = (rg is not None and rg > FG_RELAXED_FCF_REV_GROWTH)
            _mild  = fcf > -0.05  # FCF yield better than -5% — not deeply destroying cash
            if not (_small and _fast and _mild):
                return False
            # Exception granted — scoring still penalises: 0 FCF yield pts,
            # -3 for FCF margin < 2%, likely -5 for low FCF conversion → net -8 to -11 pts.

        # Kill 2: Revenue consistency — 3 of 5 years must show positive growth
        rc = s.get("revConsistency")
        if rc is not None and rc < 0.40:
            return False  # majority of years declining = structural issue, not a fast grower

        # Kill 3: Profitability floor — must have SOME evidence of earnings quality
        has_pe       = (pe and 0 < pe < 300)
        has_eps_gr   = (eg5 and eg5 > 0)
        if not (has_pe or has_eps_gr):
            return False

        # Kill 4: Exclude distress — Altman Z < 1 is bankruptcy danger zone
        az = s.get("altmanZ")
        if az is not None and az < 1.0:
            return False

        # A2: Leverage kill — too much debt makes growth fragile and funding uncertain
        _nde_a2 = s.get("netDebtEbitda")
        if _nde_a2 is not None and _nde_a2 > 5.0:
            return False  # net debt > 5× EBITDA — too leveraged to sustain growth

        # Kill 5: Financial Services with low ROIC — PEG cheap but capital-inefficient
        # Insurance/bank PEGs look cheap at cycle peaks; require ROIC ≥ 10% as minimum quality floor
        _sector_fg = (s.get("sector") or "")
        if "financial" in _sector_fg.lower() or "insurance" in _sector_fg.lower():
            roic_fg = s.get("roic")
            if roic_fg is not None and roic_fg < 0.10:
                return False

        # Kill 6: Extreme share dilution — >15%/yr destroys shareholder value faster than growth creates it
        sg = s.get("sharesGrowth")
        if sg is not None and sg > 0.15:
            return False

        return True

    def _fast_grower_score(s):
        rg      = s.get("revGrowth") or 0
        rg_prev = s.get("revGrowthPrev")          # prior year — for acceleration detection
        rg5     = s.get("revGrowth5y") or 0
        rg5h    = s.get("fiveYRevGrowth") or 0
        eg5     = s.get("epsGrowth5y") or 0
        eg      = s.get("epsGrowth") or 0
        eg_prev = s.get("epsGrowthPrev")

        # Core 1: Current revenue growth (primary signal, capped to avoid SPAC distortions)
        sc = min(rg * 28, 18)  # 20% growth = 5.6 pts, 60% = 18 pts (cap)

        # Core 2: Multi-year revenue growth consistency — Lynch wanted steady compounders
        best_5yr = max(rg5, rg5h)   # take the better of forward/historical 5yr
        if best_5yr > 0.25:  sc += 14   # exceptional compounder (25%+/yr for 5 years)
        elif best_5yr > 0.20: sc += 10
        elif best_5yr > 0.15: sc += 7
        elif best_5yr > 0.10: sc += 4
        elif best_5yr > 0.05: sc += 1

        # Core 3: EPS growth trajectory — Lynch: earnings should grow with revenue
        if eg5 > 0.25:  sc += 12
        elif eg5 > 0.20: sc += 9
        elif eg5 > 0.15: sc += 6
        elif eg5 > 0.10: sc += 3
        elif eg5 > 0.05: sc += 1

        # Consistency bonus: both revenue AND earnings growing fast = high conviction
        if best_5yr > 0.15 and eg5 > 0.15:
            sc += 6

        # ── Growth acceleration / deceleration vs prior year ────────────────
        # Lynch's key insight: you want to catch fast growers BEFORE they decelerate
        # Accelerating = current growth BETTER than last year (momentum building)
        # Decelerating = current growth WORSE than last year (late cycle, avoid)
        if rg_prev is not None and rg > 0.10:
            rg_delta = rg - rg_prev   # positive = accelerating
            if rg_delta > 0.10:       sc += 10  # meaningful acceleration (+10pp+)
            elif rg_delta > 0.05:     sc += 6   # moderate acceleration
            elif rg_delta > 0:        sc += 3   # slight acceleration
            elif rg_delta < -0.10:    sc -= 8   # meaningful deceleration (red flag)
            elif rg_delta < -0.05:    sc -= 4   # moderate deceleration (caution)

        # EPS acceleration (even stronger signal — earnings leverage on revenue)
        if eg_prev is not None and eg > 0:
            eg_delta = eg - eg_prev
            if eg_delta > 0.10:  sc += 6   # EPS accelerating strongly
            elif eg_delta > 0:   sc += 3   # EPS acceleration
            elif eg_delta < -0.10: sc -= 4  # EPS decelerating

        # Positive growth 3 consecutive years (all 3 years we have show growth)
        rg_yr2 = s.get("revGrowthYr2")
        if rg > 0.10 and rg_prev and rg_prev > 0.10 and rg_yr2 and rg_yr2 > 0.05:
            sc += 8  # 3-year consistent growth = high conviction fast grower

        # Revenue consistency 5-year: penalise structurally declining businesses
        rc_fg = s.get("revConsistency")
        if rc_fg is not None:
            if rc_fg >= 0.80:   sc += 7
            elif rc_fg >= 0.60: sc += 3
            elif rc_fg < 0.40:  sc -= 5  # majority of years negative = not a fast grower

        # Share dilution: buybacks = capital discipline, heavy dilution = value destruction
        sg_fg = s.get("sharesGrowth")
        if sg_fg is not None:
            if sg_fg < -0.03:   sc += 5
            elif sg_fg < 0:     sc += 2
            elif sg_fg > 0.05:  sc -= 4

        # PEG — Lynch's defining metric: growth at a reasonable price
        # Prefer forward PEG (consistent fwdPE/fwdGrowth) to avoid trailing-vs-forward mismatch
        best_peg_fg = s.get("fwdPEG") or s.get("peg")
        _roic_fg = s.get("roic"); _fcf_fg = s.get("fcfYield")
        _fin_fg       = "financial" in (s.get("sector") or "").lower()
        _basic_mat_fg = "basic materials" in (s.get("sector") or "").lower()
        _energy_fg    = (s.get("sector") or "") == "Energy"
        # PEG: secondary valuation check — confirms whether the ROIC is priced fairly
        # Max 15pts (was 22) — ROIC now leads, PEG validates the entry price
        peg_sc_fg = 0
        if best_peg_fg and 0 < best_peg_fg < 0.75:   peg_sc_fg = 15
        elif best_peg_fg and 0 < best_peg_fg < 1.0:  peg_sc_fg = 11
        elif best_peg_fg and 0 < best_peg_fg < 1.5:  peg_sc_fg = 6
        elif best_peg_fg and 0 < best_peg_fg < 2.0:  peg_sc_fg = 3
        # Quality bonus: ROIC+FCF confirms the low PEG is moat-backed, not an earnings spike
        if peg_sc_fg and _roic_fg and _roic_fg > 0.15 and _fcf_fg and _fcf_fg > 0.04:
            peg_sc_fg = min(22, round(peg_sc_fg * 1.25))
        # Deceleration penalty: fast-falling growth makes a low PEG misleading
        if peg_sc_fg and rg_prev is not None and (rg - rg_prev) < -0.10:
            peg_sc_fg = round(peg_sc_fg * 0.60)
        # Cyclical penalty: insurance/financial PEGs are structurally unreliable
        if peg_sc_fg and _fin_fg and best_peg_fg and best_peg_fg < 1.5:
            peg_sc_fg = round(peg_sc_fg * 0.50)
        # Cyclical penalty: commodity miners — PEG based on trough-earnings is misleading
        if peg_sc_fg and _basic_mat_fg and best_peg_fg and best_peg_fg < 1.5:
            peg_sc_fg = round(peg_sc_fg * 0.60)
        # Cyclical penalty: E&P energy — revenue growth driven by commodity prices, not business quality
        if peg_sc_fg and _energy_fg and best_peg_fg and best_peg_fg < 1.5:
            peg_sc_fg = round(peg_sc_fg * 0.60)
        # Filter 4: Growth optimism penalty — analysts significantly more bullish than history
        _go_fg = s.get("growthOptimism")
        _ego_fg = s.get("epsGrowthOptimism")
        if peg_sc_fg and _go_fg is not None and _go_fg > 0.50:
            peg_sc_fg = round(peg_sc_fg * 0.70)  # revenue estimates too rosy
        # Double penalty if BOTH revenue AND EPS estimates far exceed history
        if peg_sc_fg and _go_fg is not None and _go_fg > 0.50 and _ego_fg is not None and _ego_fg > 0.50:
            peg_sc_fg = round(peg_sc_fg * 0.80)  # combined red flag — analyst euphoria
        sc += peg_sc_fg

        # FCF data missing entirely = uncertainty penalty (different from FCF ≤ 0 which is a kill)
        # A legitimate fast grower should have measurable FCF; None means FMP has no data.
        # Micro-caps frequently have incomplete FMP coverage despite real FCF — reduce penalty.
        fcf_fg = s.get("fcfYield")
        if fcf_fg is None:
            if s.get("mktCap", 0) < FG_MICRO_CAP_MAX:
                sc -= 2   # reduced: data gap reflects thin coverage, not poor quality
            else:
                sc -= 5   # full penalty for mid/large caps — no excuse for missing FCF data

        # Negative forward EPS growth = earnings expected to shrink — contradicts "fast grower" thesis
        eg5_fg = s.get("epsGrowth5y") or 0
        if eg5_fg < 0:
            sc -= 8  # declining projected earnings is a fundamental contradiction for this category

        # Filter 3: FCF conversion — cash earnings vs reported earnings quality check
        fcc_fg = s.get("fcfConversion")
        if fcc_fg is not None:
            if fcc_fg >= 0.80:   sc += 6   # FCF quality confirmed
            elif fcc_fg >= 0.60: sc += 3
            elif fcc_fg < 0.40:  sc -= 5   # earnings not converting to cash

        # FCF Margin: how much of each revenue dollar becomes FCF (capital efficiency of growth)
        fcm_fg = s.get("fcfMargin")
        if fcm_fg is not None:
            if fcm_fg >= 0.20:   sc += 8   # >20% FCF margin = elite cash generator
            elif fcm_fg >= 0.12: sc += 5
            elif fcm_fg >= 0.06: sc += 2
            elif fcm_fg < 0.02:  sc -= 3   # thin FCF margin means growth eats all cash

        # FCF growth consistency: is the FCF trend stable across years?
        fgc_fg = s.get("fcfGrowthConsistency")
        if fgc_fg is not None:
            if fgc_fg >= 0.80:   sc += 5
            elif fgc_fg >= 0.60: sc += 2
            elif fgc_fg < 0.40:  sc -= 3

        # ── ROIC: PRIMARY quality signal — evaluated BEFORE PEG ─────────────
        # High ROIC = durable moat. The biggest winners sustain ROIC > 15% for years.
        # PEG tells you the price; ROIC tells you if the business deserves a high price.
        roic = s.get("roic")
        if roic and roic > 0.30:   sc += 20  # elite compounder — ROIC sustains itself
        elif roic and roic > 0.25: sc += 16
        elif roic and roic > 0.20: sc += 12
        elif roic and roic > 0.15: sc += 8   # moat confirmed — growth is worth paying for
        elif roic and roic > 0.10: sc += 3
        elif roic and roic is not None and roic < 0.08:
            sc -= 5  # low ROIC growth = burning capital to grow, not compounding

        # ROE — secondary; can be lever-inflated, so lower weight than ROIC
        roe = s.get("roe")
        if roe and roe > 0.25:   sc += 4
        elif roe and roe > 0.15: sc += 2

        # Gross margin — pricing power / scalability
        gm = s.get("grossMargin")
        if gm and gm > 0.60:   sc += 6   # software/IP-like margins
        elif gm and gm > 0.40: sc += 4
        elif gm and gm > 0.25: sc += 2

        # FCF quality (positive = bonus, negative = neutral — many fast growers reinvest heavily)
        fcf = s.get("fcfYield")
        if fcf and fcf > 0.08:  sc += 7
        elif fcf and fcf > 0.04: sc += 4
        elif fcf and fcf > 0.01: sc += 2

        # P/FCF — cash-flow based value check (lower = more reasonable vs growth)
        pfcf = s.get("pFcf")
        if pfcf and 0 < pfcf < 20:    sc += 4   # cheap on FCF basis
        elif pfcf and 0 < pfcf < 35:  sc += 2

        # Earnings beat rate — management consistently delivers vs expectations
        br = s.get("beatRate")
        if br and br >= 0.875:  sc += 8   # beat 7+ of 8 quarters = very reliable compounder
        elif br and br >= 0.75: sc += 5
        elif br and br >= 0.625: sc += 2

        # A4: EPS beat streak — consecutive quarters beating = execution momentum building
        ebs = s.get("epsBeatStreak")
        if ebs is not None:
            if ebs >= 4:   sc += 8   # 4+ consecutive beats — strong execution momentum
            elif ebs >= 2: sc += 4

        # Valuation sanity
        pe = s.get("pe")
        if pe and 0 < pe < 25:   sc += 3
        elif pe and 0 < pe < 40: sc += 1

        # Insider buying = management confidence in their own growth story
        if s.get("insiderBuys", 0) >= 3:   sc += 6
        elif s.get("insiderBuys", 0) >= 1: sc += 3

        # ── Market cap size bonus: surfaces underfollowed small caps ─────────
        # Smaller companies have more analytical blind spots = more mispricing = more alpha.
        # Additive to quality score — a junk small-cap still scores low overall.
        _mc_fg = s.get("mktCap", 0)
        if _mc_fg < FG_MICRO_CAP_MAX:
            sc += FG_MICRO_CAP_BONUS    # $100M–$500M: most neglected
        elif _mc_fg < FG_SMALL_CAP_MAX:
            sc += FG_SMALL_CAP_BONUS    # $500M–$2B: still under-covered
        elif _mc_fg < FG_SMALL_MID_MAX:
            sc += FG_SMALL_MID_BONUS    # $2B–$5B: marginal nudge

        return sc

    _fg_headers = [
        "Rank", "Ticker", "Company", "Sector", "Price",
        "Fwd PEG", "PEG", "Fwd P/E", "P/E", "ROIC",
        "FCF Yield", "FCF Margin", "FCF Conv.", "FCF Consist.",
        "Rev Growth", "Rev Gr Prev", "Rev Growth 5Y", "EPS Growth 5Y",
        "Rev Consist.", "Grwth Gap", "Shares Δ", "Beat Rate", "52w vs High", "ROE", "Gross Margin",
        "P/FCF", "MktCap ($B)", "Cap Size", "Score", "🏦 Insider",
    ]
    _fg_widths = [5, 8, 22, 15, 8, 7, 6, 7, 7, 8, 8, 9, 8, 9, 9, 10, 11, 11, 9, 8, 8, 8, 9, 7, 11, 7, 10, 7, 6, 14]

    fast_growers = build_lynch_tab(wb, stocks, "Fast Growers", "4", _fast_grower_filter, _fast_grower_score,
                                   "1B5E20", "Revenue >20% or multi-year 15%+ CAGR. Lynch 10-baggers. PEG is king.",
                                   custom_headers=_fg_headers, custom_widths=_fg_widths)

    # ─── Slow Growers: income focus — dividend sustainability is key ───
    def _slow_grower_filter(s):
        if not _is_common_stock(s): return False
        dy = s.get("divYield")
        rg = s.get("revGrowth")
        pio = s.get("piotroski")
        fcf = s.get("fcfYield")
        # Require: >2% yield, low growth, quality floor, FCF must cover dividend
        return (dy and 0.02 < dy < 0.15          # cap at 15% — above that is usually distressed
                and (not rg or rg < 0.10)
                and (pio is None or pio >= 5)     # quality gate
                and (fcf is None or fcf > dy * 0.5))  # FCF covers at least half the dividend

    def _slow_grower_score(s):
        dy = s.get("divYield") or 0
        sc = dy * 80                              # yield weighted, but capped by filter
        pio = s.get("piotroski")
        if pio and pio >= 8: sc += 10
        elif pio and pio >= 7: sc += 6
        elif pio and pio >= 5: sc += 2
        fcf = s.get("fcfYield")
        if fcf and fcf > 0.07: sc += 5
        elif fcf and fcf > 0.04: sc += 3
        de = s.get("de")
        if de is not None and de < 0.5: sc += 4
        elif de is not None and de < 1.0: sc += 2
        if s.get("pe") and 0 < s.get("pe") < 18: sc += 3
        roe = s.get("roe")
        if roe and roe > 0.15: sc += 3
        if s.get("mos") and s.get("mos") > 0.1: sc += 3
        return sc

    slow_growers = build_lynch_tab(wb, stocks, "Slow Growers", "5", _slow_grower_filter, _slow_grower_score,
                                   "455A64", "Dividend >2%, low growth. Buy for income, not appreciation.")

    # ─── Cyclicals: Lynch BUY list — identify companies at the TROUGH ────────
    # Lynch's key insight: cyclicals are the OPPOSITE of normal stocks.
    # BUY when P/E is HIGH (earnings depressed at trough) → sell when P/E is LOW (peak earnings).
    # Also BUY when P/E is not available (reporting losses = maximum trough opportunity).
    # The goal is to catch cyclicals before earnings recover, not after.
    def _cyclical_filter(s):
        if not _is_common_stock(s): return False
        pio = s.get("piotroski")
        pb  = s.get("pb")
        pe  = s.get("pe")
        az  = s.get("altmanZ")

        # Must be in a recognised cyclical sector
        if s.get("sector") not in CYCLICAL_SECTORS:
            return False

        # Size: large enough to survive the trough
        if s.get("mktCap", 0) < 500e6:
            return False

        # Quality floor — must still be financially alive
        if pio is not None and pio < 4:
            return False

        # Exclude companies in imminent-bankruptcy territory
        if az is not None and az < 0.8:
            return False

        # Lynch BUY trough signals (need at least one):
        # A) Elevated P/E  (10–60) — earnings depressed but company not dead
        trough_pe    = (pe and 10 < pe < 65)
        # B) No PE / reporting losses — maximum earnings trough
        loss_trough  = (not pe and pb and 0 < pb < 2.0)
        # C) Very low P/B + revenue declining — sector in downturn but assets preserved
        asset_trough = (pb and 0 < pb < 0.9)

        return trough_pe or loss_trough or asset_trough

    def _cyclical_score(s):
        sc = 0
        pe  = s.get("pe")
        pb  = s.get("pb")
        fcf = s.get("fcfYield")
        de  = s.get("de")
        rg  = s.get("revGrowth")

        # ── P/E trough signal (INVERTED vs normal stocks) ──────────────
        # Lynch: high P/E at trough is a BUY, low P/E at peak is a SELL
        if not pe:
            sc += 18   # no earnings / loss = deepest trough = best entry point
        elif 20 < pe < 50:
            sc += 22   # classic trough band — depressed but not terminal
        elif 15 < pe <= 20:
            sc += 16   # slightly elevated — early-trough territory
        elif 10 < pe <= 15:
            sc += 8    # moderate — above average but maybe mid-cycle
        elif pe <= 10:
            sc += 2    # low PE = peak earnings — Lynch would be SELLING, not buying
        elif pe > 50:
            sc += 10   # very elevated — extreme trough or speculative losses

        # ── P/B: asset value preserved during earnings decline ──────────
        if pb and 0 < pb < 0.5:    sc += 18  # extreme asset discount
        elif pb and 0 < pb < 0.8:  sc += 13
        elif pb and 0 < pb < 1.2:  sc += 8
        elif pb and 0 < pb < 2.0:  sc += 4

        # ── FCF: still generating cash despite earnings compression ─────
        # Cash flow positive while earnings are down = best quality signal for cyclicals
        if fcf and fcf > 0.10:    sc += 18  # exceptional — earning cash at trough
        elif fcf and fcf > 0.07:  sc += 14
        elif fcf and fcf > 0.04:  sc += 9
        elif fcf and fcf > 0.01:  sc += 4

        # ── Revenue trend: at/near trough (declining or flat) is GOOD ───
        # Lynch: buy before revenue recovers, not after
        if rg and -0.20 <= rg < 0:      sc += 8   # revenue declining = near trough
        elif rg is None or rg == 0:      sc += 4   # flat = possibly at bottom
        elif rg and -0.40 <= rg < -0.20: sc += 5   # severe decline — maybe near bottom
        elif rg and 0 < rg < 0.08:       sc += 2   # slight recovery starting
        elif rg and rg >= 0.08:           sc -= 3  # revenue recovering = you may be late

        # ── Debt survival: must be able to outlast the downcycle ────────
        if de is not None and de < 0.3:   sc += 9
        elif de is not None and de < 0.7: sc += 6
        elif de is not None and de < 1.5: sc += 3

        # ── Piotroski: fundamental health despite depressed earnings ────
        pio = s.get("piotroski")
        if pio and pio >= 8:   sc += 8
        elif pio and pio >= 6: sc += 5
        elif pio and pio >= 4: sc += 2

        # ── Dividend maintained at trough = financial strength signal ───
        div = s.get("divYield")
        if div and div > 0.05:   sc += 6
        elif div and div > 0.02: sc += 3

        # ── EV/EBITDA: at trough, EV/EBITDA is high (same logic as P/E) ───
        ev_eb = s.get("evEbitda")
        if ev_eb and ev_eb > 20:    sc += 5   # elevated EV/EBITDA = earnings depressed
        elif ev_eb and ev_eb > 12:  sc += 3

        # ── 52-week positioning: near lows = more of the trough is priced in ──
        pvs52h = s.get("priceVs52H")  # price / 52wk high
        if pvs52h and pvs52h < 0.55:    sc += 10  # >45% off 52wk high = deep trough
        elif pvs52h and pvs52h < 0.70:  sc += 7
        elif pvs52h and pvs52h < 0.85:  sc += 4

        # ── Insider buying at trough = management confidence ────────────
        if s.get("insiderBuys", 0) >= 3:   sc += 10
        elif s.get("insiderBuys", 0) >= 1: sc += 5

        return sc

    _cy_headers = [
        "Rank", "Ticker", "Company", "Sector", "Price",
        "P/E", "EV/EBITDA", "P/B", "FCF Yield", "D/E", "Rev Growth",
        "Div Yield", "52w Pos", "Piotroski", "Altman Z",
        "MktCap ($B)", "Score", "🏦 Insider",
    ]
    _cy_widths = [5, 8, 22, 15, 8, 7, 9, 6, 8, 7, 9, 8, 7, 8, 8, 10, 6, 14]

    cyclicals = build_lynch_tab(wb, stocks, "Cyclicals", "6", _cyclical_filter, _cyclical_score,
                                "E65100",
                                "Lynch trough-buying: BUY at high/no P/E (depressed earnings) + low P/B + positive FCF.",
                                custom_headers=_cy_headers, custom_widths=_cy_widths)

    # ─── Turnarounds: recovering businesses — distinct from IV Discount ────────
    # Lynch: "Turnarounds are companies that have had serious problems and are now recovering."
    # Different from IV Discount (always-good business temporarily cheap):
    # Turnarounds had real problems (low Piotroski, earnings trough) and are now showing ACTIVE recovery.
    # Key: must show concrete recovery signals — not just cheap, but measurably improving.
    def _turnaround_filter(s):
        if not _is_common_stock(s): return False
        mos   = s.get("mos")
        pb    = s.get("pb")
        pio   = s.get("piotroski")
        rg    = s.get("revGrowth")
        eg    = s.get("epsGrowth")
        nig   = s.get("netIncomeGrowth")
        fcf   = s.get("fcfYield")
        az    = s.get("altmanZ")

        if s.get("mktCap", 0) < 300e6:  # must be substantial enough to recover
            return False

        # Turnarounds are NOT already-great businesses (those belong in IV Discount)
        # If Piotroski is already >= 8, it's a quality business — not a true turnaround
        if pio is not None and pio >= 8:
            return False

        # Fast growers masquerading as turnarounds: high ROIC + strong revenue growth = not distressed
        # These belong in Fast Growers, not here. A genuine turnaround has weak/recovering ROIC.
        roic_ta = s.get("roic"); rg_ta = s.get("revGrowth")
        if (roic_ta is not None and roic_ta > 0.20
                and rg_ta is not None and rg_ta > 0.25):
            return False

        # Not in active bankruptcy — some financial life remaining
        if az is not None and az < 0.5:
            return False

        # Valuation: beaten-down price (cheap relative to DCF or book)
        deep_discount = (mos and mos > 0.20)    # DCF says 20%+ undervalued
        asset_cheap   = (pb and 0 < pb < 0.8)   # trading well below book

        if not (deep_discount or asset_cheap):
            return False

        # B1: Liquidity gate — tight liquidity needs stronger evidence before qualifying
        _cr_b1 = s.get("currentRatio"); _ncr_b1 = s.get("netCashRatio")
        _required_signals = 3 if (_cr_b1 is not None and _cr_b1 < 1.0
                                   and (_ncr_b1 is None or _ncr_b1 < 0)) else 2

        # RECOVERY SIGNAL — must show at least two concrete improvement signals:
        # Without real recovery signals, it's just a cheap deteriorating business (value trap)
        recovery_signals = 0
        if rg  and rg  > 0.03:   recovery_signals += 1  # revenue growing again
        if eg  and eg  > 0:      recovery_signals += 1  # EPS improving
        if nig and nig > 0:      recovery_signals += 1  # net income improving
        if fcf and fcf > 0:      recovery_signals += 1  # cash flow positive
        if s.get("revGrowth5y") and s.get("revGrowth5y") > 0.05:
            recovery_signals += 1  # analysts expect sustained growth ahead
        if pio and pio >= 5:     recovery_signals += 1  # improving financial health
        # Beat rate >= 0.625 (5 of 8 quarters beating) = execution is consistently improving
        if s.get("beatRate") and s.get("beatRate") >= 0.625: recovery_signals += 1
        # B5: EPS beat streak — 3+ consecutive beats = management executing on recovery
        if s.get("epsBeatStreak") and s.get("epsBeatStreak") >= 3: recovery_signals += 1

        return recovery_signals >= _required_signals

    def _turnaround_score(s):
        sc = 0

        # ── Valuation cheapness (how big is the opportunity) ────────────
        mos = s.get("mos")
        if mos and mos > 0.50:    sc += 20
        elif mos and mos > 0.35:  sc += 15
        elif mos and mos > 0.20:  sc += 10

        pb = s.get("pb")
        if pb and 0 < pb < 0.4:   sc += 12
        elif pb and 0 < pb < 0.7: sc += 8
        elif pb and 0 < pb < 1.0: sc += 4

        # ── Revenue recovery momentum ────────────────────────────────────
        rg = s.get("revGrowth")
        if rg and rg > 0.20:   sc += 14   # strong recovery = high conviction
        elif rg and rg > 0.10: sc += 10
        elif rg and rg > 0.03: sc += 6

        # B2: Multi-year consecutive revenue recovery — distinguishes real turnarounds from blips
        _rg_prev_b2 = s.get("revGrowthPrev"); _rg_yr2_b2 = s.get("revGrowthYr2")
        if rg and rg > 0.03 and _rg_prev_b2 and _rg_prev_b2 > 0 and _rg_yr2_b2 and _rg_yr2_b2 > 0:
            sc += 10  # 3 consecutive recovery years — real turnaround, not a blip
        elif rg and rg > 0.03 and _rg_prev_b2 and _rg_prev_b2 > 0:
            sc += 5   # 2 consecutive recovery years — early confirmation

        # ── EPS / earnings recovery ──────────────────────────────────────
        eg = s.get("epsGrowth")
        if eg and eg > 0.30:   sc += 12
        elif eg and eg > 0.10: sc += 8
        elif eg and eg > 0:    sc += 4

        nig = s.get("netIncomeGrowth")
        if nig and nig > 0.30:   sc += 8
        elif nig and nig > 0.10: sc += 5
        elif nig and nig > 0:    sc += 2

        # ── Cash flow turning around ─────────────────────────────────────
        fcf = s.get("fcfYield")
        if fcf and fcf > 0.08:   sc += 12
        elif fcf and fcf > 0.04: sc += 8
        elif fcf and fcf > 0:    sc += 4

        # ── Financial health improving (Piotroski 5-7 = recovering zone) ─
        pio = s.get("piotroski")
        if pio and pio >= 7:     sc += 10  # well into recovery
        elif pio and pio >= 5:   sc += 6
        elif pio and pio >= 3:   sc += 2

        # ── Altman Z: grey zone = turnaround in progress ─────────────────
        az = s.get("altmanZ")
        if az and az > 3.0:       sc += 5   # back in safe zone = recovery confirmed
        elif az and az > 1.8:     sc += 8   # grey zone, improving = best turnaround signal
        elif az and az > 0.8:     sc += 3   # still distressed but surviving

        # ── Analyst growth outlook confirms the turnaround ───────────────
        rg5 = s.get("revGrowth5y")
        if rg5 and rg5 > 0.15:   sc += 7
        elif rg5 and rg5 > 0.08: sc += 4
        elif rg5 and rg5 > 0.03: sc += 2

        # ── Earnings beat rate — consistently beating = execution improving ─
        br = s.get("beatRate")
        if br and br >= 0.875:  sc += 10  # beat 7/8 quarters = high-conviction recovery
        elif br and br >= 0.75: sc += 7
        elif br and br >= 0.625: sc += 4

        # ── 52-week positioning: deep drawdown = more of the pain is priced in ─
        pvs52h = s.get("priceVs52H")
        if pvs52h and pvs52h < 0.45:    sc += 8   # >55% off high = deep distress priced in
        elif pvs52h and pvs52h < 0.65:  sc += 5

        # ── Insider buying = management backs the recovery ───────────────
        if s.get("insiderBuys", 0) >= 3:   sc += 12  # big signal for turnarounds
        elif s.get("insiderBuys", 0) >= 1: sc += 6

        return sc

    _ta_headers = [
        "Rank", "Ticker", "Company", "Sector", "Price",
        "MoS", "P/B", "Rev Growth", "EPS Growth", "FCF Yield",
        "Beat Rate", "52w Pos", "52w vs Low", "Net D/E", "Piotroski", "Altman Z", "Rev Growth 5Y",
        "MktCap ($B)", "Score", "🏦 Insider",
    ]
    _ta_widths = [5, 8, 22, 15, 8, 7, 6, 9, 9, 9, 8, 7, 9, 7, 8, 8, 11, 10, 6, 14]

    turnarounds = build_lynch_tab(wb, stocks, "Turnarounds", "7", _turnaround_filter, _turnaround_score,
                                  "B71C1C",
                                  "Recovery plays: beaten-down price + ≥2 active recovery signals. NOT already-great businesses.",
                                  custom_headers=_ta_headers, custom_widths=_ta_widths)

    # ─── Asset Plays: Hidden value vs asset prices — Lynch style ───────────
    # Lynch: "Find companies where specific assets are worth more than the whole."
    # Five qualifying paths:
    #   1. Classic: P/B < 1 (market undervalues the book)
    #   2. Tangible book > price (ignoring goodwill/intangibles reveals real asset discount)
    #   3. Cash fortress: very high liquidity + minimal debt + moderate valuation
    #   4. Net cash positive: cash + investments > total debt (company has net cash per share)
    #   5. Graham net-net: trading below net working capital — Graham's deepest value
    _ASSET_HEAVY_SECTORS = {
        "Financial Services", "Real Estate", "Energy", "Basic Materials",
        "Utilities", "Industrials",
    }

    def _asset_play_filter(s):
        if not _is_common_stock(s): return False
        pb     = s.get("pb")
        tb     = s.get("tangibleBook")
        price  = s.get("price", 0)
        pio    = s.get("piotroski")
        mktcap = s.get("mktCap", 0)
        cr     = s.get("currentRatio")
        de     = s.get("de")
        ncr    = s.get("netCashRatio")   # net cash / price
        gnn    = s.get("grahamNetNet")   # Graham net-net per share

        if mktcap < 150e6:  # no micro-cap junk
            return False
        # Quality floor — not a financial basket case
        if pio is not None and pio < 4:
            return False

        # Path 1: Classic P/B < 1 (book value > market cap)
        below_book = (pb and 0 < pb < 1.0)

        # Path 2: Trading below TANGIBLE book — strips goodwill/intangibles
        # (a company with P/B=1.2 but tangible book = 1.5× price is a real asset play)
        below_tangible = (tb and tb > 0 and price > 0 and (tb / price) >= 0.95)

        # Path 3: Cash fortress — so much net cash it distorts value
        # Current ratio >3.0 + D/E < 0.3 + P/B < 2.0 = assets >> liabilities >> market cap
        cash_fortress = (cr and cr > 3.0
                         and de is not None and de < 0.3
                         and pb and 0 < pb < 2.0)

        # Path 4: Net cash positive — company has more cash than debt
        # ncr > 0.25 means net cash > 25% of price (significant "freebie")
        net_cash_play = (ncr is not None and ncr > 0.25)

        # Path 5: Graham net-net — trading below net working capital per share
        # This is the deepest possible value — Graham called it the "margin of safety"
        graham_net_net = (gnn is not None and price > 0 and gnn > price * 0.5)

        if not (below_book or below_tangible or cash_fortress or net_cash_play or graham_net_net):
            return False

        # Must have some financial activity — not a zombie shell
        has_activity = (
            s.get("fcfYield") is not None or
            (s.get("pe") and s.get("pe") > 0) or
            s.get("revGrowth") is not None or
            s.get("roe") is not None
        )
        return has_activity

    def _asset_play_score(s):
        pb    = s.get("pb")
        tb    = s.get("tangibleBook")
        price = s.get("price", 0)
        sc    = 0

        # ── P/B discount ────────────────────────────────────────────
        if pb and 0 < pb < 1.0:
            sc += (1.0 - pb) * 32       # 0.5× P/B → 16 pts, 0.2× → 25.6 pts
        elif pb and 1.0 <= pb < 1.5:
            sc += (1.5 - pb) * 6        # partial credit 1.0–1.5×

        # ── TANGIBLE BOOK vs price — the gold standard for Lynch asset plays ──
        # When tangible book > price, you're buying real assets at a discount
        if tb and tb > 0 and price > 0:
            tb_ratio = tb / price
            if tb_ratio >= 3.0:   sc += 24  # tangible book is 3× price — extraordinary
            elif tb_ratio >= 2.0: sc += 18
            elif tb_ratio >= 1.5: sc += 13
            elif tb_ratio >= 1.2: sc += 8
            elif tb_ratio >= 1.0: sc += 4   # at or below tangible book
            elif tb_ratio >= 0.8: sc += 1

        # ── Net cash per share — Lynch's "freebie" metric ───────────────────
        # If a company has more cash than debt, you're being paid to hold the business
        ncr = s.get("netCashRatio")
        if ncr is not None:
            if ncr >= 0.75:   sc += 22  # net cash > 75% of price = extraordinary freebie
            elif ncr >= 0.50: sc += 17
            elif ncr >= 0.30: sc += 12
            elif ncr >= 0.15: sc += 7
            elif ncr >= 0.05: sc += 3

        # ── Graham Net-Net — deepest possible value signal ──────────────────
        # Trading below net working capital is Graham's ultimate cheap stock
        gnn = s.get("grahamNetNet")
        if gnn is not None and price > 0:
            gnn_ratio = gnn / price
            if gnn_ratio >= 2.0:   sc += 18  # trading at less than half net working capital
            elif gnn_ratio >= 1.5: sc += 13
            elif gnn_ratio >= 1.0: sc += 8   # at net working capital per share
            elif gnn_ratio >= 0.7: sc += 4

        # ── Cash / liquidity quality ─────────────────────────────────
        cr = s.get("currentRatio")
        if cr and cr > 4.0:   sc += 8   # cash-rich relative to near-term liabilities
        elif cr and cr > 3.0: sc += 5
        elif cr and cr > 2.0: sc += 3
        elif cr and cr > 1.5: sc += 1

        # ── Debt load — key for asset plays (low debt = cleaner asset story) ──
        de = s.get("de")
        if de is not None and de < 0.2:   sc += 8
        elif de is not None and de < 0.5: sc += 5
        elif de is not None and de < 1.0: sc += 2

        # ── FCF yield — the asset base is generating cash ───────────
        fcf = s.get("fcfYield")
        if fcf and fcf > 0.12:   sc += 10
        elif fcf and fcf > 0.08: sc += 7
        elif fcf and fcf > 0.05: sc += 4
        elif fcf and fcf > 0.02: sc += 1

        # ── Piotroski — financial health of the asset base ──────────
        pio = s.get("piotroski")
        if pio and pio >= 8:   sc += 8
        elif pio and pio >= 7: sc += 5
        elif pio and pio >= 5: sc += 2

        # ── ROE — the assets are actually working ───────────────────
        roe = s.get("roe")
        if roe and roe > 0.15:   sc += 6
        elif roe and roe > 0.08: sc += 3
        elif roe and roe > 0.02: sc += 1

        # ── DCF confirms cheapness ───────────────────────────────────
        mos = s.get("mos")
        if mos and mos > 0.40:   sc += 6
        elif mos and mos > 0.25: sc += 4
        elif mos and mos > 0.10: sc += 2

        # ── Asset-heavy sector bonus (book values are more tangible) ──
        if s.get("sector") in _ASSET_HEAVY_SECTORS:
            sc += 3

        # ── Insider buying — insiders see the hidden value ───────────
        if s.get("insiderBuys", 0) >= 3:   sc += 7
        elif s.get("insiderBuys", 0) >= 1: sc += 3

        return sc

    _ap_headers = [
        "Rank", "Ticker", "Company", "Sector", "Price",
        "P/B", "Tangible Book", "Net Cash/Sh", "Graham NN",
        "Curr Ratio", "D/E", "FCF Yield", "ROE", "MoS", "Piotroski",
        "MktCap ($B)", "Score", "🏦 Insider",
    ]
    _ap_widths = [5, 8, 22, 15, 8, 6, 12, 11, 10, 9, 7, 9, 7, 7, 8, 10, 6, 14]

    asset_plays = build_lynch_tab(wb, stocks, "Asset Plays", "8", _asset_play_filter, _asset_play_score,
                                  "0D47A1", "P/B<1, tangible book, net cash, or Graham net-net. Lynch hidden value.",
                                  custom_headers=_ap_headers, custom_widths=_ap_widths)

    quality_compounders = build_quality_compounders(wb, stocks)

    # Sector Valuations + ETF Rotation
    _sector_rows, _etf_rets = build_sector_valuations(wb, stocks)
    _etf_rows = build_sector_etf_rotation(wb, stocks, _etf_rets)

    # Insider Buying
    build_insider_tab(wb, stocks, insider_data)

    build_picks_tracking(wb, stocks)

    # Auto-log strategy picks every run (deduped by date+ticker+strategy)
    current_prices = {t: s["price"] for t, s in stocks.items() if s.get("price")}
    log_picks({
        "IV Discount": iv_rows, "Quality Compounders": quality_compounders,
        "Stalwarts": stalwarts, "Fast Growers": fast_growers,
        "Turnarounds": turnarounds, "Slow Growers": slow_growers,
        "Cyclicals": cyclicals, "Asset Plays": asset_plays,
    }, current_prices)

    # ── AI analysis (single call — result shared across Overview + AI tab) ──
    picks_data = {
        "IV Discount (Buffett/DCF)": iv_rows,
        "Quality Compounders (Buffett)": quality_compounders,
        "Stalwarts (Lynch)": stalwarts,
        "Fast Growers (Lynch)": fast_growers,
        "Turnarounds (Lynch)": turnarounds,
        "Slow Growers / Income (Lynch)": slow_growers,
        "Cyclicals (Lynch)": cyclicals,
        "Asset Plays (Lynch)": asset_plays,
    }
    phase_start("ai_analysis", "Running multi-agent AI analysis (4 specialists + judge)")
    ai_result = call_claude_analysis(picks_data, stocks, macro=macro_data)

    # Auto-log AI picks every run (no flag needed)
    if ai_result:
        log_ai_picks(ai_result, stocks)

    # Tab 1b: AI Top Picks (uses pre-created sheet so it stays in position 2)
    build_ai_picks_tab(wb, ai_result, stocks, ws=ws_ai_picks)

    # ── AI Portfolio Manager ─────────────────────────────────────────
    phase_start("portfolio", "Running AI Portfolio Manager")
    print("\n  💼 Running AI Portfolio Manager...")
    portfolio = load_portfolio()

    # Reuse the candidate pool + sector context built inside call_claude_analysis
    # by re-building the same meta-score pool here for the PM context
    _pm_meta = {}
    _pm_strat_short = {
        "IV Discount (Buffett/DCF)": "IV Discount",
        "Quality Compounders (Buffett)": "Quality Compounder",
        "Stalwarts (Lynch)": "Stalwart",
        "Fast Growers (Lynch)": "Fast Grower",
        "Turnarounds (Lynch)": "Turnaround",
        "Slow Growers / Income (Lynch)": "Slow Grower",
        "Cyclicals (Lynch)": "Cyclical",
        "Asset Plays (Lynch)": "Asset Play",
    }
    for _tab_name, _rows in picks_data.items():
        _short = _pm_strat_short.get(_tab_name, _tab_name)
        for _ri, _row in enumerate(_rows[:15]):
            _t = _row.get("Ticker", "?")
            _sc = _row.get("Score", 0) or 0
            if _t not in _pm_meta:
                _pm_meta[_t] = {"strategies": [], "best_rank": _ri+1, "max_score": _sc, "row": _row}
            _pm_meta[_t]["strategies"].append(_short)
            if _sc > _pm_meta[_t]["max_score"]: _pm_meta[_t]["max_score"] = _sc
            if _ri+1 < _pm_meta[_t]["best_rank"]: _pm_meta[_t]["best_rank"] = _ri+1

    # Build compact candidates block for PM (top 40 by meta-score)
    _pm_top = sorted(_pm_meta.values(),
                     key=lambda x: -(len(x["strategies"])*25 + x["max_score"]*0.4))[:40]

    def _pm_fmt(m):
        r2 = m["row"]; t2 = r2.get("Ticker","?"); s2 = stocks.get(t2,{})
        strats2 = "+".join(m["strategies"])
        parts2 = [f"{t2}({r2.get('Company','')[:18]}) [{strats2}]"]
        roic2 = s2.get("roic"); fcf2 = s2.get("fcfYield")
        peg2 = s2.get("fwdPEG") or s2.get("peg"); rg2 = s2.get("revGrowth")
        if roic2: parts2.append(f"ROIC={roic2*100:.0f}%")
        if fcf2:  parts2.append(f"FCF={fcf2:.1%}")
        if peg2:  parts2.append(f"PEG={peg2:.2f}")
        if rg2:   parts2.append(f"RG={rg2:+.0%}")
        mos2 = s2.get("mos")
        if mos2:  parts2.append(f"MoS={mos2:.0%}")
        return "  " + " | ".join(parts2)

    _pm_candidates = "\n".join(_pm_fmt(m) for m in _pm_top)

    # Simple sector context for PM
    _pm_sec_lines = []
    _pm_sec = {}
    for _sv in stocks.values():
        _sec = _sv.get("sector","Unknown")
        if _sec not in _pm_sec: _pm_sec[_sec] = []
        _peg = _sv.get("peg")
        if _peg and 0 < _peg < 15: _pm_sec[_sec].append(_peg)
    for _sec, _pegs in sorted(_pm_sec.items(), key=lambda x: (sum(x[1])/len(x[1])) if x[1] else 99):
        if len(_pegs) >= 5:
            _pm_sec_lines.append(f"  {_sec}: med PEG={sorted(_pegs)[len(_pegs)//2]:.1f} ({len(_pegs)} stocks)")
    _pm_sector_block = "\n".join(_pm_sec_lines[:12])

    # ── Auto-exit holdings in sectors excluded from quality portfolio ────
    _EXCLUDED_PTF_SECTORS = {"Basic Materials", "Real Estate"}
    _today_auto = datetime.date.today().isoformat()
    _auto_sell_tickers = []
    for _ah in list(portfolio.get("holdings", [])):
        _at = _ah.get("ticker", "")
        _asector = stocks.get(_at, {}).get("sector") or _ah.get("sector") or ""
        if _asector in _EXCLUDED_PTF_SECTORS:
            _alive_p = fetch_live_price(_at) or _ah.get("entry_price", 0) or 0
            _ashares = _ah.get("shares", 0)
            _aproceeds = round(_alive_p * _ashares, 2)
            _aret = ((_alive_p / _ah["entry_price"]) - 1) if (_ah.get("entry_price") and _ah["entry_price"] > 0) else 0
            portfolio["cash"] = round(portfolio.get("cash", 0) + _aproceeds, 2)
            portfolio["transactions"].append({
                "date": _today_auto,
                "action": "SELL",
                "ticker": _at,
                "company": _ah.get("company", ""),
                "shares": _ashares,
                "price": _alive_p,
                "proceeds": _aproceeds,
                "return_pct": round(_aret * 100, 2),
                "rationale": f"Auto-exit: {_asector} excluded from quality growth portfolio",
            })
            _auto_sell_tickers.append(_at)
            print(f"    🚫 Auto-sell {_at} ({_asector}) @ ${_alive_p:.2f} ({_aret:+.1%}) — excluded sector")
    if _auto_sell_tickers:
        portfolio["holdings"] = [_h for _h in portfolio["holdings"] if _h["ticker"] not in _auto_sell_tickers]
        save_portfolio(portfolio)

    pm_decisions = run_portfolio_manager(portfolio, _pm_candidates, _pm_sector_block, stocks)
    if pm_decisions:
        portfolio = apply_portfolio_decisions(portfolio, pm_decisions, stocks)
        save_portfolio(portfolio)
    portfolio["_last_thesis"] = pm_decisions.get("portfolio_thesis", "")

    # Fetch SPY data for portfolio tab (reuse from picks tracking if already fetched)
    _ptf_spy_prices = {}; _ptf_spy_today = None
    if portfolio.get("started"):
        _ptf_spy_prices = fetch_spy_history(portfolio["started"])
        _ptf_spy_today  = fetch_live_price("SPY")

    # Tab 1c: AI Portfolio (uses pre-created sheet so it stays in position 3)
    _ptf_nav, _ptf_ret, _ptf_spy_ret = build_portfolio_tab(
        wb, portfolio, stocks, _ptf_spy_prices, _ptf_spy_today,
        ws=ws_ai_portf)

    # Overview (last — needs data from all tabs)
    build_overview_tab(ws_overview, stocks, iv_rows, stalwarts, fast_growers, turnarounds,
                       slow_growers, cyclicals, asset_plays, quality_compounders,
                       _fmp_call_count, ai=ai_result,
                       portfolio=portfolio, portfolio_nav=_ptf_nav,
                       portfolio_ret=_ptf_ret, portfolio_spy_ret=_ptf_spy_ret,
                       macro=macro_data)

    # Save
    phase_start("save", "Saving Excel + HTML dashboard")
    wb.save(output_file)

    # Generate HTML dashboard (same data, no extra fetching)
    html_content = build_html_report(
        stocks, iv_rows, stalwarts, fast_growers, turnarounds,
        slow_growers, cyclicals, asset_plays, quality_compounders,
        sector_rows=_sector_rows, etf_rows=_etf_rows,
        ai=ai_result, macro=macro_data,
        portfolio=portfolio, fmp_call_count=_fmp_call_count,
    )
    html_file = output_file.replace(".xlsx", ".html")
    with open(html_file, "w", encoding="utf-8") as _hf:
        _hf.write(html_content)
    print(f"  🌐 HTML dashboard: {html_file}")

    # Push to GitHub Pages (if configured)
    pages_url = push_html_to_github(html_content)
    if pages_url:
        print(f"  🌐 Live at: {pages_url}")

    total_elapsed = time.time() - _run_start
    n_picks = len(ai_result.get("picks", [])) if ai_result else 0

    print(f"\n{'=' * 65}")
    print(f"  ✅ DONE! Saved to: {output_file}")
    print(f"  ⏱  Total run time: {_fmt_elapsed(total_elapsed)}")
    print(f"  📊 FMP API calls: {_fmp_call_count}")
    print(f"  📦 Cache: {CACHE_FILE} (valid {CACHE_DAYS} days)")
    if pages_url:
        print(f"  🌐 Dashboard: {pages_url}")
    if NTFY_TOPIC:
        print(f"  🔔 Sending phone notification to ntfy.sh/{NTFY_TOPIC}...")
    print(f"{'=' * 65}")

    # Phone notification — includes clickable dashboard link
    notify_phone(
        title="FMP Screener Done ✅",
        message=(
            f"Run complete in {_fmt_elapsed(total_elapsed)}\n"
            f"{n_picks} AI picks | {_fmp_call_count} FMP calls\n"
            + (f"📱 View: {pages_url}" if pages_url else f"File: {os.path.basename(output_file)}")
        ),
        tags="chart_with_upwards_trend",
    )


if __name__ == "__main__":
    main()
