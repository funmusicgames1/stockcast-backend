"""
fetcher.py — Pulls stock price data directly from Yahoo Finance v8 API.
Falls back to Financial Modeling Prep (FMP) API for any tickers that fail.
Aborts pipeline if more than 100 tickers fail both sources.
"""

import os
import time
import random
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ── Session setup ──────────────────────────────────────────────────────────────
try:
    from curl_cffi import requests as curl_requests
    _SESSION = curl_requests.Session(impersonate="chrome110")
    logger.info("Using curl_cffi Chrome impersonation session")
except ImportError:
    import requests as curl_requests
    _SESSION = curl_requests.Session()
    _SESSION.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    })
    logger.warning("curl_cffi not available, falling back to requests")

import requests as _std_requests  # standard requests for FMP (no TLS spoofing needed)

# ── API settings ───────────────────────────────────────────────────────────────
YF_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
YF_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}
FMP_BASE = "https://financialmodelingprep.com/api/v3"
ABORT_THRESHOLD = 100  # abort pipeline if more than this many tickers fail both sources

# Yahoo doesn't use plain index tickers the way you'd expect — "SPX" is
# actually a real, unrelated company (SPX Technologies), not the S&P 500
# index. The index needs Yahoo's own symbol (^GSPC). Applied only at the
# URL-fetch step; everywhere else (stock_data keys, Supabase rows, JSON
# output) keeps using "SPX" as the display/storage ticker.
YF_SYMBOL_ALIAS = {"SPX": "^GSPC"}


def _yf_symbol(ticker: str) -> str:
    return YF_SYMBOL_ALIAS.get(ticker, ticker)

# ── Stock universe ─────────────────────────────────────────────────────────────
STOCK_UNIVERSE = [
    # Broad market indices / ETFs
    "SPY", "QQQ", "SPX",
    # Mega cap tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "ORCL", "IBM", "ADBE",
    # Semiconductors
    "AMD", "INTC", "QCOM", "AVGO", "MU", "AMAT", "LRCX", "KLAC", "MRVL", "NXPI", "TXN", "ADI",
    # Large cap finance / banking
    "JPM", "BAC", "GS", "MS", "V", "MA", "AXP", "WFC", "C", "BLK", "SCHW", "COF",
    # Fintech / Capital Markets
    "HOOD", "SOFI", "COIN", "UPST", "AFRM", "NU", "PYPL", "BILL",
    # Healthcare / Pharma / Biotech
    "JNJ", "PFE", "MRNA", "UNH", "CVS", "ABT", "LLY", "MRK", "BMY", "GILD",
    "AMGN", "REGN", "VRTX", "BIIB", "DXCM", "ISRG", "HCA", "CI", "HUM",
    # Consumer staples
    "WMT", "TGT", "COST", "MCD", "SBUX", "NKE", "PG", "KO", "PEP",
    "CL", "GIS", "HSY", "MO",
    # Consumer discretionary
    "HD", "LOW", "TJX", "BKNG", "MAR", "HLT", "F", "GM", "RIVN",
    "NIO", "LI", "XPEV", "CVNA",
    # Energy
    "XOM", "CVX", "OXY", "SLB", "COP", "EOG", "PSX", "VLO", "MPC", "HAL",
    # AI Infrastructure / Bitcoin Mining
    "IREN", "CORZ", "MARA", "CLSK", "RIOT", "HUT",
    # Industrial / Aerospace / Defense
    "BA", "GE", "CAT", "HON", "LMT", "RTX", "NOC", "GD", "MMM", "DE", "ITW", "ETN",
    # Aerospace / Space Technology
    "RKLB", "ASTS",
    # Media / Telecom / Entertainment
    "DIS", "NFLX", "T", "VZ", "CMCSA", "SPOT", "TTWO", "EA", "LYV",
    # AI / Cloud / SaaS
    "PLTR", "CRM", "SNOW", "DDOG", "NET", "MDB", "ZS", "PANW", "CRWD",
    "NOW", "WDAY", "VEEV", "HUBS", "TEAM", "OKTA", "ZM",
    # REITs
    "O", "AMT", "PLD", "EQIX", "CCI", "WELL", "SPG",
    # Materials / Chemicals
    "LIN", "APD", "NEM", "FCX", "AA",
    # Transportation / Logistics
    "UPS", "FDX", "DAL", "UAL", "AAL", "LUV", "UBER", "LYFT", "DASH",
    # High volatility
    "GME", "AMC",
    # Biotech high volatility
    "SRPT", "EXAS", "BEAM", "EDIT",
]
STOCK_UNIVERSE = list(dict.fromkeys(STOCK_UNIVERSE))


# ── Helpers ────────────────────────────────────────────────────────────────────
def _compute_metrics(closes: list, volumes: list) -> dict:
    """Compute all derived metrics from a list of closes and volumes."""
    current_price = closes[-1]
    prev_close    = closes[-2]
    week_ago      = closes[-6]  if len(closes) >= 6  else prev_close
    month_ago     = closes[-22] if len(closes) >= 22 else prev_close

    daily_change_pct   = ((current_price - prev_close) / prev_close) * 100
    weekly_change_pct  = ((current_price - week_ago)   / week_ago)   * 100
    monthly_change_pct = ((current_price - month_ago)  / month_ago)  * 100

    avg_volume    = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else (sum(volumes) / len(volumes) if volumes else 1)
    recent_volume = volumes[-1] if volumes else 0
    volume_ratio  = recent_volume / avg_volume if avg_volume > 0 else 1.0

    momentum_score = 50
    momentum_score += max(-20, min(20, daily_change_pct   * 5))
    momentum_score += max(-15, min(15, weekly_change_pct  * 2))
    momentum_score += max(-15, min(15, monthly_change_pct))
    if volume_ratio > 1.5:
        momentum_score += min((volume_ratio - 1) * 5, 10)
    momentum_score = max(0, min(100, momentum_score))

    return {
        "current_price":       round(current_price, 2),
        "prev_close":          round(prev_close, 2),
        "daily_change_pct":    round(daily_change_pct, 2),
        "weekly_change_pct":   round(weekly_change_pct, 2),
        "monthly_change_pct":  round(monthly_change_pct, 2),
        "volume_ratio":        round(volume_ratio, 2),
        "momentum_score":      round(momentum_score, 1),
        "avg_volume":          int(avg_volume),
        "recent_volume":       int(recent_volume),
    }


# ── Primary source: Yahoo Finance v8 API ──────────────────────────────────────
def _fetch_yf(ticker: str, days: int = 90) -> dict | None:
    """Fetch OHLCV from Yahoo Finance v8 API. Returns closes+volumes or None."""
    end_ts   = int(datetime.today().timestamp())
    start_ts = int((datetime.today() - timedelta(days=days)).timestamp())
    url      = f"{YF_BASE}{_yf_symbol(ticker)}"
    params   = {"period1": start_ts, "period2": end_ts, "interval": "1d",
                 "includePrePost": "false", "events": "div,splits"}
    try:
        resp = _SESSION.get(url, headers=YF_HEADERS, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        data   = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None
        r      = result[0]
        closes  = r.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
        volumes = r.get("indicators", {}).get("quote",    [{}])[0].get("volume",   [])
        clean_c, clean_v = [], []
        for c, v in zip(closes, volumes):
            if c is not None:
                clean_c.append(float(c))
                clean_v.append(float(v) if v is not None else 0.0)
        if len(clean_c) < 5:
            return None
        return {"closes": clean_c, "volumes": clean_v}
    except Exception:
        return None


# ── Fallback source: Financial Modeling Prep ──────────────────────────────────
def _fetch_fmp_single(ticker: str, fmp_key: str) -> dict | None:
    """
    Fetch a single ticker quote from FMP.
    Used when bulk endpoint returns 403 (free tier restriction).
    """
    url = f"{FMP_BASE}/quote/{ticker}"
    params = {"apikey": fmp_key}
    try:
        resp = _std_requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, list) or not data:
            return None
        item = data[0]
        price = item.get("price")
        prev  = item.get("previousClose")
        vol   = item.get("volume", 0)
        avg_v = item.get("avgVolume", vol or 1)
        if not price or not prev:
            return None
        # Build synthetic closes list sufficient for metric computation
        synthetic_closes  = [prev] * 21 + [price]
        synthetic_volumes = [avg_v] * 21 + [vol]
        return {"closes": synthetic_closes, "volumes": synthetic_volumes}
    except Exception:
        return None


def _fetch_fmp_batch(tickers: list) -> dict:
    """
    Fetch quote data for a list of tickers from FMP.
    Tries bulk endpoint first; falls back to individual requests if 403.
    Returns {ticker: {closes: [...], volumes: [...]}} for successful fetches.
    """
    fmp_key = os.getenv("FMP_API_KEY")
    if not fmp_key:
        logger.warning("FMP_API_KEY not set — cannot use FMP fallback")
        return {}

    results = {}

    # Try bulk endpoint first (works on paid tier)
    bulk_failed = False
    chunk_size  = 50
    for i in range(0, len(tickers), chunk_size):
        chunk   = tickers[i:i + chunk_size]
        symbols = ",".join(chunk)
        url     = f"{FMP_BASE}/quote/{symbols}"
        params  = {"apikey": fmp_key}
        try:
            resp = _std_requests.get(url, params=params, timeout=15)
            if resp.status_code == 403:
                logger.warning("[FMP] Bulk endpoint returned 403 — switching to individual requests")
                bulk_failed = True
                break
            if resp.status_code != 200:
                logger.warning(f"[FMP] Bulk endpoint returned HTTP {resp.status_code}")
                continue
            data = resp.json()
            if not isinstance(data, list):
                continue
            for item in data:
                ticker = item.get("symbol")
                price  = item.get("price")
                prev   = item.get("previousClose")
                vol    = item.get("volume", 0)
                avg_v  = item.get("avgVolume", vol or 1)
                if ticker and price and prev:
                    results[ticker] = {
                        "closes":  [prev] * 21 + [float(price)],
                        "volumes": [avg_v] * 21 + [float(vol or 0)],
                    }
        except Exception as e:
            logger.warning(f"[FMP] Bulk fetch error: {e}")
        time.sleep(0.5)

    # Fall back to individual requests if bulk failed with 403
    if bulk_failed:
        logger.info(f"[FMP] Fetching {len(tickers)} tickers individually...")
        for ticker in tickers:
            raw = _fetch_fmp_single(ticker, fmp_key)
            if raw:
                results[ticker] = raw
                logger.info(f"[FMP] ✓ {ticker}")
            else:
                logger.warning(f"[FMP] ✗ {ticker} — no data")
            time.sleep(0.3)

    return results


def _fetch_fmp_history(ticker: str, days: int = 90) -> dict | None:
    """
    Fetch historical daily prices from FMP for a single ticker.
    Used as a more accurate fallback when the bulk quote is insufficient.
    """
    fmp_key = os.getenv("FMP_API_KEY")
    if not fmp_key:
        return None
    end_date   = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    url    = f"{FMP_BASE}/historical-price-full/{ticker}"
    params = {"from": start_date, "to": end_date, "apikey": fmp_key}
    try:
        resp = _std_requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        data    = resp.json()
        history = data.get("historical", [])
        if len(history) < 5:
            return None
        # FMP returns newest first — reverse to chronological
        history.reverse()
        closes  = [float(d["adjClose"]) for d in history if d.get("adjClose")]
        volumes = [float(d["volume"])   for d in history if d.get("volume")]
        if len(closes) < 5:
            return None
        return {"closes": closes, "volumes": volumes}
    except Exception:
        return None


# ── Main fetch function ────────────────────────────────────────────────────────
def fetch_stock_data() -> dict:
    """
    Fetch price history for all stocks.
    Primary: Yahoo Finance v8 API via curl_cffi Chrome impersonation.
    Fallback: FMP historical API for any tickers that fail Yahoo Finance.
    Aborts if more than ABORT_THRESHOLD tickers fail both sources.
    """
    logger.info(f"Fetching data for {len(STOCK_UNIVERSE)} stocks...")
    logger.info("Warming up connection to Yahoo Finance...")
    time.sleep(2)

    stock_data  = {}
    yf_failed   = []
    both_failed = []

    # ── Pass 1: Yahoo Finance ──────────────────────────────────────────
    batch_size = 20
    batches = [STOCK_UNIVERSE[i:i + batch_size] for i in range(0, len(STOCK_UNIVERSE), batch_size)]

    for batch_num, batch in enumerate(batches):
        logger.info(f"[YF] Batch {batch_num + 1}/{len(batches)} ({len(batch)} tickers)...")
        for ticker in batch:
            raw = _fetch_yf(ticker)
            if raw is None:
                yf_failed.append(ticker)
                continue
            try:
                stock_data[ticker] = _compute_metrics(raw["closes"], raw["volumes"])
            except Exception as e:
                logger.warning(f"Could not process {ticker}: {e}")
                yf_failed.append(ticker)
            time.sleep(0.3 + random.uniform(0, 0.2))

        if batch_num < len(batches) - 1:
            pause = 3 + random.uniform(0, 1)
            logger.info(f"[YF] Batch {batch_num + 1} done. Pausing {pause:.1f}s...")
            time.sleep(pause)

    logger.info(f"[YF] Done. Success: {len(stock_data)}, Failed: {len(yf_failed)}")

    # ── Pass 2: FMP fallback for YF failures ──────────────────────────
    if yf_failed:
        logger.info(f"[FMP] Fetching fallback for {len(yf_failed)} failed tickers...")
        fmp_results = _fetch_fmp_batch(yf_failed)

        # For tickers not in bulk result, try historical endpoint
        still_missing = [t for t in yf_failed if t not in fmp_results]
        if still_missing:
            logger.info(f"[FMP] Trying historical endpoint for {len(still_missing)} tickers...")
            for ticker in still_missing:
                raw = _fetch_fmp_history(ticker)
                if raw:
                    fmp_results[ticker] = raw
                time.sleep(0.3)

        for ticker, raw in fmp_results.items():
            try:
                stock_data[ticker] = _compute_metrics(raw["closes"], raw["volumes"])
                logger.info(f"[FMP] Recovered {ticker}")
            except Exception as e:
                logger.warning(f"[FMP] Could not process {ticker}: {e}")

        both_failed = [t for t in yf_failed if t not in stock_data]
        if both_failed:
            logger.warning(f"Both sources failed for {len(both_failed)} tickers: {both_failed}")

    # ── Abort check ────────────────────────────────────────────────────
    if len(both_failed) > ABORT_THRESHOLD:
        raise RuntimeError(
            f"ABORT: {len(both_failed)} tickers failed both Yahoo Finance and FMP "
            f"(threshold: {ABORT_THRESHOLD}). Pipeline stopped to avoid bad predictions."
        )

    logger.info(f"Final stock data: {len(stock_data)} tickers. Skipped: {len(both_failed)}")
    return stock_data


# ── Actual prices for yesterday's performance ─────────────────────────────────
def fetch_actual_prices(tickers: list, date_str: str) -> dict:
    """
    Fetch actual closing prices for a specific date.
    Primary: Yahoo Finance v8 API. Fallback: FMP historical.
    Returns {ticker: close_price}.
    """
    logger.info(f"Fetching actual prices for {date_str} ({len(tickers)} tickers)...")

    target   = datetime.strptime(date_str, "%Y-%m-%d")
    start_ts = int((target - timedelta(days=5)).timestamp())
    end_ts   = int((target + timedelta(days=2)).timestamp())

    actuals    = {}
    yf_missing = []

    # Pass 1: Yahoo Finance
    # IMPORTANT: match the bar whose own calendar date equals date_str exactly.
    # Do NOT walk backward to the nearest earlier non-null close here — if the
    # exact day's bar is missing or still null (e.g. the day's adjclose hasn't
    # finalized yet, shortly after close), silently substituting an older
    # day's close would make two different requested dates (e.g. "today" vs
    # "yesterday") return identical prices without ever surfacing as missing,
    # which defeats both the FMP fallback below and the caller's own explicit
    # weekend/holiday fallback (main.py walks further back on purpose when it
    # wants that). Exact-date matching keeps "no data for this date" visibly
    # missing so it gets handled deliberately instead of silently wrong.
    for ticker in tickers:
        url    = f"{YF_BASE}{_yf_symbol(ticker)}"
        params = {"period1": start_ts, "period2": end_ts, "interval": "1d", "includePrePost": "false"}
        try:
            resp = _SESSION.get(url, headers=YF_HEADERS, params=params, timeout=15)
            if resp.status_code != 200:
                yf_missing.append(ticker)
                continue
            data   = resp.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                yf_missing.append(ticker)
                continue
            r          = result[0]
            timestamps = r.get("timestamp", [])
            closes     = r.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
            exact_close = None
            for ts, c in zip(timestamps, closes):
                bar_date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                if bar_date_str == date_str and c is not None:
                    exact_close = float(c)
                    break

            # The historical adjclose bar for "today" can stay null for hours
            # after the close while Yahoo's backend finalizes it — but the
            # quote metadata's regularMarketPrice/regularMarketTime update
            # right at the 4 PM close itself. Use that as the same-day close
            # whenever the historical bar hasn't caught up yet.
            if exact_close is None:
                meta = r.get("meta", {})
                mkt_time  = meta.get("regularMarketTime")
                mkt_price = meta.get("regularMarketPrice")
                if mkt_time and mkt_price is not None:
                    mkt_date_str = datetime.utcfromtimestamp(mkt_time).strftime("%Y-%m-%d")
                    if mkt_date_str == date_str:
                        exact_close = float(mkt_price)

            if exact_close is not None:
                actuals[ticker] = round(exact_close, 2)
            else:
                yf_missing.append(ticker)
        except Exception:
            yf_missing.append(ticker)
        time.sleep(0.2)

    # Pass 2: FMP fallback for missing actuals
    # Same exact-date requirement as Yahoo above — FMP's own "latest close"
    # can also lag right after market close, so matching the specific date
    # (not just taking the newest entry in the window) matters here too.
    if yf_missing:
        logger.info(f"[FMP] Fetching actual prices for {len(yf_missing)} missing tickers...")
        fmp_key = os.getenv("FMP_API_KEY")
        if fmp_key:
            for ticker in yf_missing:
                try:
                    resp = _std_requests.get(
                        f"{FMP_BASE}/historical-price-full/{ticker}",
                        params={
                            "from": (target - timedelta(days=5)).strftime("%Y-%m-%d"),
                            "to":   (target + timedelta(days=2)).strftime("%Y-%m-%d"),
                            "apikey": fmp_key,
                        },
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        for day in resp.json().get("historical", []):
                            if day.get("date") == date_str and day.get("adjClose"):
                                actuals[ticker] = round(float(day["adjClose"]), 2)
                                break
                except Exception:
                    pass
                time.sleep(0.3)

    logger.info(f"Fetched actual prices for {len(actuals)}/{len(tickers)} tickers")
    return actuals


def fetch_open_price_at_time(tickers: list, date_str: str, et_hour: int = 9, et_minute: int = 31) -> dict:
    """
    Historical backfill helper: fetch each ticker's 1-minute bar closest to
    a specific ET time on a specific PAST date. Used to backfill the
    market-open checkpoint for a day that predates the live run_market_open()
    feature — this is a genuine historical price lookup (an objective market
    fact), not a prediction, so backfilling it carries none of the integrity
    concerns that backfilling a forecast would.

    Only works for recent dates — Yahoo's 1-minute granularity is only
    available for roughly the last 30 days.
    """
    import pytz

    et = pytz.timezone("America/New_York")
    naive_target = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=et_hour, minute=et_minute)
    target_et = et.localize(naive_target)
    target_ts = int(target_et.timestamp())
    window_start = target_ts - 600
    window_end   = target_ts + 600

    result = {}
    for ticker in tickers:
        try:
            url    = f"{YF_BASE}{_yf_symbol(ticker)}"
            params = {"period1": window_start, "period2": window_end, "interval": "1m"}
            resp   = _SESSION.get(url, headers=YF_HEADERS, params=params, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            r    = (data.get("chart", {}).get("result") or [None])[0]
            if not r:
                continue
            timestamps = r.get("timestamp", [])
            closes     = r.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            best, best_diff = None, None
            for ts, c in zip(timestamps, closes):
                if c is None:
                    continue
                diff = abs(ts - target_ts)
                if best_diff is None or diff < best_diff:
                    best, best_diff = float(c), diff
            if best is not None:
                result[ticker] = round(best, 2)
        except Exception:
            pass
        time.sleep(0.2)

    logger.info(f"Fetched historical open prices for {len(result)}/{len(tickers)} tickers near {et_hour:02d}:{et_minute:02d} ET on {date_str}.")
    return result
