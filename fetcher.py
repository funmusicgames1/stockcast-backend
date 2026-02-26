"""
fetcher.py — Pulls stock price data directly from Yahoo Finance v8 API.
Uses curl_cffi to impersonate Chrome at the TLS level, bypassing bot detection
on cloud IPs like GitHub Actions and Railway.
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import random
import json

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

# Yahoo Finance API settings
YF_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
YF_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}

# Universe of stocks to consider — broad coverage across all major sectors
STOCK_UNIVERSE = [
    # Mega cap tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "ORCL", "IBM", "ADBE",
    # Semiconductors
    "AMD", "INTC", "QCOM", "AVGO", "MU", "AMAT", "LRCX", "KLAC", "MRVL", "NXPI", "TXN", "ADI",
    # Large cap finance / banking
    "JPM", "BAC", "GS", "MS", "V", "MA", "AXP", "WFC", "C", "BLK", "SCHW", "COF",
    # Fintech / Capital Markets
    "HOOD", "SOFI", "COIN", "UPST", "AFRM", "NU", "PYPL", "SQ", "BILL",
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
    "BA", "GE", "CAT", "HON", "LMT", "RTX", "NOC", "GD", "MMM",
    "DE", "ITW", "ETN",
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


def _fetch_ticker_direct(ticker: str, days: int = 90) -> dict | None:
    """
    Fetch OHLCV data for a single ticker directly from Yahoo Finance v8 API.
    Returns a dict with closes and volumes as lists, or None on failure.
    """
    end_ts = int(datetime.today().timestamp())
    start_ts = int((datetime.today() - timedelta(days=days)).timestamp())

    url = f"{YF_BASE}{ticker}"
    params = {
        "period1": start_ts,
        "period2": end_ts,
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }

    try:
        resp = _SESSION.get(url, headers=YF_HEADERS, params=params, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"{ticker}: HTTP {resp.status_code}")
            return None

        data = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            logger.warning(f"{ticker}: empty result")
            return None

        r = result[0]
        timestamps = r.get("timestamp", [])
        closes = r.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
        volumes = r.get("indicators", {}).get("quote", [{}])[0].get("volume", [])

        if not timestamps or not closes or len(closes) < 5:
            return None

        # Build a clean series filtering out None values
        clean_closes = []
        clean_volumes = []
        for c, v in zip(closes, volumes):
            if c is not None:
                clean_closes.append(float(c))
                clean_volumes.append(float(v) if v is not None else 0.0)

        if len(clean_closes) < 5:
            return None

        return {"closes": clean_closes, "volumes": clean_volumes}

    except Exception as e:
        logger.warning(f"{ticker}: fetch failed — {e}")
        return None


def fetch_stock_data() -> dict:
    """
    Fetch price history for all stocks by calling Yahoo Finance API directly.
    Processes tickers in small batches with delays to avoid rate limiting.
    """
    logger.info(f"Fetching data for {len(STOCK_UNIVERSE)} stocks via direct API...")

    stock_data = {}
    failed = []

    # Brief warm-up
    logger.info("Warming up connection to Yahoo Finance...")
    time.sleep(2)

    batch_size = 20
    batches = [STOCK_UNIVERSE[i:i+batch_size] for i in range(0, len(STOCK_UNIVERSE), batch_size)]

    for batch_num, batch in enumerate(batches):
        logger.info(f"Processing batch {batch_num + 1}/{len(batches)} ({len(batch)} tickers)...")

        for ticker in batch:
            raw = _fetch_ticker_direct(ticker)
            if raw is None:
                failed.append(ticker)
                continue

            closes = raw["closes"]
            volumes = raw["volumes"]

            try:
                current_price = closes[-1]
                prev_close = closes[-2]
                week_ago = closes[-6] if len(closes) >= 6 else prev_close
                month_ago = closes[-22] if len(closes) >= 22 else prev_close

                daily_change_pct = ((current_price - prev_close) / prev_close) * 100
                weekly_change_pct = ((current_price - week_ago) / week_ago) * 100
                monthly_change_pct = ((current_price - month_ago) / month_ago) * 100

                avg_volume = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 20 else sum(volumes) / len(volumes)
                recent_volume = volumes[-1] if volumes else 0
                volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0

                # Simple momentum score 0-100
                momentum_score = 50
                if daily_change_pct > 0:
                    momentum_score += min(daily_change_pct * 5, 20)
                else:
                    momentum_score += max(daily_change_pct * 5, -20)
                if weekly_change_pct > 0:
                    momentum_score += min(weekly_change_pct * 2, 15)
                else:
                    momentum_score += max(weekly_change_pct * 2, -15)
                if monthly_change_pct > 0:
                    momentum_score += min(monthly_change_pct, 15)
                else:
                    momentum_score += max(monthly_change_pct, -15)
                if volume_ratio > 1.5:
                    momentum_score += min((volume_ratio - 1) * 5, 10)
                momentum_score = max(0, min(100, momentum_score))

                stock_data[ticker] = {
                    "current_price": round(current_price, 2),
                    "prev_close": round(prev_close, 2),
                    "daily_change_pct": round(daily_change_pct, 2),
                    "weekly_change_pct": round(weekly_change_pct, 2),
                    "monthly_change_pct": round(monthly_change_pct, 2),
                    "volume_ratio": round(volume_ratio, 2),
                    "momentum_score": round(momentum_score, 1),
                    "avg_volume": int(avg_volume),
                    "recent_volume": int(recent_volume),
                }
            except Exception as e:
                logger.warning(f"Could not process {ticker}: {e}")
                failed.append(ticker)
                continue

            time.sleep(0.3 + random.uniform(0, 0.2))  # small delay per ticker

        # Pause between batches
        if batch_num < len(batches) - 1:
            pause = 3 + random.uniform(0, 1)
            logger.info(f"Batch {batch_num + 1} done. Pausing {pause:.1f}s...")
            time.sleep(pause)

    logger.info(f"Successfully fetched data for {len(stock_data)} stocks. Failed: {len(failed)}")
    if failed:
        logger.warning(f"Failed tickers: {failed}")

    return stock_data


def fetch_actual_prices(tickers: list, date_str: str) -> dict:
    """
    Fetch actual closing prices for a specific date directly from Yahoo Finance API.
    Returns {ticker: close_price} for the trading day on or before date_str.
    """
    logger.info(f"Fetching actual prices for {date_str} ({len(tickers)} tickers)...")

    target = datetime.strptime(date_str, "%Y-%m-%d")
    start_ts = int((target - timedelta(days=5)).timestamp())
    end_ts = int((target + timedelta(days=2)).timestamp())

    actuals = {}

    for ticker in tickers:
        url = f"{YF_BASE}{ticker}"
        params = {
            "period1": start_ts,
            "period2": end_ts,
            "interval": "1d",
            "includePrePost": "false",
        }
        try:
            resp = _SESSION.get(url, headers=YF_HEADERS, params=params, timeout=15)
            if resp.status_code != 200:
                continue

            data = resp.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                continue

            r = result[0]
            timestamps = r.get("timestamp", [])
            closes = r.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])

            if not timestamps or not closes:
                continue

            # Find the price on or before target date
            target_ts = int(target.timestamp()) + 86400  # end of target day
            best_close = None
            for ts, c in zip(timestamps, closes):
                if ts <= target_ts and c is not None:
                    best_close = float(c)

            if best_close is not None:
                actuals[ticker] = round(best_close, 2)

        except Exception as e:
            logger.warning(f"Could not fetch actual price for {ticker}: {e}")
            continue

        time.sleep(0.2)

    logger.info(f"Fetched actual prices for {len(actuals)}/{len(tickers)} tickers")
    return actuals
