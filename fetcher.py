"""
fetcher.py — Pulls stock price data using yfinance (free, no API key needed).
Fetches price history, volume, market cap, and recent performance
for a broad universe of stocks to feed into the AI analyzer.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import random

logger = logging.getLogger(__name__)

# Rate limiting config — prevents Yahoo Finance from blocking requests
DELAY_BETWEEN_TICKERS = 0.3   # seconds between each ticker
BATCH_SIZE = 25                # fetch in batches then pause
BATCH_PAUSE = 3.0              # seconds to pause between batches
MAX_RETRIES = 2                # retry failed tickers this many times
RETRY_DELAY = 5.0              # seconds to wait before retrying

# Universe of stocks to consider — broad coverage across all major sectors
# The AI will pick the top 10 expected winners and losers from this pool
STOCK_UNIVERSE = [
    # Mega cap tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "ORCL", "IBM", "ADBE",
    # Semiconductors
    "AMD", "INTC", "QCOM", "AVGO", "MU", "AMAT", "LRCX", "KLAC", "MRVL", "NXPI", "TXN", "ADI",
    # Large cap finance / banking
    "JPM", "BAC", "GS", "MS", "V", "MA", "AXP", "WFC", "C", "BLK", "SCHW", "COF",
    # Fintech / Capital Markets (HOOD sector)
    "HOOD", "SOFI", "COIN", "UPST", "AFRM", "NU", "PYPL", "SQ", "ADYEY", "BILL",
    # Healthcare / Pharma / Biotech
    "JNJ", "PFE", "MRNA", "UNH", "CVS", "ABT", "LLY", "MRK", "BMY", "GILD",
    "AMGN", "REGN", "VRTX", "BIIB", "ILMN", "DXCM", "ISRG", "HCA", "CI", "HUM",
    # Consumer staples
    "WMT", "TGT", "COST", "MCD", "SBUX", "NKE", "PG", "KO", "PEP", "MDLZ",
    "CL", "GIS", "K", "HSY", "MO",
    # Consumer discretionary
    "AMZN", "HD", "LOW", "TJX", "BKNG", "MAR", "HLT", "F", "GM", "RIVN",
    "LCID", "NIO", "LI", "XPEV", "CVNA",
    # Energy
    "XOM", "CVX", "OXY", "SLB", "COP", "EOG", "PSX", "VLO", "MPC", "HAL",
    # AI Infrastructure / Bitcoin Mining (IREN sector)
    "IREN", "CORZ", "MARA", "CLSK", "RIOT", "HUT", "BTBT", "CIFR",
    # Industrial / Aerospace / Defense
    "BA", "GE", "CAT", "HON", "LMT", "RTX", "NOC", "GD", "MMM", "EMR",
    "DE", "ITW", "ETN", "PH", "ROK",
    # Aerospace / Space Technology (RKLB sector)
    "RKLB", "LUNR", "RDW", "ASTS", "MNTS", "SPCE",
    # Media / Telecom / Entertainment
    "DIS", "NFLX", "T", "VZ", "CMCSA", "WBD", "PARA", "SONY", "SPOT", "TTWO",
    "EA", "ATVI", "LYV",
    # AI / Cloud / SaaS
    "PLTR", "CRM", "SNOW", "DDOG", "NET", "MDB", "GTLB", "ZS", "PANW", "CRWD",
    "NOW", "WDAY", "VEEV", "HUBS", "TEAM", "OKTA", "ZM", "DOCN", "ESTC",
    # E-commerce / Retail tech
    "SHOP", "ETSY", "EBAY", "WISH", "POSHM",
    # REITs
    "MPW", "O", "AMT", "PLD", "EQIX", "CCI", "SBAC", "WELL", "SPG", "AVB",
    # Materials / Chemicals
    "LIN", "APD", "ECL", "DD", "NEM", "FCX", "AA", "X", "CLF",
    # Transportation / Logistics
    "UPS", "FDX", "DAL", "UAL", "AAL", "LUV", "UBER", "LYFT", "DASH",
    # Small / mid cap high volatility
    "GME", "AMC", "BBBY", "KOSS", "EXPR",
    # Biotech high volatility
    "SAVA", "SRPT", "RARE", "EXAS", "PACB", "BEAM", "EDIT", "NTLA",
]
# Deduplicate
STOCK_UNIVERSE = list(dict.fromkeys(STOCK_UNIVERSE))
# Deduplicate
STOCK_UNIVERSE = list(dict.fromkeys(STOCK_UNIVERSE))


def fetch_single_ticker(ticker: str, start: str, end: str, retries: int = MAX_RETRIES) -> pd.DataFrame | None:
    """Fetch data for a single ticker with retry logic."""
    for attempt in range(retries + 1):
        try:
            t = yf.Ticker(ticker)
            df = t.history(start=start, end=end, auto_adjust=True)
            if df.empty:
                return None
            return df
        except Exception as e:
            if attempt < retries:
                wait = RETRY_DELAY * (attempt + 1) + random.uniform(0, 2)
                logger.warning(f"Retry {attempt + 1}/{retries} for {ticker} in {wait:.1f}s: {e}")
                time.sleep(wait)
            else:
                logger.warning(f"Failed to fetch {ticker} after {retries + 1} attempts: {e}")
                return None


def fetch_stock_data() -> dict:
    """
    Fetch price history for all stocks using batch download.
    Downloads all tickers in one request — much faster and avoids rate limits.
    Falls back to individual fetching for any tickers that fail.
    """
    logger.info(f"Fetching data for {len(STOCK_UNIVERSE)} stocks...")

    end_date = datetime.today()
    start_date = end_date - timedelta(days=90)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    stock_data = {}

    # Split into batches of 50 to avoid oversized requests
    batch_size = 50
    batches = [STOCK_UNIVERSE[i:i+batch_size] for i in range(0, len(STOCK_UNIVERSE), batch_size)]

    all_closes = {}
    all_volumes = {}

    for batch_num, batch in enumerate(batches):
        logger.info(f"Downloading batch {batch_num + 1}/{len(batches)} ({len(batch)} tickers)...")
        try:
            raw = yf.download(
                tickers=" ".join(batch),
                start=start_str,
                end=end_str,
                auto_adjust=True,
                progress=False,
                threads=True,
                group_by="ticker",
            )

            if raw.empty:
                logger.warning(f"Batch {batch_num + 1} returned empty data.")
                time.sleep(5)
                continue

            # Extract close and volume for each ticker
            for ticker in batch:
                try:
                    if len(batch) == 1:
                        close = raw["Close"]
                        volume = raw["Volume"]
                    else:
                        if ticker not in raw.columns.get_level_values(0):
                            continue
                        close = raw[ticker]["Close"]
                        volume = raw[ticker]["Volume"]

                    close = close.dropna()
                    volume = volume.dropna()

                    if len(close) >= 5:
                        all_closes[ticker] = close
                        all_volumes[ticker] = volume
                except Exception as e:
                    logger.warning(f"Could not extract {ticker} from batch: {e}")
                    continue

            time.sleep(2)  # pause between batches

        except Exception as e:
            logger.warning(f"Batch {batch_num + 1} failed: {e}")
            time.sleep(5)
            continue

    logger.info(f"Batch download complete. Processing {len(all_closes)} tickers...")

    # Process each ticker's data
    for ticker, close_prices in all_closes.items():
        try:
            volumes = all_volumes.get(ticker, pd.Series(dtype=float))

            current_price = float(close_prices.iloc[-1])
            prev_close = float(close_prices.iloc[-2])
            week_ago = float(close_prices.iloc[-6]) if len(close_prices) >= 6 else prev_close
            month_ago = float(close_prices.iloc[-22]) if len(close_prices) >= 22 else prev_close

            daily_change_pct = ((current_price - prev_close) / prev_close) * 100
            weekly_change_pct = ((current_price - week_ago) / week_ago) * 100
            monthly_change_pct = ((current_price - month_ago) / month_ago) * 100

            avg_volume_30d = float(volumes.tail(30).mean()) if len(volumes) > 0 else 0
            latest_volume = float(volumes.iloc[-1]) if len(volumes) > 0 else 0
            volume_ratio = latest_volume / avg_volume_30d if avg_volume_30d > 0 else 1.0

            last_10 = close_prices.tail(11).pct_change().dropna()
            up_days = int((last_10 > 0).sum())
            momentum_score = (up_days / 10) * 100

            daily_returns = close_prices.pct_change().dropna().tail(30)
            volatility = float(daily_returns.std() * 100)

            stock_data[ticker] = {
                "ticker": ticker,
                "current_price": round(current_price, 2),
                "prev_close": round(prev_close, 2),
                "daily_change_pct": round(daily_change_pct, 2),
                "weekly_change_pct": round(weekly_change_pct, 2),
                "monthly_change_pct": round(monthly_change_pct, 2),
                "volume_ratio": round(volume_ratio, 2),
                "momentum_score": round(momentum_score, 1),
                "volatility_30d": round(volatility, 2),
                "avg_volume_30d": int(avg_volume_30d),
                "latest_volume": int(latest_volume),
            }

            logger.info(f"✓ {ticker}: ${current_price} ({daily_change_pct:+.1f}%)")

        except Exception as e:
            logger.warning(f"Error processing {ticker}: {e}")
            continue

    logger.info(f"Successfully fetched data for {len(stock_data)} stocks.")
    return stock_data

    logger.info(f"Successfully fetched data for {len(stock_data)} stocks.")
    return stock_data


def fetch_actual_prices(tickers: list, date_str: str) -> dict:
    """
    Fetch actual closing prices for a specific date using batch download.
    Returns the closing price on or nearest to date_str for each ticker.
    """
    logger.info(f"Fetching actual prices for {date_str} ({len(tickers)} tickers)...")

    target = datetime.strptime(date_str, "%Y-%m-%d")
    # Fetch a 5-day window around the target date to handle weekends/holidays
    start = (target - timedelta(days=4)).strftime("%Y-%m-%d")
    end = (target + timedelta(days=2)).strftime("%Y-%m-%d")

    actuals = {}
    try:
        raw = yf.download(
            tickers=" ".join(tickers),
            start=start,
            end=end,
            auto_adjust=True,
            progress=False,
            threads=True,
            group_by="ticker",
        )

        if raw.empty:
            logger.warning("Batch actual prices download returned empty.")
            return actuals

        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    close = raw["Close"].dropna()
                else:
                    if ticker not in raw.columns.get_level_values(0):
                        continue
                    close = raw[ticker]["Close"].dropna()

                if close.empty:
                    continue

                # Find the price on or before the target date
                close.index = close.index.tz_localize(None) if close.index.tzinfo else close.index
                target_naive = target.replace(tzinfo=None)
                available = close[close.index <= target_naive + timedelta(days=1)]

                if available.empty:
                    continue

                actuals[ticker] = round(float(available.iloc[-1]), 2)

            except Exception as e:
                logger.warning(f"Could not extract actual price for {ticker}: {e}")
                continue

    except Exception as e:
        logger.warning(f"Batch actual prices download failed: {e}")

    return actuals
