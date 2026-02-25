"""
fetcher.py — Pulls stock price data using yfinance (free, no API key needed).
Fetches price history, volume, market cap, and recent performance
for a broad universe of stocks to feed into the AI analyzer.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Universe of stocks to consider — mix of large cap, mid cap, volatile names
# The AI will pick the top 10 expected winners and losers from this pool
STOCK_UNIVERSE = [
    # Mega cap tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    # Semiconductors
    "AMD", "INTC", "QCOM", "AVGO", "MU", "AMAT",
    # Finance
    "JPM", "BAC", "GS", "MS", "V", "MA", "AXP",
    # Healthcare / Pharma
    "JNJ", "PFE", "MRNA", "UNH", "CVS", "ABT",
    # Consumer
    "AMZN", "WMT", "TGT", "COST", "MCD", "SBUX", "NKE",
    # Energy
    "XOM", "CVX", "OXY", "SLB",
    # Industrial / Aerospace
    "BA", "GE", "CAT", "HON",
    # Media / Telecom
    "DIS", "NFLX", "T", "VZ", "CMCSA",
    # Retail / Other
    "WBA", "M", "GME", "AMC",
    # AI / Cloud
    "PLTR", "CRM", "SNOW", "DDOG", "NET",
    # REITs
    "MPW", "O", "AMT",
]
# Deduplicate
STOCK_UNIVERSE = list(dict.fromkeys(STOCK_UNIVERSE))


def fetch_single_ticker(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    """Fetch data for a single ticker using Ticker.history() — avoids TzCache bug."""
    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start, end=end, auto_adjust=True)
        if df.empty:
            return None
        return df
    except Exception as e:
        logger.warning(f"Failed to fetch {ticker}: {e}")
        return None


def fetch_stock_data() -> dict:
    """
    Fetch price history and key metrics for all stocks in the universe.
    Uses individual Ticker.history() calls to avoid the batch TzCache bug.
    Returns a dict keyed by ticker with price/volume/change data.
    """
    logger.info(f"Fetching data for {len(STOCK_UNIVERSE)} stocks...")

    end_date = datetime.today()
    start_date = end_date - timedelta(days=90)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    stock_data = {}

    for ticker in STOCK_UNIVERSE:
        try:
            df = fetch_single_ticker(ticker, start_str, end_str)

            if df is None or df.empty:
                logger.warning(f"No data for {ticker}, skipping.")
                continue

            df = df.dropna(subset=["Close"])
            if len(df) < 5:
                continue

            close_prices = df["Close"]
            volumes = df["Volume"]

            current_price = float(close_prices.iloc[-1])
            prev_close = float(close_prices.iloc[-2])
            week_ago = float(close_prices.iloc[-6]) if len(close_prices) >= 6 else prev_close
            month_ago = float(close_prices.iloc[-22]) if len(close_prices) >= 22 else prev_close

            daily_change_pct = ((current_price - prev_close) / prev_close) * 100
            weekly_change_pct = ((current_price - week_ago) / week_ago) * 100
            monthly_change_pct = ((current_price - month_ago) / month_ago) * 100

            avg_volume_30d = float(volumes.tail(30).mean())
            latest_volume = float(volumes.iloc[-1])
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


def fetch_actual_prices(tickers: list, date_str: str) -> dict:
    """
    Fetch actual closing prices for a specific date.
    Used the next day to calculate prediction accuracy.

    Args:
        tickers: list of ticker symbols
        date_str: date in YYYY-MM-DD format

    Returns:
        dict of {ticker: actual_close_price}
    """
    logger.info(f"Fetching actual prices for {date_str}...")

    target = datetime.strptime(date_str, "%Y-%m-%d")
    start = (target - timedelta(days=3)).strftime("%Y-%m-%d")
    end = (target + timedelta(days=2)).strftime("%Y-%m-%d")

    actuals = {}
    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            df = t.history(start=start, end=end, auto_adjust=True)
            df = df.dropna(subset=["Close"])
            if df.empty:
                continue
            actuals[ticker] = round(float(df["Close"].iloc[-1]), 2)
        except Exception as e:
            logger.warning(f"Failed to fetch actual price for {ticker}: {e}")
            continue

    return actuals
