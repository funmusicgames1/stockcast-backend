"""
news.py — Fetches recent news headlines for market sentiment analysis.
Uses NewsAPI free tier (100 requests/day). We batch smartly to stay within limits.
"""

import os
import logging
from datetime import datetime, timedelta
from newsapi import NewsApiClient

logger = logging.getLogger(__name__)


def fetch_market_news() -> dict:
    """
    Fetches two categories of news:
    1. General market / macro / geopolitical headlines
    2. Per-sector news to identify trends

    Returns a dict with 'macro' and 'sector' keys.
    Free tier: we use ~5 requests total, well within 100/day limit.
    """
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        logger.warning("NEWS_API_KEY not set — skipping news fetch.")
        return {"macro": [], "sector": {}}

    newsapi = NewsApiClient(api_key=api_key)

    from_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    results = {"macro": [], "sector": {}}

    # --- 1. Macro / market headlines ---
    macro_queries = [
        "stock market economy federal reserve",
        "geopolitical trade war tariffs inflation",
    ]

    macro_headlines = []
    for query in macro_queries:
        try:
            resp = newsapi.get_everything(
                q=query,
                language="en",
                from_param=from_date,
                sort_by="relevancy",
                page_size=10,
            )
            for article in resp.get("articles", []):
                title = article.get("title", "")
                desc = article.get("description", "")
                if title and "[Removed]" not in title:
                    macro_headlines.append(f"{title}. {desc or ''}".strip())
        except Exception as e:
            logger.warning(f"Macro news fetch failed for '{query}': {e}")

    results["macro"] = macro_headlines[:20]  # cap at 20 headlines

    # --- 2. Sector-specific news ---
    sector_queries = {
        "technology": "tech stocks AI semiconductors earnings",
        "energy": "oil energy stocks OPEC",
        "healthcare": "pharma biotech FDA approval",
        "finance": "bank stocks earnings interest rates",
    }

    for sector, query in sector_queries.items():
        try:
            resp = newsapi.get_everything(
                q=query,
                language="en",
                from_param=from_date,
                sort_by="relevancy",
                page_size=5,
            )
            headlines = []
            for article in resp.get("articles", []):
                title = article.get("title", "")
                if title and "[Removed]" not in title:
                    headlines.append(title)
            results["sector"][sector] = headlines
        except Exception as e:
            logger.warning(f"Sector news fetch failed for '{sector}': {e}")
            results["sector"][sector] = []

    total = len(results["macro"]) + sum(len(v) for v in results["sector"].values())
    logger.info(f"Fetched {total} news headlines across macro + 4 sectors.")
    return results
