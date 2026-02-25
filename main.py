"""
main.py — Orchestrates the full MarketPulse daily pipeline.

Flow:
1. Fetch stock price data (yfinance)
2. Fetch news & sentiment (NewsAPI)
3. Run AI analysis (Anthropic Claude)
4. Save predictions to Supabase
5. Fetch yesterday's actual prices and save accuracy data
6. Export data.json for the frontend

This script is triggered daily at 6:00 AM CST by Railway cron.
"""

import os
import logging
import sys
from datetime import date, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file (local dev only)
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


def run():
    logger.info("=" * 60)
    logger.info("MarketPulse daily pipeline starting...")
    logger.info(f"Date: {date.today().isoformat()}")
    logger.info("=" * 60)

    # ----------------------------------------------------------------
    # STEP 1: Fetch stock data
    # ----------------------------------------------------------------
    logger.info("Step 1/6: Fetching stock data...")
    from fetcher import fetch_stock_data, fetch_actual_prices
    stock_data = fetch_stock_data()

    if len(stock_data) < 20:
        logger.error(f"Only {len(stock_data)} stocks fetched — possible rate limit. Aborting pipeline.")
        sys.exit(1)

    logger.info(f"Got data for {len(stock_data)} stocks.")

    # ----------------------------------------------------------------
    # STEP 2: Fetch news
    # ----------------------------------------------------------------
    logger.info("Step 2/6: Fetching news headlines...")
    from news import fetch_market_news
    news = fetch_market_news()
    logger.info(f"Got {len(news.get('macro', []))} macro headlines.")

    # ----------------------------------------------------------------
    # STEP 3: AI analysis
    # ----------------------------------------------------------------
    logger.info("Step 3/6: Running AI analysis...")
    from analyzer import analyze
    predictions = analyze(stock_data, news)

    if not predictions:
        logger.error("AI analysis failed. Aborting pipeline.")
        sys.exit(1)

    logger.info(f"Got {len(predictions['winners'])} winners, {len(predictions['losers'])} losers.")

    # ----------------------------------------------------------------
    # STEP 4: Save today's predictions to Supabase
    # ----------------------------------------------------------------
    logger.info("Step 4/6: Saving predictions to database...")
    from database import save_predictions, get_predictions_for_date, get_actuals_for_date, save_actuals
    saved = save_predictions(predictions)
    if not saved:
        logger.warning("Failed to save predictions — continuing anyway.")

    # ----------------------------------------------------------------
    # STEP 5: Fetch yesterday's actuals and calculate accuracy
    # ----------------------------------------------------------------
    logger.info("Step 5/6: Fetching yesterday's actual results...")
    yesterday_str = (date.today() - timedelta(days=1)).isoformat()
    yesterday_predictions = get_predictions_for_date(yesterday_str)
    yesterday_actuals = []

    if yesterday_predictions:
        # Get all tickers from yesterday's predictions
        all_tickers = (
            [e["ticker"] for e in yesterday_predictions.get("winners", [])] +
            [e["ticker"] for e in yesterday_predictions.get("losers", [])]
        )

        if all_tickers:
            # Fetch a wider window to get both yesterday and the day before
            # Use 5 days back to handle weekends (Mon predictions need Fri as prev close)
            from datetime import timedelta as td
            yesterday_dt = date.today() - td(days=1)
            day_before_dt = date.today() - td(days=2)

            actual_prices = fetch_actual_prices(all_tickers, yesterday_str)
            day_before_str = day_before_dt.isoformat()
            prev_prices = fetch_actual_prices(all_tickers, day_before_str)

            # If yesterday was Monday, prev should be Friday — fetch up to 5 days back
            if not prev_prices:
                for days_back in range(3, 7):
                    fallback_str = (date.today() - td(days=days_back)).isoformat()
                    prev_prices = fetch_actual_prices(all_tickers, fallback_str)
                    if prev_prices:
                        logger.info(f"Using {fallback_str} as previous close (weekend fallback).")
                        break

            actuals_pct = {}
            for ticker in all_tickers:
                if ticker in actual_prices and ticker in prev_prices:
                    if prev_prices[ticker] > 0 and actual_prices[ticker] != prev_prices[ticker]:
                        pct = ((actual_prices[ticker] - prev_prices[ticker]) / prev_prices[ticker]) * 100
                        actuals_pct[ticker] = round(pct, 2)
                        logger.info(f"  {ticker}: prev={prev_prices[ticker]}, actual={actual_prices[ticker]}, change={actuals_pct[ticker]:+.2f}%")

            # Save actuals to DB
            actuals_list = [
                {"ticker": t, "actual_change_pct": pct}
                for t, pct in actuals_pct.items()
            ]
            yesterday_actuals = actuals_list
            save_actuals(yesterday_str, actuals_pct, yesterday_predictions)
            logger.info(f"Saved actuals for {len(actuals_pct)} tickers.")
    else:
        logger.info("No yesterday predictions found (first run?).")

    # ----------------------------------------------------------------
    # STEP 6: Export data.json for frontend
    # ----------------------------------------------------------------
    logger.info("Step 6/6: Exporting data.json...")
    from exporter import build_frontend_json, write_json, fetch_index_data

    index_data = fetch_index_data()
    payload = build_frontend_json(
        today_predictions=predictions,
        yesterday_predictions=yesterday_predictions,
        yesterday_actuals=yesterday_actuals,
        index_data=index_data,
        stock_data=stock_data,
    )

    success = write_json(payload)
    if success:
        logger.info("data.json written successfully.")
    else:
        logger.error("Failed to write data.json.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 60)

    # Push data.json to GitHub frontend repo so Vercel redeploys
    push_to_github(os.getenv("OUTPUT_JSON_PATH", "./data.json"))


def push_to_github(json_path: str) -> bool:
    """
    Push data.json to the frontend GitHub repo so Vercel auto-redeploys
    with fresh predictions after every daily run.
    """
    import base64
    import requests

    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")  # e.g. funmusicgames1/stockcast-frontend

    if not token or not repo:
        logger.warning("GITHUB_TOKEN or GITHUB_REPO not set — skipping GitHub push.")
        return False

    try:
        with open(json_path, "r") as f:
            content = f.read()

        encoded = base64.b64encode(content.encode()).decode()

        api_url = f"https://api.github.com/repos/{repo}/contents/data.json"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

        # Check if file already exists (need SHA to update)
        get_resp = requests.get(api_url, headers=headers)
        sha = get_resp.json().get("sha") if get_resp.status_code == 200 else None

        payload = {
            "message": f"Update predictions {date.today().isoformat()}",
            "content": encoded,
        }
        if sha:
            payload["sha"] = sha

        put_resp = requests.put(api_url, headers=headers, json=payload)

        if put_resp.status_code in (200, 201):
            logger.info("data.json pushed to GitHub frontend repo successfully.")
            return True
        else:
            logger.error(f"GitHub push failed: {put_resp.status_code} {put_resp.text}")
            return False

    except Exception as e:
        logger.error(f"GitHub push error: {e}")
        return False


if __name__ == "__main__":
    run()
