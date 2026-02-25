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

    if not stock_data:
        logger.error("No stock data returned. Aborting pipeline.")
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
            # Fetch actual closing prices for yesterday
            actual_prices = fetch_actual_prices(all_tickers, yesterday_str)

            # Convert prices to % change vs the day before yesterday
            day_before_str = (date.today() - timedelta(days=2)).isoformat()
            prev_prices = fetch_actual_prices(all_tickers, day_before_str)

            actuals_pct = {}
            for ticker in all_tickers:
                if ticker in actual_prices and ticker in prev_prices:
                    if prev_prices[ticker] > 0:
                        pct = ((actual_prices[ticker] - prev_prices[ticker]) / prev_prices[ticker]) * 100
                        actuals_pct[ticker] = round(pct, 2)

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


if __name__ == "__main__":
    run()
