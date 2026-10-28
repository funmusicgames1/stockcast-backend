"""
main.py — Orchestrates the StockCast pipeline. Runs in one of two modes:

  --mode predictions  (6:00 AM CST, weekdays)
      1. Fetch stock price data (Yahoo Finance v8 API + FMP fallback)
      2. Fetch news & sentiment (NewsAPI)
      3. Run AI analysis (Claude + Gemini in parallel)
      4. Save today's predictions to Supabase
      5. Read back yesterday's predictions + actuals (already scored by last
         night's --mode accuracy run — no recomputation needed here)
      6. Export + push data.json, send push notification

  --mode accuracy  (end of trading day, weekdays)
      1. Read today's own predictions back from Supabase (saved this morning)
      2. Fetch today's actual closing prices and score them against the
         predictions, using yesterday's close as the baseline
      3. Save the actuals to Supabase
      4. Export + push data.json, so Historical Performance shows today's
         results the same evening instead of waiting for tomorrow morning

Default mode is "predictions" for backward compatibility with manual runs.
"""

import os
import sys
import logging
import argparse
from datetime import timedelta
from dotenv import load_dotenv
from datetime_utils import today_cst

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


def run_predictions():
    logger.info("=" * 60)
    logger.info("StockCast PREDICTIONS run starting...")
    logger.info(f"Date: {today_cst().isoformat()}")
    logger.info("=" * 60)

    # ----------------------------------------------------------------
    # STEP 1: Fetch stock data
    # ----------------------------------------------------------------
    logger.info("Step 1/6: Fetching stock data...")
    from fetcher import fetch_stock_data
    try:
        stock_data = fetch_stock_data()
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)

    if len(stock_data) < 20:
        logger.error(f"Only {len(stock_data)} stocks fetched — aborting pipeline.")
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
    # STEP 3: AI analysis (Claude + Gemini)
    # ----------------------------------------------------------------
    logger.info("Step 3/6: Running AI analysis (Claude + Gemini)...")
    from analyzer import analyze
    model_predictions = analyze(stock_data, news)

    if not model_predictions:
        logger.error("All AI models failed. Aborting pipeline.")
        sys.exit(1)

    claude_predictions = model_predictions.get("claude")
    gemini_predictions = model_predictions.get("gemini")

    if claude_predictions:
        logger.info(f"[Claude] {len(claude_predictions['winners'])} winners, {len(claude_predictions['losers'])} losers.")
    if gemini_predictions:
        logger.info(f"[Gemini] {len(gemini_predictions['winners'])} winners, {len(gemini_predictions['losers'])} losers.")

    # Use whichever model succeeded for DB save (prefer Claude)
    primary_predictions = claude_predictions or gemini_predictions

    # ----------------------------------------------------------------
    # STEP 4: Save today's predictions to Supabase
    # ----------------------------------------------------------------
    logger.info("Step 4/6: Saving predictions to database...")
    from database import save_predictions, get_predictions_for_date, get_actuals_for_date
    saved = save_predictions(primary_predictions)
    if not saved:
        logger.warning("Failed to save predictions — continuing anyway.")

    # ----------------------------------------------------------------
    # STEP 5: Read back yesterday's predictions + actuals
    #   (already scored by last night's --mode accuracy run; nothing to
    #   compute here, just surface it for the Historical Performance view)
    # ----------------------------------------------------------------
    logger.info("Step 5/6: Reading yesterday's scored results...")
    yesterday_str = (today_cst() - timedelta(days=1)).isoformat()
    yesterday_predictions = get_predictions_for_date(yesterday_str)
    yesterday_actuals = []

    if yesterday_predictions:
        actuals_rows = get_actuals_for_date(yesterday_str)
        yesterday_actuals = [
            {"ticker": a["ticker"], "actual_change_pct": a["actual_change_pct"]}
            for a in actuals_rows
        ]
        logger.info(f"Found {len(yesterday_actuals)} scored actuals for {yesterday_str}.")
    else:
        logger.info(f"No predictions on record for {yesterday_str} (first run, or weekend/holiday gap).")

    # ----------------------------------------------------------------
    # STEP 6: Export data.json for frontend
    # ----------------------------------------------------------------
    logger.info("Step 6/6: Exporting data.json...")
    from exporter import build_frontend_json, write_json, fetch_index_data

    index_data = fetch_index_data()
    payload    = build_frontend_json(
        today_predictions      = primary_predictions,
        yesterday_predictions  = yesterday_predictions,
        yesterday_actuals      = yesterday_actuals,
        yesterday_date_str     = yesterday_str,
        index_data             = index_data,
        stock_data             = stock_data,
        claude_predictions     = claude_predictions,
        gemini_predictions     = gemini_predictions,
    )

    _finish(payload)

    from notify import send_prediction_notification
    send_prediction_notification(
        winners=primary_predictions.get("winners", []),
        losers=primary_predictions.get("losers",  [])
    )


def run_accuracy():
    logger.info("=" * 60)
    logger.info("StockCast ACCURACY (end-of-day) run starting...")
    logger.info(f"Date: {today_cst().isoformat()}")
    logger.info("=" * 60)

    from database import get_predictions_for_date, save_actuals
    from fetcher import fetch_actual_prices

    today_str = today_cst().isoformat()
    today_predictions = get_predictions_for_date(today_str)

    if not today_predictions:
        logger.warning(f"No predictions on record for {today_str} — nothing to score. Exiting cleanly.")
        return

    all_tickers = (
        [e["ticker"] for e in today_predictions.get("winners", [])] +
        [e["ticker"] for e in today_predictions.get("losers",  [])]
    )

    if not all_tickers:
        logger.warning("Today's prediction record has no tickers — nothing to score.")
        return

    yesterday_str  = (today_cst() - timedelta(days=1)).isoformat()
    actual_prices  = fetch_actual_prices(all_tickers, today_str)      # today's close
    prev_prices    = fetch_actual_prices(all_tickers, yesterday_str)  # baseline close

    # Weekend/holiday fallback — walk further back until a prior close is found
    if not prev_prices:
        for days_back in range(2, 7):
            fallback_str = (today_cst() - timedelta(days=days_back)).isoformat()
            prev_prices  = fetch_actual_prices(all_tickers, fallback_str)
            if prev_prices:
                logger.info(f"Using {fallback_str} as previous close (weekend/holiday fallback).")
                break

    actuals_pct = {}
    for ticker in all_tickers:
        if ticker in actual_prices and ticker in prev_prices:
            if prev_prices[ticker] > 0 and actual_prices[ticker] != prev_prices[ticker]:
                pct = ((actual_prices[ticker] - prev_prices[ticker]) / prev_prices[ticker]) * 100
                actuals_pct[ticker] = round(pct, 2)
                logger.info(f"  {ticker}: prev={prev_prices[ticker]}, actual={actual_prices[ticker]}, change={actuals_pct[ticker]:+.2f}%")

    if not actuals_pct:
        logger.warning("Could not compute actuals for any ticker — skipping save/export.")
        return

    save_actuals(today_str, actuals_pct, today_predictions)
    logger.info(f"Saved actuals for {len(actuals_pct)} tickers under {today_str}.")

    yesterday_actuals = [{"ticker": t, "actual_change_pct": pct} for t, pct in actuals_pct.items()]

    # Re-export data.json so the site shows today's completed scoring tonight.
    # "today" block is reconstructed from the DB record — matches the current
    # single-model persistence (only the model that actually saved survives;
    # right now that's Gemini, since no ANTHROPIC_API_KEY is set).
    # full_ranked_list is intentionally left empty here — refreshing ~100
    # tickers' price history again at EOD isn't worth doubling the stock-data
    # pull; it gets refreshed normally on tomorrow's predictions run.
    logger.info("Exporting updated data.json with today's completed accuracy...")
    from exporter import build_frontend_json, write_json, fetch_index_data

    index_data = fetch_index_data()
    payload    = build_frontend_json(
        today_predictions      = today_predictions,
        yesterday_predictions  = today_predictions,
        yesterday_actuals      = yesterday_actuals,
        yesterday_date_str     = today_str,
        index_data             = index_data,
        stock_data             = None,
        claude_predictions     = None,
        gemini_predictions     = today_predictions,
    )

    _finish(payload)


def _finish(payload: dict) -> None:
    from exporter import write_json
    success = write_json(payload)
    if not success:
        logger.error("Failed to write data.json.")
        sys.exit(1)

    logger.info("data.json written successfully.")
    push_to_github(os.getenv("OUTPUT_JSON_PATH", "./data.json"))

    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 60)


def push_to_github(json_path: str) -> bool:
    """
    Push data.json to the frontend GitHub repo so Vercel auto-redeploys
    with fresh data after every run (morning predictions AND evening accuracy).
    """
    import base64
    import requests

    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("FRONTEND_REPO")  # e.g. funmusicgames1/stockcast-frontend

    if not token or not repo:
        logger.warning("GITHUB_TOKEN or FRONTEND_REPO not set — skipping GitHub push.")
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

        get_resp = requests.get(api_url, headers=headers)
        sha = get_resp.json().get("sha") if get_resp.status_code == 200 else None

        payload = {
            "message": f"Update data {today_cst().isoformat()}",
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["predictions", "accuracy"], default="predictions")
    args = parser.parse_args()

    if args.mode == "predictions":
        run_predictions()
    else:
        run_accuracy()
