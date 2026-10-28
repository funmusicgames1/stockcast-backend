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
    # Scheduled via two cron triggers an hour apart (11:00 + 12:00 UTC) to
    # target 7:00 AM ET year-round despite GitHub Actions cron having no DST
    # awareness (see the workflow file). Only enforce the time window for
    # the automated schedule trigger — a manual/workflow_dispatch run (e.g.
    # for testing) should run immediately regardless of the clock.
    if os.getenv("TRIGGER_EVENT") == "schedule":
        import pytz
        from datetime import datetime as _dt

        et = pytz.timezone("America/New_York")
        now_et = _dt.now(et)
        if now_et.weekday() >= 5:
            logger.info(f"{now_et.strftime('%A')} — market closed. Exiting (expected on weekends).")
            return
        minutes_since_midnight = now_et.hour * 60 + now_et.minute
        target_minutes = 7 * 60
        if not (target_minutes - 5 <= minutes_since_midnight <= target_minutes + 150):
            logger.info(
                f"Current ET time is {now_et.strftime('%H:%M')} — not within the predictions-run "
                f"window. Exiting (expected: this is one of two DST-safety cron firings)."
            )
            return

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

    # prev_close is just yesterday's close — already known at 6 AM, no need
    # to wait for the market-open checkpoint to show it. stock_data already
    # has it per ticker (computed in fetcher._compute_metrics).
    def _attach_prev_close(predictions):
        if not predictions:
            return predictions
        for key in ("winners", "losers"):
            for entry in predictions.get(key, []):
                # stock_data's "current_price" is Yahoo's most recent completed
                # daily close — at 7 AM, before today's market opens, that IS
                # yesterday's close, exactly the baseline today's prediction
                # needs. stock_data's own "prev_close" field means something
                # different (the close one day further back, used internally
                # for momentum calc) — using it here would attach the wrong
                # day's close.
                entry["prev_close"] = stock_data.get(entry["ticker"], {}).get("current_price")
        return predictions

    claude_predictions = _attach_prev_close(claude_predictions)
    gemini_predictions = _attach_prev_close(gemini_predictions)

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
    from database import save_predictions, get_predictions_for_date, get_actuals_for_date, get_open_prices_for_date
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
    yesterday_open_prices_map = get_open_prices_for_date(yesterday_str)

    if yesterday_predictions:
        actuals_rows = get_actuals_for_date(yesterday_str)
        yesterday_actuals = [
            {"ticker": a["ticker"], "actual_change_pct": a["actual_change_pct"], "close_price": a.get("close_price")}
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
        today_predictions          = primary_predictions,
        yesterday_predictions      = yesterday_predictions,
        yesterday_actuals          = yesterday_actuals,
        yesterday_date_str         = yesterday_str,
        index_data                 = index_data,
        stock_data                 = stock_data,
        claude_predictions         = claude_predictions,
        gemini_predictions         = gemini_predictions,
        yesterday_open_prices_map  = yesterday_open_prices_map,
    )

    _finish(payload)

    from notify import send_prediction_notification
    send_prediction_notification(
        winners=primary_predictions.get("winners", []),
        losers=primary_predictions.get("losers",  [])
    )


def run_backfill(json_path: str):
    """
    One-off disaster-recovery path: restores a day's predictions that were
    genuinely generated (e.g. shown live on the site that morning) but never
    made it into Supabase because the DB was unreachable at save time — this
    is NOT for inventing predictions after the fact. Only use this with a
    real prediction set that actually existed; never backfill a day that
    was never genuinely predicted.

    Saves the given JSON (same shape as an AI prediction result: market_summary,
    winners, losers) under today's CST date, then immediately runs the normal
    end-of-day accuracy scoring against it, since we're still inside the same
    calendar day the predictions belong to.
    """
    import json

    logger.info("=" * 60)
    logger.info(f"StockCast BACKFILL run starting (source: {json_path})...")
    logger.info(f"Date: {today_cst().isoformat()}")
    logger.info("=" * 60)

    with open(json_path) as f:
        predictions = json.load(f)

    from database import save_predictions
    saved = save_predictions(predictions)
    if not saved:
        logger.error("Backfill save failed — aborting, not attempting accuracy scoring.")
        sys.exit(1)

    logger.info(f"Backfilled predictions saved under {today_cst().isoformat()}. Running accuracy scoring now...")
    run_accuracy()


def run_fix_prev_close(date_str: str):
    """
    One-off correction: re-fetches each ticker's real closing price for the
    prior trading day and overwrites prev_close on an already-saved
    prediction row, without touching predicted_change_pct/reason/company —
    the actual AI-generated prediction content stays exactly as it was.
    Only for fixing a wrong baseline value, never for regenerating content.

    Only supports today_cst() — save_predictions() always writes under
    today's date internally, so running this for any other date would
    silently save the fix under the wrong day.
    """
    from datetime import timedelta as _td
    from database import get_predictions_for_date, save_predictions
    from fetcher import fetch_actual_prices

    logger.info("=" * 60)
    logger.info(f"StockCast FIX-PREV-CLOSE run starting (date: {date_str})...")
    logger.info("=" * 60)

    if date_str != today_cst().isoformat():
        logger.error(f"--mode fix_prev_close only supports today ({today_cst().isoformat()}), not {date_str} — aborting.")
        return

    predictions = get_predictions_for_date(date_str)
    if not predictions:
        logger.warning(f"No predictions on record for {date_str} — nothing to fix.")
        return

    entries = predictions.get("winners", []) + predictions.get("losers", [])
    tickers = [e["ticker"] for e in entries]

    baseline_str = (today_cst() - _td(days=1)).isoformat()
    actual_prices = fetch_actual_prices(tickers, baseline_str)

    if not actual_prices:
        for days_back in range(2, 7):
            fallback_str = (today_cst() - _td(days=days_back)).isoformat()
            actual_prices = fetch_actual_prices(tickers, fallback_str)
            if actual_prices:
                logger.info(f"Using {fallback_str} as previous close (weekend/holiday fallback).")
                break

    fixed = 0
    for entry in entries:
        correct = actual_prices.get(entry["ticker"])
        if correct is not None and entry.get("prev_close") != correct:
            logger.info(f"  {entry['ticker']}: prev_close {entry.get('prev_close')} -> {correct}")
            entry["prev_close"] = correct
            fixed += 1

    if fixed == 0:
        logger.info("Nothing needed fixing.")
        return

    save_predictions(predictions)
    logger.info(f"Fixed prev_close on {fixed} entries for {date_str}.")

    # Re-export "today" with the corrected predictions — NOT run_accuracy(),
    # since that only exports after successfully scoring against a market
    # close that may not exist yet (e.g. this runs mid-morning, before
    # today's close). Mirrors run_predictions()'s own Step 5/6 tail.
    logger.info("Re-exporting data.json with corrected prev_close...")
    from database import get_actuals_for_date, get_open_prices_for_date
    from exporter import build_frontend_json, fetch_index_data

    yesterday_str = baseline_str
    yesterday_predictions = get_predictions_for_date(yesterday_str)
    yesterday_actuals = []
    if yesterday_predictions:
        actuals_rows = get_actuals_for_date(yesterday_str)
        yesterday_actuals = [
            {"ticker": a["ticker"], "actual_change_pct": a["actual_change_pct"], "close_price": a.get("close_price")}
            for a in actuals_rows
        ]
    yesterday_open_prices_map = get_open_prices_for_date(yesterday_str)

    index_data = fetch_index_data()
    payload    = build_frontend_json(
        today_predictions          = predictions,
        yesterday_predictions      = yesterday_predictions,
        yesterday_actuals          = yesterday_actuals,
        yesterday_date_str         = yesterday_str,
        index_data                 = index_data,
        stock_data                 = None,
        claude_predictions         = None,
        gemini_predictions         = predictions,
        yesterday_open_prices_map  = yesterday_open_prices_map,
    )
    _finish(payload)


def run_backfill_open(json_path: str, date_str: str):
    """
    One-off historical backfill for the market-open checkpoint, for a day
    that predates the run_market_open() feature. Unlike run_backfill()
    (predictions), this carries no integrity concern — a historical open
    price is an objective market fact recorded by the exchange, not a
    forecast, so looking it up after the fact is a genuine lookup, not a
    fabrication. Only works for recent dates (Yahoo's 1-minute bars are
    only available for roughly the last 30 days).

    json_path: same prediction JSON used for --mode backfill (needs each
    entry's prev_close already attached).
    """
    import json as _json

    logger.info("=" * 60)
    logger.info(f"StockCast BACKFILL-OPEN run starting (source: {json_path}, date: {date_str})...")
    logger.info("=" * 60)

    with open(json_path) as f:
        predictions = _json.load(f)

    entries = predictions.get("winners", []) + predictions.get("losers", [])
    prev_close_map = {e["ticker"]: e["prev_close"] for e in entries if e.get("prev_close") is not None}

    if not prev_close_map:
        logger.warning("No tickers with prev_close in the given file — nothing to backfill.")
        return

    from fetcher import fetch_open_price_at_time
    open_prices = fetch_open_price_at_time(list(prev_close_map.keys()), date_str)

    rows = []
    for ticker, prev_close in prev_close_map.items():
        if ticker in open_prices and prev_close:
            open_price = open_prices[ticker]
            gap_pct = ((open_price - prev_close) / prev_close) * 100
            rows.append({"ticker": ticker, "prev_close": prev_close, "open_price": open_price, "gap_pct": round(gap_pct, 2)})
            logger.info(f"  {ticker}: prev={prev_close}, open={open_price}, gap={gap_pct:+.2f}%")

    if not rows:
        logger.warning("Could not compute any historical open prices — skipping save/export.")
        return

    from database import save_open_prices, get_predictions_for_date, get_actuals_for_date, get_open_prices_for_date
    saved = save_open_prices(date_str, rows)
    if saved:
        logger.info(f"Saved {len(rows)} historical open-price checkpoints for {date_str}.")
    else:
        logger.error(f"Failed to save open-price checkpoints for {date_str} — see error above. Export below will be missing this data.")

    logger.info("Re-exporting data.json with the backfilled open prices...")
    from exporter import build_frontend_json, fetch_index_data

    today_predictions = get_predictions_for_date(date_str)
    actuals_rows = get_actuals_for_date(date_str)
    yesterday_actuals = [
        {"ticker": a["ticker"], "actual_change_pct": a["actual_change_pct"], "close_price": a.get("close_price")}
        for a in actuals_rows
    ]
    prices_map = get_open_prices_for_date(date_str)
    index_data = fetch_index_data()
    payload    = build_frontend_json(
        today_predictions          = today_predictions,
        yesterday_predictions      = today_predictions,
        yesterday_actuals          = yesterday_actuals,
        yesterday_date_str         = date_str,
        index_data                 = index_data,
        stock_data                 = None,
        claude_predictions         = None,
        gemini_predictions         = today_predictions,
        today_open_prices_map      = prices_map,
        yesterday_open_prices_map  = prices_map,
    )
    _finish(payload)


def run_market_open():
    """
    Checkpoint run right at market open (9:30 AM ET / 8:30 AM CT — these are
    always exactly 1 hour apart year-round, since ET and CT shift DST on the
    same date). Purpose: for options trading specifically, a lot of the
    predicted move can already happen in the first few minutes of trading —
    this records that gap so it's visible instead of hidden.

    Scheduled via TWO cron triggers an hour apart (to survive the UTC/DST
    mismatch GitHub Actions cron can't express), but only the one that
    actually lands within a few minutes of real market open does anything —
    the other exits immediately. No new AI call, no new predictions — just
    a price snapshot against predictions already saved this morning.
    """
    import pytz
    from datetime import datetime as _dt

    et     = pytz.timezone("America/New_York")
    now_et = _dt.now(et)

    if now_et.weekday() >= 5:
        logger.info(f"{now_et.strftime('%A')} — market closed. Exiting (expected on weekends).")
        return

    minutes_since_midnight = now_et.hour * 60 + now_et.minute
    market_open_minutes    = 9 * 60 + 30
    if not (market_open_minutes - 2 <= minutes_since_midnight <= market_open_minutes + 10):
        logger.info(
            f"Current ET time is {now_et.strftime('%H:%M')} — not within the market-open "
            f"window. Exiting (expected: this is one of two DST-safety cron firings)."
        )
        return

    logger.info("=" * 60)
    logger.info("StockCast MARKET-OPEN checkpoint starting...")
    logger.info(f"ET time: {now_et.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    from database import get_predictions_for_date, save_open_prices, get_open_prices_for_date, get_actuals_for_date
    from fetcher import fetch_actual_prices

    today_str = today_cst().isoformat()
    today_predictions = get_predictions_for_date(today_str)

    if not today_predictions:
        logger.warning(f"No predictions on record for {today_str} — nothing to checkpoint.")
        return

    all_tickers = (
        [e["ticker"] for e in today_predictions.get("winners", [])] +
        [e["ticker"] for e in today_predictions.get("losers",  [])]
    )
    if not all_tickers:
        logger.warning("Today's prediction record has no tickers — nothing to checkpoint.")
        return

    yesterday_str = (today_cst() - timedelta(days=1)).isoformat()
    open_prices   = fetch_actual_prices(all_tickers, today_str)      # live price right now, at the open
    prev_prices   = fetch_actual_prices(all_tickers, yesterday_str)  # yesterday's close (baseline)

    if not prev_prices:
        for days_back in range(2, 7):
            fallback_str = (today_cst() - timedelta(days=days_back)).isoformat()
            prev_prices  = fetch_actual_prices(all_tickers, fallback_str)
            if prev_prices:
                logger.info(f"Using {fallback_str} as previous close (weekend/holiday fallback).")
                break

    rows = []
    for ticker in all_tickers:
        if ticker in open_prices and ticker in prev_prices and prev_prices[ticker] > 0:
            gap_pct = ((open_prices[ticker] - prev_prices[ticker]) / prev_prices[ticker]) * 100
            rows.append({
                "ticker":     ticker,
                "prev_close": prev_prices[ticker],
                "open_price": open_prices[ticker],
                "gap_pct":    round(gap_pct, 2),
            })
            logger.info(f"  {ticker}: prev_close={prev_prices[ticker]}, open={open_prices[ticker]}, gap={gap_pct:+.2f}%")

    if not rows:
        logger.warning("Could not compute any open-price checkpoints — skipping save/export.")
        return

    saved = save_open_prices(today_str, rows)
    if saved:
        logger.info(f"Saved {len(rows)} open-price checkpoints under {today_str}.")
    else:
        logger.error(f"Failed to save open-price checkpoints for {today_str} — see error above. Export below will be missing this data.")

    logger.info("Exporting updated data.json with market-open checkpoint...")
    from exporter import build_frontend_json, fetch_index_data

    yesterday_predictions = get_predictions_for_date(yesterday_str)
    yesterday_actuals = []
    if yesterday_predictions:
        actuals_rows = get_actuals_for_date(yesterday_str)
        yesterday_actuals = [
            {"ticker": a["ticker"], "actual_change_pct": a["actual_change_pct"], "close_price": a.get("close_price")}
            for a in actuals_rows
        ]

    today_prices_map     = get_open_prices_for_date(today_str)
    yesterday_prices_map = get_open_prices_for_date(yesterday_str)
    index_data = fetch_index_data()
    payload    = build_frontend_json(
        today_predictions          = today_predictions,
        yesterday_predictions      = yesterday_predictions,
        yesterday_actuals          = yesterday_actuals,
        yesterday_date_str         = yesterday_str,
        index_data                 = index_data,
        stock_data                 = None,
        claude_predictions         = None,
        gemini_predictions         = today_predictions,
        today_open_prices_map      = today_prices_map,
        yesterday_open_prices_map  = yesterday_prices_map,
    )

    _finish(payload)


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

    save_actuals(today_str, actuals_pct, today_predictions, close_prices=actual_prices)
    logger.info(f"Saved actuals for {len(actuals_pct)} tickers under {today_str}.")

    yesterday_actuals = [
        {"ticker": t, "actual_change_pct": pct, "close_price": actual_prices.get(t)}
        for t, pct in actuals_pct.items()
    ]

    # Re-export data.json so the site shows today's completed scoring tonight.
    # "today" block is reconstructed from the DB record — matches the current
    # single-model persistence (only the model that actually saved survives;
    # right now that's Gemini, since no ANTHROPIC_API_KEY is set).
    # full_ranked_list is intentionally left empty here — refreshing ~100
    # tickers' price history again at EOD isn't worth doubling the stock-data
    # pull; it gets refreshed normally on tomorrow's predictions run.
    # yesterday_date_str is today_str here (EOD scores TODAY's own predictions),
    # so today's and yesterday's open-price checkpoint map are the same date.
    logger.info("Exporting updated data.json with today's completed accuracy...")
    from exporter import build_frontend_json, write_json, fetch_index_data
    from database import get_open_prices_for_date

    todays_open_prices_map = get_open_prices_for_date(today_str)
    index_data = fetch_index_data()
    payload    = build_frontend_json(
        today_predictions          = today_predictions,
        yesterday_predictions      = today_predictions,
        yesterday_actuals          = yesterday_actuals,
        yesterday_date_str         = today_str,
        index_data                 = index_data,
        stock_data                 = None,
        claude_predictions         = None,
        gemini_predictions         = today_predictions,
        today_open_prices_map      = todays_open_prices_map,
        yesterday_open_prices_map  = todays_open_prices_map,
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
    parser.add_argument("--mode", choices=["predictions", "market_open", "accuracy", "backfill", "backfill_open", "fix_prev_close"], default="predictions")
    parser.add_argument("--file", help="JSON file path for --mode backfill / backfill_open (see backfill_data/)")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD) for --mode backfill_open / fix_prev_close")
    args = parser.parse_args()

    if args.mode == "predictions":
        run_predictions()
    elif args.mode == "market_open":
        run_market_open()
    elif args.mode == "accuracy":
        run_accuracy()
    elif args.mode == "backfill":
        if not args.file:
            parser.error("--mode backfill requires --file <path to prediction JSON>")
        run_backfill(args.file)
    elif args.mode == "backfill_open":
        if not args.file or not args.date:
            parser.error("--mode backfill_open requires --file <path> and --date <YYYY-MM-DD>")
        run_backfill_open(args.file, args.date)
    elif args.mode == "fix_prev_close":
        if not args.date:
            parser.error("--mode fix_prev_close requires --date <YYYY-MM-DD>")
        run_fix_prev_close(args.date)
