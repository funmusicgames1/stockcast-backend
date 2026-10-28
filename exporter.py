"""
exporter.py — Generates the data.json file that the frontend reads.
This file is the bridge between the backend and the static frontend.
"""

import json
import os
import logging
from datetime_utils import today_cst

logger = logging.getLogger(__name__)


def build_full_ranked_list(stock_data: dict) -> list:
    """
    Build a full ranked list of all stocks by momentum + volume signal.
    Used for the downloadable CSV — gives users the complete picture
    beyond just the top 10 shown on screen.
    """
    ranked = []
    for ticker, d in stock_data.items():
        # Simple composite score: momentum + volume signal + weekly trend
        score = (
            d.get("momentum_score", 50) * 0.4 +
            min(d.get("volume_ratio", 1.0) * 10, 30) * 0.3 +
            (d.get("weekly_change_pct", 0) * 2) * 0.3
        )
        ranked.append({
            "ticker": ticker,
            "current_price": d.get("current_price"),
            "daily_change_pct": d.get("daily_change_pct"),
            "weekly_change_pct": d.get("weekly_change_pct"),
            "monthly_change_pct": d.get("monthly_change_pct"),
            "volume_ratio": d.get("volume_ratio"),
            "momentum_score": d.get("momentum_score"),
            "composite_score": round(score, 2),
        })
    ranked.sort(key=lambda x: x["composite_score"], reverse=True)
    return ranked


def build_frontend_json(
    today_predictions: dict,
    yesterday_predictions: dict | None,
    yesterday_actuals: list,
    yesterday_date_str: str,
    index_data: dict,
    stock_data: dict = None,
    claude_predictions: dict | None = None,
    gemini_predictions: dict | None = None,
    today_open_prices_map: dict | None = None,
    yesterday_open_prices_map: dict | None = None,
) -> dict:
    """
    Assemble the full JSON payload the frontend needs.
    Includes separate claude and gemini prediction sets for model comparison.

    yesterday_date_str: the calendar date the "yesterday"/Historical Performance
    block actually belongs to. Morning (predictions) runs pass the prior calendar
    day, since that's the most recently fully-scored day on record. EOD (accuracy)
    runs pass today's own date, since they're scoring today's predictions against
    today's close the same evening instead of waiting for tomorrow morning.

    today_open_prices_map / yesterday_open_prices_map: {ticker: {"prev_close",
    "open_price", "gap_pct"}} from the market-open checkpoint run (8:31 AM CST),
    keyed to whichever calendar date each block actually needs — these are often
    the SAME date (an EOD run's "yesterday" slot is today's own date) but not
    always, so they're passed separately rather than assumed to match.
    """

    # Build actuals lookup — each entry carries both the % change and the raw
    # closing price (close_price), so Historical Performance can show real
    # dollar figures (Prev Close / Open / Close), not just percentages.
    actuals_map = {a["ticker"]: a for a in yesterday_actuals}

    def enrich_with_actuals(entries: list, prediction_type: str) -> list:
        enriched = []
        for entry in entries:
            ticker      = entry["ticker"]
            actual_row  = actuals_map.get(ticker, {})
            actual      = actual_row.get("actual_change_pct")
            close_price = actual_row.get("close_price")
            accuracy    = None

            if actual is not None:
                predicted         = entry["predicted_change_pct"]
                direction_correct = (predicted > 0 and actual > 0) or (predicted < 0 and actual < 0)
                if direction_correct:
                    diff           = abs(abs(predicted) - abs(actual))
                    magnitude_score = max(0, 50 - (diff * 10))
                    accuracy       = round(50 + magnitude_score)
                else:
                    accuracy = max(0, round(50 - abs(actual - predicted) * 5))

                outcome = "beat" if abs(actual) > abs(predicted) and direction_correct else \
                          "miss" if not direction_correct else "close"
            else:
                outcome = "pending"

            enriched.append({
                **entry,
                "prediction_type": prediction_type,
                "actual_change_pct": actual,
                "close_price": close_price,
                "accuracy_score": accuracy,
                "outcome": outcome,
            })
        return enriched

    def enrich_with_open_price(entries: list, price_map: dict | None) -> list:
        enriched = []
        for entry in entries:
            row = (price_map or {}).get(entry["ticker"])
            # prev_close is already attached by main.py at prediction time
            # (it's just yesterday's close, known at 6 AM) — only fall back
            # to the checkpoint row if the entry itself doesn't have it yet
            # (e.g. a day backfilled before that enrichment existed).
            prev_close = entry.get("prev_close")
            open_price = gap_pct = gap_captured_pct = None
            if row:
                if prev_close is None:
                    prev_close = row.get("prev_close")
                open_price = row.get("open_price")
                gap_pct    = row.get("gap_pct")
                predicted  = entry.get("predicted_change_pct")
                if gap_pct is not None and predicted:
                    gap_captured_pct = round((gap_pct / predicted) * 100, 1)
            enriched.append({
                **entry,
                "prev_close":       prev_close,
                "open_price":       open_price,
                "gap_pct":          gap_pct,
                "gap_captured_pct": gap_captured_pct,
            })
        return enriched

    # Enrich yesterday for each model separately
    def build_yesterday_block(preds: dict | None) -> dict:
        if not preds:
            return {"winners": [], "losers": [], "has_actuals": False}
        winners = enrich_with_open_price(enrich_with_actuals(preds.get("winners", []), "winner"), yesterday_open_prices_map)
        losers  = enrich_with_open_price(enrich_with_actuals(preds.get("losers",  []), "loser"),  yesterday_open_prices_map)
        return {
            "date":        yesterday_date_str,
            "winners":     winners,
            "losers":      losers,
            "has_actuals": len(actuals_map) > 0,
        }

    from datetime import datetime
    import pytz
    cst      = pytz.timezone("America/Chicago")
    now_cst  = datetime.now(cst)
    generated_at = now_cst.strftime("%Y-%m-%dT%H:%M:%S%z")

    payload = {
        "generated_at": generated_at,
        "today": {
            "date": today_cst().isoformat(),
            "claude": {
                "market_summary": claude_predictions.get("market_summary", "") if claude_predictions else None,
                "winners":        enrich_with_open_price(claude_predictions.get("winners", []), today_open_prices_map) if claude_predictions else [],
                "losers":         enrich_with_open_price(claude_predictions.get("losers",  []), today_open_prices_map) if claude_predictions else [],
                "available":      claude_predictions is not None,
            },
            "gemini": {
                "market_summary": gemini_predictions.get("market_summary", "") if gemini_predictions else None,
                "winners":        enrich_with_open_price(gemini_predictions.get("winners", []), today_open_prices_map) if gemini_predictions else [],
                "losers":         enrich_with_open_price(gemini_predictions.get("losers",  []), today_open_prices_map) if gemini_predictions else [],
                "available":      gemini_predictions is not None,
            },
        },
        "yesterday": {
            "date":   yesterday_date_str,
            "claude": build_yesterday_block(yesterday_predictions),
            "gemini": build_yesterday_block(yesterday_predictions),  # same actuals, different predictions will be stored later
        },
        "indices":          index_data,
        "full_ranked_list": build_full_ranked_list(stock_data) if stock_data else [],
    }

    return payload


def write_json(payload: dict) -> bool:
    """Write the payload to data.json."""
    output_path = os.getenv("OUTPUT_JSON_PATH", "./data.json")

    try:
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        logger.info(f"data.json written to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write data.json: {e}")
        return False


def fetch_index_data() -> dict:
    """
    Fetch S&P 500, NASDAQ, DOW from Yahoo Finance's chart API.
    FMP's /quote endpoint 403s on index symbols (^GSPC etc.) on the free
    tier — index quotes require a paid FMP plan. Yahoo serves these with
    no key and no plan restriction, and is already the primary source for
    everything else in this pipeline.
    """
    import requests

    indices = {"sp500": "%5EGSPC", "nasdaq": "%5EIXIC", "dow": "%5EDJI"}
    headers = {"User-Agent": "Mozilla/5.0"}
    result  = {}

    for name, symbol in indices.items():
        try:
            url  = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            resp = requests.get(url, params={"range": "1d", "interval": "1d"}, headers=headers, timeout=15)
            resp.raise_for_status()
            meta  = resp.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev  = meta.get("chartPreviousClose")
            if price is not None and prev:
                change_pct = ((price - prev) / prev) * 100
                result[name] = {
                    "value":      round(float(price), 2),
                    "change_pct": round(change_pct, 2),
                    "direction":  "up" if change_pct >= 0 else "down",
                }
                continue
            result[name] = {"value": None, "change_pct": None, "direction": "neutral"}
        except Exception as e:
            logger.warning(f"Yahoo index fetch failed for {name}: {e}")
            result[name] = {"value": None, "change_pct": None, "direction": "neutral"}

    return result
