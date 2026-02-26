"""
exporter.py — Generates the data.json file that the frontend reads.
This file is the bridge between the backend and the static frontend.
"""

import json
import os
import logging
from datetime import date, timedelta

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
    index_data: dict,
    stock_data: dict = None,
    claude_predictions: dict | None = None,
    gemini_predictions: dict | None = None,
) -> dict:
    """
    Assemble the full JSON payload the frontend needs.
    Includes separate claude and gemini prediction sets for model comparison.
    """

    # Build actuals lookup
    actuals_map = {a["ticker"]: a["actual_change_pct"] for a in yesterday_actuals}

    def enrich_with_actuals(entries: list, prediction_type: str) -> list:
        enriched = []
        for entry in entries:
            ticker   = entry["ticker"]
            actual   = actuals_map.get(ticker)
            accuracy = None

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
                "accuracy_score": accuracy,
                "outcome": outcome,
            })
        return enriched

    # Enrich yesterday for each model separately
    def build_yesterday_block(preds: dict | None) -> dict:
        if not preds:
            return {"winners": [], "losers": [], "has_actuals": False}
        return {
            "date":        (date.today() - timedelta(days=1)).isoformat(),
            "winners":     enrich_with_actuals(preds.get("winners", []), "winner"),
            "losers":      enrich_with_actuals(preds.get("losers",  []), "loser"),
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
            "date": date.today().isoformat(),
            "claude": {
                "market_summary": claude_predictions.get("market_summary", "") if claude_predictions else None,
                "winners":        claude_predictions.get("winners", [])         if claude_predictions else [],
                "losers":         claude_predictions.get("losers",  [])         if claude_predictions else [],
                "available":      claude_predictions is not None,
            },
            "gemini": {
                "market_summary": gemini_predictions.get("market_summary", "") if gemini_predictions else None,
                "winners":        gemini_predictions.get("winners", [])         if gemini_predictions else [],
                "losers":         gemini_predictions.get("losers",  [])         if gemini_predictions else [],
                "available":      gemini_predictions is not None,
            },
        },
        "yesterday": {
            "date":   (date.today() - timedelta(days=1)).isoformat(),
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
    Fetch S&P 500, NASDAQ, DOW current values using yfinance.
    Returns a lightweight dict for the market overview strip.
    """
    import yfinance as yf

    tickers = {"sp500": "^GSPC", "nasdaq": "^IXIC", "dow": "^DJI"}
    result = {}

    for name, symbol in tickers.items():
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="2d")
            if len(hist) >= 2:
                current = float(hist["Close"].iloc[-1])
                prev = float(hist["Close"].iloc[-2])
                change_pct = ((current - prev) / prev) * 100
                result[name] = {
                    "value": round(current, 2),
                    "change_pct": round(change_pct, 2),
                    "direction": "up" if change_pct >= 0 else "down",
                }
            else:
                result[name] = {"value": None, "change_pct": None, "direction": "neutral"}
        except Exception as e:
            logger.warning(f"Failed to fetch index {symbol}: {e}")
            result[name] = {"value": None, "change_pct": None, "direction": "neutral"}

    return result
