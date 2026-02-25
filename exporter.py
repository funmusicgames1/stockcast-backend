"""
exporter.py — Generates the data.json file that the frontend reads.
This file is the bridge between the backend and the static frontend.
"""

import json
import os
import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def build_frontend_json(
    today_predictions: dict,
    yesterday_predictions: dict | None,
    yesterday_actuals: list,
    index_data: dict,
) -> dict:
    """
    Assemble the full JSON payload the frontend needs.

    Args:
        today_predictions: today's winner/loser predictions from AI
        yesterday_predictions: yesterday's predictions (for performance section)
        yesterday_actuals: actual % changes fetched after market close
        index_data: S&P 500, NASDAQ, DOW snapshot (from yfinance)

    Returns:
        dict ready to serialize as data.json
    """

    # Build actuals lookup
    actuals_map = {a["ticker"]: a["actual_change_pct"] for a in yesterday_actuals}

    # Enrich yesterday's predictions with actual results
    def enrich_with_actuals(entries: list, prediction_type: str) -> list:
        enriched = []
        for entry in entries:
            ticker = entry["ticker"]
            actual = actuals_map.get(ticker)
            accuracy = None

            if actual is not None:
                predicted = entry["predicted_change_pct"]
                # Accuracy: direction correct = 50 pts base,
                # magnitude closeness = up to 50 pts
                direction_correct = (predicted > 0 and actual > 0) or (predicted < 0 and actual < 0)
                if direction_correct:
                    diff = abs(abs(predicted) - abs(actual))
                    magnitude_score = max(0, 50 - (diff * 10))
                    accuracy = round(50 + magnitude_score)
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

    yesterday_winners_enriched = []
    yesterday_losers_enriched = []

    if yesterday_predictions:
        yesterday_winners_enriched = enrich_with_actuals(
            yesterday_predictions.get("winners", []), "winner"
        )
        yesterday_losers_enriched = enrich_with_actuals(
            yesterday_predictions.get("losers", []), "loser"
        )

    payload = {
        "generated_at": date.today().isoformat() + "T06:00:00-06:00",
        "today": {
            "date": date.today().isoformat(),
            "market_summary": today_predictions.get("market_summary", ""),
            "winners": today_predictions.get("winners", []),
            "losers": today_predictions.get("losers", []),
        },
        "yesterday": {
            "date": (date.today() - timedelta(days=1)).isoformat(),
            "winners": yesterday_winners_enriched,
            "losers": yesterday_losers_enriched,
            "has_actuals": len(actuals_map) > 0,
        },
        "indices": index_data,
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
