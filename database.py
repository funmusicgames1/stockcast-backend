"""
database.py — All Supabase read/write operations.

Tables needed (create these in Supabase SQL editor):

CREATE TABLE predictions (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    market_summary TEXT,
    winners JSONB NOT NULL,
    losers JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE actuals (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    predicted_change_pct FLOAT,
    actual_change_pct FLOAT,
    prediction_type TEXT,  -- 'winner' or 'loser'
    UNIQUE(date, ticker)
);
"""

import os
import logging
from datetime import date, timedelta
from supabase import create_client, Client

logger = logging.getLogger(__name__)


def get_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment.")
    return create_client(url, key)


def save_predictions(predictions: dict) -> bool:
    """
    Save today's predictions to the predictions table.
    If a record for today already exists, update it.
    """
    try:
        client = get_client()
        today = date.today().isoformat()

        data = {
            "date": today,
            "market_summary": predictions.get("market_summary", ""),
            "winners": predictions["winners"],
            "losers": predictions["losers"],
        }

        # Upsert — insert or update if date already exists
        result = client.table("predictions").upsert(data, on_conflict="date").execute()
        logger.info(f"Saved predictions for {today} to Supabase.")
        return True

    except Exception as e:
        logger.error(f"Failed to save predictions to Supabase: {e}")
        return False


def save_actuals(date_str: str, actuals_map: dict, predictions: dict) -> bool:
    """
    After market close, save the actual price changes alongside predictions.

    actuals_map: {ticker: actual_close_price}
    predictions: the prediction record for date_str
    """
    try:
        client = get_client()
        rows = []

        for entry in predictions.get("winners", []):
            ticker = entry["ticker"]
            if ticker in actuals_map:
                rows.append({
                    "date": date_str,
                    "ticker": ticker,
                    "predicted_change_pct": entry["predicted_change_pct"],
                    "actual_change_pct": actuals_map[ticker],
                    "prediction_type": "winner",
                })

        for entry in predictions.get("losers", []):
            ticker = entry["ticker"]
            if ticker in actuals_map:
                rows.append({
                    "date": date_str,
                    "ticker": ticker,
                    "predicted_change_pct": entry["predicted_change_pct"],
                    "actual_change_pct": actuals_map[ticker],
                    "prediction_type": "loser",
                })

        if rows:
            client.table("actuals").upsert(rows, on_conflict="date,ticker").execute()
            logger.info(f"Saved {len(rows)} actual results for {date_str}.")
        return True

    except Exception as e:
        logger.error(f"Failed to save actuals: {e}")
        return False


def get_predictions_for_date(date_str: str) -> dict | None:
    """Fetch predictions for a specific date."""
    try:
        client = get_client()
        result = (
            client.table("predictions")
            .select("*")
            .eq("date", date_str)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Failed to fetch predictions for {date_str}: {e}")
        return None


def get_actuals_for_date(date_str: str) -> list:
    """Fetch actual results for a specific date."""
    try:
        client = get_client()
        result = (
            client.table("actuals")
            .select("*")
            .eq("date", date_str)
            .execute()
        )
        return result.data or []
    except Exception as e:
        logger.error(f"Failed to fetch actuals for {date_str}: {e}")
        return []


def get_recent_history(days: int = 30) -> list:
    """
    Fetch the last N days of predictions + actuals combined.
    Used to build the archive section of the frontend.
    """
    try:
        client = get_client()
        from_date = (date.today() - timedelta(days=days)).isoformat()

        result = (
            client.table("predictions")
            .select("date, market_summary, winners, losers")
            .gte("date", from_date)
            .order("date", desc=True)
            .execute()
        )

        history = []
        for record in result.data or []:
            actuals = get_actuals_for_date(record["date"])
            actuals_map = {a["ticker"]: a for a in actuals}
            history.append({
                **record,
                "actuals": actuals_map,
            })

        return history

    except Exception as e:
        logger.error(f"Failed to fetch history: {e}")
        return []
