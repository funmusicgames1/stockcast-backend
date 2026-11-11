"""
levels.py — Daily OHLC + Pivot/Support/Resistance + gap-based X1-X5 levels
for a small fixed watchlist (QQQ, SPY, IWM, TSLA, AMZN, META). Independent of
the AI prediction pipeline — these are objective calculations, not forecasts.

Delivered in two stages (see main.py run_levels_morning/run_levels_gap):
  1. OHLC + Pivot/Support/Resistance -- fully knowable right after the prior
     day's close, sent at 7:00 AM ET.
  2. X1-X5 gap levels -- need today's first 3-min candle, only available
     after today's open, sent at ~9:34 AM ET.
"""

import logging

logger = logging.getLogger(__name__)

LEVELS_TICKERS = ["QQQ", "SPY", "IWM", "TSLA", "AMZN", "META"]


def compute_pivot_levels(prev_trading_date_str: str) -> list[dict]:
    """
    OHLC + classic Pivot/Support/Resistance from the prior trading day's bar.
    pivot = (H+L+C)/3 ; support = 2*pivot - H ; resistance = 2*pivot - L
    """
    from fetcher import fetch_daily_ohlc

    ohlc = fetch_daily_ohlc(LEVELS_TICKERS, prev_trading_date_str)

    rows = []
    for ticker in LEVELS_TICKERS:
        bar = ohlc.get(ticker)
        if not bar or bar.get("high") is None or bar.get("low") is None or bar.get("close") is None:
            logger.warning(f"[{ticker}] No daily OHLC for {prev_trading_date_str} — skipping.")
            continue

        h, l, c = bar["high"], bar["low"], bar["close"]
        pivot      = (h + l + c) / 3
        support    = 2 * pivot - h
        resistance = 2 * pivot - l

        rows.append({
            "ticker":     ticker,
            "prev_date":  prev_trading_date_str,
            "open":       bar["open"],
            "high":       h,
            "low":        l,
            "close":      c,
            "pivot":      round(pivot, 2),
            "support":    round(support, 2),
            "resistance": round(resistance, 2),
        })

    logger.info(f"Computed Pivot/S/R for {len(rows)}/{len(LEVELS_TICKERS)} tickers from {prev_trading_date_str}.")
    return rows


def compute_gap_levels(today_date_str: str, prev_trading_date_str: str) -> list[dict]:
    """
    X1-X5 gap levels, per the user's formula:
      A1 = prev day's last 3-min candle close (15:57 ET)
      B1 = today's first 3-min candle open (9:30 ET)
      gap up (A1 < B1):   X1 = prev candle High, X2 = today candle High
      gap down (A1 > B1): X1 = prev candle Low,  X2 = today candle Low
      X3 = X2 - X1 ; X4 = X3/2
      X5 = X1 - X4 (gap up) or X2 + X4 (gap down)
    """
    from fetcher import fetch_3min_candle

    prev_candles  = fetch_3min_candle(LEVELS_TICKERS, prev_trading_date_str, 15, 57)
    today_candles = fetch_3min_candle(LEVELS_TICKERS, today_date_str, 9, 30)

    rows = []
    for ticker in LEVELS_TICKERS:
        prev  = prev_candles.get(ticker)
        today = today_candles.get(ticker)
        if not prev or not today:
            logger.warning(f"[{ticker}] Missing 3-min candle data — skipping gap levels.")
            continue

        a1, b1 = prev["close"], today["open"]
        if a1 is None or b1 is None:
            continue

        gap_up = a1 < b1
        x1 = prev["high"] if gap_up else prev["low"]
        x2 = today["high"] if gap_up else today["low"]
        x3 = x2 - x1
        x4 = x3 / 2
        x5 = (x1 - x4) if gap_up else (x2 + x4)

        rows.append({
            "ticker":        ticker,
            "a1":            a1,
            "b1":            b1,
            "gap_direction": "up" if gap_up else "down",
            "x1": round(x1, 2),
            "x2": round(x2, 2),
            "x3": round(x3, 2),
            "x4": round(x4, 2),
            "x5": round(x5, 2),
        })

    logger.info(f"Computed gap X1-X5 for {len(rows)}/{len(LEVELS_TICKERS)} tickers.")
    return rows
