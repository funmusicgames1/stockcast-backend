"""
analyzer.py — Sends stock data + news to Anthropic Claude and gets back
structured predictions for top 10 winners and top 10 losers.
"""

import os
import json
import logging
import anthropic

logger = logging.getLogger(__name__)


def build_prompt(stock_data: dict, news: dict) -> str:
    """Build the analysis prompt with all market data and news context."""

    # Summarize stock data compactly
    stock_lines = []
    for ticker, d in stock_data.items():
        stock_lines.append(
            f"{ticker}: price=${d['current_price']}, "
            f"1d={d['daily_change_pct']:+.1f}%, "
            f"1w={d['weekly_change_pct']:+.1f}%, "
            f"1m={d['monthly_change_pct']:+.1f}%, "
            f"vol_ratio={d['volume_ratio']:.1f}x, "
            f"momentum={d['momentum_score']:.0f}/100, "
            f"volatility={d['volatility_30d']:.1f}%"
        )

    stocks_block = "\n".join(stock_lines)

    macro_headlines = "\n".join(f"- {h}" for h in news.get("macro", [])[:15])

    sector_blocks = ""
    for sector, headlines in news.get("sector", {}).items():
        if headlines:
            sector_blocks += f"\n{sector.upper()}:\n"
            sector_blocks += "\n".join(f"  - {h}" for h in headlines[:4])

    prompt = f"""You are a quantitative stock market analyst. Based on the following market data and news, predict which stocks are most likely to move significantly today.

=== STOCK UNIVERSE DATA ===
(Format: ticker: current price, 1-day change, 1-week change, 1-month change, volume ratio vs 30d avg, momentum score 0-100, 30d volatility)

{stocks_block}

=== MACRO & GEOPOLITICAL NEWS ===
{macro_headlines}

=== SECTOR NEWS ===
{sector_blocks}

=== YOUR TASK ===
Analyze all stocks and identify:
1. TOP 10 EXPECTED WINNERS — stocks most likely to rise today
2. TOP 10 EXPECTED LOSERS — stocks most likely to fall today

For each stock provide:
- Predicted % move for the day (be specific, e.g. +2.4% not just "positive")
- A concise reason (max 6 words) based on the data and news

Consider: momentum, volume spikes, sector news catalysts, macro conditions, recent trend reversals, earnings proximity, and geopolitical impacts.

Respond ONLY with valid JSON in this exact format, no other text:

{{
  "date": "YYYY-MM-DD",
  "market_summary": "One sentence summary of today's market conditions",
  "winners": [
    {{
      "rank": 1,
      "ticker": "XXXX",
      "company": "Full Company Name",
      "sector": "Sector Name",
      "predicted_change_pct": 3.2,
      "reason": "Short reason here"
    }}
  ],
  "losers": [
    {{
      "rank": 1,
      "ticker": "XXXX",
      "company": "Full Company Name",
      "sector": "Sector Name",
      "predicted_change_pct": -2.8,
      "reason": "Short reason here"
    }}
  ]
}}

Rules:
- winners list must have exactly 10 items, ranked by predicted gain descending
- losers list must have exactly 10 items, ranked by predicted loss descending (most negative first)
- predicted_change_pct for winners must be positive numbers
- predicted_change_pct for losers must be negative numbers
- reason must be 6 words or fewer
- Only pick tickers from the stock universe provided
- Return ONLY the JSON object, no markdown, no explanation
"""
    return prompt


def analyze(stock_data: dict, news: dict) -> dict | None:
    """
    Run AI analysis and return structured prediction dict.
    Returns None if analysis fails.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set.")
        return None

    client = anthropic.Anthropic(api_key=api_key)

    prompt = build_prompt(stock_data, news)
    logger.info("Sending analysis request to Claude...")

    try:
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_response = message.content[0].text.strip()
        logger.info("Received response from Claude.")

        # Strip markdown code fences if present
        if raw_response.startswith("```"):
            raw_response = raw_response.split("```")[1]
            if raw_response.startswith("json"):
                raw_response = raw_response[4:]
            raw_response = raw_response.strip()

        predictions = json.loads(raw_response)

        # Validate structure
        assert "winners" in predictions and len(predictions["winners"]) == 10
        assert "losers" in predictions and len(predictions["losers"]) == 10

        logger.info("Predictions parsed and validated successfully.")
        return predictions

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Claude response as JSON: {e}")
        logger.error(f"Raw response: {raw_response[:500]}")
        return None
    except AssertionError:
        logger.error("Claude response missing required fields or wrong count.")
        return None
    except Exception as e:
        logger.error(f"Anthropic API call failed: {e}")
        return None
