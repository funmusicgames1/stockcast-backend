"""
analyzer.py — Runs AI analysis using both Claude and Gemini in parallel.
Returns separate prediction sets for each model.
"""

import os
import json
import logging
import concurrent.futures

logger = logging.getLogger(__name__)


def build_prompt(stock_data: dict, news: dict) -> str:
    """Build the shared analysis prompt for both models."""
    stock_lines = []
    for ticker, d in stock_data.items():
        stock_lines.append(
            f"{ticker}: price=${d['current_price']}, "
            f"1d={d['daily_change_pct']:+.1f}%, "
            f"1w={d['weekly_change_pct']:+.1f}%, "
            f"1m={d['monthly_change_pct']:+.1f}%, "
            f"vol_ratio={d['volume_ratio']:.1f}x, "
            f"momentum={d['momentum_score']:.0f}/100"
        )

    stocks_block    = "\n".join(stock_lines)
    macro_headlines = "\n".join(f"- {h}" for h in news.get("macro", [])[:15])
    sector_blocks   = ""
    for sector, headlines in news.get("sector", {}).items():
        if headlines:
            sector_blocks += f"\n{sector.upper()}:\n"
            sector_blocks += "\n".join(f"  - {h}" for h in headlines[:4])

    prompt = f"""You are a quantitative stock market analyst. Based on the following market data and news, predict which stocks are most likely to move significantly today.

=== STOCK UNIVERSE DATA ===
(Format: ticker: current price, 1-day change, 1-week change, 1-month change, volume ratio vs 30d avg, momentum score 0-100)

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


def _parse_response(raw: str) -> dict | None:
    """Strip markdown fences and parse JSON response."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    try:
        predictions = json.loads(raw)
        assert "winners" in predictions and len(predictions["winners"]) == 10
        assert "losers"  in predictions and len(predictions["losers"])  == 10
        return predictions
    except (json.JSONDecodeError, AssertionError) as e:
        logger.error(f"Parse/validation failed: {e}")
        return None


def _run_claude(prompt: str) -> dict | None:
    """Call Anthropic Claude API and return parsed predictions."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set — skipping Claude")
        return None
    try:
        import anthropic
        client  = anthropic.Anthropic(api_key=api_key)
        logger.info("[Claude] Sending request...")
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text
        logger.info("[Claude] Response received.")
        result = _parse_response(raw)
        if result:
            result["model"] = "claude"
        return result
    except Exception as e:
        logger.error(f"[Claude] API call failed: {e}")
        return None


def _run_gemini(prompt: str) -> dict | None:
    """Call Google Gemini API and return parsed predictions. Retries once on 429."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not set — skipping Gemini")
        return None
    try:
        import time
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=api_key)

        for attempt in range(3):  # up to 3 attempts
            try:
                logger.info(f"[Gemini] Sending request (attempt {attempt + 1})...")
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.7,
                        max_output_tokens=2048,
                    )
                )
                raw = response.text
                logger.info("[Gemini] Response received.")
                result = _parse_response(raw)
                if result:
                    result["model"] = "gemini"
                return result
            except Exception as e:
                err_str = str(e)
                if "429" in err_str and attempt < 2:
                    wait = 30 * (attempt + 1)
                    logger.warning(f"[Gemini] Rate limited (429). Waiting {wait}s before retry...")
                    time.sleep(wait)
                else:
                    raise
    except Exception as e:
        logger.error(f"[Gemini] API call failed: {e}")
        return None


def analyze(stock_data: dict, news: dict) -> dict | None:
    """
    Run AI analysis using both Claude and Gemini in parallel.
    Returns {"claude": {...}, "gemini": {...}}.
    At least one model must succeed — otherwise returns None to abort pipeline.
    """
    prompt = build_prompt(stock_data, news)

    # Run both models concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_claude = executor.submit(_run_claude, prompt)
        future_gemini = executor.submit(_run_gemini, prompt)
        claude_result = future_claude.result()
        gemini_result = future_gemini.result()

    if claude_result:
        logger.info("[Claude] Predictions parsed and validated.")
    else:
        logger.warning("[Claude] Failed — will be absent from output.")

    if gemini_result:
        logger.info("[Gemini] Predictions parsed and validated.")
    else:
        logger.warning("[Gemini] Failed — will be absent from output.")

    # Abort only if BOTH models fail
    if not claude_result and not gemini_result:
        logger.error("Both Claude and Gemini failed. Aborting pipeline.")
        return None

    return {
        "claude": claude_result,
        "gemini": gemini_result,
    }
