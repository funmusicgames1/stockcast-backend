# MarketPulse — Backend

AI-powered daily stock prediction engine. Runs once daily at 6:00 AM CST, outputs a `data.json` file that the frontend reads.

---

## Architecture

```
yfinance (free)  ──┐
                   ├──► main.py ──► Anthropic Claude ──► Supabase DB
NewsAPI (free)   ──┘                                  └──► data.json ──► Frontend
```

**Pipeline (runs daily at 6 AM CST):**
1. Fetch 3 months of price history for ~50 stocks via yfinance
2. Fetch macro + sector news via NewsAPI
3. Send all data to Claude → get top 10 winners + losers with % predictions
4. Save predictions to Supabase
5. Fetch yesterday's actual prices, calculate accuracy, save to Supabase
6. Write `data.json` for the frontend to consume

---

## Setup Guide

### Step 1 — Get Your API Keys

**Anthropic API Key**
1. Go to [console.anthropic.com](https://console.anthropic.com)
2. Sign up / log in → API Keys → Create Key
3. Copy the key

**NewsAPI Key**
1. Go to [newsapi.org](https://newsapi.org)
2. Sign up for free → copy your API key (100 requests/day free)

**Supabase**
1. Go to [supabase.com](https://supabase.com) → New Project
2. Once created: Settings → API
3. Copy **Project URL** and **anon public key**
4. Go to SQL Editor → paste contents of `supabase_setup.sql` → Run

---

### Step 2 — Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and fill in all four values:
```
ANTHROPIC_API_KEY=sk-ant-...
NEWS_API_KEY=abc123...
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=eyJ...
OUTPUT_JSON_PATH=./data.json
```

---

### Step 3 — Test Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline once
python main.py
```

You should see logs for each step. On success, a `data.json` file will be created.

---

### Step 4 — Deploy to Railway

1. Push this folder to a GitHub repository:
```bash
git init
git add .
git commit -m "Initial backend"
git remote add origin https://github.com/YOUR_USERNAME/marketpulse-backend.git
git push -u origin main
```

2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Select your repository
4. Go to **Variables** tab and add all four environment variables from your `.env`
5. Also set: `OUTPUT_JSON_PATH=/app/data.json`

Railway will automatically detect `railway.toml` and schedule the cron at `0 12 * * *` (12:00 UTC = 6:00 AM CST).

---

### Step 5 — Connect Frontend to data.json

The backend writes `data.json` after every run. Your frontend needs to fetch this file.

**Option A (simplest):** Host `data.json` as a static file on Vercel alongside your frontend HTML. Update the frontend JS to `fetch('/data.json')` and render the data dynamically.

**Option B:** Use Railway's public URL — your backend can also serve `data.json` via a tiny HTTP server. Add this to `main.py` after the pipeline completes if needed.

---

## File Structure

```
marketpulse-backend/
├── main.py              # Orchestrator — runs the full pipeline
├── fetcher.py           # yfinance stock data fetcher
├── news.py              # NewsAPI news + sentiment fetcher
├── analyzer.py          # Anthropic Claude AI predictions
├── database.py          # Supabase read/write layer
├── exporter.py          # Builds and writes data.json
├── supabase_setup.sql   # Run once in Supabase SQL editor
├── railway.toml         # Railway deployment + cron config
├── requirements.txt     # Python dependencies
├── .env.example         # Environment variable template
├── .gitignore           # Keeps .env and data.json out of git
└── README.md
```

---

## data.json Structure

The frontend consumes this file:

```json
{
  "generated_at": "2026-02-24T06:00:00-06:00",
  "today": {
    "date": "2026-02-24",
    "market_summary": "Tech momentum continues amid...",
    "winners": [
      {
        "rank": 1,
        "ticker": "NVDA",
        "company": "NVIDIA Corp",
        "sector": "Semiconductors",
        "predicted_change_pct": 3.2,
        "reason": "AI demand + earnings beat"
      }
    ],
    "losers": [...]
  },
  "yesterday": {
    "date": "2026-02-23",
    "winners": [
      {
        "rank": 1,
        "ticker": "NVDA",
        "predicted_change_pct": 3.8,
        "actual_change_pct": 4.5,
        "accuracy_score": 92,
        "outcome": "beat"
      }
    ],
    "losers": [...],
    "has_actuals": true
  },
  "indices": {
    "sp500":  { "value": 5842.3, "change_pct": 0.74, "direction": "up" },
    "nasdaq": { "value": 18310.6, "change_pct": 1.12, "direction": "up" },
    "dow":    { "value": 42156.0, "change_pct": -0.23, "direction": "down" }
  }
}
```

---

## Cost Estimate

| Service | Cost |
|---|---|
| yfinance | Free forever |
| NewsAPI | Free (100 req/day) |
| Supabase | Free (500MB) |
| Railway | Free tier |
| Anthropic API | ~$0.05–0.15/day |

**Total: ~$1.50–4.50/month** (Anthropic API only)

---

## Disclaimer

This tool is for informational purposes only. AI predictions are not financial advice. Never invest based solely on automated predictions.
