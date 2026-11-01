# StockCast — Backend

AI-powered daily stock prediction engine with same-day market-open and
end-of-day scoring. Writes `data.json`, which the frontend
(`stockcast-frontend` repo, served on Vercel) reads directly.

---

## Architecture

```
Yahoo Finance (free)  ──┐
FMP (fallback)        ──┼──► main.py ──► Claude + Gemini (parallel) ──► Supabase
NewsAPI (free)         ──┘                                          └──► data.json ──► GitHub (frontend repo) ──► Vercel
```

The pipeline runs in **three separate modes on three separate schedules**,
not one daily run:

| Mode | When | What it does |
|---|---|---|
| `predictions` | 7:00 AM ET, weekdays | Fetch stock data + news, call Claude & Gemini, save today's predictions |
| `market_open` | 9:31 AM ET, weekdays | Snapshot each ticker's real price ~1 min after the market opens, record the gap vs. prior close — for options trading, a lot of a predicted move can already happen before you can trade it |
| `accuracy` | 6:00 PM ET, weekdays | Score *today's own* predictions against today's real close — same evening, not the next morning |

**Why three separate runs instead of one**: the original design scored
*yesterday's* predictions each morning, a full day behind. Splitting it
this way means Historical Performance shows a day's result the same
evening it happened.

### Scheduling: cron-job.org, not GitHub's native `schedule:` trigger

**GitHub Actions' built-in cron scheduler has been unreliable since
2026-08-26** (a known, widely-reported platform issue — see GitHub
community discussions #205984, #206019). Scheduled runs were arriving
hours late or not at all, while manual `workflow_dispatch` always worked
instantly.

The actual fix: an external cron service, **[cron-job.org](https://cron-job.org)**
(free account under this project), calls GitHub's `workflow_dispatch` API
directly on a reliable clock, bypassing the broken native scheduler
entirely:

```
POST https://api.github.com/repos/funmusicgames1/stockcast-backend/actions/workflows/daily-pipeline.yml/dispatches
Authorization: Bearer <GitHub PAT, repo scope>
Body: {"ref":"main","inputs":{"mode":"predictions"}}
```

Three jobs are configured there, each in the `America/New_York` timezone
(cron-job.org handles DST natively — no manual adjustment needed across
the Nov 1 / March transitions):

- **StockCast - Morning Predictions** — 7:00 AM ET, weekdays
- **StockCast - Market Open Checkpoint** — 9:31 AM ET, weekdays
- **StockCast - EOD Accuracy** — 6:00 PM ET, weekdays

The old `on: schedule:` cron entries are still present in
`.github/workflows/daily-pipeline.yml` as a harmless backup — if GitHub's
scheduler ever fires one, it's a no-op or a redundant (idempotent) rerun,
never harmful. They're not the primary trigger anymore.

### CST-aware dates, not bare `date.today()`

GitHub Actions runners run in UTC. A run executing near UTC midnight
(e.g. a manual trigger late at night) could land on the wrong calendar
day if dates were computed naively. `datetime_utils.today_cst()` is used
everywhere a "today"/"yesterday" label is needed — it resolves the
*actual* America/Chicago calendar date regardless of what time in UTC
the job happens to run.

---

## Modes, in detail

Run any mode manually: `python main.py --mode <mode>` (or trigger via
GitHub Actions' "Run workflow" button / `gh workflow run`).

- **`predictions`** — the 6-step morning pipeline (fetch stock data →
  news → AI → save → read back yesterday's already-scored results →
  export). The time-window guard (only run near 7 AM ET) is skipped for
  manual/`workflow_dispatch` triggers — only enforced for the automated
  `schedule` trigger (checked via a `TRIGGER_EVENT` env var), so manual
  runs/testing are never blocked by the clock.
- **`market_open`** — price checkpoint, same manual-bypass behavior as
  above. No new AI call.
- **`accuracy`** — scores today's own predictions against today's real
  close, saves `actuals` (including the raw `close_price`, not just the
  %), re-exports.
- **`backfill --file <path>`** — disaster-recovery only: restores a
  prediction set that was **genuinely generated** (e.g. visible live on
  the site) but never saved to Supabase (e.g. a DB outage at save time).
  Never for inventing a prediction after the fact — that would defeat
  the whole point of tracking real predictive accuracy.
- **`backfill_open --file <path> --date YYYY-MM-DD`** — historical
  market-open backfill for a day that predates the `market_open`
  feature. Unlike predictions, an opening price is an objective market
  fact, not a forecast — looking it up after the fact via Yahoo's
  1-minute bars (available for roughly the last 30 days) carries none of
  the integrity concerns backfilling a prediction would.
- **`fix_prev_close --date YYYY-MM-DD`** — one-off correction for a
  wrong `prev_close` baseline on an already-saved day, without touching
  the actual AI-generated `predicted_change_pct`/`reason`/`company`.
  Only supports `today_cst()` — `save_predictions()` always writes under
  today's date internally, so this would corrupt a different day if used
  on one.

---

## Known issues (as of this writing)

- **`ANTHROPIC_API_KEY` is invalid** — every Claude call fails with
  `401 invalid x-api-key`. The pipeline degrades gracefully (Gemini-only
  output, `claude.available: false` in `data.json`), but Claude
  predictions have been absent since this started. Needs a fresh key
  from console.anthropic.com.
- **GitHub's native scheduler** — see above. The cron-job.org jobs are
  the real fix; the old `on: schedule:` entries are vestigial.

---

## Setup Guide

### Step 1 — API Keys

- **Anthropic**: console.anthropic.com → API Keys (currently broken, see
  Known Issues)
- **Gemini**: aistudio.google.com → Get API key
- **NewsAPI**: newsapi.org → free tier, 100 req/day
- **FMP**: financialmodelingprep.com → free tier (stock fallback only;
  index data now comes from Yahoo, not FMP — FMP's `/quote` endpoint
  403s on index symbols like `^GSPC` on the free tier)
- **Supabase**: supabase.com → New Project → Settings → API → copy the
  **service_role** key specifically, not the anon/public one (the RLS
  policies require it) → SQL Editor → run `supabase_setup.sql`
- **Firebase** (optional, push notifications): Firebase Console → Project
  Settings → Service Accounts → generate a key
- **cron-job.org**: free account → Settings → API → generate an API key
  (only needed if recreating the scheduled jobs)
- **GitHub PAT**: Settings → Developer settings → Tokens (classic) →
  `repo` scope, used both for pushing `data.json` to the frontend repo
  and for cron-job.org's `workflow_dispatch` calls

### Step 2 — Environment variables (GitHub Actions secrets)

```
ANTHROPIC_API_KEY
GEMINI_API_KEY
NEWS_API_KEY
FMP_API_KEY
SUPABASE_URL
SUPABASE_KEY        # service_role key, not anon
GITHUB_TOKEN        # PAT_TOKEN secret, repo scope
FRONTEND_REPO       # e.g. funmusicgames1/stockcast-frontend
FIREBASE_PROJECT_ID
FIREBASE_CLIENT_EMAIL
FIREBASE_PRIVATE_KEY
OUTPUT_JSON_PATH    # ./data.json
```

### Step 3 — Test locally

```bash
pip install -r requirements.txt
python main.py --mode predictions
```

### Step 4 — Deployment

Runs entirely on **GitHub Actions** (not Railway — the project moved off
Railway at some point before this documentation was last accurate).
Triggered by cron-job.org as described above, or manually via
`workflow_dispatch`.

### Step 5 — Frontend

`stockcast-frontend` repo serves `index.html` + `data.json` on Vercel.
Every pipeline run pushes a fresh `data.json` via the GitHub Contents
API, which triggers a Vercel redeploy automatically.

---

## File Structure

```
stockcast-backend/
├── main.py              # Orchestrator — all 6 modes live here
├── fetcher.py            # Yahoo Finance + FMP fallback; symbol-alias
│                          # layer (SPX -> ^GSPC); historical open-price
│                          # lookup (1-minute bars)
├── news.py                # NewsAPI news + sentiment fetcher
├── analyzer.py            # Claude + Gemini predictions, run in parallel
├── database.py            # Supabase read/write layer (predictions,
│                          # actuals, open_prices)
├── exporter.py             # Builds data.json; enriches with open-price
│                          # and actuals data
├── datetime_utils.py      # today_cst() — CST-aware date, used instead
│                          # of bare date.today() everywhere
├── backfill_data/          # JSON files used by --mode backfill /
│                          # backfill_open (one per backfilled date)
├── supabase_setup.sql      # Run once (or incrementally) in Supabase
│                          # SQL editor — predictions, actuals,
│                          # open_prices tables + RLS policies
├── .github/workflows/
│   └── daily-pipeline.yml  # workflow_dispatch + vestigial schedule:
│                          # entries (see Scheduling above)
├── requirements.txt
└── README.md
```

---

## data.json Structure

```json
{
  "generated_at": "2026-10-28T20:36:20-0500",
  "today": {
    "date": "2026-10-29",
    "claude": { "market_summary": null, "winners": [], "losers": [], "available": false },
    "gemini": {
      "market_summary": "...",
      "available": true,
      "winners": [
        {
          "rank": 1, "ticker": "SOFI", "company": "SoFi Technologies, Inc.",
          "sector": "Finance", "predicted_change_pct": 8.5, "reason": "...",
          "prev_close": 16.4,
          "open_price": 16.92,
          "gap_pct": 3.17
        }
      ],
      "losers": [...]
    }
  },
  "yesterday": {
    "date": "2026-10-28",
    "gemini": {
      "has_actuals": true,
      "winners": [
        {
          "rank": 1, "ticker": "SOFI", "predicted_change_pct": 8.5,
          "prev_close": 16.4, "open_price": 16.92, "gap_pct": 3.17,
          "close_price": 17.14, "actual_change_pct": 4.51,
          "accuracy_score": 60, "outcome": "close"
        }
      ],
      "losers": [...]
    }
  },
  "indices": {
    "sp500":  { "value": 7722.6, "change_pct": 0.6, "direction": "up" },
    "nasdaq": { "value": 26477.81, "change_pct": 1.18, "direction": "up" },
    "dow":    { "value": 52377.57, "change_pct": 0.03, "direction": "up" }
  },
  "full_ranked_list": [...]
}
```

Notes on fields that are new since the original design:
- `prev_close` is attached at prediction time (6 AM) — it's just
  yesterday's close, no need to wait for market open.
- `open_price` / `gap_pct` stay `null` until the 9:31 AM checkpoint
  actually runs. `gap_pct` is the **plain** % move from prev close to
  open — not a ratio against the predicted target (an earlier version
  computed that ratio and it read as a bug when it exceeded 100%, so it
  was replaced with the plain gap).
- `close_price` on a scored (`yesterday`) entry is the real closing
  price used to compute `actual_change_pct`.

---

## Cost Estimate

| Service | Cost |
|---|---|
| Yahoo Finance | Free forever |
| FMP | Free tier |
| NewsAPI | Free (100 req/day) |
| Supabase | Free (500MB) |
| GitHub Actions | Free tier (public/private repo minutes) |
| cron-job.org | Free |
| Gemini API | Free tier covers current volume |
| Anthropic API | Currently $0 — key is broken, not being charged |

**Total: effectively $0/month** right now, since Claude isn't actually
running. Budget ~$1.50-4.50/month again once a valid Anthropic key is
added.

---

## Disclaimer

This tool is for informational purposes only. AI predictions are not
financial advice. Never invest based solely on automated predictions.
