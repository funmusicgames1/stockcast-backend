-- =============================================================
-- MarketPulse — Supabase Database Setup
-- Run this in your Supabase project's SQL Editor (supabase.com)
-- =============================================================

-- Table 1: Daily predictions (winners + losers from AI)
CREATE TABLE IF NOT EXISTS predictions (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE NOT NULL UNIQUE,
    market_summary TEXT,
    winners     JSONB NOT NULL DEFAULT '[]',
    losers      JSONB NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Table 2: Actual % changes (filled in next day after market close)
CREATE TABLE IF NOT EXISTS actuals (
    id                  BIGSERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    ticker              TEXT NOT NULL,
    predicted_change_pct FLOAT,
    actual_change_pct   FLOAT,
    close_price         FLOAT,
    prediction_type     TEXT CHECK (prediction_type IN ('winner', 'loser')),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, ticker)
);

-- Table 3: Market-open price checkpoint (how much of the predicted move
-- already happened by the time the market opened, before the first trade
-- could realistically be placed)
CREATE TABLE IF NOT EXISTS open_prices (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    ticker      TEXT NOT NULL,
    prev_close  FLOAT,
    open_price  FLOAT,
    gap_pct     FLOAT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, ticker)
);

-- Table 4: Daily Levels — OHLC + Pivot/Support/Resistance + gap X1-X5 for a
-- small fixed watchlist (QQQ, SPY, IWM, TSLA, AMZN, META). Two separate saves
-- merge into the same row via upsert on (date, ticker): the 7 AM run fills
-- open/high/low/close/pivot/support/resistance, the ~9:34 AM run fills
-- a1/b1/gap_direction/x1-x5 once today's first 3-min candle closes.
CREATE TABLE IF NOT EXISTS daily_levels (
    id             BIGSERIAL PRIMARY KEY,
    date           DATE NOT NULL,
    ticker         TEXT NOT NULL,
    prev_date      DATE,
    open           FLOAT,
    high           FLOAT,
    low            FLOAT,
    close          FLOAT,
    pivot          FLOAT,
    support        FLOAT,
    resistance     FLOAT,
    a1             FLOAT,
    b1             FLOAT,
    gap_direction  TEXT CHECK (gap_direction IN ('up', 'down')),
    x1             FLOAT,
    x2             FLOAT,
    x3             FLOAT,
    x4             FLOAT,
    x5             FLOAT,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, ticker)
);

-- If daily_levels already existed before this column was added:
-- ALTER TABLE daily_levels ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Indexes for fast date-based queries
CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(date DESC);
CREATE INDEX IF NOT EXISTS idx_actuals_date ON actuals(date DESC);
CREATE INDEX IF NOT EXISTS idx_actuals_ticker ON actuals(ticker);
CREATE INDEX IF NOT EXISTS idx_open_prices_date ON open_prices(date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_levels_date ON daily_levels(date DESC);

-- Enable Row Level Security (RLS) — allow public read, restrict writes to service role
ALTER TABLE predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE actuals ENABLE ROW LEVEL SECURITY;
ALTER TABLE open_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE daily_levels ENABLE ROW LEVEL SECURITY;

-- Allow anyone to read (your frontend will read data.json, but just in case)
CREATE POLICY "Public read predictions" ON predictions FOR SELECT USING (true);
CREATE POLICY "Public read actuals"     ON actuals     FOR SELECT USING (true);
CREATE POLICY "Public read open_prices" ON open_prices FOR SELECT USING (true);
CREATE POLICY "Public read daily_levels" ON daily_levels FOR SELECT USING (true);

-- Only allow inserts/updates from authenticated service role (your backend)
CREATE POLICY "Service write predictions" ON predictions
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service write actuals" ON actuals
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service write open_prices" ON open_prices
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service write daily_levels" ON daily_levels
    FOR ALL USING (auth.role() = 'service_role');
