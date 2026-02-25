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
    prediction_type     TEXT CHECK (prediction_type IN ('winner', 'loser')),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, ticker)
);

-- Indexes for fast date-based queries
CREATE INDEX IF NOT EXISTS idx_predictions_date ON predictions(date DESC);
CREATE INDEX IF NOT EXISTS idx_actuals_date ON actuals(date DESC);
CREATE INDEX IF NOT EXISTS idx_actuals_ticker ON actuals(ticker);

-- Enable Row Level Security (RLS) — allow public read, restrict writes to service role
ALTER TABLE predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE actuals ENABLE ROW LEVEL SECURITY;

-- Allow anyone to read (your frontend will read data.json, but just in case)
CREATE POLICY "Public read predictions" ON predictions FOR SELECT USING (true);
CREATE POLICY "Public read actuals"     ON actuals     FOR SELECT USING (true);

-- Only allow inserts/updates from authenticated service role (your backend)
CREATE POLICY "Service write predictions" ON predictions
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Service write actuals" ON actuals
    FOR ALL USING (auth.role() = 'service_role');
