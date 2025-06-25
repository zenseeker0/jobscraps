-- Migration: add session tracking columns to search_history

-- 1. Add new columns with defaults
ALTER TABLE search_history
    ADD COLUMN IF NOT EXISTS session_id TEXT,
    ADD COLUMN IF NOT EXISTS session_sequence INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duration INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS new_jobs_inserted INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS site_breakdown JSONB DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS duplicate_breakdown JSONB DEFAULT '{}'::jsonb;

-- 2. Backfill existing rows using row_number ordering by timestamp
WITH ordered AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY timestamp) AS rn
    FROM search_history
)
UPDATE search_history sh
SET session_sequence = o.rn,
    session_id = md5(sh.timestamp::text || '-' || o.rn::text)
FROM ordered o
WHERE sh.id = o.id;

-- 3. Enforce not-null constraints after backfill
ALTER TABLE search_history
    ALTER COLUMN session_id SET NOT NULL,
    ALTER COLUMN session_sequence SET NOT NULL,
    ALTER COLUMN new_jobs_inserted SET NOT NULL;

-- 4. Create indexes for improved query performance
CREATE INDEX IF NOT EXISTS idx_search_history_session_id ON search_history(session_id);
CREATE INDEX IF NOT EXISTS idx_search_history_date_session ON search_history(timestamp, session_sequence);
CREATE INDEX IF NOT EXISTS idx_search_history_duration ON search_history(duration);
CREATE INDEX IF NOT EXISTS idx_search_history_new_jobs ON search_history(new_jobs_inserted);
CREATE INDEX IF NOT EXISTS idx_search_history_site_breakdown ON search_history USING GIN (site_breakdown);
CREATE INDEX IF NOT EXISTS idx_search_history_duplicate_breakdown ON search_history USING GIN (duplicate_breakdown);
