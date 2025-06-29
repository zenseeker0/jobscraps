-- Migration: add columns for extended search metrics
CREATE TABLE IF NOT EXISTS search_sessions (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP
);
ALTER TABLE search_history DROP COLUMN IF EXISTS session_id;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS session_id INTEGER REFERENCES search_sessions(id);
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS new_jobs_inserted INTEGER;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS site_breakdown JSONB;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS duplicate_breakdown JSONB;
ALTER TABLE search_history
    ALTER COLUMN site_breakdown TYPE JSONB USING site_breakdown::JSONB;
ALTER TABLE search_history
    ALTER COLUMN duplicate_breakdown TYPE JSONB USING duplicate_breakdown::JSONB;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS remote_jobs_count INTEGER;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS avg_salary NUMERIC(12,2);
