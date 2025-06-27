-- Migration: add columns for extended search metrics
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS session_id TEXT;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS new_jobs_inserted INTEGER;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS site_breakdown TEXT;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS duplicate_breakdown TEXT;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS remote_jobs_count INTEGER;
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS avg_salary NUMERIC(12,2);
