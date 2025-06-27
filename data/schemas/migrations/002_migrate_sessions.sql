-- Migration: convert text session IDs to integer references
-- 1. Create search_sessions table if it doesn't exist
CREATE TABLE IF NOT EXISTS search_sessions (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP
);

-- 2. Add a new integer column for the foreign key
ALTER TABLE search_history ADD COLUMN IF NOT EXISTS session_id_int INTEGER;

-- 3. Create a temporary table mapping old session_id text to new integers
CREATE TEMP TABLE temp_sessions AS
SELECT DISTINCT session_id AS old_id
FROM search_history
WHERE session_id IS NOT NULL;

ALTER TABLE temp_sessions ADD COLUMN id SERIAL PRIMARY KEY;

-- 4. Populate search_sessions using earliest timestamp for each session
INSERT INTO search_sessions (id, start_time)
SELECT t.id, MIN(h.timestamp)
FROM temp_sessions t
LEFT JOIN search_history h ON h.session_id = t.old_id
GROUP BY t.id;

-- 5. Update search_history to use the new integer IDs
UPDATE search_history h
SET session_id_int = t.id
FROM temp_sessions t
WHERE h.session_id = t.old_id;

-- 6. Drop the old text column and rename the new column
ALTER TABLE search_history DROP COLUMN IF EXISTS session_id;
ALTER TABLE search_history RENAME COLUMN session_id_int TO session_id;

-- 7. Add foreign key constraint
ALTER TABLE search_history
    ADD CONSTRAINT fk_search_sessions
    FOREIGN KEY (session_id)
    REFERENCES search_sessions(id);
