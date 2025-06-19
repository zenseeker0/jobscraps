-- PostgreSQL Schema for jobscraps database
-- Migrated from SQLite with improvements for PostgreSQL

-- Create scraped_jobs table with improved data types
CREATE TABLE IF NOT EXISTS scraped_jobs (
    id TEXT PRIMARY KEY,
    site TEXT,
    job_url TEXT,
    job_url_direct TEXT,
    title TEXT,
    company TEXT,
    location TEXT,
    date_posted TEXT,
    job_type TEXT,
    salary_source TEXT,
    interval TEXT,
    min_amount DECIMAL(12,2),
    max_amount DECIMAL(12,2),
    currency TEXT,
    is_remote BOOLEAN,
    job_level TEXT,
    job_function TEXT,
    listing_type TEXT,
    emails TEXT,
    description TEXT,
    company_industry TEXT,
    company_url TEXT,
    company_logo TEXT,
    company_url_direct TEXT,
    company_addresses TEXT,
    company_num_employees TEXT,
    company_revenue TEXT,
    company_description TEXT,
    skills TEXT,
    experience_range TEXT,
    company_rating TEXT,
    company_reviews_count TEXT,
    vacancy_count TEXT,
    work_from_home_type TEXT,
    date_scraped TIMESTAMP,
    search_query TEXT
);

-- Create search_history table
CREATE TABLE IF NOT EXISTS search_history (
    id SERIAL PRIMARY KEY,
    search_query TEXT,
    parameters TEXT,
    timestamp TIMESTAMP,
    jobs_found INTEGER
);

-- Create indexes for performance based on common queries
-- Primary indexes for searching and sorting
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_title ON scraped_jobs(title);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_company ON scraped_jobs(company);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query ON scraped_jobs(search_query);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_search_query_lower ON scraped_jobs(LOWER(search_query));

-- Additional useful indexes
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_posted ON scraped_jobs(date_posted);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_location ON scraped_jobs(location);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_is_remote ON scraped_jobs(is_remote);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_date_scraped ON scraped_jobs(date_scraped);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_site ON scraped_jobs(site);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_title_company ON scraped_jobs(title, company);
CREATE INDEX IF NOT EXISTS idx_scraped_jobs_company_location ON scraped_jobs(company, location);

-- Search history indexes
CREATE INDEX IF NOT EXISTS idx_search_history_timestamp ON search_history(timestamp);
CREATE INDEX IF NOT EXISTS idx_search_history_search_query ON search_history(search_query);

-- Comments for documentation
COMMENT ON TABLE scraped_jobs IS 'Main table storing job postings scraped from various job sites';
COMMENT ON TABLE search_history IS 'Log of search operations performed by the scraper';

COMMENT ON COLUMN scraped_jobs.is_remote IS 'Boolean flag indicating if job allows remote work';
COMMENT ON COLUMN scraped_jobs.min_amount IS 'Minimum salary amount in specified currency';
COMMENT ON COLUMN scraped_jobs.max_amount IS 'Maximum salary amount in specified currency';
COMMENT ON COLUMN scraped_jobs.date_scraped IS 'Timestamp when this job was scraped';
COMMENT ON COLUMN scraped_jobs.search_query IS 'Search query that found this job';