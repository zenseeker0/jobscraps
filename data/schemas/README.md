# Database Schemas

## jobscraps_schema.sql
Complete PostgreSQL schema for the JobScraps database including:
- `scraped_jobs` table with all job posting fields
- `search_history` table for tracking scraping operations  
- Indexes for optimized querying
- Column specifications and data types

## Generating Current Schema
```bash
pg_dump -h HOST -p 5432 -U USER -d jobscraps \
  --schema-only --clean --if-exists --no-owner --no-privileges \
  > data/schemas/jobscraps_schema.sql
```

## Key Tables
- **scraped_jobs**: Main table with 122k+ job records
- **search_history**: Tracks all scraping operations
