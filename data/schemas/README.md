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
- **scraped_jobs**: Main table storing all job records
- **search_history**: Tracks scraping operations

## Schema Upgrades
Migration scripts are stored in `data/schemas/migrations`.

### Applying Individual Migrations
Use `psql` to apply each migration in order:

```bash
psql -h HOST -U USER -d jobscraps -f data/schemas/migrations/001_add_search_metrics.sql
psql -h HOST -U USER -d jobscraps -f data/schemas/migrations/002_migrate_sessions.sql
```

### Using the Helper Script
To run all migrations sequentially, execute:

```bash
python dev/run_migrations.py
```

The script uses `configs/db/db_config.json` by default. Pass `--database-type working` to target the working database or `--config` to specify a custom config path.
