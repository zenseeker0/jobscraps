# JobScraps

A comprehensive job scraping and data management system built with Python, PostgreSQL, and intelligent backup management.

## 📋 Prerequisites

- **Python** 3.10 or newer
- **PostgreSQL** 12 or newer with a user that can create databases

### Key Python Dependencies
- [python-jobspy](https://pypi.org/project/python-jobspy/) - scraping library
- [psycopg2-binary](https://pypi.org/project/psycopg2-binary/) - PostgreSQL adapter
- [typer](https://pypi.org/project/typer/) - CLI framework
- [pandas](https://pypi.org/project/pandas/) - data manipulation

## 🚀 Quick Start

1. **Clone repository**
   ```bash
   git clone https://github.com/zenseeker0/jobscraps
   cd jobscraps
   ```

2. **Install Python dependencies**
   ```bash
   pip install -e .
   ```

3. **Create PostgreSQL databases**
   ```bash
   createdb jobscraps
   createdb jobscraps_working
   ```

4. **Configure database**
   ```bash
   cp configs/db/db_config.json.template configs/db/db_config.json
   # Edit configs/db/db_config.json with your PostgreSQL details
   ```
   Set `PGPASSWORD` in your environment so backup operations can run non-interactively:
   ```bash
   export PGPASSWORD=your_db_password
   ```

5. **Create a job search configuration**
   ```bash
   cp configs/testing/test_job_search_config.json configs/search/job_search_config.json
   # Edit configs/search/job_search_config.json with your search parameters
   ```

6. **Start scraping**
   ```bash
   jobscraps scrape
   ```

7. **Explore the CLI**
   ```bash
   jobscraps --help
   ```
   Most commands operate on the production database. Add `--working` to safely
   use the working copy.

## 📊 Key Features

- **Multi-source job scraping** from Indeed, LinkedIn, Glassdoor
- **Intelligent duplicate detection** with sophisticated ranking algorithm
- **PostgreSQL database** with automated backup management
- **Production/working database** separation for safe data analysis
- **Intelligent backup system** - backups only when needed, retention management
- **Data cleaning pipeline** with configurable filters (salary, company, title, duplicates)
- **Safety-first design** - non-destructive analysis vs explicit destructive operations
- **Session analytics** with per-search metrics and salary averages

## 📁 Project Structure

```
jobscraps/
├── jobscraps/
│   ├── cli.py                  # Typer-based command line interface
│   ├── scraper.py              # Scraping logic
│   ├── scraping_orchestrator.py
│   ├── backup_manager.py
│   ├── data_cleaner.py
│   ├── duplicate_manager.py
│   ├── base_manager.py
│   ├── session_manager.py
│   ├── config.py
│   ├── console_interface.py
│   └── database/
│       ├── backup.py
│       ├── config.py
│       └── core.py
├── configs/
│   ├── db/
│   ├── search/
│   ├── filters/
│   └── testing/
├── scripts/
├── data/
├── outputs/                # Gitignored job data
└── dev/
```

## 🔄 Typical Workflows

### Daily Scraping
```bash
# Scrape jobs with automatic post-scraping backup
jobscraps scrape
```

### Data Analysis (Recommended)
```bash
# Create working copy with automatic data cleaning
jobscraps create-working-copy

# Work safely on working database (no backups needed)
jobscraps --working delete-by-salary 80000,100000
jobscraps --working delete-by-company
jobscraps --working delete-by-title
```

### Manual Duplicate Management
```bash
# Analyze duplicates (non-destructive, creates delete_ids.txt)
jobscraps process-duplicates

# Review and edit configs/filters/delete_ids.txt as needed

# Apply deletions (destructive, requires "Y")
jobscraps delete-by-ids
```

### Backup Management
```bash
# List available backups
jobscraps list-backups

# Create manual backup
jobscraps backup

# Restore from backup (requires "Y")
jobscraps restore-backup filename.sql.gz
```

## 📈 Data Pipeline

1. **Collection**: JobSpy scrapes multiple job boards
2. **Storage**: Raw data stored in production PostgreSQL database
3. **Backup**: Intelligent post-scraping backups capture new data state
4. **Working Copy**: Template copy for safe data analysis (10 seconds)
5. **Cleaning**: 4-step pipeline removes 70%+ of jobs (salary → company → title → duplicates)
6. **Analysis**: Clean dataset ready for business intelligence tooling

## 🛡️ Safety Features

### Intelligent Backup System
- **Production-only backups**: Only creates backups when operating on production database
- **Post-scraping timing**: Captures new data state after successful scraping
- **Skip unnecessary backups**: Working database operations and read-only operations skip backups
- **Retention management**: Automatic cleanup maintains 40-45 backups within 5GB limit

### Safety Model
- **Non-destructive operations**: Analysis commands (info messages)
  - `--process-duplicates` - Analyzes duplicates, creates delete_ids.txt
- **Destructive operations**: Require explicit uppercase "Y"
  - `--delete-by-*` commands, `--clear`, database restore

### Production Database Protection
- **Warning system**: Alerts when attempting data operations on production
- **Working database recommendation**: Suggests safer alternatives
- **Backup-before-operation**: Automatic safety backups for production operations

## 📊 Performance Stats

- **Data sources**: Indeed, LinkedIn, Glassdoor
- **Working copy creation**: ~10 seconds using PostgreSQL templates
- **Data cleaning pipeline**: 2-5 minutes depending on data size
- **Backup creation**: 30-60 seconds (compressed dumps)

## 🔧 Command Reference

### Core Operations
```bash
jobscraps scrape                    # Scrape jobs + post-backup
jobscraps create-working-copy       # Create working DB + auto-clean
jobscraps process-duplicates        # Analyze duplicates (non-destructive)
```

### Data Management (Working Database Recommended)
```bash
jobscraps --working delete-by-salary          # Clean by salary
jobscraps --working delete-by-company         # Clean by company patterns
jobscraps --working delete-by-title           # Clean by title patterns
jobscraps --working delete-by-ids             # Delete specific IDs
jobscraps delete-before-date YYYY-MM-DD      # Remove older jobs
jobscraps clear                              # Delete all job data
jobscraps backup-reset                       # Backup then clear database
```

### Backup Management
```bash
jobscraps backup                    # Manual backup
jobscraps list-backups              # Show available backups
jobscraps restore-backup file.sql.gz # Restore from backup
jobscraps cleanup-backups           # Force retention cleanup
jobscraps test-backup file.sql.gz   # Validate backup integrity
jobscraps test-backup-compatibility file.sql.gz  # Check restore compatibility
```

## 🛠️ Troubleshooting

- **Database connection errors**: confirm `db_config.json` credentials and that PostgreSQL is running.
- **Permission denied creating working copy**: ensure your database user has `CREATEDB` privileges.
- **Restore fails with compatibility warnings**: use `jobscraps test-backup-compatibility` before running `restore-backup`.

## 🤝 Contributing

This project is actively maintained. Major components include:
- `cli.py` - Entry point for all commands
- `scraping_orchestrator.py` - Coordinates scraping and cleaning
- `backup_manager.py` - Handles backups and restores
- `data_cleaner.py` and `duplicate_manager.py` - Data quality utilities
- `database/` - Connection and backup helpers

## 📄 License

MIT License - see LICENSE file for details.
