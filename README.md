# JobScraps

A comprehensive job scraping and data management system built with Python, PostgreSQL, and intelligent backup management.

## 🚀 Quick Start

1. **Clone repository**
   ```bash
   git clone https://github.com/zenseeker0/jobscraps
   cd jobscraps
   ```

2. **Install dependencies**
   ```bash
   pip install -e .
   ```

3. **Configure database**
   ```bash
   cp configs/db/db_config.json.template configs/db/db_config.json
   # Edit configs/db/db_config.json with your PostgreSQL details
   ```

4. **Verify setup**
   ```bash
   python dev/verify_setup.py
   ```

5. **Start scraping**
   ```bash
   jobscraps scrape
   ```

6. **Explore the CLI**
   ```bash
   jobscraps --help
   ```

## 📊 Key Features

- **Multi-source job scraping** from Indeed, LinkedIn, Glassdoor
- **Intelligent duplicate detection** with sophisticated ranking algorithm
- **PostgreSQL database** with automated backup management
- **Production/working database** separation for safe data analysis
- **Intelligent backup system** - backups only when needed, retention management
- **Data cleaning pipeline** with configurable filters (salary, company, title, duplicates)
- **Safety-first design** - non-destructive analysis vs explicit destructive operations
- **Retool integration** for business intelligence dashboards
- **Session analytics** with per-search metrics and salary averages

## 📁 Project Structure

```
jobscraps/
├── scraper.py              # Legacy scraping entry point
├── cli.py                  # Typer-based command line interface
├── scraping_orchestrator.py# Coordinates scraping and cleaning
├── backup_manager.py       # Backup and restore utilities
├── data_cleaner.py         # Data cleaning routines
├── duplicate_manager.py    # Duplicate detection helpers
├── console_interface.py    # Console abstraction for user prompts
├── database/               # Database access and backup helpers
├── configs/
│   ├── db/            # Database configuration
│   ├── search/        # Job search parameters
│   ├── filters/       # Deletion filter patterns
│   └── testing/       # Sample configs for development
├── scripts/                # Helper utilities
├── data/                   # Schemas and samples
├── outputs/                # Logs and analysis (gitignored)
├── backups/                # PostgreSQL backups with retention (gitignored)
└── dev/                    # Development and migration tools
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
```

### Backup Management
```bash
jobscraps backup                    # Manual backup
jobscraps list-backups              # Show available backups
jobscraps restore-backup file.sql.gz # Restore from backup
jobscraps cleanup-backups           # Force retention cleanup
```

### Analytics
```bash
python scripts/db_stats_cli.py session-stats  # Aggregated session statistics
python scripts/db_stats_cli.py history        # Detailed search log
```

## 🤝 Contributing

This project is actively maintained. Major components include:
- `cli.py` - Entry point for all commands
- `scraping_orchestrator.py` - Coordinates scraping and cleaning
- `backup_manager.py` - Handles backups and restores
- `data_cleaner.py` and `duplicate_manager.py` - Data quality utilities
- `database/` - Connection and backup helpers

## 📄 License

MIT License - see LICENSE file for details.
