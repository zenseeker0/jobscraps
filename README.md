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
   pip install -r requirements.txt
   ```

3. **Configure database**
   ```bash
   cp configs/db_config.json.template configs/db_config.json
   # Edit configs/db_config.json with your PostgreSQL details
   ```

4. **Verify setup**
   ```bash
   python dev/verify_setup.py
   ```

5. **Start scraping**
   ```bash
   python scraper.py --scrape
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

## 📁 Project Structure

```
jobscraps/
├── scraper.py              # Main application with intelligent backup system
├── configs/                # Configuration files
│   ├── db_config.json     # Unified database configuration
│   ├── job_search_config.json # JobSpy search parameters
│   ├── delete_companies.txt   # Company filter patterns
│   └── delete_titles.txt      # Job title filter patterns
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
python scraper.py --scrape
```

### Data Analysis (Recommended)
```bash
# Create working copy with automatic data cleaning
python scraper.py --create-working-copy

# Work safely on working database (no backups needed)
python scraper.py --working --delete-by-salary 80000,100000
python scraper.py --working --delete-by-company
python scraper.py --working --delete-by-title
```

### Manual Duplicate Management
```bash
# Analyze duplicates (non-destructive, creates delete_ids.txt)
python scraper.py --process-duplicates

# Review and edit configs/delete_ids.txt as needed

# Apply deletions (destructive, requires "Y")
python scraper.py --delete-by-ids
```

### Backup Management
```bash
# List available backups
python scraper.py --list-backups

# Create manual backup
python scraper.py --backup

# Restore from backup (requires "Y")
python scraper.py --restore-backup filename.sql.gz
```

## 📈 Data Pipeline

1. **Collection**: JobSpy scrapes multiple job boards (122k+ jobs)
2. **Storage**: Raw data stored in production PostgreSQL database
3. **Backup**: Intelligent post-scraping backups capture new data state
4. **Working Copy**: Template copy for safe data analysis (10 seconds)
5. **Cleaning**: 4-step pipeline removes 70%+ of jobs (salary → company → title → duplicates)
6. **Analysis**: Clean dataset (~30k high-quality jobs) ready for Retool/BI

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

- **Total jobs collected**: 122,943+ (production database)
- **High-quality jobs**: ~30,000 (after 70%+ cleaning reduction)
- **Data sources**: Indeed, LinkedIn, Glassdoor
- **Working copy creation**: ~10 seconds (PostgreSQL template copy)
- **Data cleaning pipeline**: 2-5 minutes depending on data size
- **Backup creation**: 30-60 seconds (compressed PostgreSQL dumps)

## 🔧 Command Reference

### Core Operations
```bash
python scraper.py --scrape                    # Scrape jobs + post-backup
python scraper.py --create-working-copy       # Create working DB + auto-clean
python scraper.py --process-duplicates        # Analyze duplicates (non-destructive)
```

### Data Management (Working Database Recommended)
```bash
python scraper.py --working --delete-by-salary          # Clean by salary
python scraper.py --working --delete-by-company         # Clean by company patterns  
python scraper.py --working --delete-by-title           # Clean by title patterns
python scraper.py --working --delete-by-ids             # Delete specific IDs
```

### Backup Management
```bash
python scraper.py --backup                    # Manual backup
python scraper.py --list-backups              # Show available backups
python scraper.py --restore-backup file.sql.gz # Restore from backup
python scraper.py --cleanup-backups           # Force retention cleanup
```

## 🤝 Contributing

This project is actively maintained and being modularized for better maintainability:

**Current Architecture** (single file):
- `scraper.py` - All functionality in one comprehensive script

**Planned Modular Architecture**:
- `database/` - Database operations and backup management
- `duplicate_detection/` - Duplicate detection and ranking
- `config/` - Configuration management
- `scraping/` - Core JobSpy integration
- `cli/` - Command-line interface

See `feature/modular-refactor` branch for ongoing modularization work.

## 📄 License

MIT License - see LICENSE file for details.