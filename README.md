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
- **Intelligent duplicate detection** with sophisticated ranking
- **PostgreSQL database** with automated backup management
- **Production/working database** separation for safe analysis
- **Data cleaning pipeline** with configurable filters
- **Retool integration** for business intelligence dashboards

## 📁 Project Structure

```
jobscraps/
├── scraper.py              # Main application
├── configs/                # Configuration templates
├── scripts/                # Helper utilities  
├── data/                   # Schemas and samples
├── outputs/                # Generated data (gitignored)
├── backups/                # Database backups (gitignored)
└── dev/                    # Development tools
```

## 🔄 Typical Workflow

```bash
# Daily scraping with automatic backup
python scraper.py --scrape

# Create working copy for analysis (with auto-cleaning)
python scraper.py --create-working-copy

# Data management (use working database for safety)
python scraper.py --working --delete-by-salary 80000,100000
python scraper.py --working --process-duplicates
```

## 📈 Data Pipeline

1. **Collection**: JobSpy scrapes multiple job boards
2. **Storage**: Raw data in production PostgreSQL  
3. **Backup**: Post-scraping backup captures new data
4. **Cleaning**: 4-step pipeline (salary → company → title → duplicates)
5. **Analysis**: Clean data available in working database

See individual directories for detailed documentation.

## 🤝 Contributing

This project is being modularized. Current structure will be refactored into:
- `database.py` - Database operations
- `duplicate_manager.py` - Duplicate detection  
- `config.py` - Configuration management
- `scraper_core.py` - Core scraping logic
- `main.py` - CLI interface

## 📄 License

MIT License - see LICENSE file for details.
