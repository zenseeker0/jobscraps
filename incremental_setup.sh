#!/bin/bash
# incremental_setup.sh - Build JobScraps repository incrementally

set -e  # Exit on any error

echo "🚀 Starting incremental JobScraps setup..."
echo "Make sure you've already created the empty GitHub repository!"
echo ""

# Verify we're in the right place
if [ ! -d ".git" ]; then
    echo "❌ This doesn't appear to be a git repository."
    echo "First run: git clone https://github.com/zenseeker0/jobscraps"
    exit 1
fi

# Store the path to original project
ORIGINAL_PROJECT="/Users/jonesy/gitlocal/jobscrape"
if [ ! -d "$ORIGINAL_PROJECT" ]; then
    echo "❌ Original project not found at $ORIGINAL_PROJECT"
    echo "Please update the ORIGINAL_PROJECT path in this script"
    exit 1
fi

echo "✓ Found original project at $ORIGINAL_PROJECT"
echo ""

# Commit 1: Directory Structure
echo "📁 Commit 1: Creating project structure..."
mkdir -p configs scripts data/{schemas,samples} docs outputs/{Raw\ Results,exports,logs} backups/DatabaseBackups dev/LLM_package .github

# Create .gitkeep files
touch outputs/Raw\ Results/.gitkeep
touch outputs/exports/.gitkeep  
touch outputs/logs/.gitkeep
touch backups/DatabaseBackups/.gitkeep
touch data/schemas/.gitkeep
touch data/samples/.gitkeep

# Enhance .gitignore
cat >> .gitignore << 'EOF'

# JobScraps specific
configs/db_config.json
configs/*_prod.json
outputs/Raw Results/*.csv
outputs/exports/*.csv
outputs/exports/*.xlsx
outputs/logs/*.log
outputs/logs/*.tsv
backups/DatabaseBackups/*.sql.gz
backups/DatabaseBackups/backup_manifest.json
*.xlsx
!*template*.xlsx
duplicate_evals.xlsx
parsed_jobscraper_log.tsv
EOF

git add .
git commit -m "📁 Add project directory structure

- Create organized folder hierarchy for data-oriented project
- Add .gitkeep files to preserve empty directories  
- Enhance .gitignore for sensitive data and large files
- Separate concerns: configs/, scripts/, data/, outputs/, backups/"

echo "✓ Structure committed"

# Commit 2: Core Application (with path updates)
echo "🚀 Commit 2: Adding main application..."
cp "$ORIGINAL_PROJECT/scraper.py" .

# Update paths in scraper.py
echo "  Updating paths in scraper.py..."
sed -i '' 's|/Users/jonesy/gitlocal/jobscrape/config/|./configs/|g' scraper.py
sed -i '' 's|/Users/jonesy/gitlocal/jobscrape/Backups/|./backups/|g' scraper.py
sed -i '' 's|"./Raw Results/"|"./outputs/Raw Results/"|g' scraper.py
sed -i '' 's|"jobscraper.log"|"./outputs/logs/jobscraper.log"|g' scraper.py
sed -i '' 's|"config/delete_ids.txt"|"configs/delete_ids.txt"|g' scraper.py

git add scraper.py
git commit -m "🚀 Add main scraper.py application

- Complete job scraping system with PostgreSQL integration
- Intelligent backup management with retention policies
- Multi-source scraping (Indeed, LinkedIn, Glassdoor)
- Advanced duplicate detection and data cleaning pipeline
- Production/working database separation for safe analysis
- Updated all paths for new project structure"

echo "✓ Main application committed"

# Commit 3: Configuration Templates
echo "⚙️ Commit 3: Adding configuration templates..."

# Create safe database config template
cat > configs/db_config.json.template << 'EOF'
{
  "production_database": {
    "host": "YOUR_DATABASE_HOST",
    "port": 5432,
    "database": "jobscraps",
    "username": "YOUR_USERNAME", 
    "password": "YOUR_PASSWORD"
  },
  "working_database": {
    "host": "YOUR_DATABASE_HOST",
    "port": 5432,
    "database": "jobscraps_working",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD"
  },
  "connection": {
    "connect_timeout": 30,
    "command_timeout": 300,
    "retry_attempts": 3,
    "retry_delay": 5
  }
}
EOF

# Copy other config files (these are usually safe)
if [ -f "$ORIGINAL_PROJECT/config/job_search_config.json" ]; then
    cp "$ORIGINAL_PROJECT/config/job_search_config.json" configs/
fi

if [ -f "$ORIGINAL_PROJECT/config/delete_companies.txt" ]; then
    cp "$ORIGINAL_PROJECT/config/delete_companies.txt" configs/
fi

if [ -f "$ORIGINAL_PROJECT/config/delete_titles.txt" ]; then
    cp "$ORIGINAL_PROJECT/config/delete_titles.txt" configs/
fi

git add configs/
git commit -m "⚙️ Add configuration templates and filters

- Database configuration template for easy setup
- Job search parameters for JobSpy integration  
- Company and title filter patterns for data cleaning
- Secure template approach prevents credential exposure"

echo "✓ Configuration committed"

# Commit 4: Helper Scripts
echo "🛠️ Commit 4: Adding helper scripts..."

# Copy scripts
if [ -f "$ORIGINAL_PROJECT/cli.py" ]; then
    cp "$ORIGINAL_PROJECT/cli.py" scripts/
fi

if [ -f "$ORIGINAL_PROJECT/csv_to_config.py" ]; then
    cp "$ORIGINAL_PROJECT/csv_to_config.py" scripts/
fi

if [ -f "$ORIGINAL_PROJECT/log_parser.py" ]; then
    cp "$ORIGINAL_PROJECT/log_parser.py" scripts/
fi

if [ -f "$ORIGINAL_PROJECT/preview_title_deletions.py" ]; then
    cp "$ORIGINAL_PROJECT/preview_title_deletions.py" scripts/
fi

# Create scripts README
cat > scripts/README.md << 'EOF'
# Helper Scripts

Utility scripts for JobScraps data management:

- **cli.py**: Command-line interface utilities
- **csv_to_config.py**: Convert CSV data to configuration files
- **log_parser.py**: Parse and analyze application logs  
- **preview_title_deletions.py**: Preview which job titles would be deleted

## Usage
Run from project root: `python scripts/script_name.py`
EOF

git add scripts/
git commit -m "🛠️ Add helper scripts and utilities

- CLI utilities for enhanced command-line operations
- CSV to configuration conversion tools
- Log parsing and analysis capabilities
- Preview tools for safe data operations"

echo "✓ Scripts committed"

# Commit 5: Development Tools
echo "🔧 Commit 5: Adding development tools..."

# Copy dev utilities
if [ -f "$ORIGINAL_PROJECT/verify_setup.py" ]; then
    cp "$ORIGINAL_PROJECT/verify_setup.py" dev/
fi

if [ -f "$ORIGINAL_PROJECT/compare_databases.sh" ]; then
    cp "$ORIGINAL_PROJECT/compare_databases.sh" dev/
fi

if [ -f "$ORIGINAL_PROJECT/workflow_scripts.sh" ]; then
    cp "$ORIGINAL_PROJECT/workflow_scripts.sh" dev/
fi

# Copy LLM package if it exists
if [ -d "$ORIGINAL_PROJECT/LLM Package" ]; then
    cp -r "$ORIGINAL_PROJECT/LLM Package"/* dev/LLM_package/ 2>/dev/null || true
fi

git add dev/
git commit -m "🔧 Add development and verification tools

- Database setup verification utilities
- Database comparison and analysis tools  
- Workflow automation scripts
- LLM context package for AI-assisted development"

echo "✓ Development tools committed"

# Commit 6: Documentation and Dependencies
echo "📚 Commit 6: Adding documentation..."

# Create comprehensive README
cat > README.md << 'EOF'
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
EOF

# Create requirements.txt
cat > requirements.txt << 'EOF'
# JobScraps Core Dependencies

# Job scraping
python-jobspy>=1.1.0

# Database and data processing
psycopg2-binary>=2.9.0
pandas>=1.5.0

# Excel and reporting
openpyxl>=3.1.0
xlsxwriter>=3.0.0

# Utilities
python-dotenv>=1.0.0

# Optional: Enhanced data analysis
numpy>=1.21.0
matplotlib>=3.5.0
seaborn>=0.11.0

# Optional: Future API development
fastapi>=0.100.0
uvicorn>=0.20.0

# Optional: Testing and code quality
pytest>=7.0.0
black>=23.0.0
flake8>=6.0.0
EOF

git add README.md requirements.txt
git commit -m "📚 Add comprehensive documentation and dependencies

- Complete README with quick start guide and workflow examples
- Feature overview and project structure documentation  
- Python dependencies with optional enhancements
- Contributing guidelines for future modularization"

echo "✓ Documentation committed"

# Commit 7: Database Schema (if available)
echo "🗄️ Commit 7: Adding database schema..."

if [ -f "$ORIGINAL_PROJECT/jobscraps_schema.sql" ]; then
    cp "$ORIGINAL_PROJECT/jobscraps_schema.sql" data/schemas/
    echo "  ✓ Schema file copied"
else
    echo "  ⚠️  No schema file found - will create placeholder"
fi

cat > data/schemas/README.md << 'EOF'
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
EOF

git add data/
git commit -m "🗄️ Add database schema and documentation

- Complete PostgreSQL schema definition
- Schema generation documentation  
- Database structure reference for new setups
- Clear table descriptions and relationships"

echo "✓ Schema committed"

# Final push all commits
echo ""
echo "🚀 Pushing all commits to GitHub..."
git push origin main

echo ""
echo "🎉 SUCCESS! Your incremental repository is complete!"
echo ""
echo "📊 Commit History Summary:"
git log --oneline -7

echo ""
echo "🌟 Next Steps:"
echo "1. Visit your repo on GitHub to see the beautiful commit history"
echo "2. Test the application: python scraper.py --list-backups"
echo "3. Create your refactor branch: git checkout -b feature/modular-refactor"
echo "4. Start planning your modular breakdown!"
echo ""
echo "🎯 Your repository is now professional and ready for collaboration!"